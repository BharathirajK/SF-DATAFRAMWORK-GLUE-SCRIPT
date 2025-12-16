# -*- coding: utf-8 -*-
"""
AWS Glue ETL Script for Snowflake Data Framework
This script handles ETL pipelines with JDBC source connections.
Configuration tables used for file options:
  - CONFIG.PROPERTIES (formerly TYPECONFIG)
  - CONFIG.PIPELINEPROPERTIES (formerly CUSTOMCONFIG)
  - CONFIG.SYSTEMPROPERTIES (new) for static values / runtime tuning
"""

from awsglue.transforms import *
from awsglue.context import GlueContext
from pyspark.context import SparkContext
from awsglue.utils import getResolvedOptions
from awsglue.job import Job
from pyspark.sql import Row, SparkSession, functions as F
from pyspark.sql.types import *
from pyspark.sql.window import Window
from pyspark.sql.utils import AnalysisException
from pyspark.sql.functions import col, expr, lit
from pyspark.sql import types as T
from pyspark.sql.types import (
    StructType,
    StructField,
    IntegerType,
    DoubleType,
    BooleanType,
    StringType,
    TimestampType,
    LongType,
)
from datetime import datetime, timezone, timedelta
from croniter import croniter
import snowflake.connector
import logging
import boto3
import sys
import traceback
import json
import pytz
import uuid
import time
import os
import re
import paramiko
import pandas as pd
import io
from io import BytesIO
import requests  # <-- needed for API
import openpyxl  # <-- Need for XLSX
from requests.auth import HTTPBasicAuth  # <-- needed for API
from typing import Optional, Tuple, Dict, Any
import pyarrow.parquet as pq # use pyarrow to read parquet in-memory

# --- CHANGED: make Azure Blob dependency optional ---
try:
    from azure.storage.blob import BlobServiceClient  # <-- for Azure Blob support
except ImportError:
    BlobServiceClient = None
# ----------------------------------------------------

import fnmatch  # <-- for wildcard matching of blob names

# ===== INITIALIZATION SECTION =====
sc = SparkContext()
glueContext = GlueContext(sc)

# Defaults that preserve current behavior if SYSTEMPROPERTIES is missing
DEFAULTS = {
    "SECRETS_REGION": "us-west-2",
    # >>> CHANGED <<< (placeholder; real values computed after SYSTEMPROPERTIES)
    "MASTERLOG_TABLE": None,
    "LOG_TABLE": None,
    "CRON_TABLE": None,
    "PIPELINE_SUCCESS_ID": "1",
    "MASTER_SUCCESS_ID": "3",
    "MASTER_FAILURE_ID": "4",
    "STAGE_TABLE_SUFFIX": "_STAGE",
    "DEFAULT_MAX_RETRIES": "3",
    "DEFAULT_RETRY_DELAY_SECONDS": "60",
    # >>> CHANGED <<< set where config tables live (hard default to SF_DE_FRAMEWORK.CONFIG)
    "CONFIG_DATABASE": "SF_DE_FRAMEWORK",
    "CONFIG_SCHEMA": "CONFIG",
    # Global Alerts
    "sourceSystemName": "DF_Snowflake",
    "GlobalAlerts_API_URL": "https://utglobalalertsapi.azurewebsites.net/api/",
    "DEFAULT_TIMEOUT": "30",
    "BusinessAlertEmail": "bharathirajk@unitedtechno.com",
}

# Will be updated after loading SYSTEMPROPERTIES
DEFAULT_AWS_REGION = DEFAULTS["SECRETS_REGION"]

# ===== LOGGING SETUP SECTION =====
def get_standard_logger(name="glueLogger", level=logging.INFO):
    logger = logging.getLogger(name)
    logger.setLevel(level)
    if not logger.handlers:
        handler = logging.StreamHandler(sys.stdout)
        formatter = logging.Formatter("[%(levelname)s] %(asctime)s - %(message)s")
        handler.setFormatter(formatter)
        logger.addHandler(handler)
    return logger


logger = get_standard_logger()
secret_cache = {}


# ===================== NEW: VALIDATION HELPERS (collect all errors) =====================
class NonRetryableError(Exception):
    pass


def _is_blank(v):
    return v is None or (isinstance(v, str) and v.strip() == "")


def _add_error(errors, msg):
    if msg not in errors:
        errors.append(msg)


def _require_fields(row_dict, checks, errors):
    for field, msg in checks:
        if field not in row_dict or _is_blank(row_dict[field]):
            _add_error(errors, msg)


def _require_active(row_dict, msg_inactive, errors):
    val = row_dict.get("ACTIVE")
    if val is None:
        _add_error(errors, msg_inactive)
        return
    sval = str(val).strip().lower()
    if not (val is True or sval in ["true", "1", "t", "y", "yes"]):
        _add_error(errors, msg_inactive)


# >>> CHANGED <<< pass in cfg() to pull from the correct DB/Schema
def validate_all_required(spark, sf_options, pid, cfg):
    """
    Validate required columns across CONFIG tables for the given pipeline.
    Returns a list of error messages (empty list if OK).
    """
    errors = []

    pipeline_df = (
        spark.read.format("snowflake")
        .options(**sf_options)
        .option("dbtable", cfg("PIPELINE"))
        .load()
    )
    source_df = (
        spark.read.format("snowflake")
        .options(**sf_options)
        .option("dbtable", cfg("SOURCE"))
        .load()
    )
    destination_df = (
        spark.read.format("snowflake")
        .options(**sf_options)
        .option("dbtable", cfg("DESTINATION"))
        .load()
    )
    lookup_df = (
        spark.read.format("snowflake")
        .options(**sf_options)
        .option("dbtable", cfg("LOOKUP"))
        .load()
    )
    secret_df = (
        spark.read.format("snowflake")
        .options(**sf_options)
        .option("dbtable", cfg("SECRET"))
        .load()
    )
    src_col_df = (
        spark.read.format("snowflake")
        .options(**sf_options)
        .option("dbtable", cfg("SOURCECOLUMN"))
        .load()
    )
    dest_col_df = (
        spark.read.format("snowflake")
        .options(**sf_options)
        .option("dbtable", cfg("DESTINATIONCOLUMN"))
        .load()
    )
    col_map_df = (
        spark.read.format("snowflake")
        .options(**sf_options)
        .option("dbtable", cfg("COLUMNMAPPING"))
        .load()
    )

    # PIPELINE
    p_row = pipeline_df.filter(col("PIPELINEID") == pid).first()
    if not p_row:
        _add_error(
            errors,
            "PIPELINEID in PIPELINE table is mandatory; empty values are not permitted.",
        )
        return errors
    p = p_row.asDict()
    _require_fields(
        p,
        [
            (
                "PIPELINENAME",
                "PIPELINENAME in PIPELINE table is mandatory; empty values are not permitted.",
            ),
            (
                "SOURCEID",
                "SOURCEID in PIPELINE table is mandatory; empty values are not permitted.",
            ),
            (
                "DESTINATIONID",
                "DESTINATIONID in PIPELINE table is mandatory; empty values are not permitted.",
            ),
            (
                "LOADTYPEID",
                "LOADTYPEID in PIPELINE table is mandatory; empty values are not permitted.",
            ),
        ],
        errors,
    )

    # SOURCE
    s_row = source_df.filter(col("SOURCEID") == p.get("SOURCEID")).first()
    if not s_row:
        _add_error(
            errors,
            "SOURCEID in SOURCE table is mandatory; empty values are not permitted.",
        )
        s = {}
    else:
        s = s_row.asDict()
        _require_fields(
            s,
            [
                (
                    "SOURCENAME",
                    "SOURCENAME in SOURCE table is mandatory; empty values are not permitted.",
                ),
                (
                    "SOURCEPATH",
                    "SOURCEPATH in SOURCE table is mandatory; empty values are not permitted.",
                ),
                (
                    "SOURCETYPEID",
                    "SOURCETYPEID in SOURCE table is mandatory; empty values are not permitted.",
                ),
                (
                    "FORMATTYPEID",
                    "FORMATTYPEID in SOURCE table is mandatory; empty values are not permitted.",
                ),
                (
                    "SECRETID",
                    "SECRETID in SOURCE table is mandatory; empty values are not permitted.",
                ),
            ],
            errors,
        )
        _require_active(
            s,
            "The values in the ACTIVE column of the SOURCE table are not set to active.",
            errors,
        )

    # DESTINATION
    d_row = destination_df.filter(col("DESTINATIONID") == p.get("DESTINATIONID")).first()
    if not d_row:
        _add_error(
            errors,
            "DESTINATIONID in DESTINATION table is mandatory; empty values are not permitted.",
        )
        d = {}
    else:
        d = d_row.asDict()
        _require_fields(
            d,
            [
                (
                    "DESTINATIONNAME",
                    "DESTINATIONNAME in DESTINATION table is mandatory; empty values are not permitted.",
                ),
                (
                    "DESTINATIONPATH",
                    "DESTINATIONPATH in DESTINATION table is mandatory; empty values are not permitted.",
                ),
                (
                    "DESTINATIONTYPEID",
                    "DESTINATIONTYPEID in DESTINATION table is mandatory; empty values are not permitted.",
                ),
                (
                    "FORMATTYPEID",
                    "FORMATTYPEID in DESTINATION table is mandatory; empty values are not permitted.",
                ),
            ],
            errors,
        )
        _require_active(
            d,
            "The values in the ACTIVE column of the DESTINATION table are not set to active.",
            errors,
        )

    # LOOKUPs referenced
    needed_lookup_ids = []
    for key in ["LOADTYPEID", "LAYERID"]:
        if p.get(key) is not None:
            needed_lookup_ids.append(int(p[key]))
    for key in ["SOURCETYPEID", "FORMATTYPEID"]:
        if s.get(key) is not None:
            needed_lookup_ids.append(int(s[key]))
    if d.get("FORMATTYPEID") is not None:
        needed_lookup_ids.append(int(d["FORMATTYPEID"]))

    if needed_lookup_ids:
        lk_df = lookup_df.filter(col("LOOKUPID").isin(needed_lookup_ids))
        for r in lk_df.collect():
            rd = r.asDict()
            _require_fields(
                rd,
                [
                    (
                        "LOOKUPID",
                        "LOOKUPID in LOOKUP table is mandatory; empty values are not permitted.",
                    ),
                    (
                        "LOOKUPNAME",
                        "LOOKUPNAME in LOOKUP table is mandatory; empty values are not permitted.",
                    ),
                    (
                        "LOOKUPVALUE",
                        "LOOKUPVALUE in LOOKUP table is mandatory; empty values are not permitted.",
                    ),
                ],
                errors,
            )
            _require_active(
                rd,
                "The values in the ACTIVE column of the LOOKUP table are not set to active.",
                errors,
            )

    # SECRETs referenced
    secret_ids = [s.get("SECRETID"), d.get("SECRETID")]
    secret_ids = [x for x in secret_ids if x is not None]
    if secret_ids:
        sec_df = secret_df.filter(col("SECRETID").isin(secret_ids))
        for r in sec_df.collect():
            rd = r.asDict()
            _require_fields(
                rd,
                [
                    (
                        "SECRETNAME",
                        "SECRETNAME in Secret table is mandatory; empty values are not permitted.",
                    ),
                    (
                        "SECRETKEY",
                        "SECRETKEY in Secret table is mandatory; empty values are not permitted.",
                    ),
                ],
                errors,
            )
            _require_active(
                rd,
                "The values in the ACTIVE column of the Secret table are not set to active.",
                errors,
            )

    # COLUMNMAPPING and columns
    dcols = (
        dest_col_df.filter(col("DESTINATIONID") == p.get("DESTINATIONID"))
        .select("DESTINATIONCOLUMNID")
        .distinct()
    )
    dcol_ids = [row["DESTINATIONCOLUMNID"] for row in dcols.collect()]
    if dcol_ids:
        cm_df = col_map_df.filter(col("DESTINATIONCOLUMNID").isin(dcol_ids))
        for r in cm_df.collect():
            rd = r.asDict()
            _require_fields(
                rd,
                [
                    (
                        "SOURCECOLUMNID",
                        "SOURCECOLUMNID in COLUMNMAPPING table is mandatory; empty values are not permitted.",
                    ),
                    (
                        "DESTINATIONCOLUMNID",
                        "DESTINATIONCOLUMNID in COLUMNMAPPING table is mandatory; empty values are not permitted.",
                    ),
                ],
                errors,
            )
            _require_active(
                rd,
                "The values in the ACTIVE column of the COLUMNMAPPING table are not set to active.",
                errors,
            )

        src_ids = [
            row["SOURCECOLUMNID"]
            for row in cm_df.select("SOURCECOLUMNID").distinct().collect()
        ]
        if src_ids:
            sc_sel = src_col_df.filter(col("SOURCECOLUMNID").isin(src_ids))
            for r in sc_sel.collect():
                rd = r.asDict()
                _require_fields(
                    rd,
                    [
                        (
                            "SOURCECOLUMNNAME",
                            "SOURCECOLUMNNAME in SOURCECOLUMN table is mandatory; empty values are not permitted.",
                        ),
                        (
                            "SOURCECOLUMNSEQUENCE",
                            "SOURCECOLUMNSEQUENCE in SOURCECOLUMN table is mandatory; empty values are not permitted.",
                        ),
                        (
                            "SOURCECOLUMNDATATYPE",
                            "SOURCECOLUMNDATATYPE in SOURCECOLUMN table is mandatory; empty values are not permitted.",
                        ),
                    ],
                    errors,
                )
                _require_active(
                    rd,
                    "The values in the ACTIVE column of the SOURCECOLUMN table are not set to active.",
                    errors,
                )

        if dcol_ids:
            dc_sel = dest_col_df.filter(col("DESTINATIONCOLUMNID").isin(dcol_ids))
            for r in dc_sel.collect():
                rd = r.asDict()
                _require_fields(
                    rd,
                    [
                        (
                            "DESTINATIONCOLUMNNAME",
                            "DESTINATIONCOLUMNNAME in DESTINATIONCOLUMN table is mandatory; empty values are not permitted.",
                        ),
                        (
                            "DESTINATIONCOLUMNSEQUENCE",
                            "DESTINATIONCOLUMNSEQUENCE in DESTINATIONCOLUMN table is mandatory; empty values are not permitted.",
                        ),
                        (
                            "DESTINATIONCOLUMNDATATYPE",
                            "DESTINATIONCOLUMNDATATYPE in DESTINATIONCOLUMN table is mandatory; empty values are not permitted.",
                        ),
                    ],
                    errors,
                )
                _require_active(
                    rd,
                    "The values in the ACTIVE column of the DESTINATIONCOLUMN table are not set to active.",
                    errors,
                )

    return errors


# ===================== END NEW HELPERS =====================

# ===== RETRY MECHANISM SECTION =====
def spark_snowflake_retry(func, max_retries=3, base_wait=5, backoff=2, action_desc=""):
    attempt = 0
    wait_time = base_wait
    last_exception = None
    while attempt < max_retries:
        try:
            return func()
        except Exception as e:
            last_exception = e
            attempt += 1
            logger.error(f"Attempt {attempt} for {action_desc} failed: {str(e)}")
            if attempt < max_retries:
                logger.info(f"Retrying {action_desc} in {wait_time} seconds...")
                time.sleep(wait_time)
                wait_time *= backoff
            else:
                logger.error(
                    f"All {max_retries} retries failed for {action_desc}. Raising error."
                )
    raise last_exception


# ===== AWS SECRETS MANAGER SECTION =====
def get_secret_cached(secret_name, region_name=None):
    """Retrieve secrets from AWS Secrets Manager with caching"""
    global DEFAULT_AWS_REGION
    region = region_name or DEFAULT_AWS_REGION

    cache_key = f"{region}:{secret_name}"
    if cache_key in secret_cache:
        logger.info(
            f"[CACHE HIT] Secret '{secret_name}' ({region}) retrieved from cache."
        )
        return secret_cache[cache_key]

    client = boto3.client("secretsmanager", region_name=region)

    try:
        response = client.get_secret_value(SecretId=secret_name)
        secret = json.loads(response["SecretString"])
        secret_cache[cache_key] = secret
        logger.info(
            f"[SUCCESS] Secret '{secret_name}' fetched from AWS Secrets Manager in {region}."
        )
        return secret

    except client.exceptions.ResourceNotFoundException:
        logger.error(
            f"[ERROR] Secret '{secret_name}' not found in Secrets Manager (region {region})."
        )
        raise
    except client.exceptions.ClientError as e:
        logger.error(
            f"[ERROR] ClientError while fetching secret '{secret_name}' (region {region}): {str(e)}"
        )
        raise
    except Exception as e:
        logger.error(
            f"[ERROR] Unexpected error while fetching secret '{secret_name}' (region {region}): {str(e)}\nTraceback:\n{traceback.format_exc()}"
        )
        raise


# ===== SNOWFLAKE CONNECTION SECTION =====
def create_snowflake_options(secret):
    required_keys = [
        "account",
        "user",
        "password",
        "database",
        "schema",
        "warehouse",
        "role",
    ]
    missing_keys = [k for k in required_keys if k not in secret or not secret[k]]
    if missing_keys:
        logger.error(
            f"[ERROR] Missing required Snowflake credential keys: {missing_keys}"
        )
        raise ValueError(f"Missing required keys in Snowflake secret: {missing_keys}")

    options = {
        "sfURL": f"{secret['account']}.snowflakecomputing.com",
        "sfUser": secret["user"],
        "sfPassword": secret["password"],
        "sfDatabase": secret["database"],
        "sfSchema": secret["schema"],
        "sfWarehouse": secret["warehouse"],
        "sfRole": secret["role"],
    }
    logger.info(
        f"[SUCCESS] Snowflake options successfully created for account '{secret['account']}'."
    )
    return options


# ===== SNOWFLAKE SQL EXECUTION SECTION =====
def execute_snowflake_sql(secret, sql: str, fetch_result: bool = False):
    attempt = 0
    max_retries = 3
    wait_time = 5
    backoff = 2
    last_exception = None

    while attempt < max_retries:
        conn, cursor = None, None
        try:
            conn = snowflake.connector.connect(
                user=secret["user"],
                password=secret["password"],
                account=secret["account"],
                warehouse=secret.get("warehouse"),
                database=secret.get("database"),
                schema=secret.get("schema"),
                role=secret.get("role"),
            )
            cursor = conn.cursor()
            logger.debug(f"Executing SQL (Attempt {attempt + 1}):\n{sql.strip()}")
            cursor.execute(sql)
            if fetch_result:
                result = cursor.fetchall()
                logger.debug(f"Query returned {len(result)} rows.")
                return result
            return True
        except Exception as e:
            last_exception = e
            attempt += 1
            logger.error(f"Snowflake SQL attempt {attempt} failed: {str(e)}")
            if attempt < max_retries:
                logger.info(f"Retrying Snowflake SQL in {wait_time} seconds...")
                time.sleep(wait_time)
                wait_time *= backoff
            else:
                logger.error(
                    f"All {max_retries} connection attempts failed. Raising error."
                )
        finally:
            try:
                if cursor:
                    cursor.close()
                if conn:
                    conn.close()
            except Exception:
                pass
    raise last_exception


# ===== TABLE EXISTENCE CHECK SECTION =====
def table_exists_in_snowflake(secret, table_name):
    try:
        parts = table_name.upper().split(".")
        if len(parts) == 3:
            db_name, schema_name, tbl_name = parts
        elif len(parts) == 2:
            db_name = secret.get("database", "").upper()
            schema_name, tbl_name = parts
        else:
            db_name = secret.get("database", "").upper()
            schema_name = secret.get("schema", "").upper()
            tbl_name = parts[0]
        sql = f"""
        SELECT COUNT(1) AS CNT 
        FROM INFORMATION_SCHEMA.TABLES 
        WHERE TABLE_CATALOG = '{db_name}' 
          AND TABLE_SCHEMA = '{schema_name}' 
          AND TABLE_NAME = '{tbl_name}'
        """
        results = execute_snowflake_sql(secret, sql, fetch_result=True)
        return results[0][0] > 0 if results else False
    except Exception as e:
        logger.error(
            f"Failed to check if table exists: {table_name}. Error: {str(e)}"
        )
        return False


# ===== STATUS MESSAGE HELPERS =====
def get_status_message_name_by_id(status_df, statusmessage_id):
    if status_df.rdd.isEmpty():
        raise Exception(
            "STATUSMESSAGE table is empty. Please populate it before proceeding."
        )
    filtered_df = status_df.filter(col("STATUSMESSAGEID") == statusmessage_id)
    if filtered_df.rdd.isEmpty():
        raise Exception(
            f"STATUSMESSAGEID '{statusmessage_id}' not found in STATUSMESSAGE table."
        )
    return filtered_df.select("STATUSMESSAGENAME").first()["STATUSMESSAGENAME"]


def get_status_by_name(status_df, name, default_id=4):
    row = (
        status_df.filter(F.upper(col("STATUSMESSAGENAME")) == name.upper())
        .select("STATUSMESSAGENAME", "STATUSMESSAGEID")
        .select("STATUSMESSAGEID", "STATUSMESSAGENAME")
        .first()
    )
    if row:
        return int(row["STATUSMESSAGEID"]), row["STATUSMESSAGENAME"]
    return int(default_id), get_status_message_name_by_id(status_df, default_id)


# ===== NEW: fetch STATUSMESSAGECONTENT by name (for MASTERLOG.MESSAGE) =====
def get_statusmessage_content_by_name(status_df, name):
    row = (
        status_df.filter(F.upper(col("STATUSMESSAGENAME")) == name.upper())
        .select("STATUSMESSAGECONTENT")
        .first()
    )
    return (
        row["STATUSMESSAGECONTENT"]
        if row and row["STATUSMESSAGECONTENT"] is not None
        else None
    )


def classify_error(status_df, err_msg: str):
    msg = (err_msg or "").lower()
    if re.search(r"login failed", msg):
        return get_status_by_name(status_df, "PIPELINE_FAILURE_AUTH_FAILED")
    if re.search(r"(invalid object name|does not exist|table not found)", msg):
        return get_status_by_name(status_df, "PIPELINE_FAILURE_TABLE_NOT_FOUND")
    return get_status_by_name(status_df, "MASTER_FAILURE")  # (= id 4)


# ===== MINIMAL: PIPELINE RETRY CONFIG (returns defaults) =====
def get_retry_config_for_pipeline(pipeline_df, pid, default_retries=3, default_delay=60):
    return int(default_retries), int(default_delay)


# ===== MASTER LOG MANAGEMENT =====
def create_master_log(spark, sf_options, glue_run_id, total_pipelines, table_name):
    try:
        start_dt = datetime.now(timezone.utc)
        logger.info(
            f"Creating MASTERLOG entry for Glue RUNID={glue_run_id} at {start_dt} with {total_pipelines} pipelines"
        )
        master_log_id = str(uuid.uuid4())
        schema = StructType(
            [
                StructField("MASTERLOGID", StringType(), True),
                StructField("RUNID", StringType(), True),
                StructField("STARTDATETIME", TimestampType(), True),
                StructField("ENDDATETIME", TimestampType(), True),
                StructField("STATUS", StringType(), True),
                StructField("MESSAGE", StringType(), True),
                StructField("TOTALPIPELINECOUNT", IntegerType(), True),
            ]
        )
        data = [(master_log_id, glue_run_id, start_dt, None, None, None, total_pipelines)]
        insert_df = spark.createDataFrame(data, schema=schema)

        spark_snowflake_retry(
            lambda: insert_df.write.format("snowflake")
            .options(**sf_options)
            .option("dbtable", table_name)
            .mode("append")
            .save(),
            action_desc="master_log insert",
        )
        logger.info(f"MASTERLOG entry created successfully → ID: {master_log_id}")
        return master_log_id
    except Exception as e:
        logger.error(
            f"Error creating MASTERLOG entry: {str(e)}\nTraceback:\n{traceback.format_exc()}"
        )
        raise


# ===== SYSTEM PROPERTIES LOADER =====
def load_system_properties(spark, sf_options, fqn_table):
    """
    Loads CONFIG.SYSTEMPROPERTIES from the given fully-qualified table.
    Returns a dict with UPPERCASE keys.
    """
    try:
        df = (
            spark.read.format("snowflake")
            .options(**sf_options)
            .option("dbtable", fqn_table)
            .load()
        )
        props = {}
        if "ACTIVE" in df.columns:
            df = df.filter(col("ACTIVE") == True)
        for r in df.select("PROPERTYKEY", "PROPERTYVALUE").collect():
            if r["PROPERTYKEY"]:
                props[str(r["PROPERTYKEY"]).upper()] = (
                    str(r["PROPERTYVALUE"]) if r["PROPERTYVALUE"] is not None else None
                )
        logger.info(f"Loaded {len(props)} system properties from {fqn_table}")
        return props
    except Exception as e:
        logger.warning(
            f"Unable to load SYSTEMPROPERTIES ({fqn_table}), using defaults. Error: {e}"
        )
        return {}


def get_prop(props, key, default_value):
    return (props.get(key.upper()) if props else None) or default_value


# ===== METADATA LOADING SECTION =====
def load_metadata(spark, sf_options, pid, cfg):
    logger.info(f"Starting metadata load for pipeline ID: {pid}")

    def load_df(table_fqn):
        try:
            logger.info(f"Loading table: {table_fqn}")
            return spark_snowflake_retry(
                lambda: spark.read.format("snowflake")
                .options(**sf_options)
                .option("dbtable", table_fqn)
                .load(),
                action_desc=f"read {table_fqn}",
            )
        except Exception as e:
            logger.error(
                f"Failed to load table: {table_fqn}. Error: {str(e)}\nTraceback:\n{traceback.format_exc()}"
            )
            raise

    pipeline_df = load_df(cfg("PIPELINE"))
    lookup_df = load_df(cfg("LOOKUP"))
    source_df = load_df(cfg("SOURCE"))
    destination_df = load_df(cfg("DESTINATION"))
    source_col_df = load_df(cfg("SOURCECOLUMN"))
    dest_col_df = load_df(cfg("DESTINATIONCOLUMN"))
    col_mapping_df = load_df(cfg("COLUMNMAPPING"))
    pipeline_props_df = load_df(cfg("PIPELINEPROPERTIES"))  # was CUSTOMCONFIG
    properties_df = load_df(cfg("PROPERTIES"))  # was TYPECONFIG
    secret_df = load_df(cfg("SECRET"))

    logger.info("All tables loaded successfully from Snowflake (config schema).")

    p_df = pipeline_df.filter(col("PIPELINEID") == pid)
    if p_df.count() == 0:
        logger.error(f"No PIPELINE row found for PIPELINEID={pid}")
        raise ValueError(f"No PIPELINE row found for PIPELINEID={pid}")
    p = p_df.first().asDict()
    logger.info(f"[Debug] PIPELINE row → LOADTYPEID={p.get('LOADTYPEID')}")

    load_type_df = lookup_df.filter(
        (col("LOOKUPNAME") == "LoadType") & (col("LOOKUPID") == p["LOADTYPEID"])
    )
    if load_type_df.count() == 0:
        logger.error(f"Invalid LOADTYPEID={p['LOADTYPEID']}")
        raise ValueError(f"Invalid LOADTYPEID={p['LOADTYPEID']}")
    p["LOAD_TYPE"] = load_type_df.first()["LOOKUPVALUE"]

    layer_df = lookup_df.filter(
        (col("LOOKUPNAME") == "Layer") & (col("LOOKUPID") == p["LAYERID"])
    )
    if layer_df.count() == 0:
        logger.error(f"Invalid LAYERID={p['LAYERID']}")
        raise ValueError(f"Invalid LAYERID={p['LAYERID']}")
    p["LAYER"] = layer_df.first()["LOOKUPVALUE"]

    d_df = destination_df.filter(col("DESTINATIONID") == p["DESTINATIONID"])
    if d_df.count() == 0:
        logger.error(f"Invalid DESTINATIONID={p['DESTINATIONID']}")
        raise ValueError(f"Invalid DESTINATIONID={p['DESTINATIONID']}")
    d = d_df.first().asDict()

    scd_lookup_id = d.get("SCDTYPE", d.get("SCDTYPEID"))
    if scd_lookup_id is None:
        d["SCDTYPE"] = 21
        d["SCD_TYPE"] = "NONE"
        logger.info(
            "DESTINATION SCDTYPE/SCDTYPEID not provided; defaulting to 21 ('NONE')."
        )
    else:
        scd_df = lookup_df.filter(
            (col("LOOKUPNAME") == "ScdType") & (col("LOOKUPID") == lit(int(scd_lookup_id)))
        )
        if scd_df.count() == 0:
            d["SCDTYPE"] = 21
            d["SCD_TYPE"] = "NONE"
            logger.warning(
                f"SCDTYPE value {scd_lookup_id} not found in LOOKUP; defaulted to 21 ('NONE')."
            )
        else:
            d["SCD_TYPE"] = scd_df.first()["LOOKUPVALUE"]
            d["SCDTYPE"] = int(scd_lookup_id)

    if "SECRETID" in d and d["SECRETID"] is not None:
        dest_secret_df = secret_df.filter(col("SECRETID") == d["SECRETID"])
        if dest_secret_df.count() == 0:
            logger.error(f"Invalid SECRETID={d['SECRETID']} in destination table")
            raise ValueError(f"Invalid SECRETID={d['SECRETID']}")
        d["SECRET_KEY"] = dest_secret_df.first()["SECRETKEY"]
    else:
        logger.info(
            "DESTINATION.SECRETID not provided; using global Snowflake credentials for destination writes."
        )
        d["SECRET_KEY"] = None

    s_df = source_df.filter(col("SOURCEID") == p["SOURCEID"])
    if s_df.count() == 0:
        logger.error(f"Invalid SOURCEID={p['SOURCEID']}")
        raise ValueError(f"Invalid SOURCEID={p['SOURCEID']}")
    s = s_df.first().asDict()

    source_type_df = lookup_df.filter(
        (col("LOOKUPNAME") == "SourceType") & (col("LOOKUPID") == s["SOURCETYPEID"])
    )
    if source_type_df.count() == 0:
        logger.error(
            f"Invalid SOURCETYPEID={s['SOURCETYPEID']} for SOURCEID={s['SOURCEID']}"
        )
        raise ValueError(f"Invalid SOURCETYPEID={s['SOURCETYPEID']}")
    s["SOURCE_TYPE"] = source_type_df.first()["LOOKUPVALUE"]

    format_type_df = lookup_df.filter(
        (col("LOOKUPNAME") == "FormatType") & (col("LOOKUPID") == s["FORMATTYPEID"])
    )
    if format_type_df.count() == 0:
        logger.error(f"Invalid FORMATTYPEID={s['FORMATTYPEID']}")
        raise ValueError(f"Invalid FORMATTYPEID={s['FORMATTYPEID']}")
    s["FORMAT_TYPE"] = format_type_df.first()["LOOKUPVALUE"]

    src_secret_df = secret_df.filter(col("SECRETID") == s["SECRETID"])
    if src_secret_df.count() == 0:
        logger.error(f"Invalid SECRETID={s['SECRETID']} in source table")
        raise ValueError(f"Invalid SECRETID={s['SECRETID']}")
    s["SECRET_KEY"] = src_secret_df.first()["SECRETKEY"]

    logger.info("Preparing column mappings...")
    mappings_df = (
        dest_col_df.filter(col("DESTINATIONID") == d["DESTINATIONID"])
        .join(col_mapping_df, ["DESTINATIONCOLUMNID"])
        .join(source_col_df, ["SOURCECOLUMNID"])
        .select(
            "SOURCECOLUMNNAME",
            "DESTINATIONCOLUMNNAME",
            "DESTINATIONCOLUMNDATATYPE",
            "DESTINATIONCOLUMNSEQUENCE",
        )
    )
    mappings = [row.asDict() for row in mappings_df.collect()]

    logger.info("Extracting file format settings...")
    file_cfg = (
        pipeline_props_df.filter(col("PIPELINEID") == pid)
        .join(properties_df, ["PROPERTYID"])
        .select("PROPERTYNAME", "PROPERTYVALUE")
    )
    file_settings = {
        row["PROPERTYNAME"].lower(): row["PROPERTYVALUE"] for row in file_cfg.collect()
    }
    required_keys = [
        "compression",
        "skipheader",
        "delimiter",
        "quotechar",
        "escape",
        "lineseparator",
    ]
    for key in required_keys:
        if key not in file_settings:
            logger.warning(
                f"Missing file setting: {key} for pipeline {pid}. Default may be required."
            )

    logger.info("Successfully loaded all metadata.")
    return {
        "pipeline": p,
        "source": s,
        "destination": d,
        "mappings": mappings,
        "file_settings": file_settings,
    }


# ===== AZURE BLOB HELPER SECTION =====
def read_from_azureblob_xlsx(spark, md):
    """
    Read XLSX files from Azure Blob storage for source_type = 'azureblob'.

    Expected secret (SECRET_KEY) structure (example):
      {
        "account_name": "yourstorageacct",
        "container": "yourcontainer",
        "connection_string": "...",   # OR
        "sas_token": "...",           # OR
        "account_key": "..."
      }

    SOURCEPATH: treated as a prefix/folder, e.g. "aero/".
    file_settings.pattern: wildcard for filenames under that prefix, e.g. "ANIMALTYPES*.xlsx"
    file_settings.sheetname: optional sheet name to read (e.g. "ANIMALTYPES")
    """

    # --- NEW: guard when azure-storage-blob is not installed ---
    if BlobServiceClient is None:
        raise NonRetryableError(
            "Azure Blob support requires the 'azure-storage-blob' package to be "
            "installed in this Glue job. Either add that dependency or disable "
            "pipelines with SOURCE_TYPE='azureblob'."
        )
    # -----------------------------------------------------------

    # file_settings from metadata (lower-cased keys)
    fs = {k.lower(): v for k, v in md.get("file_settings", {}).items()}
    pattern = fs.get("pattern") or "*"
    skipheader = fs.get("skipheader", "1")
    sheet_name = fs.get("sheetname")  # <<< NEW: SheetName support

    source = md["source"]
    source_path = source.get("SOURCEPATH") or ""
    secret_key = source.get("SECRET_KEY")

    if not secret_key:
        raise ValueError("AzureBlob source requires SECRET_KEY to be populated in metadata.")

    az_secret = get_secret_cached(secret_key)

    connection_string = az_secret.get("connection_string")
    account_name = az_secret.get("account_name")
    container_name = az_secret.get("container")
    sas_token = az_secret.get("sas_token")
    account_key = az_secret.get("account_key")

    if not container_name:
        raise ValueError(
            "AzureBlob secret must contain 'container' or 'connection_string' including container info."
        )

    if connection_string:
        blob_service_client = BlobServiceClient.from_connection_string(connection_string)
        container_client = blob_service_client.get_container_client(container_name)
    else:
        if not account_name:
            raise ValueError(
                "AzureBlob secret must contain 'account_name' when 'connection_string' is not provided."
            )
        credential = sas_token or account_key
        if not credential:
            raise ValueError(
                "AzureBlob secret must contain either 'sas_token', 'account_key', or 'connection_string'."
            )
        account_url = f"https://{account_name}.blob.core.windows.net"
        blob_service_client = BlobServiceClient(account_url=account_url, credential=credential)
        container_client = blob_service_client.get_container_client(container_name)

    prefix = source_path or ""
    if prefix and not prefix.endswith("/"):
        prefix = prefix + "/"

    logger.info(
        f"Reading from Azure Blob container='{container_name}', prefix='{prefix}', pattern='{pattern}', sheet_name='{sheet_name}'"
    )

    matched_pandas_dfs = []

    for blob in container_client.list_blobs(name_starts_with=prefix):
        blob_name = blob.name
        rel_name = blob_name[len(prefix) :] if blob_name.startswith(prefix) else blob_name
        if not fnmatch.fnmatch(rel_name, pattern):
            continue

        logger.info(f"Reading Azure Blob object: {blob_name}")
        blob_data = container_client.download_blob(blob_name).readall()

        # header=0 if we want to skip header row (Excel uses row index), None means let pandas infer
        header_row = 0 if str(skipheader).strip() != "0" else None

        # <<< NEW: respect SheetName if present >>>
        if sheet_name:
            df_pd = pd.read_excel(
                BytesIO(blob_data),
                sheet_name=sheet_name,
                header=header_row,
            )
        else:
            df_pd = pd.read_excel(
                BytesIO(blob_data),
                header=header_row,
            )
        # >>> END NEW <<<

        matched_pandas_dfs.append(df_pd)

    if not matched_pandas_dfs:
        raise ValueError(
            f"No blobs found in container '{container_name}' matching prefix '{prefix}' and pattern '{pattern}'."
        )

    if len(matched_pandas_dfs) == 1:
        combined_pd = matched_pandas_dfs[0]
    else:
        combined_pd = pd.concat(matched_pandas_dfs, ignore_index=True)

    df = spark.createDataFrame(combined_pd)
    logger.info(f"AzureBlob read success. Loaded {len(combined_pd.index)} rows.")
    return df

#================Flatten JSON Start ===================================
def _is_struct(col_type):
    return isinstance(col_type, T.StructType)

def _is_array(col_type):
    return isinstance(col_type, T.ArrayType)

def flatten_structs(df):
    """
    Flatten StructType columns WITHOUT prefix.
    Example: config.region -> region
    If duplicate column names appear, auto-rename them.
    """
    struct_cols = [f.name for f in df.schema.fields if isinstance(f.dataType, T.StructType)]
    if not struct_cols:
        return df, False

    exprs = []
    used = set()

    for field in df.schema.fields:
        name = field.name
        dtype = field.dataType

        if isinstance(dtype, T.StructType):
            # flatten each field inside struct
            for inner in dtype.fields:
                new_name = inner.name

                # auto-fix duplicate names
                if new_name in used:
                    i = 1
                    while f"{new_name}_{i}" in used:
                        i += 1
                    new_name = f"{new_name}_{i}"

                exprs.append(F.col(f"{name}.{inner.name}").alias(new_name))
                used.add(new_name)

        else:
            # keep regular columns
            col_name = name
            if col_name in used:
                # auto-fix duplicates
                i = 1
                while f"{col_name}_{i}" in used:
                    i += 1
                col_name = f"{col_name}_{i}"

            exprs.append(F.col(name).alias(col_name))
            used.add(col_name)

    return df.select(*exprs), True

def explode_arrays(df, explode_arrays_of_structs=True, join_primitive_arrays=False, join_sep="|"):
    """
    Explode top-level array columns of StructType into rows.
    - explode_arrays_of_structs: if True, arrays of structs are exploded (one row per element)
    - join_primitive_arrays: if True, primitive arrays (array<string/int>) are converted to joined strings; else kept as arrays
    Returns (df, changed)
    """
    arr_fields = [f for f in df.schema.fields if _is_array(f.dataType)]
    if not arr_fields:
        return df, False

    # prefer to explode the first array field (to avoid exploding multiple arrays and generating huge cartesian)
    for f in arr_fields:
        name = f.name
        elem_type = f.dataType.elementType
        if isinstance(elem_type, T.StructType) and explode_arrays_of_structs:
            df = df.withColumn(name, F.explode_outer(F.col(name)))
            return df, True
        else:
            # primitive arrays
            if join_primitive_arrays:
                df = df.withColumn(name, F.when(F.col(name).isNotNull(), F.concat_ws(join_sep, F.col(name))).otherwise(None))
                return df, True
            # keep arrays as-is (no explode) — return unchanged for primitive arrays
    return df, False

def normalize_nested_json(df,
                          explode_arrays_of_structs=True,
                          join_primitive_arrays=False,
                          max_iters=20):
    """
    Flatten structs fully (no prefix) and optionally explode arrays.
    """
    it = 0
    changed = True
    while changed and it < max_iters:
        it += 1
        changed = False

        # flatten structs (now without prefix)
        df, c1 = flatten_structs(df)
        changed = changed or c1

        # explode array-of-structs
        df, c2 = explode_arrays(df,
                                explode_arrays_of_structs=explode_arrays_of_structs,
                                join_primitive_arrays=join_primitive_arrays)
        changed = changed or c2

    return df
#================Flatten JSON END ===================================

# ===== DATA PROCESSING SECTION =====
def copy_parse_dedupe(spark, md, s3_staging_dir=None):
    fs = {k.lower(): v for k, v in md.get("file_settings", {}).items()}

    source_path = md["source"]["SOURCEPATH"]
    logger.info(f"copy_parse_dedupe - source path: {source_path}")

    # ---- OPTION 2 CHANGE: normalize 'sql' style aliases to Azure SQL ----
    source_type_raw = md["source"].get("SOURCE_TYPE")
    source_type = (source_type_raw or "").lower()
    logger.info(f"copy_parse_dedupe - source type: {source_type}")

    if source_type in ["sql", "mssql", "azure_sql"]:
        logger.info(
            f"Normalizing source type '{source_type}' to 'azuresql' for JDBC handling."
        )
        source_type = "azuresql"
    # --------------------------------------------------------------------

    # <<< NEW: capture format type for awss3/xlsx support >>>
    format_type = (md["source"].get("FORMAT_TYPE") or "").lower()
    logger.info(f"copy_parse_dedupe - format type: {format_type}")

    try:
        read_options = {}
        if fs.get("delimiter"):
            read_options["sep"] = fs["delimiter"]
        if fs.get("skipheader"):
            read_options["header"] = "true" if int(fs["skipheader"]) > 0 else "false"
        if fs.get("quotechar"):
            read_options["quote"] = fs["quotechar"]
        if fs.get("multiline"):
            read_options["multiline"] = "true" if int(fs["multiline"]) > 0 else "false"
        logger.info("Read options prepared successfully.")
    except Exception as e:
        logger.error(f"Failed to prepare read options: {str(e)}")
        raise

    try:
        # JDBC sources
        if source_type in [
            "jdbc",
            "sqlserver",
            "postgresql",
            "mysql",
            "oracle",
            "redshift",
            "azuresql",
            "saphana",
        ]:
            secret = get_secret_cached(md["source"]["SECRET_KEY"])
            url, driver = build_jdbc_url(source_type, secret)

            df = (
                spark.read.format("jdbc")
                .option("url", url)
                .option("dbtable", source_path)
                .option("user", secret["username"])
                .option("password", secret["password"])
                .option("driver", driver)
                .load()
            )

        # Salesforce
        elif source_type in ["salesforce", "sf", "sfdc"]:
            sf_secret = get_secret_cached(md["source"]["SECRET_KEY"])
            sf_username = sf_secret.get("username")
            sf_password = sf_secret.get("password")
            sf_token = (
                sf_secret.get("security_token") or sf_secret.get("token") or ""
            )
            sf_login = sf_secret.get("login_url") or "https://login.salesforce.com"
            sf_version = sf_secret.get("version") or "60.0"

            if not sf_username or not sf_password:
                raise ValueError(
                    "Salesforce secret must include 'username' and 'password' (and usually 'security_token')."
                )

            sf_pwd_concat = f"{sf_password}{sf_token}"

            reader = (
                spark.read.format("com.springml.spark.salesforce")
                .option("username", sf_username)
                .option("password", sf_pwd_concat)
                .option("login", sf_login)
                .option("version", sf_version)
            )

            sp_raw = (source_path or "").strip()
            if sp_raw.lower().startswith("select"):
                df = reader.option("soql", sp_raw).load()
            else:
                mapped_src_cols = [
                    (m.get("SOURCECOLUMNNAME") or "").strip()
                    for m in md.get("mappings", [])
                ]
                mapped_src_cols = [c for c in mapped_src_cols if c]
                select_cols = (
                    ", ".join(sorted(set(mapped_src_cols)))
                    if mapped_src_cols
                    else "Id"
                )
                soql = f"SELECT {select_cols} FROM {sp_raw}"
                logger.info(f"Built SOQL for Salesforce read: {soql}")
                df = reader.option("soql", soql).load()

            logger.info("Successfully read data from Salesforce")

        # ==== HUBSPOT READER (contacts / companies via REST API) ====
        elif source_type in ["hubspot", "hubspotapi", "hubspot_rest"]:
            hs_secret = get_secret_cached(md["source"]["SECRET_KEY"])

            token = (
                hs_secret.get("access_token")
                or hs_secret.get("AccessToken")
                or hs_secret.get("token")
                or hs_secret.get("pat")
            )
            if not token:
                raise ValueError(
                    "HubSpot secret must contain one of: AccessToken, access_token, token, pat"
                )

            base_url = (
                hs_secret.get("base_url")
                or hs_secret.get("BASE_URL")
                or "https://api.hubapi.com"
            )

            object_name = (md["source"]["SOURCEPATH"] or "contacts").strip()

            mapped_src_cols = [
                (m.get("SOURCECOLUMNNAME") or "").strip()
                for m in md.get("mappings", [])
            ]
            mapped_src_cols = [c for c in mapped_src_cols if c]

            api_property_keys = [
                c.lower()
                for c in mapped_src_cols
                if c.lower() not in ["recordid", "id", "hs_object_id"]
            ]

            # --- NEW: configurable bulk size (page size) for HubSpot ---
            hubspot_limit = int(
                fs.get("page_size")            # from PIPELINEPROPERTIES (file_settings)
                or hs_secret.get("page_size")  # from secret JSON
                or 100                         # default behaviour
            )
            # -----------------------------------------------------------

            url = f"{base_url.rstrip('/')}/crm/v3/objects/{object_name}"
            headers = {
                "Authorization": f"Bearer {token}",
                "Content-Type": "application/json",
            }

            all_rows = []
            after = None

            while True:
                # CHANGED: use configurable bulk size instead of fixed 100
                params = {"limit": hubspot_limit}
                if api_property_keys:
                    params["properties"] = ",".join(api_property_keys)
                if after:
                    params["after"] = after

                resp = requests.get(url, headers=headers, params=params)
                if resp.status_code != 200:
                    raise RuntimeError(
                        f"HubSpot API error {resp.status_code}: {resp.text}"
                    )

                data = resp.json()

                for item in data.get("results", []):
                    props = item.get("properties", {}) or {}
                    row = {}
                    for src_name in mapped_src_cols:
                        key_l = src_name.lower()
                        if key_l in ["recordid", "id", "hs_object_id"]:
                            row[src_name] = item.get("id")
                        else:
                            row[src_name] = props.get(key_l)
                    all_rows.append(row)

                paging = data.get("paging") or {}
                after = (paging.get("next") or {}).get("after")
                if not after:
                    break

            schema = StructType(
                [StructField(c, StringType(), True) for c in mapped_src_cols]
            )

            df = spark.createDataFrame(all_rows or [], schema=schema)

            logger.info(
                f"Successfully read {df.count()} rows from HubSpot {object_name}"
            )

        # ==== S3 via SourceType = awss3 (XLSX / Excel / CSV / JSON) ====
        elif source_type in ["awss3", "aws_s3"]:
            # fs already built at top of copy_parse_dedupe: fs = {k.lower(): v ...}
            sheet_name = fs.get("sheetname")  # <<< NEW: SheetName support for S3 Excel

            s3_secret = get_secret_cached(md["source"]["SECRET_KEY"])

            path = source_path
            if path.startswith("s3://"):
                tmp = path[5:]
                bucket, key = tmp.split("/", 1)
            else:
                bucket = s3_secret["bucket"]
                key = path.lstrip("/")

            s3_region = s3_secret.get("region_name", DEFAULT_AWS_REGION)
            s3_client = boto3.client("s3", region_name=s3_region)

            logger.info(
                f"Reading S3 object bucket={bucket}, key={key}, region={s3_region}, sheet_name='{sheet_name}'"
            )
            obj = s3_client.get_object(Bucket=bucket, Key=key)
            body = obj["Body"].read()

            lower_key = key.lower()

            if format_type in ["xlsx", "excel", "xls"] or lower_key.endswith((".xlsx", ".xls")):
                # Use SheetName if provided, otherwise default to first sheet
                if sheet_name:
                    df_pd = pd.read_excel(BytesIO(body), sheet_name=sheet_name)
                else:
                    df_pd = pd.read_excel(BytesIO(body))
                df = spark.createDataFrame(df_pd)

            else:
                raise ValueError(
                    f"Unsupported S3 format for awss3 source: format_type='{format_type}', key='{key}'"
                )

            logger.info("S3 (awss3) read success.")

        # ==== Azure Blob via SourceType = azureblob (XLSX) ====
        elif source_type in ["azureblob","azure_blob"]:
            df = read_from_azureblob_xlsx(spark, md)

        # SFTP
        elif source_type == "sftp":
            if read_options.get("header") == "true":
                read_options["header"] = 0

            if "quote" in read_options:
                read_options["quotechar"] = read_options.pop("quote")

            secret_name = md["source"]["SECRET_KEY"]
            sftp_secret = get_secret_cached(secret_name)

            client = paramiko.SSHClient()
            client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            client.connect(
                sftp_secret["host"],
                sftp_secret["port"],
                sftp_secret["username"],
                sftp_secret["password"],
            )

            sftp = client.open_sftp()
            with sftp.open(source_path, "r") as f:
                file_data = f.read()

            if source_path.endswith(".csv"):
                text_data = file_data.decode("utf-8")
                df_pd = pd.read_csv(io.StringIO(text_data), **read_options)
                df = spark.createDataFrame(df_pd)

            elif source_path.endswith(".json"):
                # read raw JSON text
                text_data = file_data.decode("utf-8")
            
                # create an RDD and read as JSON (honors multiline option)
                rdd = spark.sparkContext.parallelize([text_data])
                multiline_flag = str(read_options.get("multiline", "false")).lower()
                df = spark.read.option("multiline", multiline_flag).json(rdd)

                # run normalization (flatten structs and explode arrays progressively)
                df = normalize_nested_json(df)
            
                # show result (use show(truncate=False) to inspect full values)
                logger.info(f"After normalizing JSON, columns: {df.columns}")
                df.show(truncate=False)
                
            elif source_path.endswith(".psv"):
                text_data = file_data.decode("utf-8")
                df_pd = pd.read_csv(io.StringIO(text_data), **read_options)
                df = spark.createDataFrame(df_pd)
            
            elif source_path.endswith(".parquet"):
                try:
                    # read bytes from sftp in binary mode
                    with sftp.open(source_path, "rb") as fb:
                        file_bytes = fb.read()
                except Exception:
                    file_bytes = file_data if isinstance(file_data, (bytes, bytearray)) else str(file_data).encode("utf-8")

                b = io.BytesIO(file_bytes)
                table = pq.read_table(b)
                df_pd = table.to_pandas()
                df = spark.createDataFrame(df_pd)
                
                # ---------- START: Dynamic per-pipeline REST API integration (drop-in replace) ----------
        elif source_type in ["restapi", "api", "http", "rest"]:
            print("api - Process start")
            # ensure token_cache exists in module scope
            try:
                token_cache
            except NameError:
                token_cache = {}
                
            # ---------- START: robust normalizer for headers / params ----------
            def _normalize_to_dict(obj, name="headers"):
                """
                Accepts:
                  - dict -> returned as-is
                  - JSON string -> json.loads -> dict (if possible)
                  - list of "Key: Value" strings -> parsed into dict
                  - list/iterable of (k,v) pairs -> dict()
                  - None -> {}
                Returns a dict.
                """
                if obj is None:
                    return {}
                # already a dict
                if isinstance(obj, dict):
                    return obj
                # JSON string
                if isinstance(obj, str):
                    s = obj.strip()
                    # try load JSON
                    try:
                        parsed = json.loads(s)
                        if isinstance(parsed, dict):
                            return parsed
                    except Exception:
                        pass
                    # try parse single header style "k:v, k2:v2" or "k: v"
                    # split on commas only if it looks like multiple headers
                    try:
                        items = [s] if (":" in s and "," not in s) else s.split(",")
                        out = {}
                        for it in items:
                            if not it:
                                continue
                            if ":" in it:
                                k, v = it.split(":", 1)
                                out[k.strip()] = v.strip()
                            else:
                                # fallback: single token -> skip
                                continue
                        if out:
                            return out
                    except Exception:
                        pass
                    return {}
                # list/iterable case
                if isinstance(obj, (list, tuple, set)):
                    out = {}
                    # list of "Key: Value" strings
                    if all(isinstance(x, str) for x in obj):
                        for it in obj:
                            if not it:
                                continue
                            if ":" in it:
                                k, v = it.split(":", 1)
                                out[k.strip()] = v.strip()
                            else:
                                # can't parse single token, skip
                                continue
                        return out
                    # list of pairs
                    try:
                        out = dict(obj)
                        if isinstance(out, dict):
                            return out
                    except Exception:
                        # fallthrough
                        pass
                # unknown type -> return empty dict
                logger.warning(f"[RESTAPI] {_normalize_to_dict.__name__}: could not normalize {name} (type={type(obj)}); using empty dict")
                return {}
            # ---------- END: robust normalizer for headers / params ----------
        
            # Helper: normalize/merge config sources (md.source, md.properties, secret)
            def _gather_api_config(md):
                """
                Returns a dict `cfg` with keys used below:
                - endpoint/base_url/token_endpoint/token_method/token_payload/token_headers
                - access_token, auth_scheme, api_key_header, default_headers, default_params
                - pagination_type, pagination_param, results_path, page_size, timeout
                Sources of truth (priority):
                 1) pipeline SOURCE table fields and md['properties'] (if present)
                 2) secret content (if SECRET_KEY provided)
                 3) reasonable defaults
                """
                cfg = {}
                # 1) pipeline-level config - expect it in md['source'] and md.get('properties')
                pipeline_src = md.get("source") or {}
                properties = md.get("file_settings") or {}
        
                # copy known keys from pipeline config (if present)
                for k in ["base_url", "token_endpoint", "token_method", "token_payload",
                          "token_headers", "access_token", "auth_scheme", "api_key_header",
                          "default_headers", "default_params", "pagination_type", "pagination_param",
                          "results_path", "page_size", "timeout", "start_page", "page_size"]:
                    if pipeline_src.get(k) is not None:
                        cfg[k] = pipeline_src.get(k)
                    elif properties.get(k) is not None:
                        cfg[k] = properties.get(k)
        
                # 2) secret fallback
                secret_name = pipeline_src.get("SECRET_KEY") or pipeline_src.get("SECRETID") or pipeline_src.get("SECRET")
                secret_content = {}
                if secret_name:
                    try:
                        secret_content = get_secret_cached(secret_name) or {}
                    except Exception:
                        secret_content = {}
                # merge secret values where pipeline didn't supply
                for k in ["base_url", "token_endpoint", "token_method", "token_payload",
                          "token_headers", "access_token", "auth_scheme", "api_key_header",
                          "default_headers", "default_params", "pagination_type", "pagination_param",
                          "results_path", "page_size", "timeout", "start_page"]:
                    if cfg.get(k) is None and secret_content.get(k) is not None:
                        cfg[k] = secret_content.get(k)
        
                # final normalization & defaults
                cfg["base_url"] = (cfg.get("base_url") or "").rstrip("/") if cfg.get("base_url") else ""
                cfg["token_method"] = (cfg.get("token_method") or "POST").upper()
                cfg["auth_scheme"] = (cfg.get("auth_scheme") or "Bearer")
                cfg["api_key_header"] = cfg.get("api_key_header") or "x-api-key"
                cfg["default_headers"] = cfg.get("default_headers") or {}
                cfg["default_params"] = cfg.get("default_params") or {}
                cfg["pagination_type"] = (cfg.get("pagination_type") or "").lower()
                cfg["pagination_param"] = cfg.get("pagination_param") or cfg.get("page_param") or "page"
                cfg["results_path"] = cfg.get("results_path") or None
                cfg["page_size"] = int(cfg.get("page_size") or 100)
                cfg["timeout"] = int(cfg.get("timeout") or DEFAULTS.get("DEFAULT_TIMEOUT", 30))
                cfg["_secret_name"] = secret_name
                cfg["_secret_content"] = secret_content
                return cfg
        
            # Helper: call API with 401-refresh attempt (uses token_endpoint from cfg)
            def _api_call_with_refresh(endpoint, method="GET", headers=None, params=None, payload=None, cfg=None, cache_key=None, max_retries=1):
                """
                Call api_request; if a 401/403 occurs and token_endpoint is configured,
                attempt to regenerate token once and retry.
            
                This version ensures authType and Token are passed to api_request when available,
                and also places the token in headers as a fallback.
                """
                headers = dict(headers or {})
                params = dict(params or {})
                attempt = 0
            
                # Helper to resolve token from cfg, secret, or cache
                def _resolve_token():
                    # 1) explicit access_token in cfg or secret content
                    tok = cfg.get("access_token") or (cfg.get("_secret_content") or {}).get("access_token")
                    # 2) cached token per pipeline/secret
                    if not tok and cache_key and cache_key in token_cache:
                        tok = token_cache[cache_key]
                    return tok
            
                # Helper to inject token into headers based on scheme
                def _inject_token_into_headers(token_value):
                    if not token_value:
                        return
                    auth_scheme = (cfg.get("auth_scheme") or "Bearer").upper()
                    if auth_scheme == "JWT":
                        headers.setdefault("Authorization", f"JWT {token_value}")
                    elif auth_scheme in ("BEARER", "OAUTH", "OAUTHTOKEN"):
                        headers.setdefault("Authorization", f"Bearer {token_value}")
                    elif auth_scheme == "API_KEY":
                        # allow custom api_key_header in cfg or secret; fallback to x-api-key
                        api_key_header = cfg.get("api_key_header") or (cfg.get("_secret_content") or {}).get("api_key_header") or "x-api-key"
                        headers.setdefault(api_key_header, token_value)
                    else:
                        # support passing raw header name (e.g., "X-Auth-Token") if auth_scheme is not standard
                        if isinstance(cfg.get("auth_scheme"), str) and ":" in cfg.get("auth_scheme"):
                            # format "HeaderName:SCHEME" not common but keep fallback
                            hdr = cfg.get("auth_scheme").split(":", 1)[0].strip()
                            headers.setdefault(hdr, token_value)
                        else:
                            headers.setdefault("Authorization", f"Bearer {token_value}")
            
                # initial resolution/injection
                token_value = _resolve_token()
                _inject_token_into_headers(token_value)
            
                # Also set authType/Token params expected by api_request if possible
                # Map our auth_scheme to api_request's authType
                def _map_auth_type():
                    ascheme = (cfg.get("auth_scheme") or "Bearer")
                    if isinstance(ascheme, str):
                        a = ascheme.upper()
                        if a in ("BEARER", "JWT", "API_KEY"):
                            return a
                    # default fallback
                    return "Bearer"
            
                authType = _map_auth_type()
                Token = token_value
            
                while attempt <= max_retries:
                    attempt += 1
                    # call api_request with both header and explicit auth args (ensures compatibility)
                    resp = api_request(
                        endpoint,
                        method=method,
                        authType=authType,
                        Token=Token,
                        headers=headers,
                        params=params,
                        payload=payload,
                        timeout=cfg.get("timeout"),
                        raise_for_status=False,
                    )
            
                    status_code = resp.get("status_code") if isinstance(resp, dict) else None
            
                    # success -> return
                    if status_code is None or (200 <= status_code < 300):
                        return resp
            
                    # on 401/403 attempt refresh and retry (only if token_endpoint is configured)
                    if status_code in (401, 403) and attempt <= max_retries:
                        logger.info(f"[RESTAPI] {status_code} received for {endpoint}. Attempting token refresh (attempt {attempt}).")
                        new_token = None
                        token_endpoint = cfg.get("token_endpoint") or (cfg.get("_secret_content") or {}).get("token_endpoint")
                        if token_endpoint:
                            # fallback: direct call to token endpoint then attempt to extract token
                            if not new_token:
                                try:
                                    token_method = (cfg.get("token_method") or "POST").upper()
                                    token_payload = cfg.get("token_payload") or (cfg.get("_secret_content") or {}).get("token_payload")
                                    token_headers = cfg.get("token_headers") or (cfg.get("_secret_content") or {}).get("token_headers")
                                    token_resp = api_request(
                                        token_endpoint,
                                        method=token_method,
                                        authType=None,
                                        Token=None,
                                        headers=token_headers,
                                        params=None,
                                        payload=(token_payload if isinstance(token_payload, dict) else (json.loads(token_payload) if isinstance(token_payload, str) else token_payload)),
                                        timeout=int(cfg.get("token_timeout") or cfg.get("timeout") or 30),
                                        raise_for_status=False,
                                    )
                                    new_token = extract_token_from_response(token_resp)
                                    # if extract_token_from_response couldn't find token but token_resp contains access_token key, then use it
                                    if not new_token and isinstance(token_resp, dict):
                                        b = token_resp.get("body")
                                        if isinstance(b, dict):
                                            new_token = b.get("access_token") or b.get("token") or b.get("token_value")
                                except Exception as e:
                                    logger.warning(f"[RESTAPI] token endpoint call failed during refresh: {e}")
            
                        if new_token:
                            # update in-memory cache and headers for subsequent attempts
                            if cache_key:
                                token_cache[cache_key] = new_token
                            Token = new_token
                            _inject_token_into_headers(new_token)
                            authType = _map_auth_type()
                            logger.info("[RESTAPI] Token refresh succeeded; retrying API call.")
                            # retry loop continues
                            continue
                        else:
                            logger.warning("[RESTAPI] Token refresh failed or not configured; returning original response.")
                            return resp
                    else:
                        # non-auth error or retries exhausted
                        return resp
                return resp
        
            # Gather pipeline-specific config (md -> secret)
            cfg = _gather_api_config(md)
            secret_name = cfg.get("_secret_name")
        
            # Build endpoint (SOURCEPATH may be full path or relative)
            full_path = (source_path or "").strip()
            if full_path.lower().startswith("http"):
                endpoint = full_path
            else:
                endpoint = f"{cfg['base_url']}/{full_path.lstrip('/')}" if cfg.get("base_url") else full_path
            
            # Prepare headers and params (merge default headers/params from cfg)
            _headers_from_cfg = _normalize_to_dict(cfg.get("default_headers"), "default_headers")
            _headers_from_secret = _normalize_to_dict(cfg.get("_secret_content", {}).get("default_headers"), "secret.default_headers")
            # pipeline config takes precedence, but merge secret fallback for anything missing
            headers = {}
            headers.update(_headers_from_secret)
            headers.update(_headers_from_cfg)
            
            _params_from_cfg = _normalize_to_dict(cfg.get("default_params"), "default_params")
            _params_from_secret = _normalize_to_dict(cfg.get("_secret_content", {}).get("default_params"), "secret.default_params")
            params = {}
            params.update(_params_from_secret)
            params.update(_params_from_cfg)

            # --- NEW: apply page_size as generic 'limit' param if not already set ---
            page_size_val = cfg.get("page_size")
            if page_size_val and "limit" not in params:
                try:
                    params["limit"] = int(page_size_val)
                except Exception:
                    pass
            # ------------------------------------------------------------------------        
            # Determine token: (1) pipeline-provided static token, (2) secret's access_token, (3) cached token, (4) token_endpoint to generate
            token = cfg.get("access_token") or (cfg.get("_secret_content") or {}).get("access_token")
            cache_key = None
            # prefer per-pipeline cache key if SOURCE table has unique id (SOURCEID or SOURCENAME)
            pipeline_key = None
            if md.get("source", {}).get("SOURCEID"):
                pipeline_key = str(md.get("source", {}).get("SOURCEID"))
            elif md.get("source", {}).get("SOURCENAME"):
                pipeline_key = str(md.get("source", {}).get("SOURCENAME"))
            cache_key = f"token:{pipeline_key}" if pipeline_key else (f"token:{secret_name}" if secret_name else None)
        
            if not token and cache_key and cache_key in token_cache:
                token = token_cache[cache_key]
        
            # If still no token but token_endpoint exists in cfg, generate and cache it
            if not token and cfg.get("token_endpoint"):
                try:
                    new_token = None
                    if not new_token:
                        token_resp = api_request(
                            cfg.get("token_endpoint"),
                            method=cfg.get("token_method", "POST"),
                            authType=None,
                            Token=None,
                            headers=cfg.get("token_headers") or cfg.get("_secret_content", {}).get("token_headers"),
                            payload=(cfg.get("token_payload") or cfg.get("_secret_content", {}).get("token_payload")),
                            timeout=cfg.get("timeout"),
                            raise_for_status=False,
                        )
                        new_token = extract_token_from_response(token_resp)
                    if new_token:
                        token = new_token
                        if cache_key:
                            token_cache[cache_key] = new_token
                        logger.info(f"[RESTAPI] Generated token dynamically for pipeline (cache_key={cache_key})")
                except Exception as e:
                    logger.warning(f"[RESTAPI] Failed generating token dynamically: {e}")
        
            # Place token into headers according to auth scheme
            if token:
                auth_scheme = (cfg.get("auth_scheme") or "Bearer").upper()
                if auth_scheme == "JWT":
                    headers.setdefault("Authorization", f"JWT {token}")
                elif auth_scheme in ("BEARER", "OAUTH", "OAUTHTOKEN"):
                    headers.setdefault("Authorization", f"Bearer {token}")
                elif auth_scheme == "API_KEY":
                    headers.setdefault(cfg.get("api_key_header") or "x-api-key", token)
                else:
                    headers.setdefault("Authorization", f"Bearer {token}")
        
            # mapped_src_cols from md.mappings (same as your existing flow)
            mapped_src_cols = [(m.get("SOURCECOLUMNNAME") or "").strip() for m in md.get("mappings", [])]
            mapped_src_cols = [c for c in mapped_src_cols if c]
        
            # Pagination + read loop (uses _api_call_with_refresh to handle 401 -> refresh)
            all_rows = []
            next_cursor = None
            page = int(cfg.get("start_page") or 1)
        
            loop_params = dict(params)  # use default params only

            resp = _api_call_with_refresh(
                endpoint,
                method="GET",
                headers=headers,
                params=loop_params,
                payload=None,
                cfg=cfg,
                cache_key=cache_key,
                max_retries=1
            )

            # Error handling
            if isinstance(resp, dict) and resp.get("error"):
                raise RuntimeError(
                    f"[RESTAPI] API error while calling {endpoint}: {resp.get('error')}"
                )

            status_code = resp.get("status_code")
            body = resp.get("body")
            if status_code and (status_code < 200 or status_code >= 300):
                raise RuntimeError(f"[RESTAPI] Unexpected status {status_code} for {endpoint}: {body}")

            # ---------- Convert API body -> Spark DataFrame (normalize nested JSON, no-prefix) ----------
            def _is_struct(t): return isinstance(t, T.StructType)
            def _is_array(t): return isinstance(t, T.ArrayType)

            def flatten_structs_no_prefix(df):
                struct_cols = [f.name for f in df.schema.fields if _is_struct(f.dataType)]
                if not struct_cols:
                    return df, False
                used = set()
                exprs = []
                for f in df.schema.fields:
                    name, dtype = f.name, f.dataType
                    if _is_struct(dtype):
                        for inner in dtype.fields:
                            new_name = inner.name
                            if new_name in used:
                                i = 1
                                while f"{new_name}_{i}" in used:
                                    i += 1
                                new_name = f"{new_name}_{i}"
                            exprs.append(F.col(f"{name}.{inner.name}").alias(new_name))
                            used.add(new_name)
                    else:
                        col_name = name
                        if col_name in used:
                            i = 1
                            while f"{col_name}_{i}" in used:
                                i += 1
                            col_name = f"{col_name}_{i}"
                        exprs.append(F.col(name).alias(col_name))
                        used.add(col_name)
                return df.select(*exprs), True

            def explode_arrays(df):
                arr_fields = [f for f in df.schema.fields if _is_array(f.dataType)]
                if not arr_fields:
                    return df, False
                for f in arr_fields:
                    elem = f.dataType.elementType
                    if isinstance(elem, T.StructType):
                        df = df.withColumn(f.name, F.explode_outer(F.col(f.name)))
                        return df, True
                return df, False

            def normalize_nested_json(df, max_iters=20):
                changed = True
                it = 0
                while changed and it < max_iters:
                    it += 1
                    changed = False
                    df, c1 = flatten_structs_no_prefix(df)
                    df, c2 = explode_arrays(df)
                    changed = c1 or c2
                return df

            def body_to_df(body):
                # If JSON string, parse it
                if isinstance(body, str):
                    try:
                        body_obj = json.loads(body)
                    except Exception:
                        return spark.createDataFrame([], T.StructType([]))
                else:
                    body_obj = body

                # If dict -> single JSON object -> read as one-row DF
                if isinstance(body_obj, dict):
                    rdd = spark.sparkContext.parallelize([json.dumps(body_obj)])
                    df = spark.read.json(rdd)
                    return normalize_nested_json(df)

                # If list -> array of JSON rows
                if isinstance(body_obj, list):
                    rdd = spark.sparkContext.parallelize([json.dumps(body_obj)])
                    df = spark.read.json(rdd)
                    # if Spark reads as single array column -> explode it
                    if len(df.columns) == 1:
                        col0 = df.columns[0]
                        if isinstance(df.schema.fields[0].dataType, T.ArrayType):
                            df = df.select(F.explode_outer(col0).alias("elem")).select("elem.*")
                    return normalize_nested_json(df)

                # fallback empty DF
                return spark.createDataFrame([], T.StructType([]))

            # produce normalized DF
            df_from_api = body_to_df(body)

            logger.info(f"[RESTAPI] DataFrame columns: {df_from_api.columns}")
            df_from_api.show(truncate=False)

            # assign this DF back to your variable used downstream
            df = df_from_api
            df.show()
            logger.info(f"[RESTAPI] Successfully read {df.count()} rows from {endpoint} for pipeline {md.get('source', {}).get('SOURCENAME')}")
        # ---------- END: Dynamic per-pipeline REST API integration ----------

        else:
            raise ValueError(
                f"Unsupported source type: {source_type}. Only JDBC, Salesforce, HubSpot, awss3, azureblob and SFTP connections are supported."
            )

        logger.info(f"Successfully read data from source: {source_type}")
    except Exception as e:
        logger.error(
            f"Error reading from source {source_type}: {str(e)}\nTraceback:\n{traceback.format_exc()}"
        )
        raise

    try:
        actual_by_lower = {c.lower(): c for c in df.columns}
        mappings_sorted = sorted(
            md["mappings"],
            key=lambda x: int((x.get("DESTINATIONCOLUMNSEQUENCE") or 0)),
        )

        def _cast_for_dtype(col_expr, dtype):
            dtype = (dtype or "").upper()
            if "DATE" in dtype or "TIMESTAMP" in dtype:
                return F.to_timestamp(col_expr)
            elif "INT" in dtype:
                return col_expr.cast(IntegerType())
            elif any(x in dtype for x in ["FLOAT", "DOUBLE", "DECIMAL"]):
                return col_expr.cast(DoubleType())
            elif "BOOLEAN" in dtype:
                return col_expr.cast(BooleanType())
            else:
                return col_expr.cast(StringType())

        select_exprs = []
        for m in mappings_sorted:
            src_name = (m["SOURCECOLUMNNAME"] or "").strip()
            dest_name = (m["DESTINATIONCOLUMNNAME"] or "").strip()
            dtype = (m["DESTINATIONCOLUMNDATATYPE"] or "").strip()

            src_actual = actual_by_lower.get(src_name.lower())
            if src_actual is None:
                logger.warning(
                    f"Source column '{src_name}' not found in data; filling NULL as '{dest_name}'"
                )
                col_expr = F.lit(None)
            else:
                col_expr = F.col(src_actual)

            select_exprs.append(_cast_for_dtype(col_expr, dtype).alias(dest_name))

        df = df.select(*select_exprs)
        logger.info("Column selection, renaming, and casting completed successfully.")
    except Exception as e:
        logger.error(
            f"Failed during column selection/renaming/casting: {str(e)}\nTraceback:\n{traceback.format_exc()}"
        )
        raise

    try:
        unique_keys = [
            k.strip() for k in md["pipeline"]["UNIQUEKEYCOLUMNS"].split(",")
        ]
        if unique_keys:
            window_spec = Window.partitionBy(unique_keys).orderBy(
                F.col(unique_keys[0]).desc()
            )
            df_dedup = (
                df.withColumn("row_num", F.row_number().over(window_spec))
                .filter(F.col("row_num") == 1)
                .drop("row_num")
            )
        else:
            df_dedup = df.distinct()
        logger.info("Deduplication completed successfully.")
    except Exception as e:
        logger.error(
            f"Error during deduplication: {str(e)}\nTraceback:\n{traceback.format_exc()}"
        )
        raise

    try:
        if s3_staging_dir:
            staging_path = f"{s3_staging_dir}/staging/{md['destination']['DESTINATIONPATH'].replace('.', '/')}"
            df_dedup.write.mode("overwrite").parquet(staging_path)
            logger.info(f"Staging data written to: {staging_path}")
    except Exception as e:
        logger.warning(
            f"Staging write failed: {str(e)} — continuing without blocking"
        )

    return df, df_dedup, unique_keys


# ===== JDBC URL BUILDER SECTION =====
def build_jdbc_url(source_type, secret):
    host = secret["host"]
    port = secret["port"]
    logger.info(f"build_jdbc_url - host : {host}")
    logger.info(f"build_jdbc_url - port : {port}")
    logger.info(f"build_jdbc_url - building JDBC URL for source type: {source_type}")

    st = str(source_type).lower()

    if st == "sqlserver":
        database = secret.get("database")
        if database:
            logger.info(f"build_jdbc_url - database : {database}")
            url = (
                f"jdbc:sqlserver://{host}:{port};"
                f"database={database};"
                "encrypt=true;"
                "trustServerCertificate=false;"
                "loginTimeout=30;"
            )
        else:
            url = f"jdbc:sqlserver://{host}:{port}"
        driver = "com.microsoft.sqlserver.jdbc.SQLServerDriver"

    elif st == "postgresql":
        db = secret.get("database") or secret.get("dbname") or "postgres"
        url = f"jdbc:postgresql://{host}:{port}/{db}"
        driver = "org.postgresql.Driver"
        logger.info(f"build_jdbc_url - using database: {db}")

    elif st == "mysql":
        url = f"jdbc:mysql://{host}:{port}"
        driver = "com.mysql.jdbc.Driver"

    elif st == "oracle":
        svc_name = (
            secret.get("service_name")
            or secret.get("SERVICE_NAME")
            or secret.get("servicename")
            or secret.get("SERVICE")
            or secret.get("service")
            or secret.get("serviceName")
        )
        sid = secret.get("sid") or secret.get("SID")
        connect_descriptor = (
            secret.get("connect_descriptor") or secret.get("CONNECT_DESCRIPTOR")
        )
        ezconnect = secret.get("ezconnect") or secret.get("EZCONNECT")
        if svc_name:
            url = f"jdbc:oracle:thin:@//{host}:{port}/{svc_name}"
        elif sid:
            url = f"jdbc:oracle:thin:@{host}:{port}:{sid}"
        elif ezconnect:
            url = f"jdbc:oracle:thin:@//{ezconnect}"
        elif connect_descriptor:
            url = f"jdbc:oracle:thin:@{connect_descriptor}"
        else:
            present = ", ".join(sorted(k for k in secret.keys()))
            raise ValueError(
                "Oracle secret must include 'service_name' or 'sid' "
                f"(present keys: {present})"
            )
        driver = "oracle.jdbc.OracleDriver"

    elif st == "redshift":
        db = secret.get("database") or secret.get("dbname") or ""
        path = f"/{db}" if db else ""
        params = secret.get("jdbc_params") or {}
        if params:
            query = "&".join(f"{k}={v}" for k, v in params.items())
            url = f"jdbc:redshift://{host}:{port}{path}?{query}"
        else:
            url = f"jdbc:redshift://{host}:{port}{path}"
        driver = secret.get("driver", "com.amazon.redshift.jdbc.Driver")

    elif st in ("azuresql", "sql", "mssql", "azure_sql"):
        database = secret.get("database")
        if not database:
            raise ValueError(
                "Azure SQL secret must include 'database' key "
                "(e.g. DataFramework) in AWS Secrets Manager."
            )

        logger.info(f"build_jdbc_url - database : {database}")

        url = (
            f"jdbc:sqlserver://{host}:{port};"
            f"database={database};"
            "encrypt=true;"
            "trustServerCertificate=false;"
            "loginTimeout=30;"
        )
        driver = "com.microsoft.sqlserver.jdbc.SQLServerDriver"

    elif st == "saphana":
        url = secret.get("url") or f"jdbc:sap://{host}:{port}"
        params = secret.get("jdbc_params") or {}
        if secret.get("currentschema"):
            params.setdefault("currentSchema", secret.get("currentschema"))
        if params:
            def _v(v):
                return str(v).lower() if isinstance(v, bool) else v

            query = "&".join(f"{k}={_v(v)}" for k, v in params.items())
            sep = "&" if "?" in url else "/?"
            url = f"{url}{sep}{query}"
        driver = secret.get("driver", "com.sap.db.jdbc.Driver")

    else:
        logger.error(f"Unsupported JDBC source type: {source_type}")
        raise ValueError(f"Unsupported JDBC source type: {source_type}")

    logger.info(f"Constructed JDBC URL: {url} with driver: {driver}")
    return url, driver


# ===== BRONZE LAYER LOADING SECTION =====
def load_bronze(
    spark, sf_options, secret, md, dedup_df=None, keys=None, stage_suffix="_STAGE"
):
    load_type = md["pipeline"]["LOAD_TYPE"].strip().lower()
    dest_table = md["destination"]["DESTINATIONPATH"]
    custom_query = md["pipeline"].get("CUSTOMQUERY")
    if custom_query and str(custom_query).strip().upper() in ("NULL", "NONE", ""):
        custom_query = None

    logger.info(f"{load_type} → Starting RAW load into table: {dest_table}")

    table_exists = table_exists_in_snowflake(secret, dest_table)
    logger.info(f"Table exists? {table_exists}")

    try:
        if table_exists:
            before = spark_snowflake_retry(
                lambda: spark.read.format("snowflake")
                .options(**sf_options)
                .option("query", f"SELECT COUNT(1) AS COUNT FROM {dest_table}")
                .load()
                .collect()[0]["COUNT"],
                action_desc="get row count",
            )
            logger.info(f"Previous row count in {dest_table}: {before}")
        else:
            before = 0
            logger.warning(f"{dest_table} does not exist yet. Assuming 0 rows.")
    except Exception as e:
        before = 0
        logger.warning(
            f"Unable to get row count from {dest_table}. Assuming 0. Error: {str(e)}"
        )

    # Build the DataFrame to load (either from CustomQuery or dedup_df)
    if custom_query:
        logger.info("Using CustomQuery to load RAW table.")
        data_df = spark_snowflake_retry(
            lambda: spark.read.format("snowflake")
            .options(**sf_options)
            .option("query", custom_query)
            .load(),
            action_desc="read custom query",
        )
        cols = data_df.columns
    else:
        if dedup_df is None:
            logger.error("dedup_df is required if no CustomQuery is provided.")
            raise ValueError("dedup_df is required if no CustomQuery is provided")
        data_df = dedup_df
        cols = [m["DESTINATIONCOLUMNNAME"] for m in md["mappings"]]

    logger.debug(f"Columns used for loading: {', '.join(cols)}")

    # Create table if it does not exist
    if not table_exists:
        if custom_query:
            logger.info("Creating table using schema from custom query.")
            ddl_cols = ", ".join(f"{c} STRING" for c in cols)
        else:
            ddl_cols = ", ".join(
                f"{m['DESTINATIONCOLUMNNAME']} {m['DESTINATIONCOLUMNDATATYPE']}"
                for m in sorted(
                    md["mappings"],
                    key=lambda x: x.get("DESTINATIONCOLUMNSEQUENCE", 0),
                )
            )
        create_sql = f"CREATE TABLE {dest_table} ({ddl_cols})"
        logger.info(f"Creating table using SQL: {create_sql}")
        execute_snowflake_sql(secret, create_sql)

    try:
        # ===== FULL LOAD =====
        if load_type == "full":
            logger.info("Performing FULL load → Truncate + Insert")
            execute_snowflake_sql(secret, f"TRUNCATE TABLE {dest_table}")
            spark_snowflake_retry(
                lambda: data_df.write.mode("append")
                .format("snowflake")
                .options(**sf_options)
                .option("dbtable", dest_table)
                .save(),
                action_desc="full load write",
            )

        # ===== BULK LOAD (NEW) =====
        elif load_type == "bulk":
            # First implementation: same behavior as FULL (truncate + insert)
            logger.info("Performing BULK load → Truncate + Insert (same as FULL for now)")
            execute_snowflake_sql(secret, f"TRUNCATE TABLE {dest_table}")
            spark_snowflake_retry(
                lambda: data_df.write.mode("append")
                .format("snowflake")
                .options(**sf_options)
                .option("dbtable", dest_table)
                .save(),
                action_desc="bulk load write",
            )

        # ===== APPEND LOAD =====
        elif load_type == "append":
            logger.info(
                "Performing APPEND load → Insert only new records based on watermark"
            )
            watermark_col = md["pipeline"].get("WATERMARKCOLUMN") or md["pipeline"].get(
                "WATERMARK_COLUMN"
            )
            max_wm = None

            if watermark_col is None:
                logger.warning(
                    "No WATERMARK_COLUMN defined in pipeline metadata. Appending all records."
                )
            else:
                if table_exists:
                    try:
                        wm_df = spark_snowflake_retry(
                            lambda: spark.read.format("snowflake")
                            .options(**sf_options)
                            .option(
                                "query",
                                f"SELECT MAX({watermark_col}) AS max_wm FROM {dest_table}",
                            )
                            .load(),
                            action_desc="max watermark read",
                        )
                        if wm_df.count() > 0 and wm_df.first()["MAX_WM"] is not None:
                            max_wm = wm_df.first()["MAX_WM"]
                            logger.info(
                                f"Max watermark value in destination table for column {watermark_col}: {max_wm}"
                            )
                    except Exception as e:
                        logger.warning(
                            f"Could not fetch MAX({watermark_col}) from {dest_table}: {str(e)}"
                        )

            if max_wm is not None:
                logger.info(f"Filtering source data where {watermark_col} > {max_wm}")
                data_df = data_df.filter(col(watermark_col) > lit(max_wm))
            else:
                logger.info("No watermark found or no previous data. Loading all source records.")

            if data_df.rdd.isEmpty():
                logger.info("No new records")
                return dest_table, before

            spark_snowflake_retry(
                lambda: data_df.write.mode("append")
                .format("snowflake")
                .options(**sf_options)
                .option("dbtable", dest_table)
                .save(),
                action_desc="append load write",
            )

        # ===== INCREMENTAL (MERGE) LOAD =====
        elif load_type == "incremental":
            logger.info("Performing INCREMENTAL load → MERGE based on primary key")
            if not keys:
                logger.error("Primary keys must be provided for INCREMENTAL load")
                raise ValueError("Primary keys must be provided for INCREMENTAL load")

            watermark_col = md["pipeline"].get("WATERMARKCOLUMN") or md["pipeline"].get(
                "WATERMARK_COLUMN"
            )
            if watermark_col and table_exists:
                try:
                    wm_df = spark_snowflake_retry(
                        lambda: spark.read.format("snowflake")
                        .options(**sf_options)
                        .option(
                            "query",
                            f"SELECT MAX({watermark_col}) AS max_wm FROM {dest_table}",
                        )
                        .load(),
                        action_desc="max watermark read (incremental)",
                    )
                    dest_max_wm = wm_df.first()["MAX_WM"] if wm_df.count() > 0 else None
                    if dest_max_wm is not None:
                        data_df = data_df.filter(col(watermark_col) > lit(dest_max_wm))
                        if data_df.rdd.isEmpty():
                            logger.info("No new records")
                            return dest_table, before
                except Exception as e:
                    logger.warning(
                        f"Could not fetch MAX({watermark_col}) from {dest_table}: {str(e)}"
                    )

            stage_table = dest_table + stage_suffix
            logger.info(
                f"Writing deduped DataFrame to Snowflake staging table: {stage_table}"
            )
            spark_snowflake_retry(
                lambda: data_df.write.mode("overwrite")
                .format("snowflake")
                .options(**sf_options)
                .option("dbtable", stage_table)
                .save(),
                action_desc="incremental stage write",
            )

            update_clause = ", ".join(
                [f"target.{c}=source.{c}" for c in cols if c not in keys]
            )
            insert_cols = ", ".join(cols)
            insert_vals = ", ".join([f"source.{c}" for c in cols])
            on_clause = " AND ".join([f"target.{k}=source.{k}" for k in keys])

            merge_sql = f"""
                MERGE INTO {dest_table} AS target
                USING {stage_table} AS source
                ON {on_clause}
                WHEN MATCHED THEN UPDATE SET {update_clause}
                WHEN NOT MATCHED THEN INSERT ({insert_cols}) VALUES ({insert_vals})
            """
            logger.debug(f"Executing merge SQL: {merge_sql.strip()}")
            execute_snowflake_sql(secret, merge_sql)

            try:
                logger.info(
                    f"Truncating staging table {stage_table} to save storage."
                )
                execute_snowflake_sql(secret, f"TRUNCATE TABLE {stage_table}")
            except Exception as e:
                logger.warning(
                    f"Failed to truncate staging table {stage_table}: {str(e)}"
                )

        else:
            logger.error(f"Unsupported LoadType: {load_type}")
            raise ValueError(f"Unsupported LoadType: {load_type}")

    except Exception as e:
        logger.error(
            f"RAW load failed for {dest_table}: {str(e)}\nTraceback:\n{traceback.format_exc()}"
        )
        raise

    logger.info(
        f"Completed RAW load → {dest_table}. Previous Row Count: {before}"
    )
    return dest_table, before

# ===== DATA INGESTION LOGGING SECTION =====
def log_data_ingestion(
    spark,
    sf_options,
    master_log_id,
    status_message_id,
    pipeline_id,
    layer,
    trigger_type="SCHEDULED",
    pipeline_start_time=None,
    pipeline_end_time=None,
    pipeline_status=None,
    errortype=None,
    error_message=None,
    error_severity=None,
    error_datetime=None,
    source_count=0,
    destination_count=0,
    inserted_count=0,
    updated_count=0,
    deleted_count=0,
    log_table_name=None,
):
    """
    Build a DataFrame that EXACTLY matches CONFIG.LOG table (23 columns, exact order):
      LOGID, PIPELINEID, PIPELINESTARTDATETIME, PIPELINEENDDATETIME, PIPELINESTATUS,
      STATUSMESSAGEID, MASTERLOGID, INSTANCEID, INSTANCESTARTDATETIME, INSTANCEENDDATETIME,
      INSTANCESTATUS, ERRORMESSAGE, ERRORSEVERITY, ERRORDATETIME, SOURCECOUNT, DESTINATIONCOUNT,
      INSERTEDCOUNT, UPDATEDCOUNT, DELETEDCOUNT, TOTALINSTANCECOUNT, TRIGGERTYPE, LOGDATETIME, LAYER
    """
    log_id = str(uuid.uuid4())
    current_time = datetime.now(timezone.utc)

    instance_id = None
    instance_start = None
    instance_end = None
    instance_status = None
    total_instance_count = 1

    schema = StructType(
        [
            StructField("LOGID", StringType(), False),
            StructField("PIPELINEID", StringType(), False),
            StructField("PIPELINESTARTDATETIME", TimestampType(), True),
            StructField("PIPELINEENDDATETIME", TimestampType(), True),
            StructField("PIPELINESTATUS", StringType(), True),
            StructField("STATUSMESSAGEID", LongType(), False),
            StructField("MASTERLOGID", StringType(), False),
            StructField("INSTANCEID", StringType(), True),
            StructField("INSTANCESTARTDATETIME", TimestampType(), True),
            StructField("INSTANCEENDDATETIME", TimestampType(), True),
            StructField("INSTANCESTATUS", StringType(), True),
            StructField("ERRORMESSAGE", StringType(), True),
            StructField("ERRORSEVERITY", StringType(), True),
            StructField("ERRORDATETIME", TimestampType(), True),
            StructField("SOURCECOUNT", IntegerType(), True),
            StructField("DESTINATIONCOUNT", IntegerType(), True),
            StructField("INSERTEDCOUNT", IntegerType(), True),
            StructField("UPDATEDCOUNT", IntegerType(), True),
            StructField("DELETEDCOUNT", IntegerType(), True),
            StructField("TOTALINSTANCECOUNT", IntegerType(), True),
            StructField("TRIGGERTYPE", StringType(), True),
            StructField("LOGDATETIME", TimestampType(), False),
            StructField("LAYER", StringType(), True),
        ]
    )

    values = [
        (
            log_id,
            pipeline_id,
            pipeline_start_time,
            pipeline_end_time,
            pipeline_status,
            int(status_message_id),
            master_log_id,
            instance_id,
            instance_start,
            instance_end,
            instance_status,
            error_message,
            error_severity,
            error_datetime if error_message else None,
            int(source_count),
            int(destination_count),
            int(inserted_count),
            int(updated_count),
            int(deleted_count),
            int(total_instance_count),
            trigger_type,
            current_time,
            layer,
        )
    ]

    log_df = spark.createDataFrame(values, schema=schema)

    spark_snowflake_retry(
        lambda: log_df.write.format("snowflake")
        .options(**sf_options)
        .option("dbtable", log_table_name)
        .mode("append")
        .save(),
        action_desc="data ingestion log",
    )

    print(
        f"Log written to Snowflake: {pipeline_status} with counts – Source: {source_count}, Dest: {destination_count}"
    )


# ===== MASTER LOG FINALIZATION =====
def finalize_master_log(secret, master_log_id, master_status, message=None, table_name=None):
    try:
        logger.info(
            f"Finalizing MASTERLOG ID {master_log_id} with status → {master_status}"
        )

        end_dt_str = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
        update_sql = (
            f"UPDATE {table_name} "
            f"SET ENDDATETIME = TO_TIMESTAMP('{end_dt_str}'), "
            f"STATUS = '{master_status}' "
            f"WHERE MASTERLOGID = '{master_log_id}'"
        )

        logger.info(f"Executing update SQL:\n{update_sql}")
        execute_snowflake_sql(secret, update_sql)
        logger.info(
            f"MASTERLOG ID {master_log_id} successfully updated with status → {master_status}"
        )

    except AnalysisException as ae:
        logger.error(
            f"Snowflake AnalysisException while updating MASTERLOG ID {master_log_id}: {ae}"
        )
        raise
    except Exception as e:
        logger.error(
            f"Unexpected error while finalizing MASTERLOG ID {master_log_id}: {str(e)}\nTraceback:\n{traceback.format_exc()}"
        )
        raise


# ===== CRON UPDATE SECTION =====
def update_cron_next_run(secret, cron_id, cron_expr, tz, table_name):
    """
    Update NEXTRUN in CRON table by CRONID (no PIPELINEID in CRON anymore).
    """
    now = datetime.now(pytz.timezone(tz))
    iter_obj = croniter(cron_expr, now)
    next_run = iter_obj.get_next(datetime)
    next_run_utc = next_run.astimezone(pytz.UTC).strftime("%Y-%m-%d %H:%M:%S")

    update_sql = f"""
        UPDATE {table_name}
        SET NEXTRUN = TO_TIMESTAMP('{next_run_utc}')
        WHERE CRONID = '{cron_id}'
    """
    execute_snowflake_sql(secret, update_sql)
    logger.info(f"Updated NEXTRUN for CRONID={cron_id} to {next_run_utc}")

def update_cron_last_run(secret, cron_id, table_name):
    """
    Update LASTRUN in CRON table by CRONID (no PIPELINEID in CRON anymore).
    """
    now_utc = datetime.now(pytz.UTC).strftime("%Y-%m-%d %H:%M:%S")
    update_sql = f"""
        UPDATE {table_name}
        SET LASTRUN = TO_TIMESTAMP('{now_utc}')
        WHERE CRONID = '{cron_id}'
    """
    execute_snowflake_sql(secret, update_sql)
    logger.info(f"Updated LASTRUN for CRONID={cron_id} to {now_utc}")


def api_request(
    url,
    method="GET",
    authType=None,
    Token=None,
    headers=None,
    payload=None,
    params=None,
    verify=True,
    raise_for_status=False,
    timeout=30,
):
    method = method.upper()
    session = requests.Session()

    hdrs = {} if headers is None else dict(headers)

    if payload is not None and "Content-Type" not in {k.title(): v for k, v in hdrs.items()}:
        hdrs.setdefault("Content-Type", "application/json")

    if authType:
        a = authType.upper()
        if a == "BASIC" and isinstance(Token, (tuple, list)) and len(Token) == 2:
            session.auth = HTTPBasicAuth(Token[0], Token[1])
        elif a == "BEARER" and isinstance(Token, str):
            hdrs.setdefault("Authorization", f"Bearer {Token}")
        elif a == "JWT" and isinstance(Token, str):
            hdrs.setdefault("Authorization", f"JWT {Token}")
        elif a == "API_KEY" and isinstance(Token, str):
            hdrs.setdefault("x-api-key", Token)

    session.headers.update(hdrs)

    try:
        if method == "GET":
            response = session.get(url, params=params, timeout=timeout, verify=verify)
        elif method == "POST":
            response = session.post(
                url, json=payload, params=params, timeout=timeout, verify=verify
            )
        elif method == "PUT":
            response = session.put(
                url, json=payload, params=params, timeout=timeout, verify=verify
            )
        elif method == "DELETE":
            response = session.delete(url, params=params, timeout=timeout, verify=verify)
        else:
            raise ValueError(f"Unsupported HTTP method: {method}")

        if raise_for_status:
            response.raise_for_status()

        try:
            body = response.json()
        except ValueError:
            body = response.text

        return {
            "status_code": response.status_code,
            "headers": dict(response.headers),
            "body": body,
            "request_headers": dict(response.request.headers),
            "url": response.url,
        }
    except requests.exceptions.RequestException as e:
        return {"error": str(e)}


def extract_token_from_response(resp_json):
    """
    flexibly extract a token string from resp_json which may be:
      - dict containing common keys
      - a plain string
      - wrapper dict containing 'body' which is a dict
    """
    if isinstance(resp_json, dict) and "body" in resp_json:
        body = resp_json.get("body")
    else:
        body = resp_json

    if isinstance(body, dict):
        for k in (
            "generatedToken",
            "token",
            "jwt",
            "access_token",
            "accessToken",
            "Token",
        ):
            v = body.get(k)
            if isinstance(v, str) and len(v) > 0:
                return v

        for v in body.values():
            if isinstance(v, str) and len(v) > 20:
                return v

    if isinstance(body, str) and len(body) > 0:
        return body

    return None


def put_generate_token(
    url,
    source_system_name,
    headers=None,
    max_retries=None,
    backoff_seconds=None,
    timeout=None,
    return_full_response_on_failure=False,
):
    payload = {"sourceSystemName": source_system_name}
    attempt = 0
    last_resp = None

    max_retries = int(
        max_retries if max_retries is not None else DEFAULTS.get("DEFAULT_MAX_RETRIES", 3)
    )
    backoff_seconds = int(
        backoff_seconds
        if backoff_seconds is not None
        else DEFAULTS.get("DEFAULT_RETRY_DELAY_SECONDS", 60)
    )
    timeout = int(
        timeout if timeout is not None else DEFAULTS.get("DEFAULT_TIMEOUT", 30)
    )

    while attempt < max_retries:
        resp = api_request(
            url,
            method="PUT",
            authType=None,
            Token=None,
            headers=headers,
            payload=payload,
            params=None,
            timeout=timeout,
        )
        if isinstance(resp, dict) and resp.get("error"):
            logger.warning(
                "Attempt %d: api_request error generating token for %s: %s",
                attempt + 1,
                source_system_name,
                resp.get("error"),
            )
            last_resp = resp
        else:
            status = resp.get("status_code")
            last_resp = resp
            if status in (200, 201):
                token = extract_token_from_response(resp)
                if token:
                    logger.info(
                        "Generated token for %s (len=%d)",
                        source_system_name,
                        len(token),
                    )
                    return token
                else:
                    logger.warning(
                        "200/201 returned but no token found in body for %s. body-type=%s",
                        source_system_name,
                        type(resp.get("body")),
                    )
                    if return_full_response_on_failure:
                        return None, resp
                    return None
            elif status == 204:
                logger.warning(
                    "204 No Content returned for %s - check API behaviour",
                    source_system_name,
                )
                if return_full_response_on_failure:
                    return None, resp
                return None
            else:
                if 400 <= (status or 0) < 500 and status != 429:
                    logger.error(
                        "Client error while generating token for %s: status=%s, body=%s",
                        source_system_name,
                        status,
                        resp.get("body"),
                    )
                    if return_full_response_on_failure:
                        return None, resp
                    return None
                logger.warning(
                    "Transient API response for %s: status=%s. Will retry.",
                    status,
                )

        attempt += 1
        sleep_seconds = backoff_seconds * attempt
        logger.info(
            "Waiting %d seconds before retrying token generation (attempt %d/%d) for %s",
            sleep_seconds,
            attempt,
            max_retries,
            source_system_name,
        )
        time.sleep(sleep_seconds)

    logger.error("Failed to generate token for %s after %d attempts", source_system_name, max_retries)
    if return_full_response_on_failure and last_resp is not None:
        return None, last_resp
    return None


def to_rfc3339_z(dt):
    """
    Convert datetime or ISO string to RFC3339 UTC string ending with 'Z'.
    - If dt is None -> returns None
    - If dt is a datetime (naive -> treated as UTC) -> returns 'YYYY-MM-DDTHH:MM:SS.ssssssZ'
    - If dt is a string that already ends with '+00:00Z' or '+00:00', normalize to Z-form.
    """
    if dt is None:
        return None
    if isinstance(dt, str):
        s = dt
        if s.endswith("+00:00Z"):
            s = s.replace("+00:00Z", "Z")
        if s.endswith("+00:00"):
            s = s.replace("+00:00", "Z")
        return s
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def build_globalconfig_payload(
    md, job_id=None, start_ts=None, end_ts=None, record_count=None, status="C"
):
    """Build the JSON model expected by GlobalConfig API using md returned by load_metadata."""
    now_dt = datetime.utcnow().replace(tzinfo=timezone.utc)
    now = to_rfc3339_z(now_dt)
    start_str = to_rfc3339_z(start_ts) if start_ts else to_rfc3339_z(now_dt)
    end_str = to_rfc3339_z(end_ts) if end_ts else to_rfc3339_z(now_dt)

    source_name = (
        DEFAULTS.get("sourceSystemName")
        or md["source"].get("SOURCENAME")
        or "Snowflake_DE"
    )
    job_id = job_id[:45] if job_id else str(uuid.uuid4())

    tpath = md["destination"].get("DESTINATIONPATH") or ""
    tparts = tpath.split(".")
    if len(tparts) >= 3:
        target_schema, target_table = tparts[-2], tparts[-1]
    elif len(tparts) == 2:
        target_schema, target_table = tparts[0], tparts[1]
    else:
        target_schema, target_table = "", ""

    s_path = md["source"].get("SOURCEPATH") or ""

    if "/" in s_path:
        source_table = s_path.split("/")[-1]
        source_schema = None

    elif "." in s_path:
        parts = s_path.split(".")
        source_table = parts[-1]
        source_schema = ".".join(parts[:-1])
    else:
        source_table = s_path
        source_schema = None

    job_config = {
        "jobIDDetails": job_id,
        "sourceSystemName": source_name,
        "targetSystemName": md["destination"].get("DESTINATIONNAME") or "Snowflake",
        "description": md["pipeline"].get("PIPELINENAME")
        or md["pipeline"].get("DESCRIPTION")
        or "",
        "criticalFlag": 4,
        "severity": 1,
        "completedAlert": "Y",
        "failedAlert": "Y",
        "delayedAlert": "Y",
        "validationlAlert": "N",
        "sourceSystemLocation": s_path,
        "targetSystemLocation": md["destination"].get("DESTINATIONPATH"),
        "sourceSchemaName": source_schema,
        "sourceTableName": source_table,
        "targetSchemaName": target_schema,
        "targetTableName": target_table,
        "operationType": md["pipeline"].get("LOAD_TYPE") or "ETL",
        "isBusinessAlert": "Y",
        "businessAlertEmail": DEFAULTS.get("BusinessAlertEmail", None),
        "helpDeskConfigID": 1,
        "runSchedule": "Y",
        "day": "Daily",
        "time": datetime.utcnow().strftime("%H:%M"),
        "estimatedTime": (
            start_ts.strftime("%H:%M:%S")
            if isinstance(start_ts, datetime)
            else (md.get("pipeline", {}).get("ESTIMATEDTIME") or None)
        ),
        "isEnabled": "S",
        "insertedDateTime": now,
        "importedDateTime": now,
        "sourceAlertLog": [
            {
                "jobIDDetails": job_id,
                "uniqueJobIDReference": job_id,
                "sourceSystemName": source_name,
                "transactionStatus": status,
                "jobStatus": status,
                "reportedDateTime": end_str,
                "sourceRecordCount": int(record_count or 0),
                "startDateTime": start_str,
                "endDateTime": end_str,
                "message": f"{job_id}_{source_name}",
                "pF1": "success" if status == "C" else "failed",
                "pF2": str(record_count or 0),
                "pF3": "automated",
            }
        ],
    }

    source_config = {
        "sourceSystemName": source_name,
        "sourceType": md["source"].get("SOURCE_TYPE", "Snowflake"),
        "description": (md.get("source", {}).get("SOURCEDESCRIPTION") or "")
        .strip()
        or "Snowflake data framework",
        "helpdeskAlert": "Y",
        "hostAddress": "string",
        "connectionConfig": "string",
        "databaseType": DEFAULTS.get("CONFIG_DATABASE", None),
        "isEnabled": "A",
        "insertedDateTime": now,
        "importedDateTime": now,
    }

    payload = {"sourceConfig": source_config, "sourceJobConfig": [job_config]}
    return payload


def insert_global_config(
    master_log_id,
    md,
    job_id,
    start_ts,
    end_ts,
    record_count,
    status,
    token=None,
    max_retries=None,
    backoff_seconds=None,
    timeout=None,
    return_full_response_on_failure=False,
):
    payload = build_globalconfig_payload(
        md,
        job_id=job_id,
        start_ts=start_ts,
        end_ts=end_ts,
        record_count=record_count,
        status=status,
    )

    hdrs = {"Content-Type": "application/json"}
    if token:
        hdrs.setdefault("Authorization", f"Bearer {token}")

    max_retries = int(
        max_retries if max_retries is not None else DEFAULTS.get("DEFAULT_MAX_RETRIES", 3)
    )
    backoff_seconds = int(
        backoff_seconds
        if backoff_seconds is not None
        else DEFAULTS.get("DEFAULT_RETRY_DELAY_SECONDS", 60)
    )
    timeout = int(
        timeout if timeout is not None else DEFAULTS.get("DEFAULT_TIMEOUT", 30)
    )

    attempt = 0
    last_resp = None

    if token is None:
        logger.error("No token - skipping GlobalConfig entry")
        return None

    while attempt < max_retries:
        resp = api_request(
            url=DEFAULTS["GlobalAlerts_API_URL"] + "GlobalConfig",
            method="PUT",
            authType="Bearer",
            Token=token,
            headers=hdrs,
            payload=payload,
            timeout=timeout,
        )

        if isinstance(resp, dict) and resp.get("error"):
            logger.warning(
                "Attempt %d: api_request error insert_global_config for %s: %s",
                attempt + 1,
                md.get("source", {}).get("SOURCENAME", DEFAULTS["sourceSystemName"]),
                resp.get("error"),
            )
            last_resp = resp
        else:
            status_code = resp.get("status_code")
            last_resp = resp
            if status_code in (200, 201):
                resp_body = resp.get("body")
                new_token = extract_token_from_response(resp)
                if new_token:
                    logger.info(
                        "insert_global_config returned token for %s (len=%d)",
                        md.get("source", {}).get("SOURCENAME", DEFAULTS["sourceSystemName"]),
                        len(new_token),
                    )
                    return new_token
                return resp_body
            elif status_code == 204:
                logger.info("insert_global_config returned 204 No Content")
                return None
            else:
                if 400 <= (status_code or 0) < 500 and status_code != 429:
                    logger.error(
                        "Client error in insert_global_config: status=%s body=%s",
                        status_code,
                        resp.get("body"),
                    )
                    if return_full_response_on_failure:
                        return None, resp
                    return None
                logger.warning(
                    "Transient API response for insert_global_config: status=%s. Will retry.",
                    status_code,
                )

        attempt += 1
        sleep_seconds = backoff_seconds * attempt
        logger.info(
            "Waiting %d seconds before retrying insert_global_config (attempt %d/%d)",
            sleep_seconds,
            attempt,
            max_retries,
        )
        time.sleep(sleep_seconds)

    logger.error("Failed insert_global_config after %d attempts", max_retries)
    if return_full_response_on_failure and last_resp is not None:
        return None, last_resp
    return None


# ===== MAIN EXECUTION SECTION =====
def main():
    logger.info("===== sf_de_framework ETL Job Started =====")
    spark = glueContext.spark_session

    glueRunId = os.environ.get("AWS_GLUE_JOB_RUN_ID") or f"manual_run_{uuid.uuid4()}"
    logger.info(f"Glue Job Run ID: {glueRunId}")

    try:
        snowflake_secret = get_secret_cached(
            "sf_de_framework_snowflake_secret",
            region_name=DEFAULTS["SECRETS_REGION"],
        )
        safe_secret = {**snowflake_secret, "password": "***"}
        logger.info(
            f"Step 1: Fetched Snowflake Secret = {json.dumps(safe_secret)}"
        )

        sf_options = create_snowflake_options(snowflake_secret)
        sf_options_safe = {**sf_options, "sfPassword": "***"}
        logger.info(
            f"Step 2: Snowflake options = {json.dumps(sf_options_safe)}"
        )

        config_db_default = DEFAULTS["CONFIG_DATABASE"]
        config_schema_default = DEFAULTS["CONFIG_SCHEMA"]

        default_sysprops_fqn = f"{config_db_default}.{config_schema_default}.SYSTEMPROPERTIES"
        sys_props = load_system_properties(spark, sf_options, default_sysprops_fqn)

        CONFIG_DB = get_prop(sys_props, "CONFIG_DATABASE", config_db_default)
        CONFIG_SCHEMA = get_prop(sys_props, "CONFIG_SCHEMA", config_schema_default)

        def cfg(tname: str) -> str:
            return f"{CONFIG_DB}.{CONFIG_SCHEMA}.{tname}"

        logger.info(
            f"[Config] Using CONFIG schema → {CONFIG_DB}.{CONFIG_SCHEMA}"
        )

        global DEFAULT_AWS_REGION
        DEFAULT_AWS_REGION = get_prop(
            sys_props, "SECRETS_REGION", DEFAULTS["SECRETS_REGION"]
        )

        MASTERLOG_TABLE = get_prop(
            sys_props,
            "MASTERLOG_TABLE",
            f"{CONFIG_DB}.{CONFIG_SCHEMA}.MASTERLOG",
        )
        LOG_TABLE = get_prop(
            sys_props, "LOG_TABLE", f"{CONFIG_DB}.{CONFIG_SCHEMA}.LOG"
        )
        CRON_TABLE = get_prop(sys_props, "CRON_TABLE", f"{CONFIG_DB}.{CONFIG_SCHEMA}.CRON")

        PIPELINE_SUCCESS_ID = int(
            get_prop(sys_props, "PIPELINE_SUCCESS_ID", DEFAULTS["PIPELINE_SUCCESS_ID"])
        )
        MASTER_SUCCESS_ID = int(
            get_prop(sys_props, "MASTER_SUCCESS_ID", DEFAULTS["MASTER_SUCCESS_ID"])
        )
        MASTER_FAILURE_ID = int(
            get_prop(sys_props, "MASTER_FAILURE_ID", DEFAULTS["MASTER_FAILURE_ID"])
        )
        STAGE_TABLE_SUFFIX = get_prop(
            sys_props, "STAGE_TABLE_SUFFIX", DEFAULTS["STAGE_TABLE_SUFFIX"]
        )
        DEFAULT_MAX_RETRIES = int(
            get_prop(
                sys_props,
                "DEFAULT_MAX_RETRIES",
                DEFAULTS["DEFAULT_MAX_RETRIES"],
            )
        )

        DEFAULT_RETRY_DELAY = int(
            get_prop(
                sys_props,
                "DEFAULT_RETRY_DELAY_SECONDS",
                DEFAULTS["DEFAULT_RETRY_DELAY_SECONDS"],
            )
        )

        logger.info(
            f"[Tables] MASTERLOG={MASTERLOG_TABLE}, LOG={LOG_TABLE}, CRON={CRON_TABLE}"
        )

        status_df = spark_snowflake_retry(
            lambda: spark.read.format("snowflake")
            .options(**sf_options)
            .option("dbtable", cfg("STATUSMESSAGE"))
            .load(),
            action_desc="read STATUSMESSAGE",
        )
        logger.info(
            f"Loaded STATUSMESSAGE table with {status_df.count()} rows"
        )

        pipeline_df = spark_snowflake_retry(
            lambda: spark.read.format("snowflake")
            .options(**sf_options)
            .option("dbtable", cfg("PIPELINE"))
            .load(),
            action_desc="read PIPELINE",
        )
        logger.info(
            f"Loaded PIPELINE table with {pipeline_df.count()} rows from {cfg('PIPELINE')}"
        )

        cron_df = spark_snowflake_retry(
            lambda: spark.read.format("snowflake")
            .options(**sf_options)
            .option("dbtable", CRON_TABLE)
            .load(),
            action_desc="read CRON",
        )
        logger.info(
            f"Loaded CRON table with {cron_df.count()} rows from {CRON_TABLE}"
        )

        lookup_df = spark_snowflake_retry(
            lambda: spark.read.format("snowflake")
            .options(**sf_options)
            .option("dbtable", cfg("LOOKUP"))
            .load(),
            action_desc="read LOOKUP",
        )
        logger.info(
            f"Loaded LOOKUP table with {lookup_df.count()} rows from {cfg('LOOKUP')}"
        )

        active_pipelines = (
            pipeline_df.filter("ACTIVE = TRUE")
            .select("PIPELINEID", "LAYERID", "CRONID")
        )
        cron_active = cron_df.filter("ACTIVE = TRUE").select(
            "CRONID", "CRONEXPRESSION", "TIMEZONE", "NEXTRUN"
        )
        joined_df = active_pipelines.join(cron_active, "CRONID")
        logger.info(
            "Joined CRON and PIPELINE tables by CRONID; determining due pipelines."
        )

        due_pipelines = []
        for row in joined_df.collect():
            pid = row["PIPELINEID"]
            cron_id = row["CRONID"]
            expr = row["CRONEXPRESSION"]
            tz = row["TIMEZONE"]
            next_run = row["NEXTRUN"]

            now_tz = datetime.now(pytz.timezone(tz))
            now_utc = now_tz.astimezone(pytz.UTC)
            if next_run:
                if next_run.tzinfo is None:
                    next_utc = pytz.UTC.localize(next_run)
                else:
                    next_utc = next_run.astimezone(pytz.UTC)
            else:
                next_utc = None

            if next_utc is None or now_utc >= next_utc:
                due_pipelines.append((pid, cron_id, expr, tz))

        if not due_pipelines:
            logger.info("No pipelines are due for execution. Exiting main().")
            return
        else:
            endpoint_token = DEFAULTS["GlobalAlerts_API_URL"] + "JWTAuthentication"
            token = put_generate_token(
                url=endpoint_token,
                source_system_name=DEFAULTS["sourceSystemName"],
            )
            if token:
                logger.info(
                    "Token generated for %s (len=%d).",
                    DEFAULTS["sourceSystemName"],
                    len(token),
                )
            else:
                logger.error(
                    "Token generation returned no token for %s; inspect API response/behavior",
                    DEFAULTS["sourceSystemName"],
                )

        logger.info(
            f"Due pipelines: {[pid for pid, _, _, _ in due_pipelines]}"
        )

        master_log_id = create_master_log(
            spark,
            sf_options,
            glue_run_id=glueRunId,
            total_pipelines=len(due_pipelines),
            table_name=MASTERLOG_TABLE,
        )
        logger.info(f"Master log created. MASTERLOG ID = {master_log_id}")

        any_failures = False

        for pid, cron_id, expr, tz in due_pipelines:
            update_cron_next_run(
                snowflake_secret, cron_id, expr, tz, table_name=CRON_TABLE
            )

            max_retries, retrydelayseconds = get_retry_config_for_pipeline(
                pipeline_df,
                pid,
                default_retries=DEFAULT_MAX_RETRIES,
                default_delay=DEFAULT_RETRY_DELAY,
            )
            attempt = 0
            while attempt < max_retries:
                layer = "UNKNOWN"
                try:
                    logger.info(
                        f"----- [Pipeline Start] PIPELINEID: {pid}, Attempt: {attempt + 1} of {max_retries} -----"
                    )
                    pipeline_start_time = datetime.now(timezone.utc)

                    md = load_metadata(spark, sf_options, pid, cfg)
                    logger.info(
                        "Loaded Metadata:\n" + json.dumps(md, indent=2, default=str)
                    )

                    layer = md["pipeline"]["LAYER"].lower()
                    logger.info(f"Pipeline {pid} is a {layer} pipeline.")

                    errors = validate_all_required(
                        spark, sf_options, pid, cfg
                    )
                    if errors:
                        pipeline_end_time = datetime.now(timezone.utc)
                        status_id, status_name = get_status_by_name(
                            status_df, "PIPELINE_FAILURE", default_id=2
                        )
                        for emsg in errors:
                            log_data_ingestion(
                                spark,
                                sf_options,
                                master_log_id=master_log_id,
                                status_message_id=status_id,
                                pipeline_id=pid,
                                layer=layer,
                                trigger_type="SCHEDULED",
                                pipeline_start_time=pipeline_start_time,
                                pipeline_end_time=pipeline_end_time,
                                pipeline_status="PIPELINE_FAILURE",
                                errortype=status_name,
                                error_message=emsg,
                                error_severity="ERROR",
                                error_datetime=pipeline_end_time,
                                source_count=0,
                                destination_count=0,
                                inserted_count=0,
                                updated_count=0,
                                deleted_count=0,
                                log_table_name=LOG_TABLE,
                            )
                        any_failures = True
                        attempt = max_retries
                        break

                    if layer in ("bronze", "silver"):
                        source_df, dedupe_df, unique_keys = copy_parse_dedupe(
                            spark, md, s3_staging_dir=None
                        )
                        logger.info(f"Unique Keys: {unique_keys}")
                        dest_tbl, before_count = load_bronze(
                            spark,
                            sf_options,
                            snowflake_secret,
                            md,
                            dedupe_df,
                            unique_keys,
                            stage_suffix=STAGE_TABLE_SUFFIX,
                        )
                        logger.info(
                            f"{layer.capitalize()} Layer ETL complete for PIPELINEID: {pid}"
                        )
                    else:
                        logger.error(
                            f"Unsupported layer '{layer}' for pipeline {pid}"
                        )
                        raise Exception(
                            f"Unsupported layer '{layer}' for pipeline {pid}"
                        )

                    try:
                        after_count = spark_snowflake_retry(
                            lambda: spark.read.format("snowflake")
                            .options(**sf_options)
                            .option(
                                "query", f"SELECT COUNT(1) AS COUNT FROM {dest_tbl}"
                            )
                            .load()
                            .collect()[0]["COUNT"],
                            action_desc=f"get row count {dest_tbl} (after)",
                        )
                        logger.info(
                            f"Fetched after_count from {dest_tbl}: {after_count}"
                        )
                    except Exception as e:
                        after_count = 0
                        logger.warning(
                            f"Could not fetch after_count from Snowflake for {dest_tbl}, Error: {str(e)}"
                        )

                    pipeline_end_time = datetime.now(timezone.utc)

                    log_data_ingestion(
                        spark,
                        sf_options,
                        master_log_id=master_log_id,
                        status_message_id=PIPELINE_SUCCESS_ID,
                        pipeline_id=pid,
                        layer=layer,
                        trigger_type="SCHEDULED",
                        pipeline_start_time=pipeline_start_time,
                        pipeline_end_time=pipeline_end_time,
                        pipeline_status=get_status_message_name_by_id(
                            status_df, PIPELINE_SUCCESS_ID
                        ),
                        errortype=None,
                        error_message=None,
                        error_severity=None,
                        error_datetime=None,
                        source_count=source_df.count()
                        if layer in ("bronze", "silver")
                        else 0,
                        destination_count=after_count
                        if layer in ("bronze", "silver")
                        else 0,
                        inserted_count=max(0, after_count - before_count),
                        updated_count=0,
                        deleted_count=0,
                        log_table_name=LOG_TABLE,
                    )
                    logger.info(
                        f"Logged data ingestion success for Pipeline {pid}."
                    )

                    update_cron_last_run(
                        snowflake_secret, cron_id, table_name=CRON_TABLE
                    )

                    insert_global_config(
                        master_log_id=master_log_id,
                        md=md,
                        job_id=glueRunId + "_" + master_log_id,
                        start_ts=pipeline_start_time,
                        end_ts=pipeline_end_time,
                        record_count=before_count,
                        status="C",
                        token=token,
                    )

                    break

                except Exception as e:
                    attempt += 1
                    logger.error(
                        f"Pipeline {pid} attempt {attempt} failed: {str(e)}\nTraceback:\n{traceback.format_exc()}"
                    )

                    if attempt < max_retries:
                        logger.info(
                            f"Retrying pipeline {pid} after {retrydelayseconds} seconds..."
                        )
                        time.sleep(retrydelayseconds)
                    else:
                        any_failures = True
                        pipeline_end_time = datetime.now(timezone.utc)
                        status_id, status_name = classify_error(
                            status_df, str(e)
                        )

                        log_data_ingestion(
                            spark,
                            sf_options,
                            master_log_id=master_log_id,
                            status_message_id=status_id,
                            pipeline_id=pid,
                            layer=layer,
                            trigger_type="SCHEDULED",
                            pipeline_start_time=pipeline_start_time,
                            pipeline_end_time=pipeline_end_time,
                            pipeline_status="PIPELINE_FAILURE",
                            errortype=status_name,
                            error_message=str(e),
                            error_severity="ERROR",
                            error_datetime=pipeline_end_time,
                            source_count=0,
                            destination_count=0,
                            inserted_count=0,
                            updated_count=0,
                            deleted_count=0,
                            log_table_name=LOG_TABLE,
                        )

                        logger.error(
                            f"Pipeline {pid} failed after {max_retries} attempts. Classified as {status_name} (ID={status_id})."
                        )

        if any_failures:
            master_status_name = get_status_message_name_by_id(
                status_df, MASTER_FAILURE_ID
            )
            finalize_master_log(
                snowflake_secret,
                master_log_id,
                master_status_name,
                None,
                table_name=MASTERLOG_TABLE,
            )
        else:
            master_status_name = get_status_message_name_by_id(
                status_df, MASTER_SUCCESS_ID
            )
            finalize_master_log(
                snowflake_secret,
                master_log_id,
                master_status_name,
                None,
                table_name=MASTERLOG_TABLE,
            )

        master_message_content = get_statusmessage_content_by_name(
            status_df, master_status_name
        )
        if master_message_content is not None:
            safe_message = master_message_content.replace("'", "''")
            update_sql = (
                f"UPDATE {MASTERLOG_TABLE} SET MESSAGE = '{safe_message}' "
                f"WHERE MASTERLOGID = '{master_log_id}'"
            )
            execute_snowflake_sql(snowflake_secret, update_sql)

    except Exception as e:
        logger.error(
            f"Unexpected error occurred in main(): {str(e)}\nTraceback:\n{traceback.format_exc()}"
        )
        raise

    logger.info("===== sf_de_framework ETL Job Finished =====")


# ===== SCRIPT ENTRY POINT =====
main()
