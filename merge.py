#!/usr/bin/env python3

import argparse
import hashlib
import json
import logging
import os
import re
import time
import traceback
from dataclasses import dataclass
from datetime import datetime
from typing import Iterable, Optional, cast

import dateutil.parser
import pytz
import psycopg2
from psycopg2 import OperationalError
from azure.storage.blob import BlobServiceClient
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    col,
    concat_ws,
    current_timestamp,
    lit,
    udf,
    when,
    lower,
    length,
    regexp_replace,
    coalesce,
    to_timestamp,
    to_date,
    year,
    trim,
)
from pyspark.sql.types import (
    BooleanType,
    DateType,
    DecimalType,
    DoubleType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

TIMEZONE = "America/New_York"
NUMERIC_REGEX = r"^-?\d+(\.\d+)?$"

@udf(TimestampType())
def parse_timestamp(val: str):
    if not val:
        return None
    try:
        dt = dateutil.parser.parse(str(val), fuzzy=False)
        tz = pytz.timezone(TIMEZONE)
        dt = dt if dt.tzinfo else tz.localize(dt)
        dt = dt.astimezone(tz)
        now = datetime.now(tz)
        return now if dt > now else dt
    except Exception:
        return None

@udf(DateType())
def parse_date(val: str):
    if not val:
        return None
    try:
        d = dateutil.parser.parse(str(val), fuzzy=False).date()
        today = datetime.now(pytz.timezone(TIMEZONE)).date()
        return d if d <= today else None
    except Exception:
        return None

@udf(StringType())
def validate_json(val: str) -> Optional[str]:
    if val is None:
        return None
    try:
        json.loads(val)
        return val
    except Exception:
        return None

@udf(StringType())
def sha3_512(val: str) -> Optional[str]:
    if val is None:
        return None
    return hashlib.sha3_512(val.encode("utf-8")).hexdigest()

@dataclass
class PostgresConfig:
    host: str
    port: str
    database: str
    user: str
    password: str

@dataclass
class StorageConfig:
    account_name: str
    account_key: str
    container_name: str

@dataclass
class SnowflakeConfig:
    url: str
    database: str
    warehouse: str
    role: str
    schema: str
    user: str
    password: str

class SecretManager:
    @staticmethod
    def get(scope: str, key: str) -> str:
        try:
            from pyspark.dbutils import DBUtils  # type: ignore
            dbutils = DBUtils(SparkSession.builder.getOrCreate())
            return dbutils.secrets.get(scope=scope, key=key)
        except Exception:
            env_key = f"{scope}_{key}".upper()
            val = os.getenv(env_key)
            if not val:
                raise ValueError(f"Missing secret for {env_key}")
            return val

class Config:
    TABLES = ["lead_assignment", "lead_xref", "lead"]

    def __init__(self) -> None:
        self._secrets = SecretManager()

    def postgres(self) -> PostgresConfig:
        return PostgresConfig(
            host=self._secrets.get("key-vault-secret", "DataProduct-LCR-Host-PROD"),
            port=self._secrets.get("key-vault-secret", "DataProduct-LCR-Port-PROD"),
            database="LeadCustodyRepository",
            user=self._secrets.get("key-vault-secret", "DataProduct-LCR-User-PROD"),
            password=self._secrets.get("key-vault-secret", "DataProduct-LCR-Pass-PROD"),
        )

    def storage(self) -> StorageConfig:
        return StorageConfig(
            account_name="quilitydatabricks",
            account_key=self._secrets.get("key-vault-secret", "DataProduct-ADLS-Key"),
            container_name="dataarchitecture",
        )

    def snowflake(self) -> SnowflakeConfig:
        return SnowflakeConfig(
            url="hmkovlx-nu26765.snowflakecomputing.com",
            database="DEV",
            warehouse="INTEGRATION_COMPUTE_WH",
            role="SG-SNOWFLAKE-DEVELOPERS",
            schema="QUILITY_EDW_STAGE",
            user=self._secrets.get("key-vault-secret", "DataProduct-SF-EDW-User"),
            password=self._secrets.get("key-vault-secret", "DataProduct-SF-EDW-Pass"),
        )

    def spark(self) -> SparkSession:
        return SparkSession.builder.getOrCreate()

class DBUtilsCompat:
    @staticmethod
    def get_dbutils():
        try:
            from pyspark.dbutils import DBUtils  # type: ignore
            return DBUtils(SparkSession.builder.getOrCreate())
        except Exception:
            class DummySecrets:
                def get(self, scope, key):
                    env_key = f"{scope}_{key}".upper()
                    val = os.getenv(env_key)
                    if not val:
                        raise ValueError(f"Missing secret for {env_key}")
                    return val

            class DummyFS:
                def ls(self, path):
                    raise NotImplementedError

                def mkdirs(self, path):
                    logging.info("[DRY-RUN] mkdir %s", path)

            class Dummy:
                secrets = DummySecrets()
                fs = DummyFS()

            logging.warning("DBUtils not available; using env vars")
            return Dummy()

class PostgresHandler:
    def __init__(self, cfg: PostgresConfig) -> None:
        self.cfg = cfg
        self.pool = self._connect()

    def _connect(self):
        return psycopg2.pool.ThreadedConnectionPool(1, 20, **self.cfg.__dict__)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        if self.pool:
            self.pool.closeall()

    def is_alive(self) -> bool:
        conn = self.pool.getconn()
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT 1")
            return True
        except OperationalError:
            return False
        finally:
            self.pool.putconn(conn)

    def _valid_table(self, table: str) -> bool:
        return bool(re.fullmatch(r"[\w\.\"]+", table))

    def _parse_table(self, table: str) -> tuple[str, str]:
        t = table.replace('"', "")
        return cast(tuple[str, str], tuple(t.split(".", 1))) if "." in t else ("public", t)

    def export_to_delta(self, table: str, path: str, spark: SparkSession) -> None:
        if not self._valid_table(table):
            raise ValueError(f"Invalid table name: {table}")
        schema, tbl = self._parse_table(table)
        url = f"jdbc:postgresql://{self.cfg.host}:{self.cfg.port}/{self.cfg.database}"
        props = {
            "user": self.cfg.user,
            "password": self.cfg.password,
            "driver": "org.postgresql.Driver",
            "fetchsize": "10000",
        }
        df = spark.read.jdbc(url=url, table=f"{schema}.{tbl}", properties=props)
        df = df.withColumns(
            {
                "ETL_CREATED_DATE": current_timestamp(),
                "ETL_LAST_UPDATE_DATE": current_timestamp(),
                "CREATED_BY": lit("ETL_PROCESS"),
                "TO_PROCESS": lit(True),
                "EDW_EXTERNAL_SOURCE_SYSTEM": lit("LeadCustodyRepository"),
            }
        )
        df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(path)

class StorageHandler:
    def __init__(self, cfg: StorageConfig, dbutils) -> None:
        self.cfg = cfg
        self.dbutils = dbutils
        self.client = self._connect()

    def _connect(self):
        conn = (
            f"DefaultEndpointsProtocol=https;AccountName={self.cfg.account_name};"
            f"AccountKey={self.cfg.account_key};EndpointSuffix=core.windows.net"
        )
        return BlobServiceClient.from_connection_string(conn)

    def path(self, stage: str, db: str, table: str) -> str:
        tbl = table.replace('"', "").split(".", 1)[-1]
        return f"abfss://{self.cfg.container_name}@{self.cfg.account_name}.dfs.core.windows.net/{stage}/{db}/{tbl}"

    def ensure_dir(self, stage: str, db: str) -> None:
        base = f"abfss://{self.cfg.container_name}@{self.cfg.account_name}.dfs.core.windows.net/{stage}/{db}"
        try:
            self.dbutils.fs.ls(base)
        except Exception:
            self.dbutils.fs.mkdirs(base)

class DataSync:
    def __init__(self, pg: PostgresHandler, storage: StorageHandler, spark: SparkSession, tables: Iterable[str], db: str) -> None:
        self.pg = pg
        self.storage = storage
        self.spark = spark
        self.tables = list(tables)
        self.db = db

    def sync(self, stage: str = "RAW") -> None:
        if not self.pg.is_alive():
            logging.error("Postgres connection is not alive")
            return
        self.storage.ensure_dir(stage, self.db)
        for table in self.tables:
            try:
                start = time.time()
                dest = self.storage.path(stage, self.db, table)
                self.pg.export_to_delta(table, dest, self.spark)
                logging.info("%s processed in %.2fs", table, time.time() - start)
            except Exception as exc:
                logging.error("Failed %s: %s\n%s", table, exc, traceback.format_exc())

class SchemaManager:
    TABLE_SCHEMAS = {
        "lead": StructType([
            StructField("STG_LCR_LEAD_KEY", StringType()),
            StructField("LEAD_GUID", StringType()),
            StructField("LEAD_CREATE_DATE", TimestampType()),
        ]),
        "lead_assignment": StructType([
            StructField("STG_LCR_LEAD_ASSIGNMENT_KEY", StringType()),
            StructField("ASSIGN_DATE", TimestampType()),
            StructField("STATUS", StringType()),
        ]),
        "lead_xref": StructType([
            StructField("STG_LCR_LEAD_XREF_KEY", StringType()),
            StructField("LEAD_GUID", StringType()),
            StructField("SOURCE_SYSTEM", StringType()),
        ]),
    }
    COLUMN_MAPPINGS = {
        "lead": {"leadguid": "LEAD_GUID", "leadcreatedate": "LEAD_CREATE_DATE"},
        "lead_assignment": {"assigndate": "ASSIGN_DATE", "status": "STATUS"},
        "lead_xref": {"leadsystem": "SOURCE_SYSTEM"},
    }
    JSON_COLUMNS = {"lead_assignment": ["METADATA"], "lead": ["LEAD_ATTRIBUTES"], "lead_xref": []}
    BOOLEAN_STRING_COLUMNS = {"IS_DELETED_SOURCE"}

    @classmethod
    def get_schema(cls, table: str) -> StructType:
        return cls.TABLE_SCHEMAS[table]

    @classmethod
    def get_column_mapping(cls, table: str) -> dict[str, str]:
        return cls.COLUMN_MAPPINGS.get(table, {})

    @classmethod
    def get_json_columns(cls, table: str) -> list[str]:
        return cls.JSON_COLUMNS.get(table, [])

    @classmethod
    def is_boolean_string_column(cls, col_name: str) -> bool:
        return col_name in cls.BOOLEAN_STRING_COLUMNS

def clean_invalid_timestamps(df: DataFrame) -> DataFrame:
    cols = [f.name for f in df.schema.fields if isinstance(f.dataType, TimestampType)]
    for c in cols:
        df = df.withColumn(
            c,
            when(
                col(c).isNull()
                | col(c).cast("string").rlike("^[A-Za-z]{1,3}$")
                | (length(col(c).cast("string")) <= 3)
                | (~col(c).cast("string").rlike(".*\\d+.*"))
                | (year(col(c)) < 1900)
                | (year(col(c)) > year(current_timestamp()) + 1),
                lit(None),
            ).otherwise(col(c)),
        )
        if c.startswith("ETL_"):
            df = df.withColumn(c, coalesce(col(c), current_timestamp()))
    return df

def validate_dataframe(df: DataFrame, target_schema: StructType, check_types: bool = True) -> None:
    errors: list[str] = []
    for field in target_schema.fields:
        name = field.name
        dtype = field.dataType
        if name not in df.columns:
            errors.append(f"Column {name} is missing")
        elif check_types and not isinstance(df.schema[name].dataType, type(dtype)):
            errors.append(f"Column {name} has type {df.schema[name].dataType}, expected {dtype}")
    if errors:
        raise ValueError("DataFrame validation failed:\n" + "\n".join(errors))

def transform_column(df: DataFrame, name: str, dtype, table: str) -> DataFrame:
    if table in SchemaManager.JSON_COLUMNS and name in SchemaManager.get_json_columns(table):
        return df.withColumn(name, validate_json(col(name).cast(StringType())))
    if isinstance(dtype, TimestampType):
        return df.withColumn(name, coalesce(to_timestamp(col(name)), parse_timestamp(col(name))))
    if isinstance(dtype, DateType):
        return df.withColumn(name, coalesce(to_date(col(name)), parse_date(col(name))))
    if isinstance(dtype, DecimalType):
        cleaned = trim(regexp_replace(col(name), ",", ""))
        return df.withColumn(
            name,
            when(col(name).isNull() | (cleaned == "") | (~cleaned.rlike(NUMERIC_REGEX)), None).otherwise(cleaned.cast(dtype)),
        )
    if isinstance(dtype, DoubleType):
        cleaned = trim(regexp_replace(col(name), ",", ""))
        return df.withColumn(
            name,
            when(col(name).isNull() | (cleaned == "") | (~cleaned.rlike(NUMERIC_REGEX)), None).otherwise(cleaned.cast(DoubleType())),
        )
    if isinstance(dtype, BooleanType):
        return df.withColumn(
            name,
            when(lower(col(name)).isin("true", "1", "yes"), lit(True))
            .when(lower(col(name)).isin("false", "0", "no"), lit(False))
            .otherwise(lit(None)),
        )
    if isinstance(dtype, StringType) and SchemaManager.is_boolean_string_column(name):
        return df.withColumn(
            name,
            when(lower(col(name).cast("string")).isin("true", "1", "yes", "t"), lit("TRUE"))
            .when(lower(col(name).cast("string")).isin("false", "0", "no", "f"), lit("FALSE"))
            .otherwise(col(name).cast(StringType())),
        )
    return df.withColumn(name, col(name).cast(StringType()))

def add_metadata_columns(df: DataFrame, schema: StructType) -> DataFrame:
    ts = current_timestamp()
    defaults = {
        "ETL_CREATED_DATE": ts,
        "ETL_LAST_UPDATE_DATE": ts,
        "CREATED_BY": lit("ETL_PROCESS"),
        "TO_PROCESS": lit(True),
        "EDW_EXTERNAL_SOURCE_SYSTEM": lit("LeadCustodyRepository"),
    }
    for k, v in defaults.items():
        df = df.withColumn(k, v.cast(schema[k].dataType))
    return df

def rename_and_add_columns(df: DataFrame, table: str) -> DataFrame:
    lower_cols = {c.lower(): c for c in df.columns}
    mapping = SchemaManager.get_column_mapping(table)
    for old, new in mapping.items():
        if old.lower() in lower_cols:
            df = df.withColumnRenamed(lower_cols[old.lower()], new)
    target = SchemaManager.get_schema(table)
    missing = {f.name for f in target.fields} - set(df.columns)
    for name in missing:
        df = df.withColumn(name, lit(None).cast(target[name].dataType))
    return df

def add_hash_key(df: DataFrame, metadata_cols: list[str], key_col: str) -> DataFrame:
    non_meta = [c for c in df.columns if c not in metadata_cols and c != key_col]
    concatenated = concat_ws("||", *[col(c).cast("string") for c in non_meta])
    return df.withColumn(key_col, sha3_512(concatenated))

def transform_columns(df: DataFrame, schema: StructType, table: str) -> DataFrame:
    df = clean_invalid_timestamps(df)
    for f in schema.fields:
        df = transform_column(df, f.name, f.dataType, table)
    return df

class TableProcessor:
    def __init__(self, table: str, config: Config, schema_mgr: SchemaManager, storage: StorageHandler) -> None:
        self.table = table
        self.config = config
        self.schema_mgr = schema_mgr
        self.storage = storage

    def load_raw_data(self) -> DataFrame:
        spark = self.config.spark()
        raw_name = self.table.replace("_", "")
        path = self.storage.path("RAW", self.config.postgres().database, raw_name)
        return spark.read.format("delta").load(path)

    def write_to_snowflake(self, df: DataFrame) -> None:
        sf = self.config.snowflake()
        opts = {
            "sfURL": sf.url,
            "sfDatabase": sf.database,
            "sfWarehouse": sf.warehouse,
            "sfRole": sf.role,
            "sfSchema": sf.schema,
            "sfUser": sf.user,
            "sfPassword": sf.password,
            "dbtable": f"STG_LCR_{self.table.upper()}",
            "column_mapping": "name",
            "on_error": "CONTINUE",
        }
        df.write.format("net.snowflake.spark.snowflake").options(**opts).mode("append").save()

    def process(self) -> None:
        df = self.load_raw_data()
        df = rename_and_add_columns(df, self.table)
        schema = self.schema_mgr.get_schema(self.table)
        validate_dataframe(df, schema, check_types=False)
        df = transform_columns(df, schema, self.table)
        validate_dataframe(df, schema)
        metadata_cols = [
            "ETL_CREATED_DATE",
            "ETL_LAST_UPDATE_DATE",
            "CREATED_BY",
            "TO_PROCESS",
            "EDW_EXTERNAL_SOURCE_SYSTEM",
        ]
        key_map = {
            "lead": "STG_LCR_LEAD_KEY",
            "lead_assignment": "STG_LCR_LEAD_ASSIGNMENT_KEY",
            "lead_xref": "STG_LCR_LEAD_XREF_KEY",
        }
        key_col = key_map[self.table]
        df = add_hash_key(df, metadata_cols + [key_col], key_col)
        df = add_metadata_columns(df, schema)
        self.write_to_snowflake(df)

class ETLPipeline:
    def __init__(self, config: Config, schema_mgr: SchemaManager, storage: StorageHandler) -> None:
        self.config = config
        self.schema_mgr = schema_mgr
        self.storage = storage

    def run(self) -> None:
        for table in Config.TABLES:
            processor = TableProcessor(table, self.config, self.schema_mgr, self.storage)
            try:
                processor.process()
                logging.info("Processed %s", table)
            except Exception as exc:
                logging.error("Failed %s: %s\n%s", table, exc, traceback.format_exc())

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    parser = argparse.ArgumentParser(description="Data sync and ingest")
    sub = parser.add_subparsers(dest="cmd", required=True)
    sub.add_parser("sync")
    sub.add_parser("ingest")
    args = parser.parse_args()

    cfg = Config()
    spark = cfg.spark()
    dbutils = DBUtilsCompat.get_dbutils()
    storage = StorageHandler(cfg.storage(), dbutils)

    if args.cmd == "sync":
        with PostgresHandler(cfg.postgres()) as pg:
            syncer = DataSync(pg, storage, spark, [
                'public."leadassignment"',
                'public."leadxref"',
                'public."lead"',
            ], cfg.postgres().database)
            syncer.sync()
    else:
        pipeline = ETLPipeline(cfg, SchemaManager(), storage)
        pipeline.run()
