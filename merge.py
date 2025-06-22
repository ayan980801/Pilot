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

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


@udf(TimestampType())
def enhanced_parse_timestamp_udf(date_str):
    if not date_str:
        return None
    if isinstance(date_str, str) and (
        len(date_str) <= 3 or not any(c.isdigit() for c in date_str)
    ):
        return None
    try:
        parsed_date = dateutil.parser.parse(str(date_str), fuzzy=False)
        ny_timezone = pytz.timezone(TIMEZONE)
        if parsed_date.tzinfo is None:
            parsed_date = ny_timezone.localize(parsed_date)
        else:
            parsed_date = parsed_date.astimezone(ny_timezone)
        current_datetime = datetime.now(ny_timezone)
        if parsed_date > current_datetime:
            return current_datetime
        return parsed_date
    except Exception:
        return None


@udf(DateType())
def enhanced_parse_date_udf(date_str):
    if not date_str:
        return None
    if isinstance(date_str, str) and (
        len(date_str) <= 3 or not any(c.isdigit() for c in date_str)
    ):
        return None
    try:
        parsed_date = dateutil.parser.parse(str(date_str), fuzzy=False).date()
        current_date = datetime.now(pytz.timezone(TIMEZONE)).date()
        if parsed_date > current_date:
            return None
        return parsed_date
    except Exception:
        return None


@udf(StringType())
def validate_json_udf(val: str) -> Optional[str]:
    if val is None:
        return None
    try:
        json.loads(val)
        return val
    except Exception:
        return None


@udf(StringType())
def sha3_512_udf(val: str) -> Optional[str]:
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
            StructField("STG_LCR_LEAD_KEY", StringType(), True),
            StructField("LEAD_GUID", StringType(), True),
            StructField("LEGACY_LEAD_ID", StringType(), True),
            StructField("INDIV_ID", StringType(), True),
            StructField("HH_ID", StringType(), True),
            StructField("ADDR_ID", StringType(), True),
            StructField("LEAD_CODE", StringType(), True),
            StructField("LEAD_TYPE_ID", DecimalType(38, 0), True),
            StructField("LEAD_TYPE", StringType(), True),
            StructField("LEAD_SOURCE", StringType(), True),
            StructField("LEAD_CREATE_DATE", TimestampType(), True),
            StructField("FIRST_NAME", StringType(), True),
            StructField("MIDDLE_NAME", StringType(), True),
            StructField("LAST_NAME", StringType(), True),
            StructField("SUFFIX", StringType(), True),
            StructField("BIRTH_DATE", StringType(), True),
            StructField("AGE", DecimalType(38, 0), True),
            StructField("SEX", StringType(), True),
            StructField("STREET_1", StringType(), True),
            StructField("STREET_2", StringType(), True),
            StructField("CITY", StringType(), True),
            StructField("STATE_ID", DecimalType(38, 0), True),
            StructField("STATE", StringType(), True),
            StructField("ZIP", StringType(), True),
            StructField("ZIP5", StringType(), True),
            StructField("COUNTY", StringType(), True),
            StructField("COUNTRY", StringType(), True),
            StructField("PHONE", StringType(), True),
            StructField("HOME_PHONE", StringType(), True),
            StructField("CELL_PHONE", StringType(), True),
            StructField("WORK_PHONE", StringType(), True),
            StructField("DO_NOT_CALL", StringType(), True),
            StructField("CALLER_ID", StringType(), True),
            StructField("EMAIL", StringType(), True),
            StructField("DYNAMIC_LEAD", StringType(), True),
            StructField("PROSPECT_ID", StringType(), True),
            StructField("EXT_PARTNER_ID", StringType(), True),
            StructField("CHANNEL_ID", DecimalType(38, 0), True),
            StructField("CHANNEL", StringType(), True),
            StructField("OPT_SOURCE_ID", StringType(), True),
            StructField("SOURCE_ID", DecimalType(38, 0), True),
            StructField("SUB_SOURCE_ID", BooleanType(), True),
            StructField("SOURCE_OF_REFERRAL", StringType(), True),
            StructField("DIVISION", StringType(), True),
            StructField("LEAD_SUB_SOURCE", StringType(), True),
            StructField("LEAD_SUB_SOURCE_ID", StringType(), True),
            StructField("LENDER", StringType(), True),
            StructField("LOAN_AMOUNT", StringType(), True),
            StructField("LOAN_DATE", DateType(), True),
            StructField("DIABETES", StringType(), True),
            StructField("HEALTH_PROBLEMS", StringType(), True),
            StructField("HEART_PROBLEMS", StringType(), True),
            StructField("HEIGHT", StringType(), True),
            StructField("HIGH_BP_CHOL", StringType(), True),
            StructField("IS_INSURED", StringType(), True),
            StructField("SMOKER", StringType(), True),
            StructField("OCCUPATION", StringType(), True),
            StructField("SPOUSE", StringType(), True),
            StructField("COBORROWER_AGE", DoubleType(), True),
            StructField("COBORROWER_BIRTH_DATE", TimestampType(), True),
            StructField("COBORROWER_HEIGHT", StringType(), True),
            StructField("COBORROWER_ON_MORTGAGE", StringType(), True),
            StructField("COBORROWER_NAME", StringType(), True),
            StructField("COBORROWER_RELATION", StringType(), True),
            StructField("COBORROWER_SEX", StringType(), True),
            StructField("COBORROWER_SMOKER", StringType(), True),
            StructField("COBORROWER_WEIGHT", StringType(), True),
            StructField("COBORROWER_OCCUPATION", StringType(), True),
            StructField("DATA_SOURCE", StringType(), True),
            StructField("LEAD_ORIGIN_URL", StringType(), True),
            StructField("MAILING_ID", StringType(), True),
            StructField("SUSPECT_CAMPAIGN_ID", DecimalType(38, 0), True),
            StructField("CONSUMER_DEBT", DoubleType(), True),
            StructField("MORTGAGE_DEBT", StringType(), True),
            StructField("UTM_CAMPAIGN", StringType(), True),
            StructField("UTM_MEDIUM", StringType(), True),
            StructField("UTM_SOURCE", StringType(), True),
            StructField("REFERRING_URL", StringType(), True),
            StructField("PCS_POLICIES_ID", DecimalType(38, 0), True),
            StructField("CREATE_DATE", TimestampType(), True),
            StructField("MODIFY_DATE", TimestampType(), True),
            StructField("SOURCE_TABLE", StringType(), True),
            StructField("IS_DELETED_SOURCE", StringType(), True),
            StructField("EXP_DATE", TimestampType(), True),
            StructField("SOURCE_TYPE", StringType(), True),
            StructField("SOURCE_TYPE_ID", DecimalType(38, 0), True),
            StructField("PRODUCT_TYPE", StringType(), True),
            StructField("LEAD_ATTRIBUTES", StringType(), True),
            StructField("CUSTODY_TARGET_AUDIENCE", StringType(), True),
            StructField("SOURCE", StringType(), True),
            StructField("PRODUCT_TYPE_ID", DecimalType(38, 0), True),
            StructField("LEAD_SOURCE_ID", StringType(), True),
            StructField("ORIGIN_SYSTEM_ID", StringType(), True),
            StructField("ORIGIN_SYSTEM", StringType(), True),
            StructField("ORIGIN_SYSTEM_ORIG", StringType(), True),
            StructField("LEAD_INGESTION_METHOD", StringType(), True),
            StructField("ETL_CREATED_DATE", TimestampType(), False),
            StructField("ETL_LAST_UPDATE_DATE", TimestampType(), False),
            StructField("CREATED_BY", StringType(), False),
            StructField("TO_PROCESS", BooleanType(), False),
            StructField("EDW_EXTERNAL_SOURCE_SYSTEM", StringType(), False),
        ]),
        "lead_xref": StructType([
            StructField("STG_LCR_LEAD_XREF_KEY", StringType(), True),
            StructField("LEAD_XREF_GUID", StringType(), True),
            StructField("LEGACY_LEAD_ID", StringType(), True),
            StructField("LEAD_CODE", StringType(), True),
            StructField("LEAD_LEVEL_ID", StringType(), True),
            StructField("LEAD_LEVEL", StringType(), True),
            StructField("DATA_SOURCE_ID", StringType(), True),
            StructField("LEVEL_DATE", TimestampType(), True),
            StructField("CREATE_DATE", TimestampType(), True),
            StructField("MODIFY_DATE", TimestampType(), True),
            StructField("AVAILABLE_FOR_PURCHASE_IND", StringType(), True),
            StructField("IS_DELETED_SOURCE", StringType(), True),
            StructField("LEAD_LEVEL_ALIAS", StringType(), True),
            StructField("ETL_CREATED_DATE", TimestampType(), False),
            StructField("ETL_LAST_UPDATE_DATE", TimestampType(), False),
            StructField("CREATED_BY", StringType(), False),
            StructField("TO_PROCESS", BooleanType(), False),
            StructField("EDW_EXTERNAL_SOURCE_SYSTEM", StringType(), False),
        ]),
        "lead_assignment": StructType([
            StructField("STG_LCR_LEAD_ASSIGNMENT_KEY", StringType(), True),
            StructField("LEAD_ASSIGNMENT_GUID", StringType(), True),
            StructField("LEAD_XREF_GUID", StringType(), True),
            StructField("AGENT_CODE", StringType(), True),
            StructField("PURCHASE_DATE", TimestampType(), True),
            StructField("PURCHASE_PRICE", DoubleType(), True),
            StructField("ASSIGN_DATE", TimestampType(), True),
            StructField("INACTIVE_IND", StringType(), True),
            StructField("STATUS", StringType(), True),
            StructField("AGENT_EXTUID", StringType(), True),
            StructField("ALLOCATE_IND", StringType(), True),
            StructField("COMMENTS", StringType(), True),
            StructField("SFG_DIRECT_AGENT_ID", StringType(), True),
            StructField("BASE_SHOP_OWNER_AGENT_ID", StringType(), True),
            StructField("TOTAL_UPLINE_AGENT_CODES", StringType(), True),
            StructField("UNPAID_IND", StringType(), True),
            StructField("APP_COUNT", StringType(), True),
            StructField("APP_APV", StringType(), True),
            StructField("ACTUAL_APP_COUNT", StringType(), True),
            StructField("ACTUAL_APV", StringType(), True),
            StructField("CREATE_DATE", TimestampType(), True),
            StructField("MODIFY_DATE", TimestampType(), True),
            StructField("SOURCE_TABLE", StringType(), True),
            StructField("METADATA", StringType(), True),
            StructField("STATUS_DATE", TimestampType(), True),
            StructField("IS_DELETED_SOURCE", BooleanType(), True),
            StructField("ORDER_NUMBER", StringType(), True),
            StructField("LEAD_STATUS_ID", StringType(), True),
            StructField("LEAD_STATUS", StringType(), True),
            StructField("HQ_PURCHASE_AMOUNT", DoubleType(), True),
            StructField("LEAD_ORDER_SYSTEM_ID", StringType(), True),
            StructField("LEAD_ORDER_SYSTEM", StringType(), True),
            StructField("ORDER_SYSTEM_ID", StringType(), True),
            StructField("ORDER_SYSTEM", StringType(), True),
            StructField("ORDER_SYSTEM_ORIG", StringType(), True),
            StructField("EXCLUSIVITY_END_DATE", TimestampType(), True),
            StructField("ETL_CREATED_DATE", TimestampType(), False),
            StructField("ETL_LAST_UPDATE_DATE", TimestampType(), False),
            StructField("CREATED_BY", StringType(), False),
            StructField("TO_PROCESS", BooleanType(), False),
            StructField("EDW_EXTERNAL_SOURCE_SYSTEM", StringType(), False),
        ]),
    }

    COLUMN_MAPPINGS = {
        "lead": {
            "leadguid": "LEAD_GUID",
            "legacyleadid": "LEGACY_LEAD_ID",
            "individ": "INDIV_ID",
            "hhid": "HH_ID",
            "addrid": "ADDR_ID",
            "leadcode": "LEAD_CODE",
            "leadtypeid": "LEAD_TYPE_ID",
            "leadtype": "LEAD_TYPE",
            "leadsource": "LEAD_SOURCE",
            "leadcreatedate": "LEAD_CREATE_DATE",
            "firstname": "FIRST_NAME",
            "middlename": "MIDDLE_NAME",
            "lastname": "LAST_NAME",
            "suffix": "SUFFIX",
            "birthdate": "BIRTH_DATE",
            "age": "AGE",
            "sex": "SEX",
            "street1": "STREET_1",
            "street2": "STREET_2",
            "city": "CITY",
            "stateid": "STATE_ID",
            "state": "STATE",
            "zip": "ZIP",
            "zip5": "ZIP5",
            "county": "COUNTY",
            "country": "COUNTRY",
            "phone": "PHONE",
            "homephone": "HOME_PHONE",
            "cellphone": "CELL_PHONE",
            "workphone": "WORK_PHONE",
            "donotcall": "DO_NOT_CALL",
            "callerid": "CALLER_ID",
            "email": "EMAIL",
            "dynamiclead": "DYNAMIC_LEAD",
            "prospectid": "PROSPECT_ID",
            "extpartnerid": "EXT_PARTNER_ID",
            "channelid": "CHANNEL_ID",
            "channel": "CHANNEL",
            "optsourceid": "OPT_SOURCE_ID",
            "sourceid": "SOURCE_ID",
            "subsourceid": "SUB_SOURCE_ID",
            "sourceofreferral": "SOURCE_OF_REFERRAL",
            "division": "DIVISION",
            "leadsubsource": "LEAD_SUB_SOURCE",
            "leadsubsourceid": "LEAD_SUB_SOURCE_ID",
            "lender": "LENDER",
            "loanamount": "LOAN_AMOUNT",
            "loandate": "LOAN_DATE",
            "diabetes": "DIABETES",
            "healthproblems": "HEALTH_PROBLEMS",
            "heartproblems": "HEART_PROBLEMS",
            "height": "HEIGHT",
            "highbpchol": "HIGH_BP_CHOL",
            "isinsured": "IS_INSURED",
            "smoker": "SMOKER",
            "occupation": "OCCUPATION",
            "spouse": "SPOUSE",
            "coborrowerage": "COBORROWER_AGE",
            "coborrowerbirthdate": "COBORROWER_BIRTH_DATE",
            "coborrowerheight": "COBORROWER_HEIGHT",
            "coborroweronmortgage": "COBORROWER_ON_MORTGAGE",
            "coborrowername": "COBORROWER_NAME",
            "coborrowerrelation": "COBORROWER_RELATION",
            "coborrowersex": "COBORROWER_SEX",
            "coborrowersmoker": "COBORROWER_SMOKER",
            "coborrowerweight": "COBORROWER_WEIGHT",
            "coborroweroccupation": "COBORROWER_OCCUPATION",
            "datasource": "DATA_SOURCE",
            "leadoriginurl": "LEAD_ORIGIN_URL",
            "mailingid": "MAILING_ID",
            "suspectcampaignid": "SUSPECT_CAMPAIGN_ID",
            "consumerdebt": "CONSUMER_DEBT",
            "mortgagedebt": "MORTGAGE_DEBT",
            "utmcampaign": "UTM_CAMPAIGN",
            "utmmedium": "UTM_MEDIUM",
            "utmsource": "UTM_SOURCE",
            "referringurl": "REFERRING_URL",
            "pcspoliciesid": "PCS_POLICIES_ID",
            "createdate": "CREATE_DATE",
            "modifydate": "MODIFY_DATE",
            "sourcetable": "SOURCE_TABLE",
            "isdeletedsource": "IS_DELETED_SOURCE",
            "expdate": "EXP_DATE",
            "sourcetype": "SOURCE_TYPE",
            "sourcetypeid": "SOURCE_TYPE_ID",
            "producttype": "PRODUCT_TYPE",
            "leadattributes": "LEAD_ATTRIBUTES",
            "custodytargetaudience": "CUSTODY_TARGET_AUDIENCE",
            "source": "SOURCE",
            "producttypeid": "PRODUCT_TYPE_ID",
            "leadsourceid": "LEAD_SOURCE_ID",
            "originsystemid": "ORIGIN_SYSTEM_ID",
            "originsystem": "ORIGIN_SYSTEM",
            "originsystem_orig": "ORIGIN_SYSTEM_ORIG",
            "leadingestionmethod": "LEAD_INGESTION_METHOD",
        },
        "lead_xref": {
            "leadxrefguid": "LEAD_XREF_GUID",
            "legacyleadid": "LEGACY_LEAD_ID",
            "leadcode": "LEAD_CODE",
            "leadlevelid": "LEAD_LEVEL_ID",
            "leadlevel": "LEAD_LEVEL",
            "datasourceid": "DATA_SOURCE_ID",
            "leveldate": "LEVEL_DATE",
            "createdate": "CREATE_DATE",
            "modifydate": "MODIFY_DATE",
            "availableforpurchaseind": "AVAILABLE_FOR_PURCHASE_IND",
            "isdeletedsource": "IS_DELETED_SOURCE",
            "leadlevelalias": "LEAD_LEVEL_ALIAS",
        },
        "lead_assignment": {
            "leadassignmentguid": "LEAD_ASSIGNMENT_GUID",
            "leadxrefguid": "LEAD_XREF_GUID",
            "agentcode": "AGENT_CODE",
            "purchasedate": "PURCHASE_DATE",
            "purchaseprice": "PURCHASE_PRICE",
            "assigndate": "ASSIGN_DATE",
            "inactiveind": "INACTIVE_IND",
            "status": "STATUS",
            "agentextuid": "AGENT_EXTUID",
            "allocateind": "ALLOCATE_IND",
            "comments": "COMMENTS",
            "sfgdirectagentid": "SFG_DIRECT_AGENT_ID",
            "baseshopowneragentid": "BASE_SHOP_OWNER_AGENT_ID",
            "totaluplineagentcodes": "TOTAL_UPLINE_AGENT_CODES",
            "unpaidind": "UNPAID_IND",
            "appcount": "APP_COUNT",
            "appapv": "APP_APV",
            "actualappcount": "ACTUAL_APP_COUNT",
            "actualapv": "ACTUAL_APV",
            "createdate": "CREATE_DATE",
            "modifydate": "MODIFY_DATE",
            "sourcetable": "SOURCE_TABLE",
            "metadata": "METADATA",
            "statusdate": "STATUS_DATE",
            "isdeletedsource": "IS_DELETED_SOURCE",
            "ordernumber": "ORDER_NUMBER",
            "leadstatusid": "LEAD_STATUS_ID",
            "leadstatus": "LEAD_STATUS",
            "hqpurchaseamount": "HQ_PURCHASE_AMOUNT",
            "leadordersystemid": "LEAD_ORDER_SYSTEM_ID",
            "leadordersystem": "LEAD_ORDER_SYSTEM",
            "ordersystemid": "ORDER_SYSTEM_ID",
            "ordersystem": "ORDER_SYSTEM",
            "ordersystem_orig": "ORDER_SYSTEM_ORIG",
            "exclusivityenddate": "EXCLUSIVITY_END_DATE",
        },
    }

    JSON_COLUMNS = {
        "lead_assignment": ["METADATA"],
        "lead": ["LEAD_ATTRIBUTES"],
        "lead_xref": [],
    }

    BOOLEAN_STRING_COLUMNS = {
        "IS_DELETED_SOURCE",
    }

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
    timestamp_cols = [
        field.name
        for field in df.schema.fields
        if isinstance(field.dataType, TimestampType)
    ]
    for ts_col in timestamp_cols:
        df = df.withColumn(
            ts_col,
            when(
                col(ts_col).isNull()
                | col(ts_col).cast("string").rlike("^[A-Za-z]{1,3}$")
                | (length(col(ts_col).cast("string")) <= 3)
                | (~col(ts_col).cast("string").rlike(".*\\d+.*"))
                | (year(col(ts_col)) < 1900)
                | (year(col(ts_col)) > year(current_timestamp()) + 1),
                lit(None),
            ).otherwise(col(ts_col)),
        )
        if ts_col.startswith("ETL_"):
            df = df.withColumn(
                ts_col,
                coalesce(col(ts_col), current_timestamp())
            )
    return df

def validate_dataframe(
    df: DataFrame, target_schema: StructType, check_types: bool = True
) -> None:
    errors = []
    for field in target_schema.fields:
        col_name = field.name
        col_type = field.dataType
        if col_name not in df.columns:
            error_msg = f"Column {col_name} is missing from the DataFrame"
            errors.append(error_msg)
            logger.error(error_msg)
        elif check_types and not isinstance(df.schema[col_name].dataType, type(col_type)):
            error_msg = (
                f"Column {col_name} has type {df.schema[col_name].dataType}, "
                f"but should be {col_type}"
            )
            errors.append(error_msg)
            logger.error(error_msg)
    if errors:
        raise ValueError(
            "DataFrame validation failed with errors:\n" + "\n".join(errors)
        )

def transform_column(df: DataFrame, name: str, dtype, table: str) -> DataFrame:
    if table in SchemaManager.JSON_COLUMNS and name in SchemaManager.get_json_columns(table):
        return df.withColumn(name, validate_json_udf(col(name).cast(StringType())))
    if isinstance(dtype, TimestampType):
        df = df.withColumn(
            name,
            when(
                (col(name).cast("string").rlike("^[A-Za-z]{1,3}$"))
                | (length(col(name).cast("string")) <= 3)
                | (~col(name).cast("string").rlike(".*\\d+.*")),
                lit(None),
            ).otherwise(col(name))
        )
        return df.withColumn(
            name,
            when(col(name).isNull(), None).otherwise(
                coalesce(
                    to_timestamp(col(name)),
                    enhanced_parse_timestamp_udf(col(name)),
                )
            ),
        )
    if isinstance(dtype, DateType):
        return df.withColumn(
            name,
            when(col(name).isNull(), None)
            .when(col(name).rlike(r"^\d{4}-\d{2}-\d{2}$"), to_date(col(name)))
            .otherwise(enhanced_parse_date_udf(col(name)))
        )
    if isinstance(dtype, DecimalType):
        precision, scale = dtype.precision, dtype.scale
        cleaned = trim(regexp_replace(col(name), ",", ""))
        return df.withColumn(
            name,
            when(
                col(name).isNull()
                | (cleaned == "")
                | (~cleaned.rlike(NUMERIC_REGEX)),
                None,
            ).otherwise(cleaned.cast(DecimalType(precision, scale)))
        )
    if isinstance(dtype, DoubleType):
        cleaned = trim(regexp_replace(col(name), ",", ""))
        return df.withColumn(
            name,
            when(
                col(name).isNull()
                | (cleaned == "")
                | (~cleaned.rlike(NUMERIC_REGEX)),
                None,
            ).otherwise(cleaned.cast(DoubleType()))
        )
    if isinstance(dtype, BooleanType):
        return df.withColumn(
            name,
            when(lower(col(name)).isin("true", "1", "yes"), lit(True))
            .when(lower(col(name)).isin("false", "0", "no"), lit(False))
            .when(col(name).isNull(), lit(None))
            .otherwise(
                when(
                    length(col(name)) == 1,
                    when(lower(col(name)) == "t", lit(True))
                    .when(lower(col(name)) == "f", lit(False))
                    .otherwise(lit(None)),
                ).otherwise(lit(None))
            ),
        )
    if isinstance(dtype, StringType) and SchemaManager.is_boolean_string_column(name):
        return df.withColumn(
            name,
            when(lower(col(name).cast("string")).isin("true", "1", "yes", "t"), lit("TRUE"))
            .when(lower(col(name).cast("string")).isin("false", "0", "no", "f"), lit("FALSE"))
            .when(col(name).isNull(), lit(None))
            .otherwise(col(name).cast(StringType()))
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
    return df.withColumn(key_col, sha3_512_udf(concatenated))

def transform_columns(df: DataFrame, schema: StructType, table: str) -> DataFrame:
    df = clean_invalid_timestamps(df)
    for f in schema.fields:
        df = transform_column(df, f.name, f.dataType, table)
    return df

class TableProcessor:
    def __init__(
        self,
        table: str,
        write_mode: str,
        historical_load: bool,
        config: Config,
        schema_mgr: SchemaManager,
        storage: StorageHandler,
    ) -> None:
        self.table = table
        self.write_mode = write_mode
        self.historical_load = historical_load
        self.config = config
        self.schema_mgr = schema_mgr
        self.storage = storage

    def load_raw_data(self) -> DataFrame:
        spark = self.config.spark()
        raw_name = self.table.replace("_", "")
        path = self.storage.path("RAW", self.config.postgres().database, raw_name)
        if self.table == "lead_assignment":
            return (
                spark.read.format("delta")
                .option("header", "true")
                .option("inferSchema", "false")
                .option("multiLine", "true")
                .option("mode", "PERMISSIVE")
                .load(path)
            )
        return (
            spark.read.format("delta")
            .option("header", "true")
            .option("inferSchema", "false")
            .load(path)
        )

    def snowflake_table_exists(self) -> bool:
        spark = self.config.spark()
        sf_cfg = self.config.snowflake()
        query = (
            f"SELECT 1 FROM information_schema.tables WHERE table_schema = '{sf_cfg.schema}'"
            f" AND table_name = 'STG_LCR_{self.table.upper()}'"
        )
        opts = {
            "sfURL": sf_cfg.url,
            "sfDatabase": sf_cfg.database,
            "sfWarehouse": sf_cfg.warehouse,
            "sfRole": sf_cfg.role,
            "sfSchema": sf_cfg.schema,
            "sfUser": sf_cfg.user,
            "sfPassword": sf_cfg.password,
            "query": query,
        }
        try:
            df = spark.read.format("net.snowflake.spark.snowflake").options(**opts).load()
            return len(df.take(1)) > 0
        except Exception as exc:
            logger.error("Failed to check table existence: %s", exc)
            return False

    def create_checkpoint(self) -> None:
        spark = self.config.spark()
        path = self.storage.path("METADATA", self.config.postgres().database, f"checkpoint_{self.table}.txt")
        spark.createDataFrame([(datetime.now().isoformat(),)], ["ts"]).coalesce(1).write.mode("overwrite").text(path)

    def write_to_snowflake(self, df: DataFrame, options: dict, mode: str = "append") -> None:
        for attempt in range(3):
            try:
                df.write.format("net.snowflake.spark.snowflake").options(**options).mode(mode).save()
                return
            except Exception as exc:
                if attempt == 2:
                    raise
                logger.warning("Snowflake write failed, retrying... %s", exc)
                time.sleep(5)

    def process(self) -> None:
        spark = self.config.spark()
        sf = self.config.snowflake()
        try:
            df = self.load_raw_data()
            logger.info("Loaded data for %s", self.table)
            df = rename_and_add_columns(df, self.table)
            validate_dataframe(df, self.schema_mgr.get_schema(self.table), check_types=False)
            target_schema = self.schema_mgr.get_schema(self.table)
            df = transform_columns(df, target_schema, self.table)
            validate_dataframe(df, target_schema)
            if self.table == "lead_assignment":
                date_columns = [
                    "PURCHASE_DATE",
                    "ASSIGN_DATE",
                    "CREATE_DATE",
                    "MODIFY_DATE",
                    "STATUS_DATE",
                    "EXCLUSIVITY_END_DATE",
                ]
                current_date = current_timestamp()
                for date_col in date_columns:
                    df = df.withColumn(
                        date_col,
                        when(col(date_col) > current_date, current_date).otherwise(col(date_col))
                    )
                df = df.withColumn(
                    "METADATA",
                    when(col("METADATA").isNull(), lit(None)).otherwise(col("METADATA").cast(StringType()))
                )
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
            df = add_metadata_columns(df, target_schema)
            target_cols = [field.name for field in target_schema.fields]
            df = df.select(*target_cols)
            df = clean_invalid_timestamps(df)
            timestamp_cols = [
                f.name for f in target_schema.fields if isinstance(f.dataType, TimestampType)
            ]
            for ts in timestamp_cols:
                df = df.withColumn(
                    ts,
                    when(
                        col(ts).isNull()
                        | regexp_replace(col(ts).cast("string"), "[0-9\\-:. ]", "").rlike(".+"),
                        current_timestamp() if ts.startswith("ETL_") else lit(None),
                    ).otherwise(col(ts))
                )
            if not self.snowflake_table_exists():
                logger.error("Target table STG_LCR_%s does not exist in Snowflake", self.table.upper())
                return
            base_opts = {
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
            if self.write_mode == "append":
                if self.historical_load:
                    truncate_opts = {**base_opts, "truncate_table": "on"}
                    spark.createDataFrame([], target_schema).write.format("net.snowflake.spark.snowflake").options(**truncate_opts).mode("overwrite").save()
                    logger.info("Table STG_LCR_%s truncated (historical append)", self.table.upper())
                self.write_to_snowflake(df, base_opts, mode="append")
                logger.info("Successfully wrote to Snowflake for table %s", self.table)
                self.create_checkpoint()
            elif self.write_mode == "incremental_insert":
                target = spark.read.format("net.snowflake.spark.snowflake").options(**base_opts).load()
                key_col = key_map[self.table]
                df_filtered = df.join(target.select(key_col), on=key_col, how="left_anti")
                validate_dataframe(df_filtered, target_schema)
                write_opts = {**base_opts, "truncate": "true"}
                self.write_to_snowflake(df_filtered, write_opts, mode="append")
                logger.info("Appended records to table STG_LCR_%s", self.table.upper())
                self.create_checkpoint()
            else:
                raise ValueError(f"Invalid write mode: {self.write_mode}")
            logger.info("Completed processing for table: %s", self.table)
        except Exception as exc:
            logger.error("Unexpected error processing table %s: %s", self.table, exc)
            logger.error(traceback.format_exc())
            raise

class ETLPipeline:
    def __init__(
        self,
        config: Config,
        schema_mgr: SchemaManager,
        storage: StorageHandler,
        write_mode: str,
        historical_load: bool,
    ) -> None:
        self.config = config
        self.schema_mgr = schema_mgr
        self.storage = storage
        self.write_mode = write_mode
        self.historical_load = historical_load

    def run(self) -> None:
        for table in Config.TABLES:
            processor = TableProcessor(
                table,
                self.write_mode,
                self.historical_load,
                self.config,
                self.schema_mgr,
                self.storage,
            )
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
    ingest_parser = sub.add_parser("ingest")
    ingest_parser.add_argument(
        "--write-mode",
        choices=["append", "incremental_insert"],
        default="append",
    )
    ingest_parser.add_argument("--historical-load", action="store_true")
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
        pipeline = ETLPipeline(
            cfg,
            SchemaManager(),
            storage,
            args.write_mode,
            args.historical_load,
        )
        pipeline.run()
