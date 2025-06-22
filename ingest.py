import os
import time
import json
import pytz
import logging
import traceback
from datetime import datetime
import dateutil.parser

from typing import Dict, List, Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    col, current_timestamp, lit, udf, to_date, to_timestamp, when, lower,
    coalesce, length, regexp_replace, year, trim, concat_ws
)
import hashlib
from pyspark.sql.types import (
    BooleanType, DateType, DecimalType, DoubleType, StringType,
    StructField, StructType, TimestampType
)

# -----------------------------------------------------------------------------
# Logging configuration
# -----------------------------------------------------------------------------
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# -----------------------------------------------------------------------------
# UDFs: Must be global (not in class) for Spark serialization!
# -----------------------------------------------------------------------------
TIMEZONE = "America/New_York"
NUMERIC_REGEX = r"^-?\d+(\.\d+)?$"

@udf(TimestampType())
def enhanced_parse_timestamp_udf(date_str):
    if not date_str:
        return None
    if isinstance(date_str, str) and (len(date_str) <= 3 or not any(c.isdigit() for c in date_str)):
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
    if isinstance(date_str, str) and (len(date_str) <= 3 or not any(c.isdigit() for c in date_str)):
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

# -----------------------------------------------------------------------------
# ConfigManager
# -----------------------------------------------------------------------------
class ConfigManager:
    """Handles all configuration and secrets."""
    TIMEZONE = TIMEZONE
    RAW_BASE_PATH = "abfss://dataarchitecture@quilitydatabricks.dfs.core.windows.net/RAW/LeadCustodyRepository"
    METADATA_BASE_PATH = "dbfs:/FileStore/DataProduct/DataArchitecture/Pipelines/LCR_EDW/Metadata"
    TABLES = ["lead_assignment", "lead_xref", "lead"]
    TABLE_PROCESSING_CONFIG = {
        "lead": True,
        "lead_xref": True,
        "lead_assignment": True,
    }

    @classmethod
    def get_secret(cls, scope: str, key: str) -> str:
        try:
            from pyspark.dbutils import DBUtils  # type: ignore
            dbutils = DBUtils(cls.get_spark())
            return dbutils.secrets.get(scope=scope, key=key)
        except Exception:
            env_key = f"{scope}_{key}".upper()
            value = os.getenv(env_key)
            if value is None:
                raise ValueError(f"Missing secret for {env_key}")
            return value

    @classmethod
    def get_sf_config_stg(cls) -> dict:
        return {
            "sfURL": "hmkovlx-nu26765.snowflakecomputing.com",
            "sfDatabase": "DEV",
            "sfWarehouse": "INTEGRATION_COMPUTE_WH",
            "sfRole": "SG-SNOWFLAKE-DEVELOPERS",
            "sfSchema": "QUILITY_EDW_STAGE",
            "sfUser": cls.get_secret("key-vault-secret", "DataProduct-SF-EDW-User"),
            "sfPassword": cls.get_secret("key-vault-secret", "DataProduct-SF-EDW-Pass"),
            "on_error": "CONTINUE",
        }

    @staticmethod
    def get_spark():
        if not hasattr(ConfigManager, "_spark"):
            ConfigManager._spark = SparkSession.builder.getOrCreate()
        return ConfigManager._spark

# -----------------------------------------------------------------------------
# SchemaManager
# -----------------------------------------------------------------------------
class SchemaManager:
    """Stores all schema definitions and column mappings."""

    TABLE_SCHEMAS: Dict[str, StructType] = {
        # ... (the full schema definitions for lead, lead_xref, lead_assignment) ...
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

    COLUMN_MAPPINGS: Dict[str, Dict[str, str]] = {
        # ... All mapping dicts, verbatim, for each table ...
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
            "ordersystemorig": "ORDER_SYSTEM_ORIG",
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
    def get_schema(cls, table_name: str):
        return cls.TABLE_SCHEMAS[table_name]

    @classmethod
    def get_column_mapping(cls, table_name: str):
        return cls.COLUMN_MAPPINGS[table_name]

    @classmethod
    def get_json_columns(cls, table_name: str):
        return cls.JSON_COLUMNS.get(table_name, [])

    @classmethod
    def is_boolean_string_column(cls, col_name: str):
        return col_name in cls.BOOLEAN_STRING_COLUMNS

# -----------------------------------------------------------------------------
# ETLUtils (All functions are global and use global UDFs)
# -----------------------------------------------------------------------------

def clean_invalid_timestamps(df: DataFrame) -> DataFrame:
    timestamp_cols = [
        field.name for field in df.schema.fields
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
            ).otherwise(col(ts_col))
        )
        if ts_col.startswith("ETL_"):
            df = df.withColumn(
                ts_col,
                coalesce(col(ts_col), current_timestamp())
            )
    return df

def validate_dataframe(df: DataFrame, target_schema: StructType, check_types: bool = True) -> None:
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

def transform_column(df: DataFrame, col_name: str, col_type, table_name: str) -> DataFrame:
    json_columns = SchemaManager.get_json_columns(table_name)
    boolean_string_columns = SchemaManager.BOOLEAN_STRING_COLUMNS
    if table_name in SchemaManager.JSON_COLUMNS and col_name in json_columns:
        logger.info(
            f"Applying JSON validation for column {col_name} in table {table_name}"
        )
        return df.withColumn(
            col_name,
            validate_json_udf(col(col_name).cast(StringType()))
        )
    if isinstance(col_type, TimestampType):
        df = df.withColumn(
            col_name,
            when(
                (col(col_name).cast("string").rlike("^[A-Za-z]{1,3}$")) |
                (length(col(col_name).cast("string")) <= 3) |
                (~col(col_name).cast("string").rlike(".*\\d+.*")),
                lit(None)
            ).otherwise(col(col_name))
        )
        return df.withColumn(
            col_name,
            when(col(col_name).isNull(), None).otherwise(
                coalesce(
                    to_timestamp(col(col_name)),
                    enhanced_parse_timestamp_udf(col(col_name)),
                )
            ),
        )
    elif isinstance(col_type, DateType):
        return df.withColumn(
            col_name,
            when(
                col(col_name).isNull(), None
            ).when(
                col(col_name).rlike(r"^\d{4}-\d{2}-\d{2}$"),
                to_date(col(col_name))
            ).otherwise(
                enhanced_parse_date_udf(col(col_name))
            )
        )
    elif isinstance(col_type, DecimalType):
        precision, scale = col_type.precision, col_type.scale
        cleaned = trim(regexp_replace(col(col_name), ",", ""))
        return df.withColumn(
            col_name,
            when(
                col(col_name).isNull() |
                (cleaned == "") |
                (~cleaned.rlike(NUMERIC_REGEX)),
                None
            ).otherwise(cleaned.cast(DecimalType(precision, scale)))
        )
    elif isinstance(col_type, DoubleType):
        cleaned = trim(regexp_replace(col(col_name), ",", ""))
        return df.withColumn(
            col_name,
            when(
                col(col_name).isNull() |
                (cleaned == "") |
                (~cleaned.rlike(NUMERIC_REGEX)),
                None
            ).otherwise(cleaned.cast(DoubleType()))
        )
    elif isinstance(col_type, BooleanType):
        return df.withColumn(
            col_name,
            when(lower(col(col_name)).isin("true", "1", "yes"), lit(True))
            .when(lower(col(col_name)).isin("false", "0", "no"), lit(False))
            .when(col(col_name).isNull(), lit(None))
            .otherwise(
                when(
                    length(col(col_name)) == 1,
                    when(lower(col(col_name)) == "t", lit(True))
                    .when(lower(col(col_name)) == "f", lit(False))
                    .otherwise(lit(None))
                ).otherwise(lit(None))
            ),
        )
    elif isinstance(col_type, StringType) and col_name in boolean_string_columns:
        return df.withColumn(
            col_name,
            when(lower(col(col_name).cast("string")).isin("true", "1", "yes", "t"), lit("TRUE"))
            .when(lower(col(col_name).cast("string")).isin("false", "0", "no", "f"), lit("FALSE"))
            .when(col(col_name).isNull(), lit(None))
            .otherwise(col(col_name).cast(StringType()))
        )
    else:
        return df.withColumn(col_name, col(col_name).cast(StringType()))

def add_metadata_columns(df: DataFrame, target_schema: StructType) -> DataFrame:
    etl_timestamp = current_timestamp()
    metadata_defaults = {
        "ETL_CREATED_DATE": etl_timestamp,
        "ETL_LAST_UPDATE_DATE": etl_timestamp,
        "CREATED_BY": lit("ETL_PROCESS"),
        "TO_PROCESS": lit(True),
        "EDW_EXTERNAL_SOURCE_SYSTEM": lit("LeadCustodyRepository"),
    }
    for col_name, default_value in metadata_defaults.items():
        df = df.withColumn(
            col_name,
            default_value.cast(target_schema[col_name].dataType)
        )
    return df

def rename_and_add_columns(df: DataFrame, table_name: str) -> DataFrame:
    df_columns_lower = {column.lower(): column for column in df.columns}
    column_mappings = SchemaManager.get_column_mapping(table_name)
    for old_col, new_col in column_mappings.items():
        if old_col.lower() in df_columns_lower:
            original_col = df_columns_lower[old_col.lower()]
            df = df.withColumnRenamed(original_col, new_col)
    target_schema: StructType = SchemaManager.get_schema(table_name)
    missing_columns = set(field.name for field in target_schema.fields) - set(df.columns)
    for col_name in missing_columns:
        df = df.withColumn(col_name, lit(None).cast(target_schema[col_name].dataType))
    return df

def add_hash_key(df: DataFrame, metadata_cols: List[str], key_col: str) -> DataFrame:
    non_meta_cols = [c for c in df.columns if c not in metadata_cols and c != key_col]
    concatenated = concat_ws("||", *[col(c).cast("string") for c in non_meta_cols])
    return df.withColumn(key_col, sha3_512_udf(concatenated))

def transform_columns(df: DataFrame, target_schema: StructType, table_name: str) -> DataFrame:
    df = clean_invalid_timestamps(df)
    for field in target_schema.fields:
        df = transform_column(df, field.name, field.dataType, table_name)
    return df

# -----------------------------------------------------------------------------
# TableProcessor (No SparkSession or logger in instance variables)
# -----------------------------------------------------------------------------
class TableProcessor:
    def __init__(self, table_name: str, write_mode: str, historical_load: bool, config: ConfigManager, schema: SchemaManager):
        self.table_name = table_name
        self.write_mode = write_mode
        self.historical_load = historical_load
        self.config = config
        self.schema = schema

    def load_raw_data(self) -> DataFrame:
        spark = self.config.get_spark()
        raw_table_name = self.table_name.replace("_", "")
        raw_dataset_path = f"{self.config.RAW_BASE_PATH}/{raw_table_name}"
        if self.table_name == "lead_assignment":
            df = (
                spark.read.format("delta")
                .option("header", "true")
                .option("inferSchema", "false")
                .option("multiLine", "true")
                .option("mode", "PERMISSIVE")
                .load(raw_dataset_path)
            )
            return df
        else:
            return (
                spark.read.format("delta")
                .option("header", "true")
                .option("inferSchema", "false")
                .load(raw_dataset_path)
            )

    def snowflake_table_exists(self) -> bool:
        spark = self.config.get_spark()
        sf_config_stg = self.config.get_sf_config_stg()
        query = (
            f"SELECT 1 FROM information_schema.tables WHERE table_schema = '{sf_config_stg['sfSchema']}'"
            f" AND table_name = 'STG_LCR_{self.table_name.upper()}'"
        )
        try:
            df = spark.read.format("net.snowflake.spark.snowflake").options(**sf_config_stg).option("query", query).load()
            return len(df.take(1)) > 0
        except Exception as e:
            logger.error(f"Failed to check table existence: {e}")
            return False

    def create_checkpoint(self) -> None:
        spark = self.config.get_spark()
        path = f"{self.config.METADATA_BASE_PATH}/checkpoint_{self.table_name}.txt"
        spark.createDataFrame([(datetime.now().isoformat(),)], ["ts"]).coalesce(1).write.mode("overwrite").text(path)

    def process(self) -> None:
        spark = self.config.get_spark()
        sf_config_stg = self.config.get_sf_config_stg()
        try:
            raw_df = self.load_raw_data()
            logger.info(f"Loaded data for {self.table_name}")
            raw_df = rename_and_add_columns(raw_df, self.table_name)
            validate_dataframe(raw_df, self.schema.get_schema(self.table_name), check_types=False)
            target_schema = self.schema.get_schema(self.table_name)
            raw_df = transform_columns(raw_df, target_schema, self.table_name)
            validate_dataframe(raw_df, target_schema)
            if self.table_name == "lead_assignment":
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
                    raw_df = raw_df.withColumn(
                        date_col,
                        when(col(date_col) > current_date, current_date).otherwise(col(date_col))
                    )
                raw_df = raw_df.withColumn(
                    "METADATA",
                    when(col("METADATA").isNull(), lit(None)).otherwise(col("METADATA").cast(StringType()))
                )
                logger.info("Applied lead assignment specific handling")
            metadata_cols = [
                "ETL_CREATED_DATE",
                "ETL_LAST_UPDATE_DATE",
                "CREATED_BY",
                "TO_PROCESS",
                "EDW_EXTERNAL_SOURCE_SYSTEM",
            ]
            key_col_mapping = {
                "lead": "STG_LCR_LEAD_KEY",
                "lead_assignment": "STG_LCR_LEAD_ASSIGNMENT_KEY",
                "lead_xref": "STG_LCR_LEAD_XREF_KEY",
            }
            key_col = key_col_mapping[self.table_name]
            raw_df = add_hash_key(raw_df, metadata_cols + [key_col], key_col)
            raw_df = add_metadata_columns(raw_df, target_schema)
            target_columns = [field.name for field in target_schema.fields]
            raw_df = raw_df.select(*target_columns)
            raw_df = clean_invalid_timestamps(raw_df)
            timestamp_cols = [
                field.name
                for field in target_schema.fields
                if isinstance(field.dataType, TimestampType)
            ]
            for ts_col in timestamp_cols:
                raw_df = raw_df.withColumn(
                    ts_col,
                    when(
                        col(ts_col).isNull() |
                        regexp_replace(col(ts_col).cast("string"), "[0-9\\-:. ]", "").rlike(".+"),
                        current_timestamp() if ts_col.startswith("ETL_") else lit(None)
                    ).otherwise(col(ts_col))
                )
            logger.info(f"DataFrame finalization completed for table {self.table_name}")
            if not self.snowflake_table_exists():
                logger.error(f"Target table STG_LCR_{self.table_name.upper()} does not exist in Snowflake")
                return
            if self.write_mode == "append":
                if self.historical_load:
                    truncate_options = {
                        **sf_config_stg,
                        "dbtable": f"STG_LCR_{self.table_name.upper()}",
                        "truncate_table": "on"
                    }
                    spark.createDataFrame([], target_schema) \
                        .write.format("net.snowflake.spark.snowflake") \
                        .options(**truncate_options) \
                        .mode("overwrite") \
                        .save()
                    logger.info(
                        f"Table STG_LCR_{self.table_name.upper()} truncated (historical append)"
                    )
                write_options = {
                    **sf_config_stg,
                    "dbtable": f"STG_LCR_{self.table_name.upper()}",
                    "on_error": "CONTINUE",
                    "column_mapping": "name"
                }
                for attempt in range(3):
                    try:
                        raw_df.write.format("net.snowflake.spark.snowflake").options(**write_options).mode("append").save()
                        break
                    except Exception as w_err:
                        if attempt == 2:
                            raise
                        logger.warning(f"Snowflake write failed, retrying... {w_err}")
                        time.sleep(5)
                logger.info(f"Successfully wrote to Snowflake for table {self.table_name}")
                self.create_checkpoint()
            elif self.write_mode == "incremental_insert":
                target = spark.read.format("net.snowflake.spark.snowflake").options(**sf_config_stg).option(
                    "dbtable", f"STG_LCR_{self.table_name.upper()}"
                ).load()
                key_col_mapping = {
                    "lead": "STG_LCR_LEAD_KEY",
                    "lead_assignment": "STG_LCR_LEAD_ASSIGNMENT_KEY",
                    "lead_xref": "STG_LCR_LEAD_XREF_KEY",
                }
                key_col = key_col_mapping[self.table_name]
                raw_df_filtered = raw_df.join(
                    target.select(key_col),
                    on=key_col,
                    how="left_anti",
                )

                validate_dataframe(raw_df_filtered, target_schema)
                write_options = {
                    **sf_config_stg,
                    "dbtable": f"STG_LCR_{self.table_name.upper()}",
                    "column_mapping": "name",
                    "on_error": "CONTINUE",
                    "truncate": "true"
                }
                for attempt in range(3):
                    try:
                        raw_df_filtered.write.format("net.snowflake.spark.snowflake").options(**write_options).mode("append").save()
                        break
                    except Exception as w_err:
                        if attempt == 2:
                            raise
                        logger.warning(f"Snowflake write failed, retrying... {w_err}")
                        time.sleep(5)
                logger.info(f"Appended records to table STG_LCR_{self.table_name.upper()}")
                self.create_checkpoint()
            else:
                raise ValueError(f"Invalid write mode: {self.write_mode}")
            logger.info(f"Completed processing for table: {self.table_name}")
        except Exception as e:
            logger.error(f"Unexpected error processing table {self.table_name}: {str(e)}")
            logger.error(traceback.format_exc())
            raise

# -----------------------------------------------------------------------------
# ETLPipeline
# -----------------------------------------------------------------------------
class ETLPipeline:
    def __init__(self, config: ConfigManager, schema: SchemaManager):
        self.config = config
        self.schema = schema

    def run(self):
        write_mode = "incremental_insert"
        historical_load = False
        for table in self.config.TABLES:
            if self.config.TABLE_PROCESSING_CONFIG.get(table, False):
                processor = TableProcessor(table, write_mode, historical_load, self.config, self.schema)
                processor.process()
            else:
                logger.info(f"Skipping processing for {table}")
        logger.info("ETL process completed successfully.")

# -----------------------------------------------------------------------------
# Entrypoint
# -----------------------------------------------------------------------------
if __name__ == "__main__":
    config = ConfigManager()
    schema = SchemaManager()
    pipeline = ETLPipeline(config, schema)
    pipeline.run()
