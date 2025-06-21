import os
import logging
import time
import traceback
import re
from typing import Dict
import psycopg2
from psycopg2 import pool, OperationalError, sql
from azure.storage.blob import BlobServiceClient
from pyspark.sql import SparkSession


class Config:
    @staticmethod
    def _get_secret(scope: str, key: str) -> str:
        try:
            from pyspark.dbutils import DBUtils

            dbutils = DBUtils(SparkSession.builder.getOrCreate())
            return dbutils.secrets.get(scope=scope, key=key)
        except Exception:
            env_key = f"{scope}_{key}".upper()
            value = os.getenv(env_key)
            if not value:
                raise ValueError(f"Missing secret for {env_key}")
            return value

    @classmethod
    def get_pg_config(cls) -> dict:
        cfg = {
            "host": cls._get_secret("key-vault-secret", "DataProduct-LCR-Host-PROD"),
            "port": cls._get_secret("key-vault-secret", "DataProduct-LCR-Port-PROD"),
            "database": "LeadCustodyRepository",
            "user": cls._get_secret("key-vault-secret", "DataProduct-LCR-User-PROD"),
            "password": cls._get_secret(
                "key-vault-secret", "DataProduct-LCR-Pass-PROD"
            ),
        }
        if not all(cfg.values()):
            raise ValueError("Incomplete PostgreSQL configuration")
        return cfg

    @classmethod
    def get_storage_config(cls) -> dict:
        cfg = {
            "account_name": "quilitydatabricks",
            "account_key": cls._get_secret("key-vault-secret", "DataProduct-ADLS-Key"),
            "container_name": "dataarchitecture",
        }
        if not cfg["account_key"]:
            raise ValueError("Missing Azure storage key")
        return cfg


class DBUtilsCompat:
    @staticmethod
    def get_dbutils():
        try:
            from pyspark.dbutils import DBUtils

            dbutils = DBUtils(SparkSession.builder.getOrCreate())
            return dbutils
        except Exception:

            class SafeSecrets:
                def get(self, scope, key):
                    env_key = f"{scope}_{key}".upper()
                    value = os.getenv(env_key)
                    if not value:
                        raise ValueError(f"Missing secret for {env_key}")
                    return value

            class SafeFS:
                def ls(self, path):
                    raise NotImplementedError

                def mkdirs(self, path):
                    logging.info(f"[DRY-RUN] Would create dir: {path}")

            class SafeFallbackUtils:
                secrets = SafeSecrets()
                fs = SafeFS()

            logging.warning("DBUtils not available; using env for secrets")
            return SafeFallbackUtils()


class PostgresDataHandler:
    def __init__(self, config: dict):
        self.config = config
        self.pg_pool = self._connect()

    def _connect(self):
        try:
            return psycopg2.pool.ThreadedConnectionPool(1, 20, **self.config)
        except OperationalError as e:
            logging.error(f"PostgreSQL connection error: {e}")
            raise

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.pg_pool:
            self.pg_pool.closeall()

    def is_alive(self) -> bool:
        conn = self.pg_pool.getconn()
        try:
            with conn.cursor() as c:
                c.execute("SELECT 1")
            return True
        except OperationalError:
            return False
        finally:
            self.pg_pool.putconn(conn)

    @staticmethod
    def _valid_table(table: str) -> bool:
        return bool(re.fullmatch(r"[\w\.\"]+", table))

    @staticmethod
    def _parse_table(table: str) -> (str, str):
        t = table.replace('"', "")
        return t.split(".", 1) if "." in t else ("public", t)

    def table_count(self, table: str) -> int:
        if not self._valid_table(table):
            raise ValueError(f"Invalid table name: {table}")
        schema, tbl = self._parse_table(table)
        conn = self.pg_pool.getconn()
        try:
            with conn.cursor() as c:
                c.execute(
                    sql.SQL("SELECT COUNT(*) FROM {}.{}").format(
                        sql.Identifier(schema), sql.Identifier(tbl)
                    )
                )
                return c.fetchone()[0]
        finally:
            self.pg_pool.putconn(conn)

    def export_to_delta(self, table: str, stage: str, db: str, spark, fetchsize=10000):
        from pyspark.sql.functions import current_timestamp, lit

        if not self._valid_table(table):
            raise ValueError(f"Invalid table name: {table}")
        logging.info(f"Exporting {table} to Delta...")
        schema, tbl = self._parse_table(table)
        jdbc_url = f"jdbc:postgresql://{self.config['host']}:{self.config['port']}/{self.config['database']}"
        props = {
            "user": self.config["user"],
            "password": self.config["password"],
            "driver": "org.postgresql.Driver",
            "fetchsize": str(fetchsize),
        }
        df = spark.read.jdbc(url=jdbc_url, table=f"{schema}.{tbl}", properties=props)
        df = df.withColumns(
            {
                "ETL_CREATED_DATE": current_timestamp(),
                "ETL_LAST_UPDATE_DATE": current_timestamp(),
                "CREATED_BY": lit("ETL_PROCESS"),
                "TO_PROCESS": lit(True),
                "EDW_EXTERNAL_SOURCE_SYSTEM": lit("LeadCustodyRepository"),
            }
        )
        path = f"abfss://dataarchitecture@quilitydatabricks.dfs.core.windows.net/{stage}/{db}/{tbl}"
        df.write.format("delta").mode("overwrite").option(
            "overwriteSchema", "true"
        ).save(path)
        logging.info(f"Exported {table} to Delta.")


class AzureDataHandler:
    def __init__(self, config: dict, dbutils):
        self.config = config
        self.dbutils = dbutils
        self.client = self._connect()

    def _connect(self):
        conn_str = (
            f"DefaultEndpointsProtocol=https;"
            f"AccountName={self.config['account_name']};"
            f"AccountKey={self.config['account_key']};"
            f"EndpointSuffix=core.windows.net"
        )
        try:
            return BlobServiceClient.from_connection_string(conn_str)
        except Exception as e:
            logging.error(f"Azure Storage connection error: {e}")
            raise

    def ensure_dir(self, stage: str, db: str):
        path = f"abfss://dataarchitecture@quilitydatabricks.dfs.core.windows.net/{stage}/{db}"
        try:
            self.dbutils.fs.ls(path)
        except Exception:
            self.dbutils.fs.mkdirs(path)


class DataSync:
    def __init__(
        self,
        pg: PostgresDataHandler,
        azure: AzureDataHandler,
        spark,
        tables: list,
        db: str,
    ):
        self.pg = pg
        self.azure = azure
        self.spark = spark
        self.tables = tables
        self.db = db

    def sync(self, stage="RAW"):
        if not self.pg.is_alive():
            logging.error("Postgres connection is not alive.")
            return
        for table in self.tables:
            try:
                self.azure.ensure_dir(stage, self.db)
                start = time.time()
                self.pg.export_to_delta(table, stage, self.db, self.spark)
                logging.info(f"{table} processed in {time.time()-start:.2f}s")
            except Exception as e:
                logging.error(f"Failed {table}: {e}\n{traceback.format_exc()}")


class AppRunner:
    @staticmethod
    def run():
        logging.basicConfig(
            level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
        )
        spark = SparkSession.builder.getOrCreate()
        dbutils = DBUtilsCompat.get_dbutils()
        pg_config = Config.get_pg_config()
        storage_config = Config.get_storage_config()
        tables_to_copy = [
            'public."leadassignment"',
            'public."leadxref"',
            'public."lead"',
        ]
        db = pg_config["database"]
        with PostgresDataHandler(pg_config) as pg:
            azure = AzureDataHandler(storage_config, dbutils)
            sync = DataSync(pg, azure, spark, tables_to_copy, db)
            sync.sync()


if __name__ == "__main__":
    AppRunner.run()
