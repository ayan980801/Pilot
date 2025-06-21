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
