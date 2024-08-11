from pyspark.sql import SparkSession
from pyspark.sql.types import(
    StructType,
    StructField, 
    StringType,
)
from pyspark.sql.functions import from_json, col
from src.data import Postgres_URL, Postgres_Properties, DB_FIELDS
import logging

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s:%(funcName)s:%(levelname)s:%(message)s"
)

def create_spark_session()