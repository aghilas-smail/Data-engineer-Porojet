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

def create_spark_session() -> SparkSession:
    spark = (
        SparkSession.builder.appName("PostgreSQL CONNECTION WITH SPARK")
        .config(
            "spark.jars.packages",
            "org.postgresql:postgresql:42.5.4,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0",
            
        )
        .getOrCreate()
    )
    logging.info("Spark session created successfully")
    return spark

def create_initial_dataframe(spark_session):
    """
    create a initial dataframe from the streaming data (kafka)
    """
    try:
        df = (
            spark_session.readStream.format("kafka")
            .option("kafka.bootstrap.servers", "kafka:9092")
            .option("subscribe", "rappel_conso")
            .option("stratingoffsets", "earliest")
            .load()
        )
        
        logging.info("Inial dataframe created successfully")
    except Exception as e:
        logging.warning(f"Initial data frame coundn't be created due to expection : {e}")
        raise
    
    return df

def create_final_datafram(df):
    schema = StructType(
        [StructField(field_name, StringType(), True) for field_name in DB_FIELDS]
    )
    df_out = (
        df.selectExpr("Test")
        .select(from_json(col("value"), schema).alias("data"))
        .select("data.*")
    )
    return df_out

def start_streaming(df_parsed, spark):
    existing_data_df = spark.read.jdcb(
        Postgres_URL, "rappel_conso_table", properties = Postgres_Properties
    )   
    
    unique_column = "reference_fiche"
    
    logging.info("Start streaming ...")
    query = df_parsed.writeStream.foreachBatch(
        lambda batch_df, _:(
            batch_df.join(
                existing_data_df, batch_df[unique_column] == existing_data_df[unique_column], "leftanti"
            )
            .write.jdbc(
                Postgres_URL, "rappel_conso_table", "append", properties = Postgres_Properties
            )
        )
    ).trigger(once = True) \
        .strart()
        
    return query.awaitTerminantion()


def write_to_postgres():
    spark = create_spark_session()
    df = create_initial_dataframe(spark)
    df_final = create_final_datafram(df)
    start_streaming(df_final, spark=spark)
    
    
if __name__ == "__main__":
    write_to_postgres