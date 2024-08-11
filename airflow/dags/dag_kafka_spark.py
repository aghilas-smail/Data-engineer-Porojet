from datetime import timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.docker.operators.docker import DockerOperator
from datetime import datetime

from src.kafka.kafka_data import stream

start_date = datetime.today() - timedelta(days=-1)

default_args = {
    "owner": "airflow",
    "start_date": start_date,
    "retries" : 1,
    "retry_delay": timedelta(seconds=5),
}


with DAG(
    # It's just like a constuctore in java
    dag_id = "kafka_spark_dag", 
    default_args = default_args,
    schedule_interval = timedelta(days=1),
    catchup= False,
)as dag:
    
    
    kafka_stream_task = PythonOperator(
        task_id = "kafka_data_stream",
        python_callable=stream,
        dag = dag,
    )
    
    spark_stream_task = DockerOperator(
        task_id= "pyspark_consumer",
        image= "rappel-conso/spark:latest",
        api_version="auto",
        auto_remove=True,
        command="./bin/spark-submit --master local[*] --packages org.postgresql:postgresql:42.5.4,org.apche.spark:spark-sql-kafka-0-10_2.12:3.5.0 ./spark_stream.py",
        docker_url='tcp://docker-proxy:2375',
        environment={'SPARK_LOCAL_HOSTNAME': 'localhost'},
        network_mode="airflow-kakfa",
        dag=dag,
    )   
    
    kafka_stream_task >> spark_stream_task 
