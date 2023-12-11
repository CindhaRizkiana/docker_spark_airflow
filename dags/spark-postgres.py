import pyspark
from pyspark.sql import SparkSession
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
from datetime import datetime, timedelta

###############################################
# Parameters
###############################################
postgres_db = "jdbc:postgresql://dataeng-postgres:5432/cinda"
postgres_user = "cinda"
postgres_pwd = "password"


# Specify the path to your PostgreSQL JDBC driver JAR file
postgresql_jdbc_jar = "/opt/bitnami/spark/jars/postgresql-42.2.18.jar"
spark_host = "spark://dataeng-spark-master:7077"

# Create a Spark session
spark = SparkSession.builder \
    .appName('ReadPostgres') \
    .config("spark.jars", postgresql_jdbc_jar) \
    .master(spark_host) \
    .getOrCreate()

# Set log level
spark.sparkContext.setLogLevel("WARN")

###############################################
# DAG Definition
###############################################
now = datetime.now()

default_args = {
    "owner": "cinda",
    "depends_on_past": False,
    "start_date": datetime(now.year, now.month, now.day),
    "email": ["airflow@airflow.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

spark_dag = DAG(
    dag_id="spark_airflow_dag",
    description="This DAG is a sample of integration between Spark and DB. It reads Postgres DB.",
    default_args=default_args,
    schedule_interval=timedelta(1),
)

start = DummyOperator(task_id="start", dag=spark_dag)

spark_job_postgres = SparkSubmitOperator(
    task_id="spark_job_postgres",
    application="/mnt/wsl/docker_spark_airflow/spark-scripts-read-postgres.py",
    name="load-postgres",
    conn_id="spark_default",
    verbose=1,
    conf={
        "spark.master": spark_host,
        "spark.jars": postgresql_jdbc_jar,
        "spark.executor.extraClassPath": postgresql_jdbc_jar,
        "spark.driver.extraClassPath": postgresql_jdbc_jar,
    },
    application_args=[postgres_db, postgres_user, postgres_pwd],
    dag=spark_dag,
)

end = DummyOperator(task_id="end", dag=spark_dag)

start >> spark_job_postgres >> end
