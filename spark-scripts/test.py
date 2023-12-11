import pyspark
from pyspark.sql import SparkSession

spark_host = "spark://dataeng-spark-master:7077"
spark = SparkSession.builder \
    .appName('ReadPostgres') \
    .config("spark.jars", "/opt/bitnami/spark/jars/postgresql-42.2.18.jar") \
    .master(spark_host) \
    .getOrCreate()

print("Spark Jars Configuration:", spark._jsc.sc().getConf().get("spark.jars"))
