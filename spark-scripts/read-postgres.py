# Import necessary libraries
import os
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# Specify the path to your PostgreSQL JDBC driver JAR file
# postgresql_jdbc_jar = "/opt/bitnami/spark/jars/postgresql-42.2.18.jar"
spark_host = "spark://dataeng-spark-master:7077"

# Create a Spark session
sparkcontext = pyspark.SparkContext.getOrCreate(conf=(
        pyspark
        .SparkConf()
        .setAppName('ReadPostgres')
        .setMaster(spark_host)
        .set("spark.jars", "/opt/bitnami/spark/jars/postgresql-42.2.18.jar")
    ))
# Set log level
sparkcontext.setLogLevel("WARN")

spark = pyspark.sql.SparkSession(sparkcontext.getOrCreate())

# Define the PostgreSQL connection properties
postgres_url = "jdbc:postgresql://localhost:5432/postgres_db"
properties = {
    "user": "cinda",
    "password": "password",
    "driver": "org.postgresql.Driver"
}

# Define the table name
table_name = "retail"

# Read data from PostgreSQL
df = spark.read \
    .format("jdbc") \
    .option("url", postgres_url) \
    .option("dbtable", table_name) \
    .option("user", properties["user"]) \
    .option("password", properties["password"]) \
    .option("driver", properties["driver"]) \
    .load()

# Display the content of the DataFrame
df.show()

# Drop rows with any null values
df = df.na.drop()

# Change Data Types of selected columns to integers
df = df.withColumn("invoiceno", col("invoiceno").cast("integer"))
df = df.withColumn("stockcode", col("stockcode").cast("integer"))

# Display the updated schema of the DataFrame
df.printSchema()

# Customer Retention Analysis
# Extract the month from the 'invoicedate' column
df = df.withColumn("month", F.month("invoicedate"))
# Group by month and calculate the number of unique customers
monthly_unique_customers = df.groupBy("month").agg(F.countDistinct("customerid").alias("unique_customers"))
# Create a window specification to order the results by month
windowSpec = Window().orderBy("month")
# Calculate retention rate by comparing current and previous month's unique customers
monthly_unique_customers = monthly_unique_customers.withColumn("prev_unique_customers", F.lag("unique_customers").over(windowSpec))
monthly_unique_customers = monthly_unique_customers.withColumn("retention_rate", (monthly_unique_customers["unique_customers"] / monthly_unique_customers["prev_unique_customers"]).cast("double"))
# Display the results of retention analysis
monthly_unique_customers.show()

# Customer Churn Analysis
# Find the maximum invoice date
max_invoice_date = df.agg(F.max("invoicedate")).collect()[0][0]
# Filter customers from the last month
last_month_customers = df.filter(F.month("invoicedate") == F.month(F.lit(max_invoice_date)))
# Find churned customers by subtracting last month's customers from all customers
churned_customers = df.select("customerid").distinct().subtract(last_month_customers.select("customerid").distinct())
# Count total customers
total_customers = df.select("customerid").distinct().count()
# Calculate churn rate
churn_rate = (F.lit(churned_customers.count()) / F.lit(total_customers)).cast("double")
# Display the churn rate
print(f"Churn Rate: {churn_rate}")

# Rank Operation
# Group by customer and calculate total spending per customer
total_spending_per_customer = df.groupBy("customerid").agg(F.sum("unitprice").alias("total_spending"))
# Rank customers based on total spending
ranked_customers = total_spending_per_customer.withColumn("rank", F.rank().over(Window.orderBy(F.desc("total_spending"))))
# Display the ranked customers
ranked_customers.show()

# Save the ranked_customers DataFrame to a CSV file
csv_output_path = "/mnt/wsl/docker_spark_airflow/sql/results/ranked_customers.csv"
ranked_customers.write.mode("overwrite").csv(csv_output_path)

# Stop the Spark session
spark.stop()