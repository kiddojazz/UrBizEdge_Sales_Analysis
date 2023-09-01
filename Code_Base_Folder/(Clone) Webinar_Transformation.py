# Databricks notebook source
#To see all your mount folders
display(dbutils.fs.mounts())

# COMMAND ----------

# MAGIC %md
# MAGIC #Section 1: Sales Data

# COMMAND ----------

# MAGIC %md
# MAGIC ###Step 1: Read the sales data

# COMMAND ----------

sales_df = spark.read.csv("/mnt/adfdatalakeurbizedge/bronzeunitycatalog/sales_data_2.csv", header=True, inferSchema=True)
#InferSchema endures the right datatype are in each individual columns

# COMMAND ----------

sales_df.columns

# COMMAND ----------

# MAGIC %md
# MAGIC ### Set Individual columns to the right datatype

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType, TimestampType, \
DoubleType

# COMMAND ----------

sales_schema = StructType(fields=[StructField("Transaction ID", IntegerType(), False),
                                    StructField("Date", TimestampType(), True),
                                    StructField("Branch", StringType(), True),
                                    StructField("Product", StringType(), True),
                                    StructField("Unit Price",  IntegerType(), True),
                                    StructField("Quantity", IntegerType(), True),
                                    StructField("Sales Discount", DoubleType(), True),
                                    StructField("Total AMount Sold", DoubleType(), True),
                                    StructField("Hour", IntegerType(), True),
                                    StructField("Time_Range", StringType(), True)
])

# COMMAND ----------

sales_bronze = spark.read. \
    schema(sales_schema)\
        .csv("/mnt/adfdatalakeurbizedge/bronzeunitycatalog/sales_data_2.csv", header=True)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Change the Column names

# COMMAND ----------

sales_bronze = sales_bronze.withColumnRenamed("Transaction ID","transaction_id")
sales_bronze = sales_bronze.withColumnRenamed("Unit Price","unit_price")
sales_bronze = sales_bronze.withColumnRenamed("Sales Discount","sales_discount")
sales_bronze = sales_bronze.withColumnRenamed("Total AMount Sold","total_amount_sold")

# COMMAND ----------

#Check the new columns changed!!!
sales_bronze.columns

# COMMAND ----------

# MAGIC %md
# MAGIC ###Show Ingested Time

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

# Add a new column with the current time_stamp
sales_bronze = sales_bronze.withColumn("ingestion_date_time", current_timestamp())
sales_bronze.show()

# COMMAND ----------

display(sales_bronze)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Write to Unity Catalog Silver Table

# COMMAND ----------

# Add the mergeSchema option this will help with the data structure
sales_bronze.write.format("delta") \
           .option("mergeSchema", "true") \
           .mode("APPEND") \
           .saveAsTable("unity_catalog_project.silver.processed_sales")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Query 

# COMMAND ----------

# MAGIC %sql
# MAGIC use unity_catalog_project.silver

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from processed_sales

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) as Total_rows from processed_sales

# COMMAND ----------

# MAGIC %md
# MAGIC #Section 2: Sales Target

# COMMAND ----------

target_df = spark.read.csv("/mnt/adfdatalakeurbizedge/bronzeunitycatalog/sales_target_2.csv", header=True, inferSchema=True)
#InferSchema endures the right datatype are in each individual columns

# COMMAND ----------

target_df.columns

# COMMAND ----------

# MAGIC %md
# MAGIC ### Set SChema for right Datatype

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType, TimestampType, \
DoubleType

# COMMAND ----------

target_schema = StructType(fields=[StructField("Years", IntegerType(), False),
                                    StructField("Date", StringType(), True),
                                    StructField("Branch", StringType(), True),
                                    StructField("Product", StringType(), True),
                                    StructField("Sales",  DoubleType(), True)
])

# COMMAND ----------

target_bronze = spark.read. \
    schema(target_schema)\
        .csv("/mnt/adfdatalakeurbizedge/bronzeunitycatalog/sales_target_2.csv", header=True)

# COMMAND ----------

display(target_bronze) # This is to show the broze table itself

# COMMAND ----------

# MAGIC %md
# MAGIC ###Show ingestion Time

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

# Add a new column with the current time_stamp
target_bronze = target_bronze.withColumn("ingestion_date_time", current_timestamp())
target_bronze.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Write to Silver Catalog

# COMMAND ----------

# Add the mergeSchema option this will help with the data structure
target_bronze.write.format("delta") \
           .option("mergeSchema", "true") \
           .mode("APPEND") \
           .saveAsTable("unity_catalog_project.silver.processed_target")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Query

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from processed_target;

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) as total_row_Target from processed_target;

# COMMAND ----------

# MAGIC %md
# MAGIC ##Section 3: Branch Data

# COMMAND ----------

branch_bronze = spark.read.csv("/mnt/adfdatalakeurbizedge/bronzeunitycatalog/Branch Data.csv", header=True, inferSchema=True)
#InferSchema endures the right datatype are in each individual columns

# COMMAND ----------

# MAGIC %md
# MAGIC ###Show Ingestion Time 

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

# Add a new column with the current time_stamp
branch_bronze = branch_bronze.withColumn("ingestion_date_time", current_timestamp())
branch_bronze.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ##Write to the SIlver Catalog

# COMMAND ----------

# Add the mergeSchema option this will help with the data structure
branch_bronze.write.format("delta") \
           .option("mergeSchema", "true") \
           .mode("OVERWRITE") \
           .saveAsTable("unity_catalog_project.silver.processed_branch")

# COMMAND ----------

# MAGIC %md
# MAGIC ##Query

# COMMAND ----------


