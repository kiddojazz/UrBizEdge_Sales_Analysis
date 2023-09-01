import pyspark
import pandas as pd
from pyspark.sql import SparkSession

#Start spark session
#We need to set a App Name, this might take longer if doing it for the first time.
spark = SparkSession.builder.config("spark.driver.host", "localhost").appName('gittest').getOrCreate()

df_Sales = spark.read.csv(r"C:\Users\Temidayo\Documents\Databricks_Project\UrBizEdge Sales\UrBizEdge_Sales_Analysis\Code_Base_Folder\Sales.csv", header=True, inferSchema=True)
#InferSchema endures the right datatype are in each individual columns

#View each columns datatypes
df_Sales.printSchema()

#Show the data structure
df_Sales.show()

display(df_Sales)