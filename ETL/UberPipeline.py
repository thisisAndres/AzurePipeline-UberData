# Databricks notebook source
# MAGIC %md
# MAGIC **Imports**

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql import Window, DataFrame


# COMMAND ----------

# MAGIC %md
# MAGIC **Extracting Data**

# COMMAND ----------

scopes = dbutils.secrets.listScopes()
print("Scopes disponibles:", scopes)


# COMMAND ----------

keys = dbutils.secrets.list("ky-uberpipeline-secretscope-v2")
print("Keys en el scope 'kv-uberpipeline-secretscope-v2':", keys)



# COMMAND ----------

try:
    ## Command to mount source container
    dbutils.fs.mount(
        source= "wasbs://data@uberpipelineproject.blob.core.windows.net",
        mount_point = "/mnt/data",
        extra_configs = {
            "fs.azure.account.key.uberpipelineproject.blob.core.windows.net":dbutils.secrets.get(scope = "ky-uberpipeline-secretscope-v2", key = "kv-uberpipeline")
        }
    )
except:
    print("Container Already Mounted")

# COMMAND ----------

df = spark.read.csv("/mnt/data/uber_data.csv", inferSchema=True, header=True)

# COMMAND ----------

df.describe().display()

# COMMAND ----------

df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC **Transforming Data**

# COMMAND ----------

df.printSchema()

# COMMAND ----------

def addIndex(indexName: str, dataFrame: DataFrame):
    w = Window.orderBy(lit(1))
    tempDf = dataFrame.withColumn(indexName, row_number().over(w))
    return tempDf


# COMMAND ----------

df = df.dropDuplicates()

# COMMAND ----------

df = addIndex('trip_id', df)

# COMMAND ----------

#Creating Datetime Dimension
date_time_dim = df.select(['tpep_pickup_datetime', 'tpep_dropoff_datetime'])

#Adding Indax
date_time_dim = addIndex('date_time_id', date_time_dim)

# Generating Pickup Columns
date_time_dim = date_time_dim.withColumn('pickup_hour', hour('tpep_pickup_datetime')) \
                             .withColumn('pickup_day', dayofmonth('tpep_pickup_datetime')) \
                             .withColumn('pickup_month', month('tpep_pickup_datetime')) \
                             .withColumn('pickup_year', year('tpep_pickup_datetime')) \
                             .withColumn('pickup_weekday', dayofweek('tpep_pickup_datetime')) \
                             .withColumnRenamed('tpep_pickup_datetime', 'pickup_datetime')

# Generating Dropoff Columns
date_time_dim = date_time_dim.withColumn('dropoff_hour', hour('tpep_dropoff_datetime')) \
                             .withColumn('dropoff_day', dayofmonth('tpep_dropoff_datetime')) \
                             .withColumn('dropoff_month', month('tpep_dropoff_datetime')) \
                             .withColumn('dropoff_year', year('tpep_dropoff_datetime')) \
                             .withColumn('dropoff_weekday', dayofweek('tpep_dropoff_datetime')) \
                             .withColumnRenamed('tpep_dropoff_datetime', 'dropoff_datetime')

# COMMAND ----------

#Creating Ratecode Dimension
ratecode_dim = df.select(['RatecodeID'])
ratecode_dim = ratecode_dim.withColumnRenamed('RatecodeID', 'ratecode')
ratecode_dim = ratecode_dim.withColumn('ratecode_name', 
                                       when(col('ratecode') == 1, 'Standard rate')
                                       .when(col('ratecode') == 2, 'JFK')
                                       .when(col('ratecode') == 3, 'Newark')
                                       .when(col('ratecode') == 4, 'Nassau or Westchester')
                                       .when(col('ratecode') == 5, 'Negotiated fare')
                                       .when(col('ratecode') == 6, 'Group ride')
                                       )
ratecode_dim = addIndex('ratecode_id', ratecode_dim)
ratecode_dim = ratecode_dim.select(['ratecode_id', 'ratecode', 'ratecode_name'])

# COMMAND ----------

#Creating Payment Type Dimension
payment_type_dim = df.select(['payment_type'])
payment_type_dim = payment_type_dim.withColumn('payment_type_name',
                                               when(col('payment_type') == 1, 'Credit Card')
                                               .when(col('payment_type') == 2, 'Cash')
                                               .when(col('payment_type') == 3, 'No Charge')
                                               .when(col('payment_type') == 4, 'Dispute')
                                               .when(col('payment_type') == 5, 'Unknown')
                                               .when(col('payment_type') == 6, 'Voided Trip')
                                               )
payment_type_dim = addIndex('payment_type_id', payment_type_dim)
payment_type_dim = payment_type_dim.select(['payment_type_id', 'payment_type', 'payment_type_name'])

# COMMAND ----------

#Creating Passenger Count Dimension
passenger_count_dim = df.select(['passenger_count'])
passenger_count_dim = addIndex('passenger_count_id', passenger_count_dim)
passenger_count_dim = passenger_count_dim.select(['passenger_count_id', 'passenger_count'])

# COMMAND ----------

#Creating Trip Discance Dimension
trip_distance_dim = df.select(['trip_distance'])
trip_distance_dim = addIndex('trip_distance_id', trip_distance_dim)
trip_distance_dim = trip_distance_dim.select(['trip_distance_id', 'trip_distance'])

# COMMAND ----------

#Creating Pickup Location Dimension
pickup_location_dim = df.select(['pickup_latitude', 'pickup_longitude'])
pickup_location_dim = addIndex('pickup_location_id', pickup_location_dim)
pickup_location_dim = pickup_location_dim.select(['pickup_location_id', 'pickup_latitude', 'pickup_longitude'])

# COMMAND ----------

#Creating Dropoff Location Dimension
dropoff_location_dim = df.select(['dropoff_latitude', 'dropoff_longitude'])
dropoff_location_dim = addIndex('dropoff_location_id', dropoff_location_dim)
dropoff_location_dim = dropoff_location_dim.select(['dropoff_location_id', 'dropoff_latitude' , 'dropoff_longitude'])


# COMMAND ----------

#Creating Fact Table
factCols = [
    'trip_id', 
    'date_time_id', 
    'passenger_count_id', 
    'trip_distance_id', 
    'pickup_location_id',
    'dropoff_location_id',
    'ratecode_id',
    'payment_type_id',
    'VendorID',
    'fare_amount',
    'extra',
    'mta_tax',
    'tip_amount',
    'improvement_surcharge',
    'total_amount']

fact = df.join(date_time_dim, df.trip_id == date_time_dim.date_time_id) \
        .join(ratecode_dim, df.trip_id == ratecode_dim.ratecode_id) \
        .join(payment_type_dim, df.trip_id == payment_type_dim.payment_type_id) \
        .join(passenger_count_dim, df.trip_id == passenger_count_dim.passenger_count_id) \
        .join(trip_distance_dim, df.trip_id == trip_distance_dim.trip_distance_id) \
        .join(pickup_location_dim, df.trip_id == pickup_location_dim.pickup_location_id) \
        .join(dropoff_location_dim, df.trip_id == dropoff_location_dim.dropoff_location_id) \
        .select(factCols)
fact = fact.withColumnRenamed('VendorID', 'vendor_id')

# COMMAND ----------

# MAGIC %md
# MAGIC **LOADING**

# COMMAND ----------

#Mounting Target Blob Container
try:
    ## Command to mount source container
    dbutils.fs.mount(
        source= "wasbs://transformed-data@uberpipelineproject.blob.core.windows.net",
        mount_point = "/mnt/transformed-data",
        extra_configs = {
            "fs.azure.account.key.uberpipelineproject.blob.core.windows.net":dbutils.secrets.get(scope = "ky-uberpipeline-secretscope-v2", key = "kv-uberpipeline")
        }
    )
except:
    print("Container already mounted ")

# COMMAND ----------

#Loading all spark data frames to target container
fact.toPandas().to_csv("/dbfs/mnt/transformed-data/fact.csv",header=True,index=False)

date_time_dim.toPandas().to_csv("/dbfs/mnt/transformed-data/date_time_dim.csv", header=True, index=False)

ratecode_dim.toPandas().to_csv('/dbfs/mnt/transformed-data/ratecode_dim.csv', header=True, index=False)

payment_type_dim.toPandas().to_csv('/dbfs/mnt/transformed-data/payment_type_dim.csv', header=True, index=False)

passenger_count_dim.toPandas().to_csv('/dbfs/mnt/transformed-data/passenger_count_dim.csv', header=True, index=False)

trip_distance_dim.toPandas().to_csv('/dbfs/mnt/transformed-data/trip_distance_dim.csv', header=True, index=False)

pickup_location_dim.toPandas().to_csv('/dbfs/mnt/transformed-data/pickup_location_dim.csv', header=True, index=False)

dropoff_location_dim.toPandas().to_csv('/dbfs/mnt/transformed-data/dropoff_location_dim.csv', header=True, index=False)

# COMMAND ----------

#Unmounting Containers
dbutils.fs.unmount('/mnt/transformed-data')
dbutils.fs.unmount('/mnt/data')
