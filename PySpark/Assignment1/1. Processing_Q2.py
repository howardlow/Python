# STAT478-18S1 Assignment 1
# Howard Low, 53626262

# Imports
from pyspark.sql.types import *
from pyspark.sql import functions as F

#@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@
#======================================= PROCESSING Q2 ===========================================
#@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@

# ================================  Q2a) Defining Schemas for meta data==============================
#------------- Daily Schema ---------------------------
schema_daily = StructType([
    StructField('id', StringType(), True),
    StructField('date', StringType(), True),
    StructField('element', StringType(), True),
    StructField('value', DoubleType(), True),
    StructField('measurement_flag', StringType(), True),
    StructField('quality_flag', StringType(), True),
    StructField('source_flag', StringType(), True),
    StructField('observation_time', StringType(), True),
])

#------------- Stations Schema ---------------------------
schema_station = StructType([
    StructField('id', StringType(), True),
    StructField('latitude', DoubleType(), True),
    StructField('longitude', DoubleType(), True),
    StructField('elevation', DoubleType(), True),
    StructField('state', StringType(), True),
    StructField('name', StringType(), True),
    StructField('gsn_flag', StringType(), True),
    StructField('hcn_crn_flag', StringType(), True),
    StructField('wmo_id', StringType(), True),    
])

#------------- Countries Schema ---------------------------
schema_country= StructType([
    StructField('code', StringType(), True),
    StructField('name', StringType(), True),   
])


#------------- States Schema ---------------------------
schema_state= StructType([
    StructField('code', StringType(), True),
    StructField('name', StringType(), True),   
])


#------------- Inventory Schema --------------------------
schema_inventory= StructType([
    StructField('id', StringType(), True),
    StructField('latitude', DoubleType(), True),   
    StructField('longtitude', DoubleType(), True),   
    StructField('element', StringType(), True),   
    StructField('firstyear', IntegerType(), True),   
    StructField('lastyear', IntegerType(), True),   
])


# ================================  Q2b) Load 2017.csv.gz ==============================
daily_2017 = (
    spark.read.format("com.databricks.spark.csv")
    .option("header", "false")
    .option("inferSchema", "false")
    .option("codec", "gzip")
    .schema(schema_daily)
    .load("hdfs:///data/ghcnd/daily/2017.csv.gz")
    .limit(1000)
)
daily_2017.cache()
daily_2017.show()

# ================================  Q2c) Load stations, states, countries and inventory and create RDD dataframe with schema ==============================
#------------- Load Stations ---------------------------
station = spark.read.text("hdfs:///data/ghcnd/stations")

station = station.select(
    F.trim(station.value.substr(0,12)).alias('id').cast(schema_station['id'].dataType),
    F.trim(station.value.substr(12,9)).cast('double').alias('latitude').cast(schema_station['latitude'].dataType),
    F.trim(station.value.substr(21,10)).cast('double').alias('longitude').cast(schema_station['longitude'].dataType),
    F.trim(station.value.substr(31,7)).cast('double').alias('elevation').cast(schema_station['elevation'].dataType),
    F.trim(station.value.substr(38,3)).alias('state').cast(schema_station['state'].dataType),
    F.trim(station.value.substr(41,31)).alias('name').cast(schema_station['name'].dataType),
    F.trim(station.value.substr(72,4)).alias('gsn_flag').cast(schema_station['gsn_flag'].dataType),
    F.trim(station.value.substr(76,4)).alias('hcn_crn_flag').cast(schema_station['hcn_crn_flag'].dataType),
    F.trim(station.value.substr(80,6)).alias('wmo_id').cast(schema_station['wmo_id'].dataType)
)

station.cache()
station.show()
station.count()

#------------- Load Countries ---------------------------
country = spark.read.text("hdfs:///data/ghcnd/countries")
country = country.select(
    F.trim(country.value.substr(0,3)).alias('code').cast(schema_country['code'].dataType),
    F.trim(country.value.substr(3,62)).alias('name').cast(schema_country['name'].dataType)
)

country.cache()
country.show()
country.count()

#------------- Load States ---------------------------
state = spark.read.text("hdfs:///data/ghcnd/states")
state = state.select(
    F.trim(state.value.substr(0,3)).alias('code').cast(schema_state['code'].dataType),
    F.trim(state.value.substr(3,28)).alias('name').cast(schema_state['name'].dataType),
)

state.cache()
state.show()
state.count()

#------------- Load Inventory ---------------------------
inventory = spark.read.text("hdfs:///data/ghcnd/inventory")
inventory = inventory.select(
    F.trim(inventory.value.substr(0,12)).alias('id').cast(schema_inventory['id'].dataType),
    F.trim(inventory.value.substr(12,9)).cast("double").alias('latitude').cast(schema_inventory['latitude'].dataType),
    F.trim(inventory.value.substr(21,10)).cast("double").alias('longtitude').cast(schema_inventory['longtitude'].dataType),
    F.trim(inventory.value.substr(31,5)).alias('element').cast(schema_inventory['element'].dataType),
    F.trim(inventory.value.substr(36,5)).cast("integer").alias('firstyear').cast(schema_inventory['firstyear'].dataType),
    F.trim(inventory.value.substr(41,5)).cast("integer").alias('lastyear').cast(schema_inventory['lastyear'].dataType),
)

inventory.cache()
inventory.show()
inventory.count()

# ================================  Q2c) Counting the no. of station without wmo_id  ==============================
station.filter(station.wmo_id == '').count()

