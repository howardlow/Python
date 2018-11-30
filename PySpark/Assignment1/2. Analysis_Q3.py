# STAT478-18S1 Assignment 1
# Howard Low, 53626262

#@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@
#======================================= ANALYSIS Q3 =============================================
#@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@

# ================================  Analysis Q3c)  Load and count the number of rows in daily from 2010 to 2015 ==============================

#load daily 2010
check_daily_2010 = (
    spark.read.format("com.databricks.spark.csv")
    .option("header", "false")
    .option("inferSchema", "false")
    .option("codec", "gzip")
    .schema(schema_daily)
    .load("hdfs:///data/ghcnd/daily/2010.csv.gz")
)

#count daily 2010
check_daily_2010.count()

#load
check_daily_2017 = (
    spark.read.format("com.databricks.spark.csv")
    .option("header", "false")
    .option("inferSchema", "false")
    .option("codec", "gzip")
    .schema(schema_daily)
    .load("hdfs:///data/ghcnd/daily/2017.csv.gz")
)

#count daily 2017
check_daily_2017.count()


# ================================  Analysis Q3d)  Load and count the number of rows in daily from 2010 to 2015 ==============================
#load daily 2010 to 2015
check_daily_2010_to_2015 = (
    spark.read.format("com.databricks.spark.csv")
    .option("header", "false")
    .option("inferSchema", "false")
    .option("codec", "gzip")
    .schema(schema_daily)
    .load("hdfs:///data/ghcnd/daily/201[0-5].*")
)

#count daily 2010 to 2015
check_daily_2010_to_2015.count()