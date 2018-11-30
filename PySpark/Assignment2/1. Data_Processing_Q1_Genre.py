# STAT478-18S1 Assignment 2
# Howard Low, 53626262

#@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@
#======================================= PROCESSING Q1 GENRE =====================================
#@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@

#*********************************** Q1a ***********************************
# ================================  Schema ==============================
schema_genre = StructType([
    StructField("track_id", StringType(), True),
    StructField("genre", StringType(), True),
])


# ================================  Load ==============================
genre = (
    spark.read.format("com.databricks.spark.csv")
    .option("header", "false")
    .option("delimiter", "\t")
    .option("inferSchema", "false")
    .schema(schema_genre)
    .load("hdfs:///data/msd/genre/msd-MAGD-genreAssignment.tsv")
)

style = (
    spark.read.format("com.databricks.spark.csv")
    .option("header", "false")
    .option("delimiter", "\t")
    .option("inferSchema", "false")
    .schema(schema_genre)
    .load("hdfs:///data/msd/genre/msd-MASD-styleAssignment.tsv")
)

top_genre = (
    spark.read.format("com.databricks.spark.csv")
    .option("header", "false")
    .option("delimiter", "\t")
    .option("inferSchema", "false")
    .schema(schema_genre)
    .load("hdfs:///data/msd/genre/msd-topMAGD-genreAssignment.tsv")
)

#*********************************** Q1c ***********************************
# ================================  Count ==============================
genre.show(10)
'''
+------------------+--------------+
|          track_id|         genre|
+------------------+--------------+
|TRAAAAK128F9318786|      Pop_Rock|
|TRAAAAV128F421A322|      Pop_Rock|
|TRAAAAW128F429D538|           Rap|
|TRAAABD128F429CF47|      Pop_Rock|
|TRAAACV128F423E09E|      Pop_Rock|
|TRAAADT12903CCC339|Easy_Listening|
|TRAAAED128E0783FAB|         Vocal|
|TRAAAEF128F4273421|      Pop_Rock|
|TRAAAEM128F93347B9|    Electronic|
|TRAAAFD128F92F423A|      Pop_Rock|
+------------------+--------------+
'''

genre.count()
# 422714

style.show(10)
'''
+------------------+--------------------+
|          track_id|               genre|
+------------------+--------------------+
|TRAAAAK128F9318786|   Metal_Alternative|
|TRAAAAV128F421A322|                Punk|
|TRAAAAW128F429D538|         Hip_Hop_Rap|
|TRAAACV128F423E09E|Rock_Neo_Psychedelia|
|TRAAAEF128F4273421|           Pop_Indie|
|TRAAAFP128F931B4E3|         Hip_Hop_Rap|
|TRAAAGR128F425B14B|    Pop_Contemporary|
|TRAAAHD128F42635A5|           Rock_Hard|
|TRAAAHJ128F931194C|           Pop_Indie|
|TRAAAHZ128E0799171|         Hip_Hop_Rap|
+------------------+--------------------+
'''

style.count()
# 273936

top_genre.show(10)
'''
+------------------+----------+
|          track_id|     genre|
+------------------+----------+
|TRAAAAK128F9318786|  Pop_Rock|
|TRAAAAV128F421A322|  Pop_Rock|
|TRAAAAW128F429D538|       Rap|
|TRAAABD128F429CF47|  Pop_Rock|
|TRAAACV128F423E09E|  Pop_Rock|
|TRAAAED128E0783FAB|     Vocal|
|TRAAAEF128F4273421|  Pop_Rock|
|TRAAAEM128F93347B9|Electronic|
|TRAAAFD128F92F423A|  Pop_Rock|
|TRAAAFP128F931B4E3|       Rap|
+------------------+----------+
'''

top_genre.count()
# 406427