# STAT478-18S1 Assignment 2
# Howard Low, 53626262

#@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@
#======================================= Audio Similarity Q1 =====================================
#@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@

#*********************************** Q1a  Loading Audio Features  ***********************************
#using area of moment feature data set
attributes_hdfs_path = "hdfs:///data/msd/audio/attributes/msd-jmir-methods-of-moments-all-v1.0.attributes.csv"
features_hdfs_path = "hdfs:///data/msd/audio/features/msd-jmir-methods-of-moments-all-v1.0.csv"

schema = attributes_to_schema(attributes_hdfs_path)
methods_of_moments = spark.read.schema(schema).csv(features_hdfs_path)

#remove quotation from MSD_TRACKID
methods_of_moments = methods_of_moments.withColumn("MSD_TRACKID", F.regexp_extract(methods_of_moments.MSD_TRACKID, '(\w{18})', 1))

#show first record
print(json.dumps(methods_of_moments.head().asDict(), indent=2))
'''
{
  "Method_of_Moments_Overall_Standard_Deviation_1": 0.1545,
  "Method_of_Moments_Overall_Standard_Deviation_2": 13.11,
  "Method_of_Moments_Overall_Standard_Deviation_3": 840.0,
  "Method_of_Moments_Overall_Standard_Deviation_4": 41080.0,
  "Method_of_Moments_Overall_Standard_Deviation_5": 7108000.0,
  "Method_of_Moments_Overall_Average_1": 0.319,
  "Method_of_Moments_Overall_Average_2": 33.41,
  "Method_of_Moments_Overall_Average_3": 1371.0,
  "Method_of_Moments_Overall_Average_4": 64240.0,
  "Method_of_Moments_Overall_Average_5": 8398000.0,
  "MSD_TRACKID": "TRHFHQZ12903C9E2D5"
}
'''

print(json.dumps(methods_of_moments.columns, indent=2))
'''
[
  "Method_of_Moments_Overall_Standard_Deviation_1",
  "Method_of_Moments_Overall_Standard_Deviation_2",
  "Method_of_Moments_Overall_Standard_Deviation_3",
  "Method_of_Moments_Overall_Standard_Deviation_4",
  "Method_of_Moments_Overall_Standard_Deviation_5",
  "Method_of_Moments_Overall_Average_1",
  "Method_of_Moments_Overall_Average_2",
  "Method_of_Moments_Overall_Average_3",
  "Method_of_Moments_Overall_Average_4",
  "Method_of_Moments_Overall_Average_5",
  "MSD_TRACKID"
]
'''

#describe the dataset
methods_of_moments.describe().show()
'''
+-------+----------------------------------------------+----------------------------------------------+----------------------------------------------+----------------------------------------------+----------------------------------------------+-----------------------------------+-----------------------------------+-----------------------------------+-----------------------------------+-----------------------------------+------------------+
|summary|Method_of_Moments_Overall_Standard_Deviation_1|Method_of_Moments_Overall_Standard_Deviation_2|Method_of_Moments_Overall_Standard_Deviation_3|Method_of_Moments_Overall_Standard_Deviation_4|Method_of_Moments_Overall_Standard_Deviation_5|Method_of_Moments_Overall_Average_1|Method_of_Moments_Overall_Average_2|Method_of_Moments_Overall_Average_3|Method_of_Moments_Overall_Average_4|Method_of_Moments_Overall_Average_5|       MSD_TRACKID|
+-------+----------------------------------------------+----------------------------------------------+----------------------------------------------+----------------------------------------------+----------------------------------------------+-----------------------------------+-----------------------------------+-----------------------------------+-----------------------------------+-----------------------------------+------------------+
|  count|                                        994623|                                        994623|                                        994623|                                        994623|                                        994623|                             994623|                             994623|                             994623|                             994623|                             994623|            994623|
|   mean|                            0.1549817600174646|                            10.384550576952208|                             526.8139724398065|                             35071.97543290272|                             5297870.369577217|                0.35084444325313446|                  27.46386798784046|                  1495.809181207552|                 143165.46163257834|                2.396783048473542E7|              null|
| stddev|                           0.06646213086143016|                            3.8680013938746827|                             180.4377549977533|                            12806.816272955573|                             2089356.436455806|                0.18557956834383774|                  8.352648595163723|                  505.8937639190234|                  50494.27617103221|                  9307340.299219644|              null|
|    min|                                           0.0|                                           0.0|                                           0.0|                                           0.0|                                           0.0|                                0.0|                                0.0|                                0.0|                          -146300.0|                                0.0|TRAAAAK128F9318786|
|    max|                                         0.959|                                         55.42|                                        2919.0|                                      407100.0|                                       4.657E7|                              2.647|                              117.0|                             5834.0|                           452500.0|                            9.477E7|TRZZZZO128F428E2D4|
+-------+----------------------------------------------+----------------------------------------------+----------------------------------------------+----------------------------------------------+----------------------------------------------+-----------------------------------+-----------------------------------+-----------------------------------+-----------------------------------+-----------------------------------+------------------+
'''

#rename columns for better visibility
methods_of_moments = (
	methods_of_moments.withColumnRenamed("Method_of_Moments_Overall_Standard_Deviation_1", "sd1")
	.withColumnRenamed("Method_of_Moments_Overall_Standard_Deviation_2", "sd2")
	.withColumnRenamed("Method_of_Moments_Overall_Standard_Deviation_3", "sd3")
	.withColumnRenamed("Method_of_Moments_Overall_Standard_Deviation_4", "sd4")
	.withColumnRenamed("Method_of_Moments_Overall_Standard_Deviation_5", "sd5")
	.withColumnRenamed("Method_of_Moments_Overall_Average_1", "a1")
	.withColumnRenamed("Method_of_Moments_Overall_Average_2", "a2")
	.withColumnRenamed("Method_of_Moments_Overall_Average_3", "a3")
	.withColumnRenamed("Method_of_Moments_Overall_Average_4", "a4")
	.withColumnRenamed("Method_of_Moments_Overall_Average_5", "a5")
)

#describe the dataset
methods_of_moments.describe().show()
'''
+-------+-------------------+------------------+-----------------+------------------+-----------------+-------------------+-----------------+-----------------+------------------+-------------------+------------------+
|summary|                sd1|               sd2|              sd3|               sd4|              sd5|                 a1|               a2|               a3|                a4|                 a5|       MSD_TRACKID|
+-------+-------------------+------------------+-----------------+------------------+-----------------+-------------------+-----------------+-----------------+------------------+-------------------+------------------+
|  count|             994623|            994623|           994623|            994623|           994623|             994623|           994623|           994623|            994623|             994623|            994623|
|   mean| 0.1549817600174646|10.384550576952208|526.8139724398065| 35071.97543290272|5297870.369577217|0.35084444325313446|27.46386798784046|1495.809181207552|143165.46163257834|2.396783048473542E7|              null|
| stddev|0.06646213086143016|3.8680013938746827|180.4377549977533|12806.816272955573|2089356.436455806|0.18557956834383774|8.352648595163723|505.8937639190234| 50494.27617103221|  9307340.299219644|              null|
|    min|                0.0|               0.0|              0.0|               0.0|              0.0|                0.0|              0.0|              0.0|         -146300.0|                0.0|TRAAAAK128F9318786|
|    max|              0.959|             55.42|           2919.0|          407100.0|          4.657E7|              2.647|            117.0|           5834.0|          452500.0|            9.477E7|TRZZZZO128F428E2D4|
+-------+-------------------+------------------+-----------------+------------------+-----------------+-------------------+-----------------+-----------------+------------------+-------------------+------------------+
'''

methods_of_moments.select("sd1", "sd2", "sd3", "sd4", "sd5").describe().show()
'''
+-------+-------------------+------------------+-----------------+------------------+------------------+
|summary|                sd1|               sd2|              sd3|               sd4|               sd5|
+-------+-------------------+------------------+-----------------+------------------+------------------+
|  count|             994623|            994623|           994623|            994623|            994623|
|   mean| 0.1549817600174646|10.384550576952206|526.8139724398065| 35071.97543290272| 5297870.369577217|
| stddev|0.06646213086143017|3.8680013938746827|180.4377549977533|12806.816272955573|2089356.4364558058|
|    min|                0.0|               0.0|              0.0|               0.0|               0.0|
|    max|              0.959|             55.42|           2919.0|          407100.0|           4.657E7|
+-------+-------------------+------------------+-----------------+------------------+------------------+
'''

methods_of_moments.select("a1", "a2", "a3", "a4", "a5", "MSD_TRACKID").describe().show()
'''
+-------+-------------------+-----------------+------------------+------------------+-------------------+------------------+
|summary|                 a1|               a2|                a3|                a4|                 a5|       MSD_TRACKID|
+-------+-------------------+-----------------+------------------+------------------+-------------------+------------------+
|  count|             994623|           994623|            994623|            994623|             994623|            994623|
|   mean|0.35084444325313446|27.46386798784046|1495.8091812075522|143165.46163257837|2.396783048473542E7|              null|
| stddev| 0.1855795683438377|8.352648595163723| 505.8937639190234| 50494.27617103221|  9307340.299219642|              null|
|    min|                0.0|              0.0|               0.0|         -146300.0|                0.0|TRAAAAK128F9318786|
|    max|              2.647|            117.0|            5834.0|          452500.0|            9.477E7|TRZZZZO128F428E2D4|
+-------+-------------------+-----------------+------------------+------------------+-------------------+------------------+
'''


#print correlation matrix
df = methods_of_moments.drop('MSD_TRACKID')
col_names = df.columns
features = df.rdd.map(lambda row: row[0:])
corr_matrix =Statistics.corr(features, method="pearson")

#convert to pandas to print
corr_df = pd.DataFrame(corr_matrix)
corr_df.index, corr_df.columns = col_names, col_names
print(corr_df.to_string())

'''
          sd1       sd2       sd3       sd4       sd5        a1        a2        a3        a4        a5
sd1  1.000000  0.426280  0.296306  0.061039 -0.055336  0.754208  0.497929  0.447565  0.167466  0.100407
sd2  0.426280  1.000000  0.857549  0.609521  0.433797  0.025228  0.406923  0.396354  0.015607 -0.040902
sd3  0.296306  0.857549  1.000000  0.803010  0.682909 -0.082415  0.125910  0.184962 -0.088174 -0.135056
sd4  0.061039  0.609521  0.803010  1.000000  0.942244 -0.327691 -0.223220 -0.158231 -0.245034 -0.220873
sd5 -0.055336  0.433797  0.682909  0.942244  1.000000 -0.392551 -0.355019 -0.285966 -0.260198 -0.211813
a1   0.754208  0.025228 -0.082415 -0.327691 -0.392551  1.000000  0.549015  0.518503  0.347112  0.278513
a2   0.497929  0.406923  0.125910 -0.223220 -0.355019  0.549015  1.000000  0.903367  0.516499  0.422549
a3   0.447565  0.396354  0.184962 -0.158231 -0.285966  0.518503  0.903367  1.000000  0.772807  0.685645
a4   0.167466  0.015607 -0.088174 -0.245034 -0.260198  0.347112  0.516499  0.772807  1.000000  0.984867
a5   0.100407 -0.040902 -0.135056 -0.220873 -0.211813  0.278513  0.422549  0.685645  0.984867  1.000000
'''

#*********************************** Q1b Load the MSD All Music Genre Dataset (MAGD) ***********************************

#the schema of the genre is defined in Data_Processing_Q1_Genre.py

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

genre_count = genre.groupby("genre").agg(F.count(genre.track_id).alias("num_of_tracks"))
genre_count.coalesce(1).write.csv("hdfs:///user/zhl15/outputs/msd/genre/genre_count.csv")
#hdfs dfs -copyToLocal /user/zhl15/outputs/msd/genre/genre_count.csv ~/assignment2/output
genre_count.show()


'''
+--------------+-------------+
|         genre|num_of_tracks|
+--------------+-------------+
|          Folk|         5865|
|        Stage |         1614|
|      Pop_Rock|       238786|
|         Vocal|         6195|
|     Religious|         8814|
|Easy_Listening|         1545|
|     Classical|          556|
|       New Age|         4010|
|    Electronic|        41075|
|          Jazz|        17836|
| International|        14242|
|         Blues|         6836|
|      Children|          477|
|           RnB|        14335|
|           Rap|        20939|
|       Country|        11772|
|   Avant_Garde|         1014|
|         Latin|        17590|
| Comedy_Spoken|         2067|
|        Reggae|         6946|
+--------------+-------------+
'''

style_count = style.groupby("genre").agg(F.count(style.track_id).alias("num_of_tracks"))
style_count.coalesce(1).write.csv("hdfs:///user/zhl15/outputs/msd/genre/style_count.csv")
#hdfs dfs -copyToLocal /user/zhl15/outputs/msd/genre/style_count.csv ~/assignment2/output
style_count.show()
'''
+-------------------+-------------+
|              genre|num_of_tracks|
+-------------------+-------------+
| Folk_International|         9849|
|Country_Traditional|        11164|
|           RnB_Soul|         6238|
|  Metal_Alternative|        14009|
|       Rock_College|        16575|
|  Rock_Contemporary|        16530|
|          Pop_Indie|        18138|
|   Rock_Alternative|        12717|
|       Experimental|        12139|
|        Metal_Heavy|        10784|
|        Electronica|        10987|
|         Grunge_Emo|         6256|
|           Big_Band|         3115|
|             Gospel|         6974|
|        Hip_Hop_Rap|        16100|
|               Punk|         9610|
|          Pop_Latin|         7699|
| Blues_Contemporary|         6874|
|        Metal_Death|         9851|
|              Dance|        15114|
+-------------------+-------------+
'''

top_genre_count = top_genre.groupby("genre").agg(F.count(top_genre.track_id).alias("num_of_tracks"))
top_genre_count.coalesce(1).write.csv("hdfs:///user/zhl15/outputs/msd/genre/top_genre_count.csv")
#hdfs dfs -copyToLocal /user/zhl15/outputs/msd/genre/top_genre_count.csv ~/assignment2/output
top_genre_count.show()
'''
+-------------+-------------+
|        genre|num_of_tracks|
+-------------+-------------+
|         Folk|         5865|
|     Pop_Rock|       238786|
|        Vocal|         6195|
|      New Age|         4010|
|         Jazz|        17836|
|   Electronic|        41075|
|        Blues|         6836|
|International|        14242|
|          Rap|        20939|
|          RnB|        14335|
|      Country|        11772|
|        Latin|        17590|
|       Reggae|         6946|
+-------------+-------------+
'''


#*********************************** Q1c Merge the genres dataset and the audio features dataset ***********************************

methods_of_moments.registerTempTable('methods_of_moments_tbl')
genre.registerTempTable('genre_tbl')
style.registerTempTable('style_tbl')
top_genre.registerTempTable('top_genre_tbl')

methods_of_moments_full_sql = """
        SELECT
            methods_of_moments_tbl.*,
            genre_tbl.genre as genre,
            style_tbl.genre as style,
            top_genre_tbl.genre as top_genre
        FROM 
            methods_of_moments_tbl        
            LEFT JOIN genre_tbl on methods_of_moments_tbl.MSD_TRACKID = genre_tbl.track_id
            LEFT JOIN style_tbl on methods_of_moments_tbl.MSD_TRACKID = style_tbl.track_id
            LEFT JOIN top_genre_tbl on methods_of_moments_tbl.MSD_TRACKID = top_genre_tbl.track_id

"""
methods_of_moments_full = spark.sql(methods_of_moments_full_sql)
methods_of_moments_full.count()
#994623

print(json.dumps(methods_of_moments_full.collect()[10].asDict(), indent=2))
'''
{
  "Method_of_Moments_Overall_Standard_Deviation_1": 0.2765,
  "Method_of_Moments_Overall_Standard_Deviation_2": 9.438,
  "Method_of_Moments_Overall_Standard_Deviation_3": 433.9,
  "Method_of_Moments_Overall_Standard_Deviation_4": 25880.0,
  "Method_of_Moments_Overall_Standard_Deviation_5": 3661000.0,
  "Method_of_Moments_Overall_Average_1": 0.6153,
  "Method_of_Moments_Overall_Average_2": 32.93,
  "Method_of_Moments_Overall_Average_3": 1862.0,
  "Method_of_Moments_Overall_Average_4": 194100.0,
  "Method_of_Moments_Overall_Average_5": 32960000.0,
  "MSD_TRACKID": "TRAABOG128F42955B1",
  "genre": "Pop_Rock",
  "style": "Rock_Contemporary",
  "top_genre": "Pop_Rock"
}
'''

