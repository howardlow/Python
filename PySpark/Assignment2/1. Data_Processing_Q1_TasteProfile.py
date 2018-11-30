# STAT478-18S1 Assignment 2
# Howard Low, 53626262

#@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@
#======================================= PROCESSING Q1 TASTEPROFILE ==============================
#@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@

#*********************************** Q1a ***********************************

# ================================  Schema ==============================
schema_tasteprofile = StructType([
    StructField("user_id", StringType(), True),
    StructField("song_id", StringType(), True),
    StructField("play_count", IntegerType(), True)
])

schema_mismatches = StructType([
    StructField("song_id", StringType(), True),
    StructField("track_id", StringType(), True),
])

# ================================  Load ==============================
#triplets
triplets = (
    spark.read.format("com.databricks.spark.csv")
    .option("header", "false")
    .option("delimiter", "\t")
    .option("inferSchema", "false")
    .schema(schema_tasteprofile)
    .load("hdfs:///data/msd/tasteprofile/triplets.tsv")
)

#mismatches
mismatches = spark.read.text("hdfs:///data/msd/tasteprofile/mismatches/sid_mismatches.txt")

mismatches = mismatches.select(
    F.trim(mismatches.value.substr(9,18)).alias('song_id').cast(schema_mismatches['song_id'].dataType),
    F.trim(mismatches.value.substr(28,18)).alias('track_id').cast(schema_mismatches['track_id'].dataType)
)

#manually accepted mismatches
manually_accepted_mismatches = spark.read.text("hdfs:///data/msd/tasteprofile/mismatches/sid_matches_manually_accepted.txt")

manually_accepted_mismatches = manually_accepted_mismatches.select(
    F.trim(manually_accepted_mismatches.value.substr(11,18)).alias('song_id').cast(schema_mismatches['song_id'].dataType),
    F.trim(manually_accepted_mismatches.value.substr(29,19)).alias('track_id').cast(schema_mismatches['track_id'].dataType)
)

#remove tidy data
manually_accepted_mismatches = manually_accepted_mismatches.filter(F.length(manually_accepted_mismatches.song_id) == 18)


#*********************************** Q1c ***********************************

# ================================  Show and Count ==============================
triplets.show(10)
'''
+--------------------+------------------+----------+
|             user_id|           song_id|play_count|
+--------------------+------------------+----------+
|f1bfc2a4597a3642f...|SOQEFDN12AB017C52B|         1|
|f1bfc2a4597a3642f...|SOQOIUJ12A6701DAA7|         2|
|f1bfc2a4597a3642f...|SOQOKKD12A6701F92E|         4|
|f1bfc2a4597a3642f...|SOSDVHO12AB01882C7|         1|
|f1bfc2a4597a3642f...|SOSKICX12A6701F932|         1|
|f1bfc2a4597a3642f...|SOSNUPV12A8C13939B|         1|
|f1bfc2a4597a3642f...|SOSVMII12A6701F92D|         1|
|f1bfc2a4597a3642f...|SOTUNHI12B0B80AFE2|         1|
|f1bfc2a4597a3642f...|SOTXLTZ12AB017C535|         1|
|f1bfc2a4597a3642f...|SOTZDDX12A6701F935|         1|
+--------------------+------------------+----------+
'''

triplets.count()
# 48373586

mismatches.show(10)
'''
+------------------+------------------+
|           song_id|          track_id|
+------------------+------------------+
|SOUMNSI12AB0182807|TRMMGKQ128F9325E10|
|SOCMRBE12AB018C546|TRMMREB12903CEB1B1|
|SOLPHZY12AC468ABA8|TRMMBOC12903CEB46E|
|SONGHTM12A8C1374EF|TRMMITP128F425D8D0|
|SONGXCA12A8C13E82E|TRMMAYZ128F429ECE6|
|SOMBCRC12A67ADA435|TRMMNVU128EF343EED|
|SOTDWDK12A8C13617B|TRMMNCZ128F426FF0E|
|SOEBURP12AB018C2FB|TRMMPBS12903CE90E1|
|SOSRJHS12A6D4FDAA3|TRMWMEL128F421DA68|
|SOIYAAQ12A6D4F954A|TRMWHRI128F147EA8E|
+------------------+------------------+
'''

mismatches.count()
# 19094

manually_accepted_mismatches.show(10)
'''
+------------------+------------------+
|           song_id|          track_id|
+------------------+------------------+
|SOFQHZM12A8C142342|TRMWMFG128F92FFEF2|
|SODXUTF12AB018A3DA|TRMWPCD12903CCE5ED|
|SOASCRF12A8C1372E6|TRMHIPJ128F426A2E2|
|SOITDUN12A58A7AACA|TRMHXGK128F42446AB|
|SOLZXUM12AB018BE39|TRMRSOF12903CCF516|
|SOTJTDT12A8C13A8A6|TRMNKQE128F427C4D8|
|SOGCVWB12AB0184CE2|TRMUNCZ128F932A95D|
|SOKDKGD12AB0185E9C|TRMOOAH12903CB4B29|
|SOPPBXP12A8C141194|TRMXJDS128F42AE7CF|
|SODQSLR12A8C133A01|TRWHMXN128F426E03C|
+------------------+------------------+
'''

manually_accepted_mismatches.count()
# 489

#Remove mismatches
tasteprofile = triplets.join(mismatches, ["song_id"],"left_anti")
tasteprofile.count()

