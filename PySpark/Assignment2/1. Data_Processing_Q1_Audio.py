# STAT478-18S1 Assignment 2
# Howard Low, 53626262

#@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@
#======================================= PROCESSING Q1 AUDIO =====================================
#@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@

#*********************************** Q1a ***********************************
DTYPES_MAPPING = {
    "real": DoubleType(),
    "string": StringType(),
    "NUMERIC": DoubleType(),
    "STRING": StringType(),
}


def attributes_to_schema(hdfs_path, dtypes_mapping=DTYPES_MAPPING):

    rows = spark.read.csv(hdfs_path).collect() # collect will take a dataframe to a python [Row(...), ...]
    structfields = []
    for row in rows:
        colname = row[0]
        dtypestring = row[1]
        structfields.append(StructField(colname, dtypes_mapping[dtypestring], True))

    return StructType(structfields)

def get_all_features():
    files = [
        "/data/msd/audio/attributes/msd-jmir-area-of-moments-all-v1.0.attributes.csv",
        "/data/msd/audio/attributes/msd-jmir-lpc-all-v1.0.attributes.csv",
        "/data/msd/audio/attributes/msd-jmir-methods-of-moments-all-v1.0.attributes.csv",
        "/data/msd/audio/attributes/msd-jmir-mfcc-all-v1.0.attributes.csv",
        "/data/msd/audio/attributes/msd-jmir-spectral-all-all-v1.0.attributes.csv",
        "/data/msd/audio/attributes/msd-jmir-spectral-derivatives-all-all-v1.0.attributes.csv",
        "/data/msd/audio/attributes/msd-marsyas-timbral-v1.0.attributes.csv",
        "/data/msd/audio/attributes/msd-mvd-v1.0.attributes.csv",
        "/data/msd/audio/attributes/msd-rh-v1.0.attributes.csv",
        "/data/msd/audio/attributes/msd-rp-v1.0.attributes.csv",
        "/data/msd/audio/attributes/msd-ssd-v1.0.attributes.csv",
        "/data/msd/audio/attributes/msd-trh-v1.0.attributes.csv",
        "/data/msd/audio/attributes/msd-tssd-v1.0.attributes.csv"
    ]

    audio_features = []
    i=0
    for attributes_file in files:
        features_file = re.sub("\.attributes", "", attributes_file)
        features_file = re.sub("attributes", "features", features_file)
        
        print("audio_features[" + str(i) + "]" + ":  " + features_file)

        schema = attributes_to_schema(attributes_file)
        data = spark.read.schema(schema).csv(features_file)
        audio_features.append(data)
        i += 1

    return audio_features


schema_sample_properities = StructType([
    StructField("track_id", StringType(), True),
    StructField("title", StringType(), True),
    StructField("artist_name", StringType(), True),
    StructField("duration", DoubleType(), True),
    StructField("7digita_Id", IntegerType(), True),
    StructField("sample_bitrate", IntegerType(), True),
    StructField("sample_length", DoubleType(), True),
    StructField("sample_rate", IntegerType(), True),
    StructField("sample_mode", IntegerType(), True),
    StructField("sample_version", IntegerType(), True),
    StructField("filesize", IntegerType(), True)
])


# ================================  Load ==============================
audio_features = get_all_features();

'''
audio_features[0]:  /data/msd/audio/features/msd-jmir-area-of-moments-all-v1.0.csv
audio_features[1]:  /data/msd/audio/features/msd-jmir-lpc-all-v1.0.csv
audio_features[2]:  /data/msd/audio/features/msd-jmir-methods-of-moments-all-v1.0.csv
audio_features[3]:  /data/msd/audio/features/msd-jmir-mfcc-all-v1.0.csv
audio_features[4]:  /data/msd/audio/features/msd-jmir-spectral-all-all-v1.0.csv
audio_features[5]:  /data/msd/audio/features/msd-jmir-spectral-derivatives-all-all-v1.0.csv
audio_features[6]:  /data/msd/audio/features/msd-marsyas-timbral-v1.0.csv
audio_features[7]:  /data/msd/audio/features/msd-mvd-v1.0.csv
audio_features[8]:  /data/msd/audio/features/msd-rh-v1.0.csv
audio_features[9]:  /data/msd/audio/features/msd-rp-v1.0.csv
audio_features[10]:  /data/msd/audio/features/msd-ssd-v1.0.csv
audio_features[11]:  /data/msd/audio/features/msd-trh-v1.0.csv
audio_features[12]:  /data/msd/audio/features/msd-tssd-v1.0.csv
'''

sample_properities = (
    spark.read.format("com.databricks.spark.csv")
    .option("header", "true")
    .option("codec", "gzip")    
    .option("inferSchema", "false")
    .schema(schema_sample_properities)
    .load("hdfs:///data/msd/audio/statistics/sample_properties.csv.gz")
)

#*********************************** Q1c ***********************************
# ================================  Show and Count ==============================

#Attributes and Features
for feature in audio_features:
    print(json.dumps(feature.head().asDict(), indent=2))
    print("")

i=0

for feature in audio_features:
    print("audio_features[" + str(i) + "]" + " count:" + str(feature.count()))
    print("")
    i += 1

'''
audio_features[0] count:994623
audio_features[1] count:994623
audio_features[2] count:994623
audio_features[3] count:994623
audio_features[4] count:994623
audio_features[5] count:994623
audio_features[6] count:995001
audio_features[7] count:994188
audio_features[8] count:994188
audio_features[9] count:994188
audio_features[10] count:994188
audio_features[11] count:994188
audio_features[12] count:994188
'''

#Sample Properities
sample_properities.show(10)

'''
+------------------+--------------------+--------------------+---------+----------+--------------+-------------+-----------+-----------+--------------+--------+
|          track_id|               title|         artist_name| duration|7digita_Id|sample_bitrate|sample_length|sample_rate|sample_mode|sample_version|filesize|
+------------------+--------------------+--------------------+---------+----------+--------------+-------------+-----------+-----------+--------------+--------+
|TRMMMYQ128F932D901|        Silent Night|    Faster Pussy cat|252.05506|   7032331|           128|60.1935770567|      22050|          1|             2|  960887|
|TRMMMKD128F425225D|         Tanssi vaan|    Karkkiautomaatti|156.55138|   1514808|            64|30.2244270016|      22050|          1|             2|  242038|
|TRMMMRX128F93187D9|   No One Could Ever|      Hudson Mohawke|138.97098|   6945353|           128|60.1935770567|      22050|          1|             2|  960887|
|TRMMMCH128F425532C|       Si Vos Querés|         Yerba Brava|145.05751|   2168257|            64|30.2083516484|      22050|          1|             2|  240534|
|TRMMMWA128F426B589|    Tangle Of Aspens|          Der Mystic|514.29832|   2264873|            64|60.3382103611|      22050|          1|             2|  480443|
|TRMMMXN128F42936A5|"Symphony No. 1 G...|    David Montgomery|816.53506|   3360982|           128|30.1360348456|      44100|          0|             1|  481070|
|TRMMMLR128F1494097|    We Have Got Love|  Sasha / Turbulence|212.37506|    552626|            64|60.3542857143|      22050|          1|             2|  480686|
|TRMMMBB12903CB7D21|   2 Da Beat Ch'yall|          Kris Kross|221.20444|   6435649|           128|30.1360348456|      44100|          0|             1|  481070|
|TRMMMHY12903CB53F1|             Goodbye|        Joseph Locke|139.17995|   8376489|           128|60.2459472422|      22050|          1|             2|  961723|
|TRMMMML128F4280EE9|Mama_ mama can't ...|The Sun Harbor's ...|104.48934|   1043208|           206|30.0408163265|      44100|          1|             1|  777413|
+------------------+--------------------+--------------------+---------+----------+--------------+-------------+-----------+-----------+--------------+--------+
'''

sample_properities.count()
# 992865