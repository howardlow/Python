# STAT478-18S1 Assignment 2
# Howard Low, 53626262

#@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@
#======================================= PROCESSING Q1 MAIN =====================================
#@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@

#*********************************** Q1a ***********************************
# ================================  Schema ==============================
schema_main_analysis = StructType([
    StructField("analysis_sample_rate", IntegerType(), True),
    StructField("audio_md5", StringType(), True),
    StructField("danceability", DoubleType(), True),
    StructField("duration", DoubleType(), True),
    StructField("end_of_fade_in", DoubleType(), True),
    StructField("energy", DoubleType(), True),
    StructField("idx_bars_confidence", IntegerType(), True),
    StructField("idx_bars_start", IntegerType(), True),
    StructField("idx_beats_confidence", IntegerType(), True),
    StructField("idx_beats_start", IntegerType(), True),
    StructField("idx_sections_confidence", IntegerType(), True),
    StructField("idx_sections_start", IntegerType(), True),
    StructField("idx_segments_confidence", IntegerType(), True),
    StructField("idx_segments_loudness_max", IntegerType(), True),
    StructField("idx_segments_loudness_max_time", IntegerType(), True),
    StructField("idx_segments_loudness_start", IntegerType(), True),
    StructField("idx_segments_pitches", IntegerType(), True),
    StructField("idx_segments_start", IntegerType(), True),
    StructField("idx_segments_timbre", IntegerType(), True),
    StructField("idx_tatums_confidence", IntegerType(), True),
    StructField("idx_tatums_start", IntegerType(), True),
    StructField("key", IntegerType(), True),
    StructField("key_confidence", DoubleType(), True),
    StructField("loudness", DoubleType(), True),
    StructField("mode", IntegerType(), True),
    StructField("mode_confidence", DoubleType(), True),
    StructField("start_of_fade_out", DoubleType(), True),
    StructField("tempo", DoubleType(), True),
    StructField("time_signature", IntegerType(), True),    
    StructField("time_signature_confidence", DoubleType(), True),
    StructField("track_id", StringType(), True)
])

schema_main_metadata = StructType([
    StructField("analyzer_version", StringType(), True),
    StructField("artist_7digitalid", IntegerType(), True),
    StructField("artist_familiarity", DoubleType(), True),
    StructField("artist_hotttnesss", DoubleType(), True),
    StructField("artist_id", StringType(), True),
    StructField("artist_latitude", DoubleType(), True),
    StructField("artist_location", StringType(), True),
    StructField("artist_longitude", DoubleType(), True),
    StructField("artist_mbid", StringType(), True),
    StructField("artist_name", StringType(), True),
    StructField("artist_playmeid", IntegerType(), True),
    StructField("genre", StringType(), True),
    StructField("idx_artist_terms", IntegerType(), True),
    StructField("idx_similar_artists", IntegerType(), True),
    StructField("release", StringType(), True),
    StructField("release_7digitalid", IntegerType(), True),
    StructField("song_hotttnesss", DoubleType(), True),
    StructField("song_id", StringType(), True),
    StructField("title", StringType(), True),
    StructField("track_7digitalid", IntegerType(), True) 
])

# ================================  Load ==============================
analysis = (
    spark.read.format("com.databricks.spark.csv")
    .option("header", "true")
    .option("codec", "gzip")
    .option("inferSchema", "false")
    .schema(schema_main_analysis)
    .load("hdfs:///data/msd/main/summary/analysis.csv.gz")
)

metadata = (
    spark.read.format("com.databricks.spark.csv")
    .option("header", "true")
    .option("codec", "gzip")
    .option("inferSchema", "false")
    .schema(schema_main_metadata)
    .load("hdfs:///data/msd/main/summary/metadata.csv.gz")
)

#*********************************** Q1c ***********************************
# ================================  Show and Count ==============================
print(json.dumps(analysis.head().asDict(), indent=2))
'''
{
  "analysis_sample_rate": 22050,
  "audio_md5": "aee9820911781c734e7694c5432990ca",
  "danceability": 0.0,
  "duration": 252.05506,
  "end_of_fade_in": 2.049,
  "energy": 0.0,
  "idx_bars_confidence": 0,
  "idx_bars_start": 0,
  "idx_beats_confidence": 0,
  "idx_beats_start": 0,
  "idx_sections_confidence": 0,
  "idx_sections_start": 0,
  "idx_segments_confidence": 0,
  "idx_segments_loudness_max": 0,
  "idx_segments_loudness_max_time": 0,
  "idx_segments_loudness_start": 0,
  "idx_segments_pitches": 0,
  "idx_segments_start": 0,
  "idx_segments_timbre": 0,
  "idx_tatums_confidence": 0,
  "idx_tatums_start": 0,
  "key": 10,
  "key_confidence": 0.777,
  "loudness": -4.829,
  "mode": 0,
  "mode_confidence": 0.688,
  "start_of_fade_out": 236.635,
  "tempo": 87.002,
  "time_signature": 4,
  "time_signature_confidence": 0.94,
  "track_id": "TRMMMYQ128F932D901"
}
'''

analysis.count()
# 1000000


print(json.dumps(metadata.head().asDict(), indent=2))
'''
{
  "analyzer_version": null,
  "artist_7digitalid": 4069,
  "artist_familiarity": 0.6498221002008776,
  "artist_hotttnesss": 0.3940318927141434,
  "artist_id": "ARYZTJS1187B98C555",
  "artist_latitude": null,
  "artist_location": null,
  "artist_longitude": null,
  "artist_mbid": "357ff05d-848a-44cf-b608-cb34b5701ae5",
  "artist_name": "Faster Pussy cat",
  "artist_playmeid": 44895,
  "genre": null,
  "idx_artist_terms": 0,
  "idx_similar_artists": 0,
  "release": "Monster Ballads X-Mas",
  "release_7digitalid": 633681,
  "song_hotttnesss": 0.5428987432910862,
  "song_id": "SOQMMHC12AB0180CB8",
  "title": "Silent Night",
  "track_7digitalid": 7032331
}
'''

metadata.count()
# 1000000
