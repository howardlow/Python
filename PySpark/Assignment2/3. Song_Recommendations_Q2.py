# STAT478-18S1 Assignment 2
# Howard Low, 53626262

#@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@
#======================================= Song Recommendation Q2  =================================
#@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@

#*********************************** Q2a Train Alternating Least Squares (ALS)***********************************
#https://www.youtube.com/watch?v=FgGjc5oabrA
#a very good video about ALS

#Load from csv file which output was saved at 3.Song_Recommendation_Q1.py
schema_tasteprofile = StructType([
    StructField("song_id", StringType(), True),
    StructField("user_id", StringType(), True),
    StructField("play_count", IntegerType(), True)
])

schema_song_id_mapping = StructType([
    StructField("song_id", StringType(), True),
    StructField("numeric_song_id", IntegerType(), True)
])

schema_user_id_mapping = StructType([
    StructField("user_id", StringType(), True),
    StructField("numeric_user_id", IntegerType(), True)
])

train_df = (
    spark.read.format("com.databricks.spark.csv")
    .option("header", "true")
    .option("inferSchema", "false")
    .schema(schema_tasteprofile)
    .load("hdfs:///user/zhl15/outputs/msd/song_recommendations/q1e/tasteprofile_train.csv")
)

test_df = (
    spark.read.format("com.databricks.spark.csv")
    .option("header", "true")
    .option("inferSchema", "false")
    .schema(schema_tasteprofile)
    .load("hdfs:///user/zhl15/outputs/msd/song_recommendations/q1e/tasteprofile_test.csv")
)

song_id_mapping_df = (
    spark.read.format("com.databricks.spark.csv")
    .option("header", "true")
    .option("inferSchema", "false")
    .schema(schema_song_id_mapping)
    .load("hdfs:///user/zhl15/outputs/msd/song_recommendations/q1e/song_id_mapping.csv")
)

user_id_mapping_df = (
    spark.read.format("com.databricks.spark.csv")
    .option("header", "true")
    .option("inferSchema", "false")
    .schema(schema_user_id_mapping)
    .load("hdfs:///user/zhl15/outputs/msd/song_recommendations/q1e/user_id_mapping.csv")
)

#join with numeric song id
training = train_df.join(song_id_mapping_df, train_df.song_id == song_id_mapping_df.song_id).drop(song_id_mapping_df.song_id)
testing = test_df.join(song_id_mapping_df, test_df.song_id == song_id_mapping_df.song_id).drop(song_id_mapping_df.song_id)


#join with numeric user id
training = training.join(user_id_mapping_df, training.user_id == user_id_mapping_df.user_id).drop(user_id_mapping_df.user_id)
testing = testing.join(user_id_mapping_df, testing.user_id == user_id_mapping_df.user_id).drop(user_id_mapping_df.user_id)

als = ALS(userCol="numeric_user_id", itemCol="numeric_song_id", ratingCol="play_count", 
	coldStartStrategy="drop", nonnegative = True)

#Define evalutor as RMSE
evaluator = RegressionEvaluator(metricName="rmse", labelCol="play_count", predictionCol="prediction")

#Tune model using ParamGridBuilder
param_grid = ParamGridBuilder()\
			.addGrid(als.rank, [12,13,14])\
			.addGrid(als.maxIter, [18,19,20])\
			.addGrid(als.regParam, [.17,.18,.19])\
			.build()			


#using cv to find best model
cv = CrossValidator(estimator=als,
                        estimatorParamMaps=param_grid,
                        evaluator=evaluator,
                        numFolds=3) 


model = cv.fit(training)

best_model = model.bestModel

predictions = best_model.transform(testing)
rmse = evaluator.evaluate(predictions)
print("Root-mean-square error = " + str(rmse))

#Root-mean-square error = 3.8223998365947263

#*********************************** Q2b Generate some recommendations for users***********************************
def get_recs_for_user(recs):
	recs = recs.select("recommendations.numeric_song_id", "recommendations.rating")
	songs = recs.select("numeric_song_id").toPandas().iloc[0,0]
	rating = recs.select("rating").toPandas().iloc[0,0]
	m = pd.DataFrame(songs, columns = ["numeric_song_id"])
	m["rating"] = rating
	m_ps = sqlContext.createDataFrame(m)
	return m_ps


# Generate top 10 songs recommendations for each user
userRecs = best_model.recommendForAllUsers(10)

####manually check for user_id = 04b7115ee6b0866cceb9f71d1eff97a34e1e0dbb####
user1 = testing.filter(testing.user_id == '04b7115ee6b0866cceb9f71d1eff97a34e1e0dbb')
user1.show(100, truncate = False)
'''
+------------------+----------------------------------------+----------+---------------+---------------+
|song_id           |user_id                                 |play_count|numeric_song_id|numeric_user_id|
+------------------+----------------------------------------+----------+---------------+---------------+
|SOGTWVV12AB0180C03|04b7115ee6b0866cceb9f71d1eff97a34e1e0dbb|2         |2587           |9              |
|SOKATYE12AB0188696|04b7115ee6b0866cceb9f71d1eff97a34e1e0dbb|1         |2298           |9              |
|SOAFQMR12A6D4F755A|04b7115ee6b0866cceb9f71d1eff97a34e1e0dbb|1         |3914           |9              |
|SOJQKCE12A67020846|04b7115ee6b0866cceb9f71d1eff97a34e1e0dbb|7         |3765           |9              |
|SOLDJQS12A8C140B19|04b7115ee6b0866cceb9f71d1eff97a34e1e0dbb|1         |4005           |9              |
|SOFGKEH12A67020E3A|04b7115ee6b0866cceb9f71d1eff97a34e1e0dbb|3         |5886           |9              |
|SOSJAMD12AB017E3A6|04b7115ee6b0866cceb9f71d1eff97a34e1e0dbb|3         |5911           |9              |
|SOWNIWL12AB017B720|04b7115ee6b0866cceb9f71d1eff97a34e1e0dbb|5         |5912           |9              |
|SOEAVMU12AB017E234|04b7115ee6b0866cceb9f71d1eff97a34e1e0dbb|1         |6675           |9              |
|SOFTZUB12AB0188C67|04b7115ee6b0866cceb9f71d1eff97a34e1e0dbb|1         |6676           |9              |
|SOHFJAQ12AB017E4AF|04b7115ee6b0866cceb9f71d1eff97a34e1e0dbb|5         |6602           |9              |
|SOHVYFM12A58A77A42|04b7115ee6b0866cceb9f71d1eff97a34e1e0dbb|1         |8324           |9              |
|SOZAKZH12AB017C7A7|04b7115ee6b0866cceb9f71d1eff97a34e1e0dbb|3         |8631           |9              |
|SOFUPXW12A8C130D7C|04b7115ee6b0866cceb9f71d1eff97a34e1e0dbb|1         |10058          |9              |
|SOJVLVS12AB01861DB|04b7115ee6b0866cceb9f71d1eff97a34e1e0dbb|1         |11093          |9              |
|SODGGNI12AB017E090|04b7115ee6b0866cceb9f71d1eff97a34e1e0dbb|1         |4475           |9              |
|SOOCUBP12AB018924D|04b7115ee6b0866cceb9f71d1eff97a34e1e0dbb|5         |4484           |9              |
|SOEYWYP12A6D4F5E9D|04b7115ee6b0866cceb9f71d1eff97a34e1e0dbb|1         |7433           |9              |
|SOQFTUV12A6D4F6351|04b7115ee6b0866cceb9f71d1eff97a34e1e0dbb|1         |7445           |9              |
|SOGAXZQ12A8C13EFF4|04b7115ee6b0866cceb9f71d1eff97a34e1e0dbb|4         |96             |9              |
|SOKQYVG12A58A75445|04b7115ee6b0866cceb9f71d1eff97a34e1e0dbb|3         |98             |9              |
|SOLLVFE12A6D4F9793|04b7115ee6b0866cceb9f71d1eff97a34e1e0dbb|1         |10             |9              |
|SODJNDO12A8C13A0C0|04b7115ee6b0866cceb9f71d1eff97a34e1e0dbb|2         |2977           |9              |
+------------------+----------------------------------------+----------+---------------+---------------+
'''

### join with metadata to find insights ###
user1 = user1.join(metadata, user1.song_id == metadata.song_id).select(user1.song_id, user1.user_id, user1.play_count, metadata.title)
user1.show(100, truncate = False)
'''
+------------------+----------------------------------------+----------+--------------------------------------+
|song_id           |user_id                                 |play_count|title                                 |
+------------------+----------------------------------------+----------+--------------------------------------+
|SOFGKEH12A67020E3A|04b7115ee6b0866cceb9f71d1eff97a34e1e0dbb|3         |Beautiful                             |
|SOGAXZQ12A8C13EFF4|04b7115ee6b0866cceb9f71d1eff97a34e1e0dbb|4         |Tu Y Yo Somos Uno Mismo               |
|SOKQYVG12A58A75445|04b7115ee6b0866cceb9f71d1eff97a34e1e0dbb|3         |Wayfaring Stranger                    |
|SOSJAMD12AB017E3A6|04b7115ee6b0866cceb9f71d1eff97a34e1e0dbb|3         |Rumours                               |
|SOWNIWL12AB017B720|04b7115ee6b0866cceb9f71d1eff97a34e1e0dbb|5         |Microdaze                             |
|SOEAVMU12AB017E234|04b7115ee6b0866cceb9f71d1eff97a34e1e0dbb|1         |Year of the Suckerpunch               |
|SOFTZUB12AB0188C67|04b7115ee6b0866cceb9f71d1eff97a34e1e0dbb|1         |High Noon                             |
|SOHFJAQ12AB017E4AF|04b7115ee6b0866cceb9f71d1eff97a34e1e0dbb|5         |West One (Shine On Me)                |
|SOEYWYP12A6D4F5E9D|04b7115ee6b0866cceb9f71d1eff97a34e1e0dbb|1         |Makes Me Wonder                       |
|SOQFTUV12A6D4F6351|04b7115ee6b0866cceb9f71d1eff97a34e1e0dbb|1         |Sacrifice                             |
|SOGTWVV12AB0180C03|04b7115ee6b0866cceb9f71d1eff97a34e1e0dbb|2         |Where I Come From                     |
|SOHVYFM12A58A77A42|04b7115ee6b0866cceb9f71d1eff97a34e1e0dbb|1         |Life Without You                      |
|SOKATYE12AB0188696|04b7115ee6b0866cceb9f71d1eff97a34e1e0dbb|1         |Monday Morning (LP Version)           |
|SOZAKZH12AB017C7A7|04b7115ee6b0866cceb9f71d1eff97a34e1e0dbb|3         |Cherish The Day                       |
|SODJNDO12A8C13A0C0|04b7115ee6b0866cceb9f71d1eff97a34e1e0dbb|2         |Ode to LRC (Album)                    |
|SOAFQMR12A6D4F755A|04b7115ee6b0866cceb9f71d1eff97a34e1e0dbb|1         |Bust A Move                           |
|SOFUPXW12A8C130D7C|04b7115ee6b0866cceb9f71d1eff97a34e1e0dbb|1         |Feel Good Inc (Stanton Warriors Remix)|
|SOJQKCE12A67020846|04b7115ee6b0866cceb9f71d1eff97a34e1e0dbb|7         |You're Crazy                          |
|SOLDJQS12A8C140B19|04b7115ee6b0866cceb9f71d1eff97a34e1e0dbb|1         |Singing in the Rain                   |
|SODGGNI12AB017E090|04b7115ee6b0866cceb9f71d1eff97a34e1e0dbb|1         |Me And John And Paul                  |
|SOOCUBP12AB018924D|04b7115ee6b0866cceb9f71d1eff97a34e1e0dbb|5         |White Heat                            |
|SOJVLVS12AB01861DB|04b7115ee6b0866cceb9f71d1eff97a34e1e0dbb|1         |Delicate Answer                       |
+------------------+----------------------------------------+----------+--------------------------------------+
'''

#songs recommended for user_id 04b7115ee6b0866cceb9f71d1eff97a34e1e0dbb, 9
userRecs.filter(userRecs.numeric_user_id == 9).show(10, truncate = False)
'''
+---------------+-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
|numeric_user_id|recommendations                                                                                                                                                                                  |
+---------------+-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
|9              |[[10310, 29.590792], [573, 23.590702], [11711, 22.897318], [11718, 22.077929], [2895, 20.90114], [8508, 20.874918], [11616, 20.463945], [1120, 19.378235], [3883, 19.195427], [10236, 18.686974]]|
+---------------+-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
'''

user_record_1 = get_recs_for_user(userRecs.filter(userRecs.numeric_user_id == 9))
'''
+---------------+------------------+
|numeric_song_id|            rating|
+---------------+------------------+
|          10310|29.590791702270508|
|            573|23.590702056884766|
|          11711| 22.89731788635254|
|          11718| 22.07792854309082|
|           2895|20.901140213012695|
|           8508| 20.87491798400879|
|          11616|20.463945388793945|
|           1120| 19.37823486328125|
|           3883| 19.19542694091797|
|          10236|18.686973571777344|
+---------------+------------------+
'''

user_record_1 = user_record_1.join(song_id_mapping_df, user_record_1.numeric_song_id == song_id_mapping_df.numeric_song_id).drop(song_id_mapping_df.numeric_song_id)
user_record_1 = user_record_1.join(metadata, user_record_1.song_id == metadata.song_id).select(user_record_1.song_id, user_record_1.rating, metadata.title)

user_record_1.show(100, truncate = False)
'''
+------------------+------------------+----------------------------------+
|song_id           |rating            |title                             |
+------------------+------------------+----------------------------------+
|SOACBLB12AB01871C7|23.590702056884766|221                               |
|SOESNDU12AB01856BA|19.37823486328125 |Limousine                         |
|SOBUMSE12AC468B4E0|20.87491798400879 |I Mörkret Med Dej                 |
|SOXMEHM12AB0185E37|20.901140213012695|Si Quieres Mátarme (feat. Feloman)|
|SOHRJHN12A6D4FCA67|18.686973571777344|Lonesome Road                     |
|SOHSCKX12AB01810A3|19.19542694091797 |Saeed                             |
|SODBVGA12A6BD4D3C0|29.590791702270508|Disco Friends                     |
|SOGLTST12A81C21560|22.89731788635254 |Lejla                             |
|SORXCIY12A8C13C4BF|20.463945388793945|No Lo Ves                         |
|SOXEIUT12AC468D7B3|22.07792854309082 |Hey                               |
+------------------+------------------+----------------------------------+
'''

####manually check for user_id = 28f35c1bd93b75ece3fd4f44028c2346f8749a7e###
user2 = testing.filter(testing.user_id == '28f35c1bd93b75ece3fd4f44028c2346f8749a7e')
user2.show(100, truncate = False)

'''
+------------------+----------------------------------------+----------+---------------+---------------+
|song_id           |user_id                                 |play_count|numeric_song_id|numeric_user_id|
+------------------+----------------------------------------+----------+---------------+---------------+
|SOLRGVL12A8C143BC3|28f35c1bd93b75ece3fd4f44028c2346f8749a7e|1         |720            |86             |
|SOHUETB12A6D4FA4CB|28f35c1bd93b75ece3fd4f44028c2346f8749a7e|14        |4186           |86             |
|SOKUPEJ12A6D4FB6F2|28f35c1bd93b75ece3fd4f44028c2346f8749a7e|1         |4064           |86             |
|SOKLRPJ12A8C13C3FE|28f35c1bd93b75ece3fd4f44028c2346f8749a7e|1         |5163           |86             |
|SORUFPG12AB0186A4D|28f35c1bd93b75ece3fd4f44028c2346f8749a7e|1         |5599           |86             |
|SOATSLL12AB017B862|28f35c1bd93b75ece3fd4f44028c2346f8749a7e|1         |6266           |86             |
|SOOGBWC12A8C140B96|28f35c1bd93b75ece3fd4f44028c2346f8749a7e|1         |5926           |86             |
|SORHUJT12A8C13B85E|28f35c1bd93b75ece3fd4f44028c2346f8749a7e|1         |7024           |86             |
|SOUPMLF12A6701EAFE|28f35c1bd93b75ece3fd4f44028c2346f8749a7e|1         |6749           |86             |
|SOAPELR12A6701F47B|28f35c1bd93b75ece3fd4f44028c2346f8749a7e|1         |8215           |86             |
|SOHYSXA12AB0186704|28f35c1bd93b75ece3fd4f44028c2346f8749a7e|1         |8145           |86             |
|SOSENYY12AC468B565|28f35c1bd93b75ece3fd4f44028c2346f8749a7e|2         |8593           |86             |
|SOZVCRW12A67ADA0B7|28f35c1bd93b75ece3fd4f44028c2346f8749a7e|1         |8136           |86             |
|SOBIFBZ12AF72A2DAF|28f35c1bd93b75ece3fd4f44028c2346f8749a7e|1         |10750          |86             |
|SOLFXKT12AB017E3E0|28f35c1bd93b75ece3fd4f44028c2346f8749a7e|1         |10332          |86             |
|SONTFRJ12A58A7EEC6|28f35c1bd93b75ece3fd4f44028c2346f8749a7e|1         |10674          |86             |
|SORHWHC12A8C13A547|28f35c1bd93b75ece3fd4f44028c2346f8749a7e|1         |10345          |86             |
|SOWUACP12A8C13A586|28f35c1bd93b75ece3fd4f44028c2346f8749a7e|5         |10421          |86             |
|SOMEOFI12A6310DC2E|28f35c1bd93b75ece3fd4f44028c2346f8749a7e|1         |11258          |86             |
|SOOZIKI12A8C1397E3|28f35c1bd93b75ece3fd4f44028c2346f8749a7e|3         |11431          |86             |
|SOVZYVV12A6701D29F|28f35c1bd93b75ece3fd4f44028c2346f8749a7e|1         |11432          |86             |
|SOHFKYE12A8C133DF7|28f35c1bd93b75ece3fd4f44028c2346f8749a7e|5         |4816           |86             |
|SOJJBVA12A58A78A79|28f35c1bd93b75ece3fd4f44028c2346f8749a7e|2         |4480           |86             |
|SOYLTSU12A6701DA15|28f35c1bd93b75ece3fd4f44028c2346f8749a7e|1         |4820           |86             |
|SOKTJXL12A8C13C90B|28f35c1bd93b75ece3fd4f44028c2346f8749a7e|1         |7566           |86             |
|SOSNMLG12A58A7C899|28f35c1bd93b75ece3fd4f44028c2346f8749a7e|1         |429            |86             |
|SOZBZSY12A6D4FA404|28f35c1bd93b75ece3fd4f44028c2346f8749a7e|2         |19             |86             |
|SOJFARO12AF72A709A|28f35c1bd93b75ece3fd4f44028c2346f8749a7e|5         |1486           |86             |
|SOOCWLV12A8C1365EB|28f35c1bd93b75ece3fd4f44028c2346f8749a7e|3         |1833           |86             |
|SOCGXXL12B0B808865|28f35c1bd93b75ece3fd4f44028c2346f8749a7e|1         |3046           |86             |
|SOMZIYZ12AB018C622|28f35c1bd93b75ece3fd4f44028c2346f8749a7e|4         |3302           |86             |
|SORJOVT12AB018C4B4|28f35c1bd93b75ece3fd4f44028c2346f8749a7e|1         |3099           |86             |
|SOXXFFO12AF72AB662|28f35c1bd93b75ece3fd4f44028c2346f8749a7e|2         |3304           |86             |
|SOZBZFF12A6310F12D|28f35c1bd93b75ece3fd4f44028c2346f8749a7e|1         |2964           |86             |
+------------------+----------------------------------------+----------+---------------+---------------+
'''

### join with metadata to find insights ###
user2 = user2.join(metadata, user2.song_id == metadata.song_id).select(user2.song_id, user2.user_id, user2.play_count, metadata.title)
user2.show(100, truncate = False)

'''
+------------------+----------------------------------------+----------+---------------------------------------------+
|song_id           |user_id                                 |play_count|title                                        |
+------------------+----------------------------------------+----------+---------------------------------------------+
|SOATSLL12AB017B862|28f35c1bd93b75ece3fd4f44028c2346f8749a7e|1         |The Zookeeper's Boy                          |
|SOOGBWC12A8C140B96|28f35c1bd93b75ece3fd4f44028c2346f8749a7e|1         |LoveStoned/I Think She Knows                 |
|SOSNMLG12A58A7C899|28f35c1bd93b75ece3fd4f44028c2346f8749a7e|1         |Back Down                                    |
|SOZBZSY12A6D4FA404|28f35c1bd93b75ece3fd4f44028c2346f8749a7e|2         |Boys Don't Cry                               |
|SOLRGVL12A8C143BC3|28f35c1bd93b75ece3fd4f44028c2346f8749a7e|1         |Bulletproof                                  |
|SORHUJT12A8C13B85E|28f35c1bd93b75ece3fd4f44028c2346f8749a7e|1         |Long Arm Of The Law                          |
|SOUPMLF12A6701EAFE|28f35c1bd93b75ece3fd4f44028c2346f8749a7e|1         |Civil War                                    |
|SOJFARO12AF72A709A|28f35c1bd93b75ece3fd4f44028c2346f8749a7e|5         |Gardenhead / Leave Me Alone                  |
|SOKTJXL12A8C13C90B|28f35c1bd93b75ece3fd4f44028c2346f8749a7e|1         |Oh!                                          |
|SOOCWLV12A8C1365EB|28f35c1bd93b75ece3fd4f44028c2346f8749a7e|3         |Two Dope Boyz (In A Cadillac)                |
|SOAPELR12A6701F47B|28f35c1bd93b75ece3fd4f44028c2346f8749a7e|1         |Could Well Be In                             |
|SOHYSXA12AB0186704|28f35c1bd93b75ece3fd4f44028c2346f8749a7e|1         |Mockingbird                                  |
|SOSENYY12AC468B565|28f35c1bd93b75ece3fd4f44028c2346f8749a7e|2         |Feel So Good (Album Version)                 |
|SOZVCRW12A67ADA0B7|28f35c1bd93b75ece3fd4f44028c2346f8749a7e|1         |When You Were Young                          |
|SOCGXXL12B0B808865|28f35c1bd93b75ece3fd4f44028c2346f8749a7e|1         |Yesterdays                                   |
|SOCGXXL12B0B808865|28f35c1bd93b75ece3fd4f44028c2346f8749a7e|1         |Yesterdays                                   |
|SOMZIYZ12AB018C622|28f35c1bd93b75ece3fd4f44028c2346f8749a7e|4         |Strong Will Continue                         |
|SORJOVT12AB018C4B4|28f35c1bd93b75ece3fd4f44028c2346f8749a7e|1         |The Pulse                                    |
|SOXXFFO12AF72AB662|28f35c1bd93b75ece3fd4f44028c2346f8749a7e|2         |Deeply Disturbed                             |
|SOZBZFF12A6310F12D|28f35c1bd93b75ece3fd4f44028c2346f8749a7e|1         |Trouble                                      |
|SOHUETB12A6D4FA4CB|28f35c1bd93b75ece3fd4f44028c2346f8749a7e|14        |The Shadow Of Your Smile                     |
|SOKUPEJ12A6D4FB6F2|28f35c1bd93b75ece3fd4f44028c2346f8749a7e|1         |Xxplosive                                    |
|SOBIFBZ12AF72A2DAF|28f35c1bd93b75ece3fd4f44028c2346f8749a7e|1         |Miss Sarajevo                                |
|SOHFKYE12A8C133DF7|28f35c1bd93b75ece3fd4f44028c2346f8749a7e|5         |One Fine Wire                                |
|SOJJBVA12A58A78A79|28f35c1bd93b75ece3fd4f44028c2346f8749a7e|2         |The Seed (2.0)                               |
|SOLFXKT12AB017E3E0|28f35c1bd93b75ece3fd4f44028c2346f8749a7e|1         |Fireflies                                    |
|SONTFRJ12A58A7EEC6|28f35c1bd93b75ece3fd4f44028c2346f8749a7e|1         |Let Me Think About It                        |
|SORHWHC12A8C13A547|28f35c1bd93b75ece3fd4f44028c2346f8749a7e|1         |Gangsta Nation                               |
|SOWUACP12A8C13A586|28f35c1bd93b75ece3fd4f44028c2346f8749a7e|5         |Angels (featuring Gary Numan) (Album Version)|
|SOYLTSU12A6701DA15|28f35c1bd93b75ece3fd4f44028c2346f8749a7e|1         |Bring The Pain                               |
|SOKLRPJ12A8C13C3FE|28f35c1bd93b75ece3fd4f44028c2346f8749a7e|1         |The Scientist                                |
|SOMEOFI12A6310DC2E|28f35c1bd93b75ece3fd4f44028c2346f8749a7e|1         |Evil Deeds                                   |
|SOOZIKI12A8C1397E3|28f35c1bd93b75ece3fd4f44028c2346f8749a7e|3         |Straight To The Bank                         |
|SORUFPG12AB0186A4D|28f35c1bd93b75ece3fd4f44028c2346f8749a7e|1         |Leaders                                      |
|SOVZYVV12A6701D29F|28f35c1bd93b75ece3fd4f44028c2346f8749a7e|1         |No es lo mismo                               |
+------------------+----------------------------------------+----------+---------------------------------------------+
'''


#songs recommended for user_id 28f35c1bd93b75ece3fd4f44028c2346f8749a7e, 86
userRecs.filter(userRecs.numeric_user_id == 86).show(10, truncate = False)
'''
+---------------+-----------------------------------------------------------------------------------------------+
|numeric_user_id|recommendations                                                                                |
+---------------+-----------------------------------------------------------------------------------------------+
|86             |[[11718, 43.473305], [10310, 36.755867], [8526, 35.880188], [616, 27.24944], [2893, 25.871975]]|
+---------------+-----------------------------------------------------------------------------------------------+
'''

user_record_2 = get_recs_for_user(userRecs.filter(userRecs.numeric_user_id == 86))
user_record_2.show()
'''
+---------------+------------------+
|numeric_song_id|            rating|
+---------------+------------------+
|           8776|20.001102447509766|
|           9930| 17.57727813720703|
|           3883| 17.13872718811035|
|           3104|16.144777297973633|
|          11756| 16.07400131225586|
|           5793|16.045568466186523|
|           5258|15.761536598205566|
|           8002|15.672818183898926|
|          11261|15.665571212768555|
|           5115|15.350676536560059|
+---------------+------------------+
'''

user_record_2 = user_record_2.join(song_id_mapping_df, user_record_2.numeric_song_id == song_id_mapping_df.numeric_song_id).drop(song_id_mapping_df.numeric_song_id)
user_record_2 = user_record_2.join(metadata, user_record_2.song_id == metadata.song_id).select(user_record_2.song_id, user_record_2.rating, metadata.title)

user_record_2.show(100, truncate = False)
'''
+------------------+------------------+-----------------------+
|song_id           |rating            |title                  |
+------------------+------------------+-----------------------+
|SOFNERN12A8C13D78D|15.672818183898926|Frosty The Snowman     |
|SORENUH12AB0180C8D|20.001102447509766|No Love                |
|SODAMRS12A8C142682|16.144777297973633|Somewhere in the Middle|
|SOHSCKX12AB01810A3|17.13872718811035 |Saeed                  |
|SOVAGPG12AB0189963|17.57727813720703 |Samba De Una Nota So´  |
|SOUSOKA12AB017D60C|15.350676536560059|Mit Dir                |
|SOESAES12A8C139A7B|16.045568466186523|Feel Alive             |
|SOMDFLO12AB0184570|16.07400131225586 |Freddie's Dead         |
|SOVAUCI12A67020405|15.665571212768555|So Ruff                |
|SOYDHXP12AB01849D4|15.761536598205566|Good Life              |
+------------------+------------------+-----------------------+
'''

#*********************************** Q2c compute the following metrics ***********************************
#compute ground truth set for every users
#sort play_count with the highest value as the 1st ranking
testing = testing.orderBy(F.desc('play_count'))

label = testing.groupby("numeric_user_id").agg(F.collect_list("numeric_song_id").alias("actual"))

def extract_predictions(recommendations):
    labels = []    
    for recommendation in recommendations:
        labels.append(int(recommendation[0]))

    return labels

#intialize UDF
extract_predictions_udf = F.udf(extract_predictions, ArrayType(IntegerType()))
pred = userRecs.withColumn("predictions", extract_predictions_udf(userRecs.recommendations))

#join predictions and actual
predictionAndLabels = pred.join(label, pred.numeric_user_id == label.numeric_user_id).select("predictions", "actual")

#convert dataframe to RDD
tuples = predictionAndLabels.rdd.map(lambda r: (r.predictions, r.actual))
metrics = RankingMetrics(tuples)

metrics.precisionAt(5)
'''
Out[68]: 0.0016959273856443336
'''

metrics.ndcgAt(10)
'''
Out[69]: 0.0016976071703782628
'''

metrics.meanAveragePrecision
'''
Out[70]: 0.00013828980997002453
'''
