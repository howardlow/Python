# STAT478-18S1 Assignment 2
# Howard Low, 53626262

#@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@
#======================================= Song Recommendation Q1  =================================
#@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@

#*********************************** Q1a Count distinct songs/user***********************************
#distinct songs excluding those mismatches
tasteprofile.select('song_id').distinct().count()
'''
Out[35]: 378309
'''

#distinct users excluding those mismatches
tasteprofile.select('user_id').distinct().count()
'''
Out[36]: 1019318
'''

#*********************************** Q1b songs has the most active user played***********************************
#most active user
tasteprofile.groupBy('user_id').agg(F.sum('play_count').alias('count')).orderBy(F.desc('count')).show(5, truncate=False)

'''
+----------------------------------------+-----+
|user_id                                 |count|
+----------------------------------------+-----+
|093cb74eb3c517c5179ae24caf0ebec51b24d2a2|13074|
|119b7c88d58d0c6eb051365c103da5caf817bea6|9104 |
|3fa44653315697f42410a30cb766a4eb102080bb|8025 |
|a2679496cd0af9779a92a13ff7c6af5c81ea8c7b|6506 |
|d7d2d888ae04d16e994d6964214a1de81392ee04|6190 |
+----------------------------------------+-----+

'''
#no. of songs that user_id 093cb74eb3c517c5179ae24caf0ebec51b24d2a2 play
#tasteprofile.filter(tasteprofile.user_id =='093cb74eb3c517c5179ae24caf0ebec51b24d2a2').count()
'''
Out[41]: 195
'''
percentage = 195/378309*100
'''
Out[43]: 0.051545165459981133
'''

#*********************************** Q1c Visualize the distribution of song popularity and the distribution of user activity***********************************
user_activity = tasteprofile.groupBy('user_id').agg(F.sum('play_count').alias('count')).orderBy(F.desc('count'))
song_popularity = tasteprofile.groupBy('song_id').agg(F.sum('play_count').alias('count')).orderBy(F.desc('count'))

#user_activity.coalesce(1).write.csv("hdfs:///user/zhl15/outputs/msd/song_recommendations/q1c/user_activity", header = True)
#song_popularity.coalesce(1).write.csv("hdfs:///user/zhl15/outputs/msd/song_recommendations/q1c/song_popularity", header = True)

#hdfs dfs -copyToLocal /user/zhl15/outputs/msd/song_recommendations ~/assignment2/output

#*********************************** Q1d Visualize the distribution of song popularity and the distribution of user activity***********************************

#######Removing songs which have been played less than M times#########
#create an list with distinct counts 
distinct_song_popularity_count = song_popularity.select("count").distinct().orderBy(F.desc('count'))
'''
In [57]: distinct_song_popularity_count.count()
Out[57]: 8000
'''
#identify 25 percentilec count value (which is bottom 2000)
distinct_song_popularity_count.collect()[6000]

'''
In [61]: distinct_song_popularity_count.collect()[6000]
Out[61]: Row(count=2000)
'''
#create a new song popularity with count > 2000
new_song_popularity = song_popularity.filter(song_popularity["count"] > 2000)


######## Removing users which have been played less than N times #########
#create an list with distinct counts 
distinct_user_activity_count = user_activity.select("count").distinct().orderBy(F.desc('count'))

'''
In [84]: distinct_user_activity_count.count()
Out[84]: 2246
'''

#identify 25 percentilec count value (which is bottom 1684)
distinct_user_activity_count.collect()[1684]

'''
In [87]: distinct_user_activity_count.collect()[1684]
Out[87]: Row(count=564)

'''

#create a new song popularity with count > 2000
new_user_activity = user_activity.filter(user_activity["count"] > 564)

#new_user_activity.filter(new_user_activity.user_id == '67be68256f7d872df2b63f6454c5f375c7405021').show()
######## Create new user-song tasteprofile #########

tasteprofile.registerTempTable('tasteprofile_tbl')
new_song_popularity.registerTempTable('new_song_popularity_tbl')
new_user_activity.registerTempTable('new_user_activity_tbl')

new_tasteprofile_sql = """
        SELECT
            tasteprofile_tbl.*        
        FROM 
            tasteprofile_tbl        
            JOIN new_song_popularity_tbl ON tasteprofile_tbl.song_id = new_song_popularity_tbl.song_id
            JOIN new_user_activity_tbl ON tasteprofile_tbl.user_id = new_user_activity_tbl.user_id
"""

new_tasteprofile = spark.sql(new_tasteprofile_sql)
new_tasteprofile.count()

'''
Out[113]: 3041584
'''

######## Removing users that has less than X no. of songs in the new_tasteprofile #########
user_songs = new_tasteprofile.groupBy('user_id').agg(F.count('user_id').alias('count')).orderBy(F.desc('count'))
user_songs.show(10, truncate = False);
'''
+----------------------------------------+-----+
|user_id                                 |count|
+----------------------------------------+-----+
|c1255748c06ee3f6440c51c439446886c7807095|834  |
|4e73d9e058d2b1f2dba9c1fe4a8f416f9f58364f|832  |
|119b7c88d58d0c6eb051365c103da5caf817bea6|796  |
|8cb51abc6bf8ea29341cb070fe1e1af5e4c3ffcc|787  |
|0c2932cb475b83b61039bdfbb72c14580b8fad2b|768  |
|db6a78c78c9239aba33861dae7611a6893fb27d5|717  |
|b7c24f770be6b802805ac0e2106624a517643c17|712  |
|96f7b4f800cafef33eae71a6bc44f7139f63cd7a|704  |
|736083bd7ecd162effb7668cab6c281945762e85|701  |
|6d625c6557df84b60d90426c0116138b617b9449|698  |
+----------------------------------------+-----+
'''

distinct_user_songs_count = user_songs.select("count").distinct().orderBy(F.desc('count'))
distinct_user_songs_count.count()
'''
In [124]: distinct_user_songs_count.count()
Out[124]: 496
'''

#identify 25 percentilec count value (which is bottom 370)
distinct_user_songs_count.collect()[370]

'''
Out[125]: Row(count=126)
'''

#create a new song popularity with count > 2000
new_user_songs = user_songs.filter(user_songs["count"] > 126)


#remove all the users that are not in new_user_songs
new_tasteprofile.registerTempTable('new_tasteprofile_tbl')
new_user_songs.registerTempTable('new_user_songs_tbl')

cleaned_tasteprofile_sql = """
        SELECT
            new_tasteprofile_tbl.*        
        FROM 
            new_tasteprofile_tbl        
            JOIN new_user_songs_tbl ON new_tasteprofile_tbl.user_id = new_user_songs_tbl.user_id
"""

cleaned_tasteprofile = spark.sql(cleaned_tasteprofile_sql)
cleaned_tasteprofile.count()
'''
Out[127]: 1643412
'''

cleaned_tasteprofile = cleaned_tasteprofile.withColumn('Id', monotonically_increasing_id())


#*********************************** Q1e Test Train Split ***********************************
#20% for testing
seed = 15
fractions = cleaned_tasteprofile.select("user_id").distinct().withColumn("fraction", lit(0.2)).rdd.collectAsMap()
test_df = cleaned_tasteprofile.stat.sampleBy("user_id", fractions, seed)
test_df.groupby("user_id").agg(F.count(test_df.user_id).alias("count")).show(10, truncate = False)

'''
+----------------------------------------+-----+
|user_id                                 |count|
+----------------------------------------+-----+
|003d0f3aac94fd261bb74c0124a90750579972d4|38   |
|0089467d6dfe5e1c2d750c30a0490dc31f7ead81|41   |
|018a9cd406ed6cf3e2e59da62fdb656144b4db9c|32   |
|01ce540d5a1b6d6015ad26ba87392e76cad44d5d|35   |
|01f93ae419fbba37a2989ef8ddda07170d6c58d4|23   |
|0201090d0a5acd36931f765779421bf86f7c1a73|48   |
|02ac25b2cd40d5e2933792b4a1254d695b7abea6|44   |
|0351986f4813752a3f8886813f4ae685160f8b46|67   |
|04b7115ee6b0866cceb9f71d1eff97a34e1e0dbb|23   |
|050199757f323eee50c5f8d3d3a5bd0d48ea0ca9|63   |
+----------------------------------------+-----+
'''

test_df.count()
'''
Out[130]: 328960
'''

#80% for training
train_df = cleaned_tasteprofile.join(test_df, ["id"], "leftanti")
train_df.groupby("user_id").agg(F.count(train_df.user_id).alias("count")).show(10, truncate = False)

'''
+----------------------------------------+-----+
|user_id                                 |count|
+----------------------------------------+-----+
|003d0f3aac94fd261bb74c0124a90750579972d4|140  |
|0089467d6dfe5e1c2d750c30a0490dc31f7ead81|145  |
|018a9cd406ed6cf3e2e59da62fdb656144b4db9c|142  |
|01ce540d5a1b6d6015ad26ba87392e76cad44d5d|104  |
|01f93ae419fbba37a2989ef8ddda07170d6c58d4|110  |
|0201090d0a5acd36931f765779421bf86f7c1a73|155  |
|02ac25b2cd40d5e2933792b4a1254d695b7abea6|166  |
|0351986f4813752a3f8886813f4ae685160f8b46|199  |
|04b7115ee6b0866cceb9f71d1eff97a34e1e0dbb|119  |
|050199757f323eee50c5f8d3d3a5bd0d48ea0ca9|209  |
+----------------------------------------+-----+
'''

train_df.count()
'''
Out[135]: 1314452
'''

#testing
train_df.select('user_id').distinct().count()
#8373

test_df.select('user_id').distinct().count()
#8373

#data cleaning up
train_df = train_df.drop("id")
test_df = test_df.drop("id")

#write to output
train_df.coalesce(1).write.csv("hdfs:///user/zhl15/outputs/msd/song_recommendations/q1e/tasteprofile_train.csv", header = True)
test_df.coalesce(1).write.csv("hdfs:///user/zhl15/outputs/msd/song_recommendations/q1e/tasteprofile_test.csv", header = True)

#1.create song_id as numerical
unique_songs = cleaned_tasteprofile.select('song_id').distinct()
unique_songs = unique_songs.withColumn('numeric_song_id', monotonically_increasing_id())
windowSpec = W.orderBy("numeric_song_id")
unique_songs = unique_songs.withColumn("numeric_song_id", F.row_number().over(windowSpec))
unique_songs.coalesce(1).write.csv("hdfs:///user/zhl15/outputs/msd/song_recommendations/q1e/song_id_mapping.csv", header = True)

#2.create user_id as numerical
unique_users = cleaned_tasteprofile.select('user_id').distinct()
unique_users = unique_users.withColumn('numeric_user_id', monotonically_increasing_id())
windowSpec = W.orderBy("numeric_user_id")
unique_users = unique_users.withColumn("numeric_user_id", F.row_number().over(windowSpec))
unique_users.coalesce(1).write.csv("hdfs:///user/zhl15/outputs/msd/song_recommendations/q1e/user_id_mapping.csv", header = True)

