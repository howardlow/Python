# STAT478-18S1 Assignment 2
# Howard Low, 53626262

#@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@
#======================================= Audio Similarity Q3 =====================================
#@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@

#*********************************** Q3b  Convert the genre column into an integer index  ***********************************
#delete rows with null genre
cleaned_methods_of_moments_full = methods_of_moments_full.filter(methods_of_moments_full.genre.isNotNull())

indexer = StringIndexer(inputCol="genre", outputCol="label")
methods_of_moments_indexed = indexer.fit(cleaned_methods_of_moments_full).transform(cleaned_methods_of_moments_full)
methods_of_moments_indexed.groupby("label").agg(F.count(methods_of_moments_indexed.label).alias("count")).show()
'''
+-----+------+
|label| count|
+-----+------+
| 12.0|  5789|
|  7.0| 11691|
| 13.0|  4000|
|  6.0| 14194|
|  1.0| 40666|
|  4.0| 17504|
| 17.0|  1012|
| 20.0|   200|
|  8.0|  8780|
|  9.0|  6931|
| 14.0|  2067|
|  0.0|237649|
| 18.0|   555|
| 10.0|  6801|
| 16.0|  1535|
|  3.0| 17775|
| 19.0|   463|
|  5.0| 14314|
|  2.0| 20899|
| 15.0|  1613|
| 11.0|  6182|
+-----+------+
'''

#*********************************** Q3c  Repeat Q2 parts (c) - (f) ***********************************


#*********************************** Test/Train Split   ***********************************
#80% for training
seed = 15
fractions = methods_of_moments_indexed.select("label").distinct().withColumn("fraction", lit(0.8)).rdd.collectAsMap()
train_df = methods_of_moments_indexed.stat.sampleBy("label", fractions, seed)
train_df.groupby("label").agg(F.count(train_df.label).alias("count")).show(100)
'''
+-----+------+
|label| count|
+-----+------+
| 12.0|  4592|
|  7.0|  9409|
| 13.0|  3202|
|  6.0| 11361|
|  1.0| 32586|
|  4.0| 14005|
| 17.0|   823|
| 20.0|   160|
|  8.0|  7033|
|  9.0|  5603|
| 14.0|  1651|
|  0.0|190117|
| 10.0|  5421|
| 18.0|   450|
|  3.0| 14230|
| 16.0|  1246|
| 19.0|   363|
|  2.0| 16816|
| 11.0|  4963|
|  5.0| 11552|
| 15.0|  1312|
+-----+------+
'''

#20% for testing
test_df = methods_of_moments_indexed.join(train_df, ["MSD_TRACKID"], "leftanti")
test_df.groupby("label").agg(F.count(test_df.label).alias("count")).show(100)
'''
+-----+-----+
|label|count|
+-----+-----+
| 12.0| 1197|
|  7.0| 2282|
|  6.0| 2833|
| 13.0|  798|
|  4.0| 3499|
|  1.0| 8080|
| 17.0|  189|
| 20.0|   40|
|  8.0| 1747|
|  9.0| 1328|
| 14.0|  416|
|  0.0|47532|
| 10.0| 1380|
| 18.0|  105|
|  3.0| 3545|
| 16.0|  289|
| 19.0|  100|
|  2.0| 4083|
|  5.0| 2762|
| 11.0| 1219|
| 15.0|  301|
+-----+-----+
'''


#*********************************** Train the models   ***********************************
assembler = (
  VectorAssembler()
  .setInputCols(["sd1","sd2","sd3","sd4","sd5","a1","a2","a3","a4","a5"])
  .setOutputCol('features')
)

training = assembler.transform(train_df)
training.show()

testing = assembler.transform(test_df)
testing.show()

#****** Logistic Regression ******
##https://spark.apache.org/docs/2.1.0/ml-classification-regression.html#one-vs-rest-classifier-aka-one-vs-all

#****** Logistic Regression One Vs Rest******
lr = LogisticRegression(maxIter=10, featuresCol="features", labelCol="label")
ovr = OneVsRest(classifier=lr)
lrModel = ovr.fit(training)


#****** Logistic Regression Normal******
lr = LogisticRegression(maxIter=10, featuresCol="features", labelCol="label")
lrModel = lr.fit(training)


#*********************************** Test the models   ***********************************
def multiclass_evaluator(df):
    evaluator = MulticlassClassificationEvaluator(predictionCol="prediction")
    metrics = {
    'precision':evaluator.evaluate(df, {evaluator.metricName: "weightedPrecision"}),
    'recall':evaluator.evaluate(df, {evaluator.metricName: "weightedRecall"}),
    'accuracy':evaluator.evaluate(df, {evaluator.metricName: "accuracy"}),
    'f1':evaluator.evaluate(df, {evaluator.metricName: "f1"})
    }
    return metrics

#****** Logistic Regression One Vs Rest******
lr_predictions = lrModel.transform(testing)

lrMetrics = multiclass_evaluator(lr_predictions)
print(lrMetrics)
'''
{'precision': 0.3950841615329622, 'recall': 0.5720871902060317, 'accuracy': 0.5720871902060316, 'f1': 0.4403477779264297}
'''

#****** Logistic Regression Normal******
lr_predictions = lrModel.transform(testing)

lrMetrics = multiclass_evaluator(lr_predictions)
print(lrMetrics)
'''
{'precision': 0.37436182105438354, 'recall': 0.5629565655475188, 'accuracy': 0.5629565655475188, 'f1': 0.42013995216394523}
'''

#****** Logistic Regression ******
lr = LogisticRegression(featuresCol="features", labelCol="label")
pipeline = Pipeline(stages=[lr])
paramGrid = ParamGridBuilder() \
.addGrid(lr.regParam, [0.5, 0.1, 0.01]) \
.addGrid(lr.threshold, [0.75, 0.5, 0.25]) \
.addGrid(lr.elasticNetParam, [0.0, 0.1, 0.2]) \
.addGrid(lr.maxIter, [2, 5, 15]) \
.build()

lr_crossval = CrossValidator(estimator=pipeline,
                          estimatorParamMaps=paramGrid,
                          evaluator=MulticlassClassificationEvaluator(),
                          numFolds=5)


#training
lr_cvModel = lr_crossval.fit(training)

#testing
lr_cvpredictions = lr_cvModel.transform(testing)

#print metrics
lr_cvMetrics = multiclass_evaluator(lr_cvpredictions)
print(lr_cvMetrics)
'''
{'precision': 0.38130072311966434, 'recall': 0.5696745297103614, 'accuracy': 0.5696745297103613, 'f1': 0.4278437302330929}
'''
evaluator = MulticlassClassificationEvaluator()
print('Test Area Under ROC', evaluator.evaluate(lr_cvpredictions))
'''
Test Area Under ROC 0.4196178856894557
'''

#best model
lr_bestModel = lr_cvModel.bestModel.stages[0]

bestLRParam = {
  'regParam':lr_bestModel._java_obj.getRegParam(),
  'maxIter':lr_bestModel._java_obj.getMaxIter(),
  'elasticNetParam':lr_bestModel._java_obj.getElasticNetParam(),
  'threshold':lr_bestModel._java_obj.getThreshold()
}
'''
In [11]: bestLRParam
Out[11]: {'regParam': 0.01, 'maxIter': 15, 'elasticNetParam': 0.0, 'threshold': 0.75}
'''
