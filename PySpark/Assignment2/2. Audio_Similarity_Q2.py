# STAT478-18S1 Assignment 2
# Howard Low, 53626262

#@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@
#======================================= Audio Similarity Q2 =====================================
#@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@

#*********************************** Q2a  Data Cleaning  ***********************************
#delete rows with null genre
cleaned_methods_of_moments_full = methods_of_moments_full.filter(methods_of_moments_full.genre.isNotNull())

#drop unused columns
#cleaned_methods_of_moments_full = cleaned_methods_of_moments_full.drop("style", "top_genre")

#*********************************** Q2b  Convert the genre column into a column representing if the song is ”Electronic”   ***********************************
#set 1 for Electronic genre and 0 for others
cleaned_methods_of_moments_full = cleaned_methods_of_moments_full.withColumn('label', F.when(F.col('genre')=="Electronic",1.0).otherwise(0.0))

#check class balance
cleaned_methods_of_moments_full.groupby("label").agg(F.count(cleaned_methods_of_moments_full.label).alias("count")).show()
'''
+-----+------+
|label| count|
+-----+------+
|  1.0| 32580|
|  0.0|303820|
+-----+------+
'''

#*********************************** Q2c  Test/Train Split   ***********************************
#80% for training
seed = 12
fractions = cleaned_methods_of_moments_full.select("label").distinct().withColumn("fraction", lit(0.8)).rdd.collectAsMap()
train_df = cleaned_methods_of_moments_full.stat.sampleBy("label", fractions, seed)
train_df.groupby("label").agg(F.count(train_df.label).alias("count")).show()
'''
+-----+------+
|label| count|
+-----+------+
|  1.0| 32580|
|  0.0|303820|
+-----+------+
'''

#20% for testing
test_df = cleaned_methods_of_moments_full.join(train_df, ["MSD_TRACKID"], "leftanti")
test_df.groupby("label").agg(F.count(test_df.label).alias("count")).show()
'''
+-----+-----+
|label|count|
+-----+-----+
|  1.0| 8086|
|  0.0|76134|
+-----+-----+
'''

#*********************************** Q2d  Train the models   ***********************************
#https://stackoverflow.com/questions/44950897/field-features-does-not-exist-sparkml?rq=1
#https://stackoverflow.com/questions/46710934/pyspark-sql-utils-illegalargumentexception-ufield-features-does-not-exist/46729342
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
#initalize logistic regression
lr = LogisticRegression(maxIter=10, featuresCol="features", labelCol="label")

#fit the model
lrModel = lr.fit(training)

print("Coefficients: " + str(lrModel.coefficients))
print("Intercept: " + str(lrModel.intercept))
'''
Coefficients: [9.581844307970197,0.19200298301209517,-0.001338435493625839,-4.253734575177473e-06,-1.055684912868115e-08,-2.042241179346503,-0.16913139402765354,0.00185575177897121,2.9826860559405963e-07,2.553780424246683e-08]
Intercept: -3.072029599112642
'''

#****** Random Forest ******
rf = RandomForestClassifier(numTrees=4 , featuresCol="features", labelCol="label")
rfModel = rf.fit(training)


#****** Gradient-boosted tree classifier ******
gbt = GBTClassifier(maxIter=10, featuresCol="features", labelCol="label")
gbtModel = gbt.fit(training)

#*********************************** Q2e  Test the models   ***********************************
#### Function for calculating binary classification metrics
def binary_evaluator(df):
    total = df.count()
    tp = df[(df.label == 1) & (df.prediction == 1)].count()
    tn = df[(df.label == 0) & (df.prediction == 0)].count()
    fp = df[(df.label == 0) & (df.prediction == 1)].count()
    fn = df[(df.label == 1) & (df.prediction == 0)].count()
    r = float(tp)/(tp + fn)
    p = float(tp) / (tp + fp)
    a = (tp + tn) / total
    f = 1/((1/r+1/p)/2)
    metrics=[tp,tn,fp,fn,total,r,p,a,f]
    return(metrics)

evaluator = BinaryClassificationEvaluator()

#****** Logistic Regression ******
lr_predictions = lrModel.transform(testing)
lrMetrics = binary_evaluator(lr_predictions)
print(lrMetrics)
'''
[88, 76048, 86, 7998, 84220, 0.010883007667573583, 0.5057471264367817, 0.9040132985039183, 0.02130750605326876]
'''
print('Test Area Under ROC', evaluator.evaluate(lr_predictions))
'''
Test Area Under ROC 0.722084581742408
'''

#****** Random Forest ******
rf_predictions = rfModel.transform(testing)
rfMetrics = binary_evaluator(rf_predictions)
print(rfMetrics)
'''
[20, 76118, 16, 8066, 84220, 0.002473410833539451, 0.5555555555555556, 0.9040370458323439, 0.004924895345973898]
'''

print('Test Area Under ROC', evaluator.evaluate(rf_predictions))
'''
Test Area Under ROC 0.7814226616373523
'''

#****** Gradient-boosted tree classifier ******
gbt_predictions = gbtModel.transform(testing)
gbtMetrics = binary_evaluator(gbt_predictions)
print(gbtMetrics)
'''
[818, 75637, 497, 7268, 84220, 0.10116250309176354, 0.6220532319391635, 0.9078009973877939, 0.17402403999574514]
'''

print('Test Area Under ROC', evaluator.evaluate(gbt_predictions))
'''
Test Area Under ROC 0.796923276266982
'''

#print out the metrics of test performance
metrics = [lrMetrics,rfMetrics,gbtMetrics]
colnames = ['tp', 'tn', 'fp', 'fn', 'total', 'recall', 'precision', 'accuracy', 'f1 score']
rownames = ['Logistic Regression', 'Random Forest', 'Gradient-boosted Tree']
binMetrics = pd.DataFrame.from_records(metrics, columns=colnames, index=rownames)

'''
                        tp     tn   fp    fn  total    recall  precision  accuracy  f1 score
Logistic Regression     88  76048   86  7998  84220  0.010883   0.505747  0.904013  0.021308
Random Forest           20  76118   16  8066  84220  0.002473   0.555556  0.904037  0.004925
Gradient-boosted Tree  818  75637  497  7268  84220  0.101163   0.622053  0.907801  0.174024
'''

#*********************************** Q2f  Fine tuning the model   ***********************************
#https://spark.apache.org/docs/2.2.0/ml-tuning.html

#****** Logistic Regression ******
lr = LogisticRegression(featuresCol="features", labelCol="label")
pipeline = Pipeline(stages=[lr])
paramGrid = ParamGridBuilder() \
.addGrid(lr.regParam, [0.5, 0.1, 0.01]) \
.addGrid(lr.threshold, [0.75, 0.5, 0.25]) \
.addGrid(lr.elasticNetParam, [0.0, 0.1, 0.2]) \
.build()

lr_crossval = CrossValidator(estimator=pipeline,
                          estimatorParamMaps=paramGrid,
                          evaluator=BinaryClassificationEvaluator(),
                          numFolds=5)

#training
lr_cvModel = lr_crossval.fit(training)

#testing
lr_cvpredictions = lr_cvModel.transform(testing)

#print metrics
lr_cvMetrics = binary_evaluator(lr_cvpredictions)
print(lr_cvMetrics)
'''
[687, 75348, 786, 7399, 84220, 0.08496166213208013, 0.4663951120162933, 0.9028140584184279, 0.14373888482058791]
'''

print('Test Area Under ROC', evaluator.evaluate(lr_cvpredictions))
'''
Test Area Under ROC 0.7155862547010459
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
Out[76]: {'regParam': 0.01, 'maxIter': 100, 'elasticNetParam': 0.0, 'threshold': 0.75}
'''

#****** Random Forest ******
rf = RandomForestClassifier(featuresCol="features", labelCol="label")
pipeline = Pipeline(stages=[rf])
paramGrid = ParamGridBuilder() \
    .addGrid(rf.numTrees, [3, 5, 7]) \
    .addGrid(rf.impurity, ['entropy', 'gini']) \
    .addGrid(rf.maxBins, [50, 25, 13]) \
    .addGrid(rf.maxDepth, [20, 10, 5]) \
    .build()

rf_crossval = CrossValidator(estimator=pipeline,
                          estimatorParamMaps=paramGrid,
                          evaluator=BinaryClassificationEvaluator(),
                          numFolds=5)

#training
rf_cvModel = rf_crossval.fit(training)

#testing
rf_cvpredictions = rf_cvModel.transform(testing)

#print metrics
rf_cvMetrics = binary_evaluator(rf_cvpredictions)
print(rf_cvMetrics)
'''
[1017, 75642, 492, 7069, 84220, 0.12577294088548108, 0.6739562624254473, 0.9102232248872002, 0.2119854090672225]
'''

print('Test Area Under ROC', evaluator.evaluate(rf_cvpredictions))
'''
Test Area Under ROC 0.8280389869506455
'''

#best model
rf_bestModel = rf_cvModel.bestModel.stages[0]

bestRFParam = {
  'numTrees':rf_bestModel._java_obj.getNumTrees(),
  'impurity':rf_bestModel._java_obj.getImpurity(),
  'maxBins':rf_bestModel._java_obj.getMaxBins(),
  'maxDepth':rf_bestModel._java_obj.getMaxDepth()
}

'''
In [91]: bestRFParam
Out[91]: {'numTrees': 7, 'impurity': 'entropy', 'maxBins': 50, 'maxDepth': 10}
'''

#****** Gradient-boosted tree classifier ******
#https://towardsdatascience.com/machine-learning-with-pyspark-and-mllib-solving-a-binary-classification-problem-96396065d2aa
gbt = GBTClassifier(featuresCol="features", labelCol="label")
pipeline = Pipeline(stages=[gbt])
paramGrid = ParamGridBuilder() \
    .addGrid(gbt.maxDepth, [2, 4, 6]) \
    .addGrid(gbt.maxBins, [20, 60]) \
    .addGrid(gbt.maxIter, [10, 20]) \
    .build()


gbt_crossval = CrossValidator(estimator=pipeline,
                          estimatorParamMaps=paramGrid,
                          evaluator=BinaryClassificationEvaluator(),
                          numFolds=5)

#training
gbt_cvModel = gbt_crossval.fit(training)

#testing
gbt_cvpredictions = gbt_cvModel.transform(testing)

#print metrics
gbt_cvMetrics = binary_evaluator(gbt_cvpredictions)
print(gbt_cvMetrics)
'''
[1111, 75534, 600, 6975, 84220, 0.1373979718031165, 0.6493278784336646, 0.9100569935882213, 0.2268041237113402]
'''

#best model
gbt_bestModel = gbt_cvModel.bestModel.stages[0]

bestGBTParam = {
  'numTrees':gbt_bestModel._java_obj.getMaxDepth(),
  'maxBins':gbt_bestModel._java_obj.getMaxBins(),
  'maxDepth':gbt_bestModel._java_obj.getMaxIter()
}

'''
In [101]: bestGBTParam
Out[101]: {'numTrees': 6, 'maxBins': 60, 'maxDepth': 20}

'''

#print out the metrics of test performance
metrics = [lr_cvMetrics,rf_cvMetrics,gbt_cvMetrics]
colnames = ['tp', 'tn', 'fp', 'fn', 'total', 'recall', 'precision', 'accuracy', 'f1 score']
rownames = ['Logistic Regression', 'Random Forest', 'Gradient-boosted Tree']
binMetrics = pd.DataFrame.from_records(metrics, columns=colnames, index=rownames)
'''
                         tp     tn   fp    fn  total    recall  precision  accuracy  f1 score
Logistic Regression     687  75348  786  7399  84220  0.084962   0.466395  0.902814  0.143739
Random Forest          1017  75642  492  7069  84220  0.125773   0.673956  0.910223  0.211985
Gradient-boosted Tree  1111  75534  600  6975  84220  0.137398   0.649328  0.910057  0.226804
'''
