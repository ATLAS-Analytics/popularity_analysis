#import pyspark.sql.functions as F
from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext#, Row
import pyspark.ml as ml
import pyspark.ml.feature as mlf
from pyspark.ml.classification import DecisionTreeClassifier
from pyspark.ml.evaluation import MulticlassClassificationEvaluator, BinaryClassificationEvaluator
#############################################################################
#Program to perform machine learning


#function to start spark instance
def get_spark():
    conf = (SparkConf()
        .setAppName("read_pigstorage")
        .set("spark.authenticate.secret","thisisasecret"))
    return SparkContext(conf=conf)

#initialise spark and inlude other python files
sc = get_spark()
sc.addPyFile("udf_namefilter.py")
sc.addPyFile("load_func.py")
sc.addPyFile("prep.py")
sc.addPyFile("labels.py")
from load_func import readIn, convDf
from prep import prep
from labels import get_labels
#from udf_namefilter import isRobot, getUser
sqlContext = SQLContext(sc)

#read in two months of data
lines01 = sc.textFile("/user/lspiedel/rucio_expanded_2017/2017-01-*")
lines02 = sc.textFile("/user/lspiedel/rucio_expanded_2017/2017-02-*")

#add schema
traces01 = readIn(lines01, '\t')
traces02 = readIn(lines02, '\t')

#make them into dataframe
df01 = sqlContext.createDataFrame(traces01)
df02 = sqlContext.createDataFrame(traces02)

#convert them to vectors
df_conv01 = convDf(df01)

#prepare for ml
df_prepped01 = prep(df_conv01)
df_prepped02 = df02.select("name").distinct()


#function to apply labels
df_labeled = get_labels(df_prepped01, df_prepped02).drop("name")
df_labeled = df_labeled.na.drop().drop("version_idx") 
cols_for_ml = df_prepped01.drop("name").drop("version_idx").schema.names

#pipline stages
#index the label
labelIndexer = mlf.StringIndexer(inputCol="Label", outputCol="Label_idx")
#vectorise the input
toVec = mlf.VectorAssembler(inputCols=cols_for_ml, outputCol="Features")
#classify
classifier = DecisionTreeClassifier(labelCol="Label_idx", featuresCol="Features", maxBins = 137)

pipeline = ml.Pipeline(stages=[labelIndexer, toVec, classifier])

train, test = df_labeled.randomSplit([0.9, 0.1], seed=12345)

df_pip = pipeline.fit(train)
predicted = df_pip.transform(test)
predicted.select("Features", "Label_idx", "prediction").show()
def evaluate(method, predicted):
    evaluator_acc = MulticlassClassificationEvaluator(
        labelCol="Label_idx", predictionCol="prediction", metricName=method)
    accuracy = evaluator.evaluate(predicted)
    return accuracy

print "precision is " + evaluate("accuracy", predicted)
print "Recall is " + evaluate("recall", predicted)
#print "precision is " + evaluate(accuracy, predicted)
#print "precision is " + evaluate(accuracy, predicted)
bin_eval = BinaryClassificationEvaluator(labelCol="Label_idx", rawPredictionCol="rawPrediction")
print bin_eval.evaluate(predicted)
treeModel = df_pip.stages[3]
# summary only
print(treeModel)
