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

#change this to change month calcualted on
month = 1
#date = "/user/lspiedel/rucio_expanded_2017/2017-%02d-*,/user/lspiedel/rucio_expanded_2017/2017-%02d-*,/user/lspiedel/rucio_expanded_2017/2017-%02d-*" % (month, month+1, month+2)
#date2 = "/user/lspiedel/rucio_expanded_2017/2017-%02d-*,/user/lspiedel/rucio_expanded_2017/2017-%02d-*,/user/lspiedel/rucio_expanded_2017/2017-%02d-*" % (month+3, month+4, month+5)
date = "/user/lspiedel/rucio_expanded_2017/2017-%02d-*" % (month)
date2 = "/user/lspiedel/rucio_expanded_2017/2017-%02d-*" % (month + 1)

#read in two months of data
lines01 = sc.textFile(date)
lines02 = sc.textFile(date2)

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
df_labeled = get_labels(df_prepped01, df_prepped02)
df_labeled = df_labeled.na.drop().drop("version_idx") 
cols_for_ml = df_prepped01.drop("name").drop("version_idx").schema.names

#pipline stages
#index the label
labelIndexer = mlf.StringIndexer(inputCol="Label", outputCol="Label_idx")
#vectorise the input
toVec = mlf.VectorAssembler(inputCols=cols_for_ml, outputCol="Features")
#classify
classifier = DecisionTreeClassifier(labelCol="Label_idx", featuresCol="Features", maxDepth=10, maxBins = 200)

#create pipline of the stages and use it to train and test
pipeline = ml.Pipeline(stages=[labelIndexer, toVec, classifier])
train, test = df_labeled.randomSplit([0.7, 0.3], seed=12345)
df_pip = pipeline.fit(train)
predicted = df_pip.transform(test)
#print result
predicted.select("name", "Label_idx", "prediction", "rawPrediction", "probability").show(30, False)

#function to evaluate result
def evaluate(method, predicted):
    evaluator_acc = MulticlassClassificationEvaluator(
        labelCol="Label_idx", predictionCol="prediction", metricName=method)
    accuracy = evaluator_acc.evaluate(predicted)
    return accuracy
bin_evaluator = BinaryClassificationEvaluator(labelCol="Label_idx", rawPredictionCol="rawPrediction")

print date
print "Precision is {}".format(evaluate("precision", predicted))
print "Recall is %f" % evaluate("weightedRecall", predicted)
print "Weighted precision is is {}".format(evaluate("weightedPrecision", predicted))
print "f1 is {}".format(evaluate("f1", predicted))
print "area under ROC curve is {}".format(bin_evaluator.evaluate(predicted))

#print summary
treeModel = df_pip.stages[2]
print(treeModel)
