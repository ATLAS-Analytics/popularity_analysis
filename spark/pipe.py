#import pyspark.sql.functions as F
from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext#, Row
import pyspark.ml as ml
import pyspark.ml.feature as mlf
from pyspark.ml.classification import DecisionTreeClassifier
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
df_conv02 = convDf(df01)

#prepare for ml
df_prepped01 = prep(df_conv01)
df_prepped02 = prep(df_conv02)

#need some function to apply labels
df_labeled = get_labels(df_prepped01, df_prepped02).drop("name")
df_labaled = df_labeled.na.drop() 
cols_for_ml = df_prepped01.drop("name").schema.names

toVec = mlf.VectorAssembler(inputCols=cols_for_ml, outputCol="Features")
vecIndexer = mlf.VectorIndexer(inputCol='Features', outputCol="Features_idx")
classifier = DecisionTreeClassifier(labelCol="Label", featuresCol="Features_idx")

pipeline = ml.Pipeline(stages=[toVec, vecIndexer, classifier])

train, test = df_labeled.randomSplit([0.9, 0.1], seed=12345)
test = toVec.transform(train).show(20, False)
#vecIndexer.transform(test).show(20, False)

df_pip = pipeline.fit(train.na.drop())
df_fin = df_pip.transform(test).show()
