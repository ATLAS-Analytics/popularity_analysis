import pyspark.sql.functions as F
from pyspark import SparkContext, SparkConf 
from pyspark.sql import SQLContext, Row
from pyspark.sql.types import BooleanType
import matplotlib.pyplot as plt
import pandas as pd
import pyspark.ml as ml
import pyspark.ml.feature as mlf

#function to start spark instance
def get_spark(): 
	conf = (SparkConf()
		.setAppName("read_pigstorage")
		.set("spark.authenticate.secret","thisisasecret"))	
	return SparkContext(conf=conf)

#plot histogram
def hist(df, col):
    binned = df.select(col).rdd.flatMap(lambda row: row).histogram(20)
    print binned
    data = { 'bins': binned[0][:-1], 'freq': binned[1] }
    plt.bar(data['bins'], data['freq'], width=100.0)
    plt.yscale("log")
    histTitle = "Histogram of " + col
    plt.title(histTitle)
    plt.show()

#initialise spark and inlude other python files
sc = get_spark()
sc.addPyFile("udf_namefilter.py")
sc.addPyFile("load_func.py")
from load_func import readIn, convDf
#from udf_namefilter import isRobot, getUser
sqlContext = SQLContext(sc)

#read in full file
lines = sc.textFile("/user/lspiedel/rucio_expanded_2017/2017-01-*")
traces = readIn(lines, '\t')
df = sqlContext.createDataFrame(traces)

df_conv = convDf(df) 
hist(df_conv, "ops")

sc.stop()
