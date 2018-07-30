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
sc.addPyFile("corr.py")
from load_func import readIn, convDf
from corr import plot, corr_pys, corr_pd
#from udf_namefilter import isRobot, getUser
sqlContext = SQLContext(sc)

#read in full file
lines = sc.textFile("/user/lspiedel/rucio_expanded_2017/2017-*")
traces = readIn(lines, '\t')
df = sqlContext.createDataFrame(traces)

df_conv = convDf(df).drop("name")
df_filter = df_conv.na.drop()
corr = corr_pys(df_filter)
plot(corr, df_conv.schema.names, "temp.png")

#ops_list = ["ops", "file_ops", "distinct_file", "panda_jobs"]
#corr = corr_pys(df_filter[ops_list])
#plot(corr, ops_list, "ops_values")

#hist(df_conv, "ops")

sc.stop()
