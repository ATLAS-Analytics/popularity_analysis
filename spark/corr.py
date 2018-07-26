import pyspark.sql.functions as F
from pyspark import SparkContext, SparkConf 
from pyspark.sql import SQLContext, Row
from pyspark.sql.types import BooleanType, LongType
import matplotlib.pyplot as plt
import pandas as pd
import pyspark.ml as ml
import pyspark.ml.feature as mlf
import numpy as np
import pyspark.mllib.stat as stat

#function to start spark instance
def get_spark(): 
	conf = (SparkConf()
		.setAppName("read_pigstorage")
		.set("spark.authenticate.secret","thisisasecret"))	
	return SparkContext(conf=conf)

#convert type
def typeConv(df, col, colType):
    return df.withColumn(col, df[col].cast(colType))

#initialise spark and inlude other python files
sc = get_spark()
sc.addPyFile("udf_namefilter.py")
sc.addPyFile("ml_func.py")
sc.addPyFile("plot.py")
sc.addPyFile("load_func.py")
from plot import plot_corr
from load_func import readIn, convDf
sqlContext = SQLContext(sc)

#read in full file
lines = sc.textFile("/user/lspiedel/rucio_expanded_2017/2017-01*")
traces = readIn(lines)
#convert to dataframe
df = sqlContext.createDataFrame(traces)
df_conv = convDf(df)

#find correlations
names = df_conv.schema.names
pd_conv = df_conv.toPandas()
np_conv = pd_conv.corr().values()
print np_conv

#correl = np.zeros((len(names),len(names)))
#i, j = 0, 0
#for col1 in df_corr.schema.names:
#    for col2 in df_corr.schema.names:
#        value = df_corr.stat.corr(col1, col2)
#        correl[i][j] = value
#        i += 1
#    i=0
#    j += 1
#print correl
#plot_corr(correl, names)
