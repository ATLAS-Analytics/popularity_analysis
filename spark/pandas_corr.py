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
from udf_namefilter import isRobot, getUser, getTime
from ml_func import to_index
from plot import plot_corr
sqlContext = SQLContext(sc)

#read in full file
lines = sc.textFile("/user/lspiedel/rucio_expanded_2017/2017-01-0*")
parts = lines.map(lambda l: l.split("\t"))

#define schema
traces = parts.map(lambda p: Row(
    timestamp=p[0], 
    user=p[1], 
    scope=p[2], 
    name=p[3], 
    project=p[4], 
    datatype=p[5], 
    run_number=p[6], 
    stream_name=p[7], 
    prod_step=p[8], 
    version=p[9], 
    eventtype=p[10], 
    rse=p[11], 
    bytes=p[12], 
    length=p[13], 
    ops=p[14],
    file_ops=p[15], 
    distinct_file=p[16], 
    panda_jobs=p[17], 
    created_at=p[18]))

#convert to dataframe
df_pig = sqlContext.createDataFrame(traces)

#generate udfs
getUser_udf = F.udf(getUser)
isRobot_udf = F.udf(isRobot, BooleanType())
getTime_udf = F.udf(getTime)

#fiter out nans
df_nona = df_pig.na.drop()
for col in df_nona.schema.names:
    df_filter = df_nona.filter((df_nona[col] != ''))

#filter user and extract name and time in unix time
df_user = df_filter.filter(~isRobot_udf("user"))
df_user = df_user.withColumn("user", getUser_udf("user"))
df_time = df_user.withColumn("timestamp", getTime_udf("timestamp"))

#find time from creation of access
df_time = typeConv(df_time, "timestamp", "long")
timeDiff = df_time["timestamp"] - df_time["created_at"]
df_diff = df_time.withColumn("diff", timeDiff)
#.drop("timestamp").drop("created_at")

#remove lines with empty strings
#cast values to correct type
for intcol in ["run_number", "ops", "length", "file_ops", "distinct_file", "panda_jobs"]:
    df_diff = typeConv(df_diff, intcol, "int")   
for longcol in ["bytes", "created_at", "diff"]:
    df_diff = typeConv(df_diff, longcol, "long")

df_corr = df_diff
#index strings
cols = ['user', 'scope', 'name', 'project', 'datatype', 'stream_name', 'prod_step', 'version', 'eventtype', 'rse']
for col in cols:
    df_corr = to_index(df_corr, col);

#find correlations
names = df_corr.schema.names
corr_pd = df_corr.toPandas()
correl = pd_corr.corr()
print correl
#plot_corr(correl, names)
