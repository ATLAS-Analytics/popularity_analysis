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

#convert type
def typeConv(df, col, colType):
    return df.withColumn(col, df[col].cast(colType))
#index strings
def to_index(df, col):
    outcol = col + "_idx"
    indexer =  mlf.StringIndexer(inputCol=col, outputCol=outcol)
    #print indexer.params()
    return indexer.fit(df).transform(df).drop(col)
#from index
def from_index(df, col):
    outcol = col[:-4]
    converter = ml.feature.IndexToString(inputCol=col, outputCol=outcol)
    return convertertransform(df).drop(col)


#initialise spark and inlude other python files
sc = get_spark()
sc.addPyFile("udf_namefilter.py")
from udf_namefilter import isRobot, getUser
sqlContext = SQLContext(sc)

#read in full file
lines = sc.textFile("/user/lspiedel/rucio_expanded_2017/2017-01-*")
parts = lines.map(lambda l: l.split("\t"))

#define schema
traces = parts.map(lambda p: Row(timestamp=p[0], 
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
getUser_udf = F.udf(getUser)
isRobot_udf = F.udf(isRobot, BooleanType())

#filter user and extract name
df_user = df_pig.filter(~isRobot_udf("user"))
df_user = df_user.withColumn("user", getUser_udf("user"))

#remove lines with empty strings
df_nona = df_user.na.drop()
for col in df_nona.schema.names:
    df_filter = df_nona.filter((df_nona[col] != ''))

#cast values to correct type
for intcol in ["run_number", "ops", "length", "file_ops", "distinct_file", "panda_jobs"]:
    df_filter = typeConv(df_filter, intcol, "int")   
for longcol in ["bytes", "created_at"]:
    df_filter = typeConv(df_filter, longcol, "long")
df_indexed = df_filter
#index strings
cols = ['user', 'scope', 'project', 'datatype', 'stream_name', 'prod_step', 'version', 'eventtype', 'rse']
for col in cols:
    df_indexed = to_index(df_indexed, col)

#indexers = [ mlf.StringIndexer(inputCol=c, outputCol="{0}_idx".format(c)) for c in cols ]

#df_indexed.show()
#pipeline = ml.Pipeline(stages=indexers)

#df_pip = pipeline.fit(df_filter)
#df_fin = df_pip.transform(df_filter)

df_indexed.describe().show()

#from_index(df_indexed, "name_idx")["name"].show()