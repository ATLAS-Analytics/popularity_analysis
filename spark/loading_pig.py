import pyspark.sql.functions as F
from pyspark import SparkContext, SparkConf 
from pyspark.sql import SQLContext, Row
import matplotlib.pyplot as plt
import pandas as pd

def get_spark(): 
	conf = (SparkConf()
		.setAppName("read_pigstorage")
		.set("spark.authenticate.secret","thisisasecret"))	
	return SparkContext(conf=conf)


sc = get_spark()

sqlContext = SQLContext(sc)

#read in full file
lines = sc.textFile("/user/lspiedel/tmp/rucio_expanded/2018-06-*")
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
    ops=int(p[14]), 
    file_ops=int(p[15]), 
    distinct_file=int(p[16]), 
    panda_jobs=int(p[17]), 
    created_at=long(p[18])))

#convert to dataframe
df_pig = sqlContext.createDataFrame(traces)

#remove lines with empty strings
df_nona = df_pig.na.drop()
for col in df_pig.schema.names:
    df_filter = df_pig.filter((df_nona[col] != ''))

#cast values to ints
for intcol in ["run_number", "bytes", "length"]:
    print 
    df_filter = df_filter.withColumn(intcol, df_filter[intcol].cast("int"))   

df_filter.printSchema()
df_filter.describe().show()
