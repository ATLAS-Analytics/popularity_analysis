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

#initialise spark and inlude other python files
sc = get_spark()
sc.addPyFile("udf_namefilter.py")
sc.addPyFile("load_func.py")
sc.addPyFile("corr.py")
from load_func import readIn, convDf
from corr import corr_pd, corr_pys, plot
#from udf_namefilter import isRobot, getUser
sqlContext = SQLContext(sc)

#read in full file
lines = sc.textFile("/user/lspiedel/rucio_expanded_2017/2017-01-01*")
traces = readIn(lines, '\t')
df = sqlContext.createDataFrame(traces)
df_conv = convDf(df)
group = ["name", "scope_idx", "project_idx", "datatype_idx", "run_number", "stream_name_idx", "prod_step_idx", "length", "bytes"]

#function take a value, x, and a column 
#returns the sum of the values in the column where the dif is less than x
def make_col(x,col):
   cnd = F.when(F.col("diff") < x, F.col(col)).otherwise(0)
   return F.sum(cnd).alias(str(x))

#prepare by grouping by dataset and finding wanted stats
def prep(df):
    e_ops = [make_col(x, "ops") for x in [86400000, 604800000, 2629746000]]

    df_byname = df.groupBy(group).agg(
        F.countDistinct("rse_idx").alias("nrse"), 
        F.countDistinct("user_idx").alias("nuser"),
        *e_ops
        )
    return df_byname

df_byname = prep(df_conv).drop("name")
#df_byname.printSchema()
print corr_pd(df_byname)
print corr_pys(df_byname)
#plot(corr, df_byname.schema.names)
sc.stop()