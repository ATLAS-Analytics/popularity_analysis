import pyspark.sql.functions as F
from pyspark import SparkContext, SparkConf 
from pyspark.sql import SQLContext, Row
from pyspark.sql.types import BooleanType
import matplotlib.pyplot as plt
import pandas as pd
import pyspark.ml as ml
import pyspark.ml.feature as mlf
##################################################################################
#Functions and code to prepare input df for machine learning

#function to start spark instance
def get_spark(): 
	conf = (SparkConf()
		.setAppName("read_pigstorage")
		.set("spark.authenticate.secret","thisisasecret"))	
	return SparkContext(conf=conf)

#function take a value, x, and a column 
#returns the sum of the values in the column where the dif is less than x
def make_col(x,col):
   cnd = F.when(F.col("diff") < x, F.col(col)).otherwise(0)
   return F.sum(cnd).alias(str(x/86400000))

#prepare by grouping by dataset and finding wanted stats
def prep(df):
    e_ops = [make_col(x, "ops") for x in [86400000, 604800000]]
    count_cnd = lambda cond: F.sum(F.when(cond, 1).otherwise(0)) 
    group = ["name", "scope_idx", "datatype_idx", "run_number", "prod_step_idx", "version_idx"]# "bytes", "length"]

    #for some reason bytes and length aren't dataset specific, so I've just taken the max here
    df_byname = df.groupBy(group).agg(
        F.max("bytes").alias("bytes"),
        F.max("length").alias("length"),
        F.countDistinct("rse_idx").alias("nrse"), 
        F.countDistinct("user_idx").alias("nuser"),
        count_cnd(F.col("eventtype") == "get_sm").alias("get_sm_count"),
        *e_ops
        )
    return df_byname
   
#can run file directly for testing
if __name__ == "__main__":
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
    lines = sc.textFile("/user/lspiedel/rucio_expanded_2017/2017-01-*")
    traces = readIn(lines, '\t')
    df = sqlContext.createDataFrame(traces)
    df_conv = convDf(df)
   
    #corr = corr_pd(df_byname)
    
    #plot(corr, df_byname.schema.names, "temp")
    sc.stop()
