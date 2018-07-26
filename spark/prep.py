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
from load_func import readIn, convDf
#from udf_namefilter import isRobot, getUser
sqlContext = SQLContext(sc)

#read in full file
lines = sc.textFile("/user/lspiedel/rucio_expanded_2017/2017-01-01*")
traces = readIn(lines, '\t')
df = sqlContext.createDataFrame(traces)
df_conv = convDf(df)
group = ["name", "scope_idx", "project_idx", "datatype_idx", "run_number", "stream_name_idx", "prod_step_idx", "length", "bytes"]

def make_col(x,col):
   cnd = F.when(F.col("diff") < x, F.col(col)).otherwise(0)
   return F.sum(cnd).alias(str(x))

e_ops = [make_col(x, "ops") for x in [86400000, 604800000, 2629746000, 31536000000]]

df_byname = df_conv.groupBy(group).agg(
    F.countDistinct("rse_idx").alias("nrse"), 
    F.countDistinct("user_idx").alias("nuser"),
    *e_ops 
    )
df_byname.show()
df_byname.describe().show()

def corr(df):
    names = df.schema.names
    pd_conv = df.toPandas()
    corr = pd_conv.corr()

    #plot heatmap
    plt.matshow(corr)
    plt.xticks(range(len(corr.columns)), corr.columns, rotation=90);
    plt.yticks(range(len(corr.columns)), corr.columns);
    plt.colorbar()
    plt.show()

corr(df_byname)

sc.stop()
