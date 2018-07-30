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





indexers = [ mlf.StringIndexer(inputCol=c, outputCol="{0}_idx".format(c)) for c in cols ]

#df_indexed.show()
#pipeline = ml.Pipeline(stages=indexers)

#df_pip = pipeline.fit(df_filter)
#df_fin = df_pip.transform(df_filter)

