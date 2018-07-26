import pyspark.ml.feature as mlf
import pyspark.ml as ml
import pyspark.sql.functions as F 
from pyspark import SparkContext, SparkConf  
from pyspark.sql import SQLContext, Row 
from pyspark.sql.types import BooleanType 


##convert type
#def typeConv(df, col, colType):
#    return df.withColumn(col, df[col].cast(colType))
##index strings
#def to_index(df, col):
#    outcol = col + "_idx"
#    indexer =  mlf.StringIndexer(inputCol=col, outputCol=outcol)
#    #print indexer.params()
#    return indexer.fit(df).transform(df).drop(col)
#from index
#def from_index(df, col):
#    outcol = col[:-4]
#    converter = mlf.IndexToString(inputCol=col, outputCol=outcol)
#    return convertertransform(df).drop(col)


#function to take in rdd read in from file and output a rdd with a schema
def readIn(lines, seperator='\t'):
    #split line
    parts = lines.map(lambda l: l.split(seperator))
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
    
    return traces
#function to preprocess dataframe
def convDf(df):
    from udf_namefilter import isRobot, getUser, getTime
    from ml_func import typeConv, to_index
    #remove lines with empty strings
    df_nona = df.na.drop()
    for col in df_nona.schema.names:
        df_filter = df_nona.filter((df_nona[col] != ''))

    #convert to dataframe
    getUser_udf = F.udf(getUser)
    isRobot_udf = F.udf(isRobot, BooleanType())
    getTime_udf = F.udf(getTime)
    #filter user and extract name
    df_user = df_filter.filter(~isRobot_udf("user"))
    df_user = df_user.withColumn("user", getUser_udf("user"))
 
    #cast values to correct type
    for intcol in ["run_number", "ops", "length", "file_ops", "distinct_file", "panda_jobs"]:
        df_user = typeConv(df_user, intcol, "int")
    for longcol in ["bytes", "created_at"]:
        df_user = typeConv(df_user, longcol, "long")
    df_indexed = df_user
    #index strings
    cols = ['user', 'scope', 'project', 'datatype', 'stream_name', 'prod_step', 'version', 'eventtype', 'rse']
    for col in cols:
        df_indexed = to_index(df_indexed, col)
    #convert timestamp to long and find time difference as diff
    df_time = df_indexed.withColumn("timestamp", getTime_udf("timestamp"))
    df_time = typeConv(df_time, "timestamp", "long")
    timeDiff = df_time["timestamp"] - df_time["created_at"]
    df_diff = df_time.withColumn("diff", timeDiff)
    
    return df_diff.drop("created_at").drop("timestamp")
