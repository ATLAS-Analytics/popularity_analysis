import pyspark.sql.functions as F
from pyspark import SparkContext, SparkConf 
from pyspark.sql import SQLContext, Row
from pyspark.sql.types import BooleanType
import matplotlib.pyplot as plt
import pandas as pd
import pyspark.ml as ml
import pyspark.ml.feature as mlf

#function to apply a label to each row in dataset
def get_labels(df_current, df_next):
    
    #just look at name column from second df
    df_nameList = df_next.select('name').withColumnRenamed("name", "name_2")
    #join by name to see which value in df_current are in df_next
    df_comp = df_current.join(df_nameList, df_current.name == df_nameList.name_2, how='left_outer')

    df_lab = df_comp.withColumn("name_2", F.when(df_comp.name_2.isNotNull(), 1).otherwise(0).alias("Label"))
    return df_lab
