import matplotlib.pyplot as plt
from matplotlib import colors
import pandas as pd
import numpy as np
import pyspark.mllib.stat as stat
#######################################################################
#correlation calculation and plotting functions

#find correlations using pandas
def corr_pd(df):
    names = df.schema.names
    pd_conv = df.toPandas()
    corr = pd_conv.corr()
    return corr

#find correlations using pyspark
def corr_pys(df):
    features = df.map(lambda row: row[0:])
    corr = stat.Statistics.corr(features, method='pearson')
    return corr

#plot correlation matrix
def plot(df, df_names, filename):
    #set plot size
    fig, ax = plt.subplots(figsize=(10,10)) 
    #set colours
    cmap = plt.cm.get_cmap("plasma",lut=10)
    bounds = np.arange(0.0, 1.1, 0.1)
    norm = colors.BoundaryNorm(bounds, cmap.N)
    
    #plot heatmap
    plot = ax.matshow(np.absolute(df), cmap=cmap, norm=norm)
    #x and y labels
    plt.xticks(range(len(df_names)), df_names, rotation=90);
    plt.yticks(range(len(df_names)), df_names);
    #colorbar
    cbar = fig.colorbar(plot)
    
    for (i, j), z in np.ndenumerate(np.absolute(df)):
        ax.text(j, i, '{:0.2f}'.format(z), ha='center', va='center',
            bbox=dict(boxstyle='round', facecolor='white', edgecolor='0.3'))

    filepath = "./corr_img/" + filename
    fig.savefig(filepath)
    plt.show()

if __name__ = "__main__":
    import pyspark.sql.functions as F
    from pyspark import SparkContext, SparkConf
    from pyspark.sql import SQLContext, Row
    from pyspark.sql.types import BooleanType
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
    sqlContext = SQLContext(sc)
    
    #read in full file
    lines = sc.textFile("/user/lspiedel/rucio_expanded_2017/2017-*")
    traces = readIn(lines, '\t')
    df = sqlContext.createDataFrame(traces)
    
    df_conv = convDf(df).drop("name")
    df_filter = df_conv.na.drop()
    corr = corr_pys(df_filter)
    plot(corr, df_conv.schema.names, "test.png")
    
    #ops_list = ["ops", "file_ops", "distinct_file", "panda_jobs"]
    #corr = corr_pys(df_filter[ops_list])
    #plot(corr, ops_list, "ops_values")
    sc.stop()
    
