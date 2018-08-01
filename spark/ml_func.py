import pyspark.ml.feature as mlf
###########################################################################
#indexing functions and type conversion


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
    converter = mlf.IndexToString(inputCol=col, outputCol=outcol)
    return convertertransform(df).drop(col)

