import pyspark.ml.feature as mlf

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

