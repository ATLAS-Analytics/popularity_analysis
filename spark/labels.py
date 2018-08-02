import pyspark.sql.functions as F
#####################################################################################
#Function to label an input dataframe based on if values are in another dataframe

#function to apply a label to each row in dataset
def get_labels(df_current, df_next):
    
    #just look at name column from second df
    df_nameList = df_next.select('name').withColumnRenamed("name", "Label")
    #join by name to see which value in df_current are in df_next
    df_comp = df_current.join(df_nameList, df_current.name == df_nameList.Label, how='left_outer')
    df_lab = df_comp.withColumn("Label", F.when(df_comp.Label.isNotNull(), "Popular").otherwise("Unpopular"))

    #print out how many of each label there are 
    df_lab.groupBy("Label").count().show()
    return df_lab
