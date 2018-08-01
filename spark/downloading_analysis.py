import pyspark.sql.functions as F
from pyspark import SparkContext, SparkConf 
from pyspark.sql import SQLContext 
import matplotlib.pyplot as plt


def get_spark(): 
	conf = (SparkConf()
		.setAppName("python_project")
		.set("spark.authenticate.secret","thisisasecret"))	
	return SparkContext(conf=conf)

######################################################################
#File to analyse distribution of downloads and plot it using matplotlib
sc = get_spark()

sqlContext = SQLContext(sc)

#read in full file
df = sqlContext.read.json("/user/lspiedel/json/test/*")
df.describe().show()

#reduce to needed columns
df_reduced = df.select('name', 'ops')
df_reduced.printSchema()

#filter to remove nulls
df_reduced = df_reduced.filter(df_reduced.name.isNotNull() | (df_reduced.name!="NONE") | (df_reduced.name!="") | (df_reduced.name!="null"))

#group by name
df_counts = df_reduced.groupBy("name").agg(F.sum("ops"))
#rename col otherwise you get two called count
df_counts = df_counts.withColumnRenamed("sum(ops)", "Number of file downloads")
df_counts = df_counts.groupBy("Number of file downloads").count()
df_counts_ordered = df_counts.orderBy("Number of file downloads")
df_counts_ordered.show(20)
#output
df_counts_pd = df_counts_ordered.toPandas()

#plot result
fig = plt.figure()
plt.yscale('log')
plt.xscale('log')
df_counts_pd.plot(x="Number of file downloads", y="count")
plt.plot(df_counts_pd["Number of file downloads"], df_counts_pd["count"], 'o')
plt.show()
