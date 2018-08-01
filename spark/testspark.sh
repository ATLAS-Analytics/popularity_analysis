#command to submit python file to spark using virtual env

spark-submit --master yarn-client --conf spark.pyspark.virtualenv.enabled=true  --conf spark.pyspark.virtualenv.type=native --conf spark.pyspark.virtualenv.requirements=$HOME/public/popularity_analysis/venv2.7/requirements.txt --conf spark.pyspark.virtualenv.bin.path=$HOME/public/popularity_analysis/venv2.7/ --conf spark.pyspark.python=$HOME/private/python/bin/python2.7 $1
