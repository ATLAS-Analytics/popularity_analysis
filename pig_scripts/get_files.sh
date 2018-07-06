

#example command to run poplularity pig script
#DAYS is dates of files to load in
#DATE is current date
#START and #END are time specifiers in unix time
pig -param DAYS='2018-07-02' -param DATE='2018-07-03' -param START=1530489600 -param END=1530575999 rucio_popularity.pig
