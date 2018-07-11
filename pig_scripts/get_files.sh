

#example command to run poplularity pig script
#DAYS is dates of files to load in
#DATE is current date
#START and #END are time specifiers in unix time
pig -param DAYS='2017-*' -param DATE='2017-12-31' -param START=1483228800 -param END=1514764799 -l /afs/cern.ch/user/lspiedel/public/rucio/tmp/ rucio_popularity.pig
