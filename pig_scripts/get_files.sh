

#example command to run poplularity pig script
#DAYS is dates of files to load in
#DATE is current date
#START and #END are time specifiers in unix time
pig -param DAYS='2018-06-*' -param DATE='2018-06-30' -param START=1527811200 -param END=1530403199 -l /afs/cern.ch/user/lspiedel/public/rucio/tmp/ rucio_popularity.pig
