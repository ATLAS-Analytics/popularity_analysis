#!/bin/bash

for DAY in {1..30}
do
    # get yesterday's date
    if [ $DAY -lt 10 ]
    then
    pop_date="2018-06-0$DAY"
    else
    pop_date=" 2018-06-$DAY"
    fi   
    
    pop_month="2018-06"

    # get the days before and after yesterday
    before=$(date -u "+%Y-%m-%d" --date="$pop_date -1 days")
    after=$(date -u "+%Y-%m-%d" --date="$pop_date +1 days")

    # get the unix timestamp for beginning and end of yesterday
    timestart=$(date -u "+%s" --date="$pop_date")
    timeend=$(date -u "+%s" --date="$pop_date +24 hours")
    echo $timestart $timeend
    # the dates that will be loaded in pig
    days={$before,$pop_date,$after}

    # first pig job to compute popularity and store them on HDFS
    command1="time pig -param DAYS=\"$days\" -param START=\"$timestart\" -param END=\"$timeend\" -param DATE=\"$pop_date\" -param MONTH=\"$pop_month\" -f rucio_popularity.pig"

    echo $days
    for i in {0..4}
    do
        eval $command1
        if [ $? -eq 0 ]; then
            break
        else
            if [ $i -eq 4 ]; then
                exit 1
            fi
        fi
    done

done
#example command to run poplularity pig script
#DAYS is dates of files to load in
#DATE is current date
#START and #END are time specifiers in unix time
#pig -param DAYS='2017-*' -param DATE='2017-12-31' -param START=1483228800 -param END=1514764799 -l /afs/cern.ch/user/lspiedel/public/rucio/tmp/ rucio_popularity.pig

#READ_IN={0..5}

#for DAY in {0..1}; do
#    echo "2018-06-0$DAY"
#    if [ $DAY -lt 10 ]
#    then
#        pig -param DAYS="2018-06-0$DAY" -param DATE="2018-06-0$DAY" -param START=1483228800 -param END=1614764799 -l /afs/cern.ch/user/l/lspiedel/public/rucio/tmp/ rucio_popularity.pig -stop-on-error
#    else
#        pig -param DAYS="2018-06-$DAY" -param DATE="2018-06-0$DAY" -param START=1483228800 -param END=1614764799 -l /afs/cern.ch/user/l/lspiedel/public/rucio/tmp/ rucio_popularity.pig -stop-on-error
#    fi
#done
