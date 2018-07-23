#!/bin/bash

for DAY in {1..30}
do

#pop_date="2018-01-01"
    # get date string
    if [ $DAY -lt 10 ]
    then
    pop_date="2018-06-0$DAY"
    else
    pop_date=" 2018-06-$DAY"
    fi   
    
    pop_month="2018-06"

    # get the days before and after pop_date
    before=$(date -u "+%Y-%m-%d" --date="$pop_date -1 days")
    after=$(date -u "+%Y-%m-%d" --date="$pop_date +1 days")

    # get the unix timestamp for beginning and end of pop_date
    timestart=$(date -u "+%s" --date="$pop_date -4 hours")
    timeend=$(date -u "+%s" --date="$pop_date +20 hours")
    echo $timestart $timeend
    # the dates that will be loaded in pig
    days={$before,$pop_date,$after}

    # first pig job to compute popularity and store them on HDFS
    command1="time pig -l ./log/ -param DAYS=\"$days\" -param START=\"$timestart\" -param END=\"$timeend\" -param DATE=\"$pop_date\" -param MONTH=\"$pop_month\" -f rucio_popularity.pig"

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

