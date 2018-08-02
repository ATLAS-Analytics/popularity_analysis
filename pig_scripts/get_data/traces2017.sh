#!/bin/bash

#script to run combine_traces.pig once for each date in a tear - lathough some days don't work as are missing dids


#date parameter
d=2017-01-01

#loop over all days of year
while [ "$d" != 2018-01-01 ]; do 
    echo $d
    #complete pig command for that day
    pig -param DATE="$d" -f combine_traces.pig
    d=$(date -I -d "$d + 1 day")
done
