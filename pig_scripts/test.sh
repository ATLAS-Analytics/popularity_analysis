#!/bin/bash

#date parameter
#d=2017-01-01

#loop over all days of year
for d in '2017-01-13' '2017-01-17' '2017-01-19' '2017-01-20' '2017-01-24' '2017-01-26' '2017-01-27' '2017-01-31'; do
#while [ "$d" != 2018-01-01 ]; do 
    echo $d
    #complete pig command for that day
    pig -param DATE="$d" -f test.pig
#    d=$(date -I -d "$d + 1 day")
done
