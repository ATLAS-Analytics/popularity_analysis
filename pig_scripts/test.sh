#!/bin/bash

for i in {2..30}
do
    if [ $i -lt 10 ]
    then
        DATE="2018-06-0$i"
    else
        DATE="2018-06-$i"
    fi
    echo $DATE
    pig -param DATE="$DATE" -f test.pig

done
