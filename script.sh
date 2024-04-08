#!/bin/bash
hadoop fs -put /root/sharebike/data/*.*  /data/

hive -f /root/sharebike/code.sql

java -jar /root/software/datax/json.jar /root/software/datax/

for json in $(ll /root/software/datax/ | awk 'NR > 1 {print $9}')
do 
    python /root/software/datax/bin/datax.py /root/software/datax/$json 
done 
























