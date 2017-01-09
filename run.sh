#!/usr/bin/env bash
#pyFile=sample/diseaseFeeByYear.py
pyFile=statistics/diseaseFeeByYear.py
#pyFile=model/fpm_ngs.py
dataPath=/home/edu/mif/python/zwj/output/data.txt
scp python/${pyFile} edu@blade-83:~/mif/python/zwj/python/${pyFile}
ssh edu@blade-83 "rm  ${dataPath};spark-submit --num-executors 6 \
--executor-memory 13g \
--executor-cores 2 \
--driver-memory 16g \
 /home/edu/mif/python/zwj/python/${pyFile}"
scp edu@blade-83:${dataPath} ./output/
