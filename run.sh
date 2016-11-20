#!/usr/bin/env bash
pyFile=statistics/population.py
dataPath=/home/edu/mif/python/zwj/output/data.txt
scp python/$pyFile edu@blade-83:~/mif/python/zwj/python/$pyFile
ssh edu@blade-83 "rm  ${dataPath};spark-submit --executor-memory 12g --executor-cores 30 /home/edu/mif/python/zwj/python/${pyFile}"
scp edu@blade-83:$dataPath ./output/
