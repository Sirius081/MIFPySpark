from pyspark import SparkContext

# split
# (year,disease),(fee,group)
sc = SparkContext()

import sys

reload(sys)
sys.setdefaultencoding("utf-8")
# filter
# disease,(fee,group)
# sum
data = sc.textFile("/mif/data/worker_hospital.txt") \
    .map(lambda line: line.split(',')) \
    .filter(lambda line: line[6] != '' and line[15] != '') \
    .map(lambda line: (line[23], 1)) \
    .reduceByKey(lambda a, b:a+b) \
    .map(lambda (k,v):(v,k))\
    .sortByKey() \
    .collect()

out = open('/home/edu/mif/python/zwj/output/data.txt', 'w')
for (disease,count) in data:
    out.write("%s,%d\n" % (count,disease))
out.close()
