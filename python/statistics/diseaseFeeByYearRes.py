from pyspark import SparkContext

import sys

reload(sys)
sys.setdefaultencoding("utf-8")

sc = SparkContext()
# year 28,disease 33,fee 8
data = sc.textFile("/zwj/mif/data/resident_hospital.txt") \
    .map(lambda line: line.split(',')) \
    .filter(lambda line: line[8] != '') \
    .map(lambda line: ((line[28][-2:], line[33]), float(line[8]))) \
    .reduceByKey(lambda a, b: a + b) \
    .map(lambda ((year, disease), sum_fee): ((year, sum_fee), disease)) \
    .sortByKey(False)\
    .collect()

out = open('/home/edu/mif/python/zwj/output/data.txt', 'w')
for ((year, sum_fee), disease) in data:
    out.write("%s,%.2f,%s\n" % (year, sum_fee, disease))
out.close()
