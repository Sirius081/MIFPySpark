from pyspark import SparkContext

import sys

reload(sys)
sys.setdefaultencoding("utf-8")

sc = SparkContext()
# year28,disease 33,count
data = sc.textFile("/zwj/mif/data/resident_hospital.txt") \
    .map(lambda line: line.split(',')) \
    .filter(lambda line: line[19] != '') \
    .map(lambda line: ((line[28][-2:], line[33]), 1)) \
    .reduceByKey(lambda a, b: a + b) \
    .map(lambda ((year, disease), sum_count): ((year, sum_count), disease)) \
    .sortByKey(False)\
    .collect()

out = open('/home/edu/mif/python/zwj/output/data.txt', 'w')
for ((year, count), disease) in data:
    out.write("%s,%d,%s\n" % (year, count, disease))
out.close()
