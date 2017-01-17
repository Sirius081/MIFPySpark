from pyspark import SparkContext

import sys

reload(sys)
sys.setdefaultencoding("utf-8")

sc = SparkContext()
# year28,disease 33,group 19
data = sc.textFile("/zwj/mif/data/resident_hospital.txt") \
    .map(lambda line: line.split(',')) \
    .filter(lambda line: line[19] != '') \
    .map(lambda line: ((line[28][-2:], line[33]), float(line[19]))) \
    .reduceByKey(lambda a, b: a + b) \
    .map(lambda ((year, disease), sum_group): ((year, sum_group), disease)) \
    .sortByKey(False)\
    .collect()

out = open('/home/edu/mif/python/zwj/output/data.txt', 'w')
for ((year, sum_fee), disease) in data:
    out.write("%s,%.2f,%s\n" % (year, sum_fee, disease))
out.close()
