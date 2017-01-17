from pyspark import SparkContext

import sys

reload(sys)
sys.setdefaultencoding("utf-8")

sc = SparkContext()
# year21,disease -1,group 17
data = sc.textFile("/zwj/mif/data/worker_hospital.txt") \
    .map(lambda line: line.split(',')) \
    .filter(lambda line: line[17] != '') \
    .map(lambda line: ((int(line[21][-2:]), line[-1]), float(line[17]))) \
    .reduceByKey(lambda a, b: a + b) \
    .map(lambda ((year, disease), sum_group): ((year, sum_group), disease)) \
    .sortByKey(False)\
    .collect()

out = open('/home/edu/mif/python/zwj/output/data.txt', 'w')
for ((year, sum_fee), disease) in data:
    out.write("%d,%.2f,%s\n" % (year, sum_fee, disease))
out.close()
