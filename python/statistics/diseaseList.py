from pyspark import SparkContext
import sys

reload(sys)
sys.setdefaultencoding("utf-8")

sc = SparkContext()
data = sc.textFile("/mif/data/worker_hospital.txt") \
    .map(lambda line: line.split(',')) \
    .map(lambda line: (line[23], 1)) \
    .reduceByKey(lambda a, b: a) \
    .sortByKey() \
    .collect()

out = open('/home/edu/mif/python/zwj/output/data.txt', 'w')
for (k, v) in data:
    out.write("%s\n" % k)
out.close()
