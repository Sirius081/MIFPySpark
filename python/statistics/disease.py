from pyspark import SparkContext

import sys

reload(sys)
sys.setdefaultencoding("utf-8")

sc = SparkContext()
# year,disease,fee,group,count
dataRes=sc.textFile("/zwj/mif/data/resident_hospital.txt") \
    .map(lambda line: line.split(',')) \
    .filter(lambda line: line[8] != '' and line[19]!='') \
    .map(lambda line: ((line[28][-2:], line[33]), (float(line[8]),float(line[19]),1)))
data = sc.textFile("/zwj/mif/data/worker_hospital.txt") \
    .map(lambda line: line.split(',')) \
    .filter(lambda line: line[6] != '' and line[17]!='') \
    .map(lambda line: ((line[21][-2:], line[-1]), (float(line[6]), float(line[17]), 1))) \
    .union(dataRes)\
    .reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1], a[2] + b[2])) \
    .collect()

out = open('/home/edu/mif/python/zwj/output/data.txt', 'w')
for ((year,disease),(sum_fee, sum_group, count)) in data:
    out.write("%s,%s,%.2f,%.2f,%d\n" % (disease, year, sum_fee, sum_group, count))
out.close()
