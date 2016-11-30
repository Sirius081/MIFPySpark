from pyspark import SparkContext

# split
# (year,disease),(fee,group,population)
sc = SparkContext()

import sys

reload(sys)
sys.setdefaultencoding("utf-8")
# filter fee !='' group !=''
# disease,(fee,group,1)
# disease,(sum_fee,sum_group,count)
# sort
data = sc.textFile("/mif/data/worker_hospital.txt") \
    .map(lambda line: line.split(',')) \
    .filter(lambda line: line[6] != '' and line[15] != '') \
    .map(lambda line: (line[23].replace(',',''), (float(line[6]), float(line[15]), 1))) \
    .reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1], a[2] + b[2])) \
    .sortByKey() \
    .collect()

out = open('/home/edu/mif/python/zwj/output/data.txt', 'w')
for (disease, (sum_fee, sum_group, count)) in data:
    out.write("%s,%.2f,%.2f,%d\n" % (disease, sum_fee, sum_group, count))
out.close()
