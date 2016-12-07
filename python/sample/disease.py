from pyspark import SparkContext
import sys

reload(sys)
sys.setdefaultencoding("utf-8")

sc = SparkContext()
data = sc.textFile("/mif/data/worker_hospital.txt")
data=data.map(lambda line: line.split(','))
data=data.filter(lambda line: line[23] == "脑梗死".decode('utf-8'))
data=data.map(lambda line: (int(line[20][-2:]), (float(line[6]), float(line[15]))))# year,(fee,group,)
data=data.sortByKey()
data=data.collect()

out = open('/home/edu/mif/python/zwj/output/data.txt', 'w')
for (year,(fee,group)) in data:
    out.write("%d\t%.2f\t%.2f\n" % (year, fee, group))
out.close()
