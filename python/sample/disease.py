from pyspark import SparkContext
import sys

sc = SparkContext()
reload(sys)
sys.setdefaultencoding("utf-8")
# 甲类费用7,乙类费用8,非基本费用9,甲类药品费11,乙类药品费12,非基本药品费13
neededFields = sc.broadcast([7, 8, 9, 11, 12, 13])
data = sc.textFile("/mif/data/worker_hospital.txt")
data = data.map(lambda line: line.split(','))
data = data.filter(lambda line: line[23] == "脑梗死".decode('utf-8'))
data = data.map(lambda line: (int(line[20][-2:]), [float(line[x]) for x in neededFields.value]))
data = data.sortByKey()
data = data.collect()

out = open('/home/edu/mif/python/zwj/output/data.txt', 'w')
for (k, v) in data:
    s = "%d" % k
    for f in v:
        s += "\t%.2f" % f
    s += "\n"
    out.write(s)
out.close()
