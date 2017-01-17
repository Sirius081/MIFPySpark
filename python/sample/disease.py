from pyspark import SparkContext
import sys

sc = SparkContext()
reload(sys)
sys.setdefaultencoding("utf-8")
# 就医序号2,甲类费用7,乙类费用8,非基本费用9,甲类药品费11,乙类药品费12,非基本药品费13,fee 6, group 15
dieases=["冠心病心律失常型","高血压病","糖尿病","冠心病","肺部感染","精神分裂症","腹痛","腰椎间盘突出"]
neededFields = sc.broadcast([2, 7, 8, 9, 11, 12, 13, 6, 15])
data = sc.textFile("/mif/data_new/worker_hospital.txt")
data = data.map(lambda line: line.split(','))
data = data.filter(lambda line: line[3] != '' and line[23] == "脑梗死".decode('utf-8'))
data = data.map(lambda line: (int(line[20][-2:]), [line[x] for x in neededFields.value]))
data = data.sortByKey()
data = data.collect()

out = open('/home/edu/mif/python/zwj/output/data.txt', 'w')
for (k, v) in data:
    s = "%d" % k
    s += "\t" + v[0]
    for f in v[1:]:
        s += "\t%.2f" % float(f)
    s += "\n"
    out.write(s)
out.close()
