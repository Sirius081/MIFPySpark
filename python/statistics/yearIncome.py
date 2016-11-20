from pyspark import SparkContext
from numpy import array
# 统计每一年总收入

sc = SparkContext()
data = sc.textFile("/mif/mode_ac43_310.txt")
# data = sc.textFile("/mif/lines50.txt")
# reader = open("/home/edu/mif/data/chargeType.txt")
# types = []
# incomes = {}  # type:income
# for line in reader:
#     sline = line.split('\t')
#     type = int(sline[0])
#     incomes[type] = 0
#     types.append(type)
# types = sc.broadcast(types)
# incomes = sc.broadcast(incomes)
# line.split
# filter by category 划入统筹和划入个人 不为空
# ((year,type),(group,single))
#((year,type),(total_group,total_single))

data = data \
    .map(lambda line: line.encode('utf-8').split(','))
data = data.filter(lambda line: line[3] == '310' and line[10] != '' and line[11] != '') \
    .map(lambda line: ((int(line[2][0:4]), int(line[4])), array([float(line[10]), float(line[11])]))) \
    .reduceByKey(lambda line1,line2:array([line1[0]+line2[0],line1[1]+line2[1]]))\
    .sortByKey()

data = data.collect()
out = open('/home/edu/mif/python/zwj/output/data.txt', 'w')

for (k, v) in data:
    out.write(str(k) + ',' + str(v) + '\r\n')
out.close()
