from pyspark import SparkContext
from pyspark.mllib.fpm import FPGrowth

sc = SparkContext()
nums = set()
reader = open('/home/edu/mif/python/zwj/data/numsOfNGS.txt')
for num in reader:
    nums.add(num.strip('\n'))

data = sc.textFile("/mif/data/worker_hospital_detail.txt")
data = data.map(lambda line: line.split(','))
# num 0 ,medical_name 2 ,count 4
data_ngs = data.filter(lambda line: line[0] in nums and len(line) > 4)
data_bkt = data_ngs.map(lambda line: ((line[0], line[2]), 1)) \
    .reduceByKey(lambda a, b: a) \
    .map(lambda (k, v): (k[0], [k[1]])) \
    .reduceByKey(lambda a, b: a + b) \
    .map(lambda (k, v): v)

model = FPGrowth.train(data_bkt, 0.6)
fitems = model.freqItemsets().collect()
out = open('/home/edu/mif/python/zwj/output/data.txt', 'w')
for itemset in fitems:
    line = reduce(lambda a, b: "%s\t%s", itemset.items).encode("utf-8")
    out.write("%s\n" % (line))
out.close()
