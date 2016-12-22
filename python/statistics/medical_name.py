from pyspark import SparkContext

sc = SparkContext()
data = sc.textFile("/mif/data/worker_hospital_detail.txt")
data = data.map(lambda line: line.split(","))
# medical_name,1
data = data.map(lambda line: (line[2], 1))
data = data.reduceByKey(lambda a, b: a + b)
data = data.sortBy(lambda (k, v): (v, k),False)

out = open('/home/edu/mif/python/zwj/output/data.txt', 'w')
for (k, v) in data:
    out.write("%s,%d"%(v,k))
out.close()