from pyspark import SparkContext

sc = SparkContext()
data = sc.textFile("/mif/data/worker_hospital_detail.txt")
# data = sc.textFile("/mif/data/sample/worker_hospital_detail_lines50.txt")
data = data.map(lambda line: line.split(","))
data=data.filter(lambda line:len(line)>2)
# medical_name,1
data = data.map(lambda line: (line[2], 1))
data = data.reduceByKey(lambda a, b: a + b)
data = data.sortBy(lambda (k, v): (v, k),False).collect()

out = open('/home/edu/mif/python/zwj/output/data.txt', 'w')
for (k, v) in data:
    out.write("%s,%d"%(v,k))
out.close()