from pyspark import SparkContext

# chronicname,1
sc=SparkContext()
data=sc.textFile("/mif/data/sampleUtf-8/职工慢性病登记信息_lines50.txt")\
    .map(lambda line :line.decode('gbk').split(","))\
    .map(lambda line:((line[2]),1))\
    .reduceByKey(lambda a,b:a+b) \
    .collect()

out = open('/home/edu/mif/python/zwj/output/data.txt', 'w')
for (k, v) in data:
    out.write(str(k) + ',' + str(v) + '\r\n')
out.close()
