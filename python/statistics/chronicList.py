from pyspark import SparkContext
import re
# chronicname,1
sc=SparkContext()
# data=sc.textFile("/mif/data/sampleUtf-8/worker_chronic_regist_lines50.txt")\
data=sc.textFile("/mif/data/worker_chronic_regist.txt")\
    .map(lambda line :line.encode('utf-8').split(","))\
    .flatMap(lambda line:map(lambda a:(a,1),re.split(",|、|\(.*\)| +",line[2])))\
    .reduceByKey(lambda a,b:a+b) \
    .collect()

out = open('/home/edu/mif/python/zwj/output/data.txt', 'w')
for (k, v) in data:
    out.write(str(k) + ',' + str(v) + '\r\n')
out.close()
