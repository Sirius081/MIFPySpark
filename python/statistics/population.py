from pyspark import SparkContext

sc = SparkContext()
data=sc.textFile("/mif/mode_ac43_310.txt")
# data = sc.textFile("/mif/lines50.txt")
data=data\
    .map(lambda line: line.encode('utf-8').split(',')) \
    .filter(lambda line: line[3]=='310' and line[4]=='10')\
    .map(lambda line:((int(line[2][0:4]),line[0]),1))\
    .reduceByKey(lambda a,b:a)\
    .map(lambda (k,v):(k[0],1))\
    .reduceByKey(lambda a,b:a+b)\
    .sortByKey()\
    .collect()


out = open('/home/edu/mif/python/zwj/output/data.txt', 'w')
for (k,v) in data:
    out.write(str(k) + ',' + str(v) + '\r\n')
out.close()

