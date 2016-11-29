from pyspark import SparkContext
# split
# (year,disease),(fee,group)
sc=SparkContext()

import sys
reload(sys)
sys.setdefaultencoding( "utf-8" )

data=sc.textFile("/mif/data/worker_hospital.txt")\
    .map(lambda line :line.split(','))\
    .filter(lambda line:line[6]!='' and line[15]!='')\
    .map(lambda line:((int(line[20][-2:]),line[23]),(float(line[6]),float(line[15]))))\
    .reduceByKey(lambda a,b:a+b)\
    .map(lambda (k,v):((k[0],v[0],v[1]),k[1]))\
    .sortByKey()\
    .collect()

out = open('/home/edu/mif/python/zwj/output/data.txt', 'w')
for ((year,fee,group),disease) in data:
    out.write("%s,%f,%f,%s\n" % (year,fee,group,disease))
out.close()
