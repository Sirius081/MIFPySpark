from pyspark import SparkContext
# split
# (year,disease),(fee,group,population)
sc=SparkContext()

import sys
reload(sys)
sys.setdefaultencoding( "utf-8" )

data=sc.textFile("/mif/data/worker_hospital.txt")\
    .map(lambda line :line.split(','))\
    .filter(lambda line:line[6]!='' and line[15]!='')\
    .map(lambda line:((int(line[20][-2:]),line[23]),(float(line[6]),float(line[15]),1)))\
    .reduceByKey(lambda a,b:(a[0]+b[0],a[1]+b[1],a[2]+b[2]))\
    .map(lambda (k,v):((k[0],v[0]/v[2]),k[1]))\
    .sortByKey()\
    .collect()

out = open('/home/edu/mif/python/zwj/output/data.txt', 'w')
for ((year,v),disease) in data:
    out.write("%s,%.2f,%s\n" % (year,v,disease))
out.close()
