from pyspark import SparkContext

import sys

reload(sys)
sys.setdefaultencoding("utf-8")
def dictToString(d,topdisease):
    vec=""
    ds=d.keys()
    for disease in topdisease:
        if(disease in ds):
            vec+=d[disease]
# split
# (hospital,disease),(fee,group,1)
# (hospital,disease),(sum_fee,sum_group,count)
# hospital,{disease:(sum_fee,sum_group,count)}
# hospital,{diseases:(sum_fee,sum_group,count)}
sc = SparkContext()
reader=open("/home/edu/mif/python/zwj/data/top_disease.txt")
topdisease=set()
for disease in reader:
    topdisease.add(disease[:-1])
#data = sc.textFile("/mif/data/sample/worker_hospital_lines50.txt") \
data = sc.textFile("/mif/data/worker_hospital.txt") \
    .map(lambda line: line.split(',')) \
    .filter(lambda line: line[23] in topdisease and line[6] != '' and line[15] != '') \
    .map(lambda line: ((line[3], line[23]), (float(line[6]), float(line[15]), 1))) \
    .reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1], a[2] + b[2])) \
    .map(lambda (k, v): (k[0], {k[1]: (v[0], v[1], v[2])})) \
    .reduceByKey(lambda d1, d2: dict(d1, **d2)) \
    .sortByKey() \
    .collect()

out = open('/home/edu/mif/python/zwj/output/data.txt', 'w')
for (h,diseases) in data:

    out.write("%s\t%s\t%.2f\t%.2f\t%d\n" % (h, d, f, g, p))
out.close()
