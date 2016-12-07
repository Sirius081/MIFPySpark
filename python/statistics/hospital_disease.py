from pyspark import SparkContext

import sys

reload(sys)
sys.setdefaultencoding("utf-8")


# field : 0fee,1group,2count
def dictToString(d, topdisease, field):
    vec = ""
    ds = d.keys()
    for disease in topdisease:
        if (disease in ds):
            vec += str(d[disease][field]) + ","
        else:
            vec += str(0) + ","

    return vec


sc = SparkContext()
reader = open("/home/edu/mif/python/zwj/data/top_disease.txt")
topdisease = []
for disease in reader:
    topdisease.append(disease[:-1].decode('utf-8'))
reader.close()
#data = sc.textFile("/mif/data/sample/worker_hospital_lines50.txt")
data = sc.textFile("/mif/data/worker_hospital.txt")
data = data.map(lambda line: line.split(','))
data = data.filter(lambda line: line[23] in topdisease and line[6] != '' and line[15] != '')
# (hospital,disease),(fee,group,1)
data = data.map(lambda line: ((line[3], line[23]), (float(line[6]), float(line[15]), 1)))

# (hospital,disease),(sum_fee,sum_group,count)
data = data.reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1], a[2] + b[2]))

# hospital,{disease:(sum_fee,sum_group,count)}
data = data.map(lambda (k, v): (k[0], {k[1]: (v[0], v[1], v[2])}))

# hospital,{diseases:(sum_fee,sum_group,count)}
data = data.reduceByKey(lambda d1, d2: dict(d1, **d2))
# hospital,string of feature
data = data.map(lambda (k, v): (k, dictToString(v, topdisease, 0)))

data = data.sortByKey()
data = data.collect()

out = open('/home/edu/mif/python/zwj/output/data.txt', 'w')
for (h, s) in data:
    out.write("%s\t%s\n" % (h,s))
out.close()
