from pyspark import SparkContext
# split
# filter
# year,(sumWage,count)
#
sc = SparkContext()
# read types
reader=open("/home/edu/mif/python/zwj/data/types.txt")
types=set(x[0:-2] for x in reader)

# data = sc.textFile("/mif/lines50.txt")
data=sc.textFile("/mif/data/mode_ac43_310.txt")
data = data.map(lambda line: line.encode('utf-8').split(",")) \
    .filter(lambda line: line[3] == '310' and (line[4] in types) and line[5] != "") \
    .map(lambda line: (int(line[2][0:4]), (float(line[5]), 1))) \
    .reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1])) \
    .sortByKey() \
    .collect()

out = open('/home/edu/mif/python/zwj/output/data.txt', 'w')
for (k, v) in data:
    out.write(str(k) + ',' + str(v[0] / v[1]*12) + '\r\n')
out.close()
