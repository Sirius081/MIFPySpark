from pyspark import SparkContext

sc=SparkContext()
nums=set()
reader=open('/home/edu/mif/python/zwj/data/numsOfNGS.txt')
for num in reader:
    nums.add(num.strip('\n'))
data=sc.textFile("/mif/data/worker_hospital_detail.txt")
data=data.map(lambda line:line.split(','))
data_ngs=data.filter(lambda line:line[0] in nums).collect()

out = open('/home/edu/mif/python/zwj/output/data.txt', 'w')
for line in data:
    try:
	line=reduce(lambda a,b:"%s,%s"%(a,b),line).encode('utf-8')
        out.write("%s\n"%(line))
    except Exception:
        continue
out.close()
