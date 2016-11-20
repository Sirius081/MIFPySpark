from pyspark import SparkContext


sc = SparkContext()
data = sc.textFile("/mif/mode_ac43.txt")
# data = sc.textFile("/mif/lines50.txt")
wage = sc.textFile("/mif/avgWage2.txt")
wage=wage.collect()
avgwage={}
for line in wage:
    sline=line.split(',')
    avgwage[sline[0]]=float(sline[1])/12
broadAvgWage=sc.broadcast(avgwage)
#3:41
#line.split
#(year+num   line)
#remove Duplicate
#filter wage with some condition
#(year,1)
#count
data=data\
    .map(lambda line : line.encode('utf-8').split(','))\
    .map(lambda line:(line[2][0:4]+line[0],line))\
    .reduceByKey(lambda line1,line2:line1)
# data=data.filter(lambda (key,line):line[3]=='310 ' and line[4]=='10' and float(line[5])<=avgwage.get(key[0:4]))
# data=data.filter(lambda (key,line):line[3]=='310' and line[4]=='10')
data=data.map(lambda (key,line):(key[0:4],1))\
    .reduceByKey(lambda a,b:a+b)\
    .sortByKey()\
    .collect()

out=open('/home/edu/mif/python/zwj/output/data.txt','w')

for(k,v)in data:
    out.write(str(k)+','+str(v)+'\r\n')
out.close()
