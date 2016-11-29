from pyspark import SparkContext
import sys
reload(sys)
sys.setdefaultencoding( "utf-8" )

sc=SparkContext()
data=sc.textFile("/mif/data/sample/worker_hospital_lines50.txt")\
	.map(lambda line :line.split(','))\
	.filter(lambda line:line[23]=='脑梗死')\
