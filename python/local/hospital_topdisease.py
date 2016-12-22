reader=open("/home/sirius/project/python/MIFPySpark/output/top_disease.txt")
topdisease=set()
for disease in reader:
    topdisease.add(disease[:-1])
reader=open("/home/sirius/project/python/MIFPySpark/output/hospital_disease.txt")
writer=open("/home/sirius/project/python/MIFPySpark/output/hospital_topdisease.txt","w")
for line in reader:
    sline=line.split('\t')
    disease=sline[1].encode('utf-8')
    if(disease in topdisease):
        writer.write(line)
reader.close()
writer.close()