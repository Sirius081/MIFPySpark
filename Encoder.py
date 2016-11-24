fn="/home/edu/mif/data/sample/职工住院明细信息.txt"
reader=open(fn)
writer=open("/home/edu/mif/data/sampleUtf-8/worker_hospital_detail_lines50.txt","w")
while True:
    line=reader.readline().decode('gbk').encode('utf-8')
    writer.write(line)
    if(len(line)==0):
        break
reader.close()
writer.close()
