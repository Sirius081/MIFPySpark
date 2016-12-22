fn="/home/edu/mif/data/worker_hospital_detail.txt"
reader=open(fn)
writer=open("/home/edu/mif/data/worker_hospital_detail1.txt","w")
count=0
while True:
    try:
        line=reader.readline().decode('gbk').encode('utf-8')
        writer.write(line)
    except Exception,e:
        count+=1
    if(len(line)==0):
        break
reader.close()
writer.close()
