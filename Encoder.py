fn="/home/edu/mif/data/职工慢性病登记信息.txt"
reader=open(fn)
writer=open("/home/edu/mif/data/worker_chronic_regist.txt","w")
while True:
    line=reader.readline().decode('gbk').encode('utf-8')
    writer.write(line)
    if(len(line)==0):
        break
reader.close()
writer.close()
