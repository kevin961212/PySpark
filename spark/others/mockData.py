#coding:utf-8
import time
import random
from builtins import str

# 模拟访问的日志
def mock(path):
    date = time.strftime('%Y-%m-%d')
    list = ['192.168',str(random.randint(0,255)),str(random.randint(0,255))]
    ip = '.'.join(list)

    # 5位的userID
    userID = getUserId()

    # 随机获取地址
    locations = ['beijing','shanghai','guangzhou','shenzhen','zhanjiang','hangzhou']
    location = locations[random.randint(0,5)]

    for j in range(0,random.randint(0,10)):
        websites = ['www.baidu.com','www.xiaomi.com','www.jd.com','www.taobao.com','www.qq.com','www.360.com']
        website = websites[random.randint(0,5)]

        operations = ['register','view','login','logout','buy','comment','jump']
        operation = operations[random.randint(0,6)]

        oneInfo = date + '\t' + ip + '\t' + "uid" + userID + '\t' + location + '\t' + website + '\t' + operation
        print(oneInfo)
        writeLogToFile(path,oneInfo)

# 创建一个方法，存放获取随机的用户id
def getUserId():
    id = str(random.randint(0,100000))
    tmpStr = ""
    if len(id) < 5:
        for i in range(0,(5-len(id))):
            tmpStr += "0"
    return tmpStr + id

# 往指定文件中追加数据
def writeLogToFile(path,log):
    with open(path,'a+') as f:
        f.writelines(log+'\n')

if __name__ == '__main__':
    outputPath = '../../data/mockData.txt'
    for i in range(1,10000):
        mock(outputPath)

