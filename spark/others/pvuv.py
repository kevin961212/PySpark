#coding:utf-8
from pyspark import SparkConf, SparkContext

# 统计pv
def pv(lines):
    # map将网站地址设为key,初始值为1
    pairSite = lines.map(lambda line :(line.split("\t")[4],1))
    # reduceByKey统计网站点击量
    reduceResult = pairSite.reduceByKey(lambda v1, v2: v1 + v2)
    # 排序,降序
    result = reduceResult.sortBy(lambda tp: tp[1], ascending=False)
    # 打印
    result.foreach(lambda one:print(one))

# 统计uv
def uv(lines):
    # map将ip和网址拼接在一起，并且去重
    distinct = lines.map(lambda line: line.split("\t")[1] + "_" + line.split("\t")[4]).distinct()
    # reduceByKey统计网站点击量
    reduceResult = distinct.map(lambda distinct: (distinct.split("_")[1], 1)).reduceByKey(lambda v1, v2: v1 + v2)
    # 排序,降序
    result = reduceResult.sortBy(lambda tp: tp[1], ascending=False)
    # 打印
    result.foreach(lambda one:print(one))

# 统计除了某个地区外的uv
def uvExceptBJ(lines):
    # filter过滤掉地址为beijing的
    filter = lines.filter(lambda line: line.split("\t")[3] != 'beijing')
    # map将ip和网址拼接在一起，并且去重
    distinct = filter.map(lambda line: line.split("\t")[1] + "_" + line.split("\t")[4]).distinct()
    # reduceByKey统计网站点击量
    reduceResult = distinct.map(lambda distinct: (distinct.split("_")[1], 1)).reduceByKey(lambda v1, v2: v1 + v2)
    # 排序,降序
    result = reduceResult.sortBy(lambda tp: tp[1], ascending=False)
    # 打印
    result.foreach(lambda one: print(one))

# 统计网站最活跃的top2地区
def getCurrSiteTop2Location(one):
    site = one[0]   # 网址
    locations = one[1]  # 地区
    locationDict = {}
    for location in locations:  # 遍历地区
        if location in locationDict:    # 如果已经存在则计数+1
            locationDict[location] += 1
        else:   # 否则初始化计数为1
            locationDict[location] = 1
    resultList = []
    # 排序
    sortedList = sorted(locationDict.items(), key=lambda kv: kv[1], reverse=True)
    # 取最多的前两个地区，及访问记录
    if len(sortedList) < 2:
        resultList = sortedList
    else:
        for i in range(2):
            resultList.append(sortedList[i])
    return site,resultList

# 统计每个网站最活跃的top2地区
def getTop2Location(lines):
    # map切分数据将网址作为key，地区作为value，并进行以key进行分组
    site_locations = lines.map(lambda line: (line.split("\t")[4], line.split("\t")[3])).groupByKey()
    # 统计网站最活跃的top2地区
    result = site_locations.map(lambda one: getCurrSiteTop2Location(one)).collect()
    # 遍历
    for elem in result:
        print(elem)

# 返回网址对应的用户id的访问次数
def getSiteInfo(one):
    userId = one[0]     # 用户id
    sites = one[1]  # 网址
    dic = {}
    for site in sites:  # 遍历网址
        if site in dic: # 如果已存在则计数+1
            dic[site] += 1
        else:   # 否则计数初始化为1
            dic[site] = 1
    resultList = []
    for site,count in dic.items():  # 遍历字典
        resultList.append((site,(userId,count)))  # 返回对应的数据，网址,(用户id,访问次数)
    return resultList

# 排序选取网址最活跃的top3用户
def getCurSiteTop3User(one):
    site = one[0]
    userId_counts = one[1]
    top3List = ["","",""]
    for userId_count in userId_counts:
        # userId_count[0] 用户id  ,   userId_count[1] 网址访问次数
        for i in range(0,len(top3List)):
            if top3List[i] == "":
                top3List[i] = userId_count
                break
            else:
                if userId_count[1] > top3List[i][1]:
                    for j in range(2,i,-1):
                        top3List[j] = top3List[j-1]
                    top3List[i] = userId_count
                break
    return site,top3List

# 统计每个网址下最活跃的top3用户
def getTop3User(lines):
    # map切分数据将用户作为key，网址作为value，并进行以key进行分组
    site_locations = lines.map(lambda line: (line.split("\t")[2], line.split("\t")[4])).groupByKey()
    # 返回网址对应的用户id的访问次数
    site_uid_count = site_locations.flatMap(lambda one: getSiteInfo(one))
    # 排序选取网址最活跃的top3用户
    result = site_uid_count.groupByKey().map(lambda one:getCurSiteTop3User(one)).collect()
    for elem in result:
        print(elem)

if __name__ == '__main__':
    conf = SparkConf().setMaster("local").setAppName("pvuv")
    sc = SparkContext(conf = conf)
    lines = sc.textFile('../../data/mockData.txt')
    # pv(lines) # 统计pv
    # uv(lines) # 统计uv
    # uvExceptBJ(lines) # 统计除了某个地区外的uv
    # getTop2Location(lines)    # 统计每个网站最活跃的top2地区
    getTop3User(lines)  # 统计每个网址下最活跃的top3用户
    sc.stop()
