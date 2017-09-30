import json
import re
from pyspark import SparkContext

# A hack to avoid having to pass 'sc' around
dummyrdd = None
def setDefaultAnswer(rdd): 
	global dummyrdd
	dummyrdd = rdd

def task1(playRDD):
    playRDD = playRDD.map(lambda line: (line.split()[0], line)).map(lambda (key, line): (key,(line, len(line.split()))))
    return playRDD.filter(lambda (key, (l,x)): x > 10)

def task2_flatmap(x):
        return [x['laureates'][i]['surname'] for i in range(0,len(x['laureates']),1)]

def task3(nobelRDD):
        x = nobelRDD.map(json.loads).map(lambda x: (x['category'], [x['laureates'][i]['surname'] for i in range(0,len(x['laureates']),1)]))
        return x.reduceByKey(lambda a,b: a + b)

def extractHostDate(logline):
    match = re.search('^(\S+).*([0-9]{2}\/[a-zA-Z]{3}\/\d{4})', logline)
    return (match.group(1), match.group(2))

def task4(logsRDD, l):
    x = logsRDD.map(extractHostDate).groupByKey()
    return x.filter(lambda (h, d): set(d) == set(l)).map(lambda (k,d): k)

def task5(bipartiteGraphRDD):
    x = bipartiteGraphRDD.groupByKey().map(lambda (x,l): (x, len(l))).map(lambda (a,b): (b,a))
    return x.groupByKey().map(lambda (x,l): (x, len(l)))

def makeHostKey(logline):
    match = re.search('^(\S+).*([0-9]{2}\/[a-zA-Z]{3}\/\d{4}).*(GET|HEAD|POST) ([^ ]*)', logline)
    return (match.group(1),(match.group(2), match.group(4)))

def task6(logsRDD, day1, day2):
    x = logsRDD.map(makeHostKey)
    rdd1 = x.filter(lambda (h,(date,url)): date == day1).map(lambda (h,(date,url)):(h,url))
    rdd2 = x.filter(lambda (h,(date,url)): date == day2).map(lambda (h,(date,url)):(h,url))
    #return rdd1.cogroup(rdd2).map(lambda (k, v): (k, (list(v[0]), list(v[1]))))
    return rdd1.cogroup(rdd2).filter(lambda (a,b): len(b[0]) > 0 and len(b[1]) > 0).mapValues(lambda x: (list(x[0]), list(x[1])))

def task7Helper(x):
    l = []
    for i in range(0,len(x['laureates']),1):
        match = x['laureates'][i]['motivation']
        if match is not None:
            if len(match.split()) > 1:
                l.append(match)
    return l

def task7(nobelRDD):
    rdd = nobelRDD.map(json.loads)
    x = rdd.flatMap(task7Helper)
    #x = a.flatMap(lambda x: [x['laureates'][i]['motivation'] for i in range(0,len(x['laureates']),1)])
    return x.map(lambda line: line.split()).flatMap(lambda m: [(m[i-1],m[i]) for i in range(1, len(m),1)]).map(lambda pair: (pair,1)).reduceByKey(lambda a, b: a + b)

def task8(bipartiteGraphRDD, currentMatching):
    #take users in currentMatching match out
    users = bipartiteGraphRDD.subtractByKey(currentMatching)
    #take products in currentMatching match out
    flip = users.map(lambda (a,b): (b,a))
    flipMatching = currentMatching.map(lambda (a,b): (b,a))
    prods = flip.subtractByKey(flipMatching).map(lambda (a,b): (b,a))
    #get neighbors
    neighbors = prods.groupByKey().mapValues(list)
    #find min prod
    min_prod = neighbors.map(lambda (user, l): (user, min(l)))

    #part2
    flip = min_prod.map(lambda (a,b): (b,a))
    flipMatching = currentMatching.map(lambda (a,b): (b,a))

    prods = flip.subtractByKey(flipMatching)
    prod_neighbors = prods.groupByKey().mapValues(list)
    min_user = prod_neighbors.map(lambda (user, l): (user, min(l))).map(lambda (a,b): (b,a))

    return min_user

