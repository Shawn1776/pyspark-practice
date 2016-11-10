# from 27 mins--> flatmap, a list of list, pythn has no flatmap but chain can work round of it.
from intertools import chain
# pyspark starts at 36mins (set speed to 1.25x, else too slow), then he teach you how to get rid of the type info

list2=sc.parallelize(range(1,1000)).map(lambda x:x*10) # 42mins
list2.first()
# return 10
list2.reduce(lambda x,y:x+y)
sc.parallelize(range(1,1000)).map(lambda x:x*10)
list2.filter((lambda x: x%100==0)).take(5)
# transformation vs action # 48mins

rdd1=sc.paralllize(range(1,100)) #3316.6666666666665          
rdd1=sc.parallelize(range(1,100))
rdd1.map( lambda x:x*x).sum()
rdd1.map( lambda x:x*x).mean()
#3316.6666666666665          

# get a rdd with number 1 to 10 Get all the elements in that RDD which are divisible by 3
# get the product of the elements of the rsulted list

lst = sc.parallelize(range(1,10)).filter(lambda x:x%3==0)
lst_2=lst.reduce(lambda x,y :x*y)
################################################################### more practical ##########################3

# file_content = sc.textFile(urlORLocal,minPartions,sueUnicode=True)

readme = sc.textFile("./spark2.0.1/README.md")
readme.take(5)
readme.map(lambda x:x.split('\n')).first()

# reduceBykey (@1hr)

people = sc.textfile(<path>).map(lambda x:x.split("\t")).first()
# reduceByKey tuple(key,value), reduce the values on the same key
# find many "M" and "F" genda in people
# Orlando	M	40	Python
# Lina	F	39	C#
# John	M	30	Python
# Jane	F	32	Python
# Michelle	F	18	Python
# Daniel M 20 C#

# for "M"
# reduceByKey() usually following a map which orginaze the data set as tuple (key,value)
m_sum = sc.textfile(<path>).map(lambda x:x.split("\t")).map(lambda t:(t[1],1)).reduceByKey(lambda x,y :x+y).collect() # collect() return a list of the question


# class is back at 1:19 mins










