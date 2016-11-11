
##    python introduction first

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
# seems reduceByKey is same as groupByKey() first, then reduce() 

m_sum = sc.textfile(<path>).map(lambda x:x.split("\t")).map(lambda t:(t[1],1)).reduceByKey(lambda x,y :x+y).collect() # collect() return a list of the question


# class is back at 1:19 mins


first_RDD.join(second_RDD)  #1:32mins, remember there was a bug of join() we can work around by joinByKey 


# create an app
from pyspark import SparkContext, SparkConf
conf=SparkConf.setAppName(name).setMaster(master) # 1:35min
sc=SparkContext(conf)
# or just sc=SparkContext() will also work

# output to file @1:42:20



@ 1:49:30 DataTable
@ 1:53:30 json files :: data with schema
#Any idea? I am using spark2.0.1 pyspark-shell File "<stdin>", line 1, in <module> AttributeError: 'SQLContext' object has no attribute 'jsonFile'﻿
work around by SQLContext.read.json(<path>) 

## pls let me know if you have any ideas but you have a warning: 
#WARN ObjectStore: Version information not found in metastore. hive.metastore.schema.verification is not enabled so recording the schema version 1.2.0 16/11/11 12:47:59 WARN ObjectStore: Failed to get database default, returning NoSuchObjectException﻿

#you can use py.pandas syntax cool!!! #2:04:27
people.filter(people.age>30).show() # sql syntax 
people[people.age>30].show()        # pandas syntax

people.groupBy('gender').avg('age').show()

# @2:08 a good disscuess about map( scale nearly linear with your data and reduce nearly linear with your computing nodes 
#                   vs the dataTable optimizor (optimizing your query takes a constant time as SQL I guess) 
#                   vs reduce and join( pain, because transfer data over your nodes, but you can partition your data well)

# run sql conmand

people.registerTempTable("people")

sqlCtx.sql("select name, age FROM people").show()
>>>
+--------+---+
|    name|age|
+--------+---+
| Orlando| 40|
|    Lina| 39|
|    John| 30|
|    Jane| 32|
|Michelle| 18|
|  Daniel| 20|
+--------+---+

sqlCtx.sql("select gender, avg(age) as Avg_age from people GROUP BY gender").show()

from pyspark.sql.types import * # for StructField
sales_fields=[
  StructField('day',StringType(),False),
  StructFiled('store',StringType(),False),
  StructField('product',StringType(),False),
  StructField('quantity', IntegerType(),False),
]









