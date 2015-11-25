from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext


from dateutil.parser import parse
from datetime import datetime

conf2 = SparkConf().setAppName("sfpd-correlation")
sc2 = SparkContext(conf=conf2)
sqlc2 = SQLContext(sc2)

df2 = sqlc2.read.format('com.databricks.spark.csv').\
options(header='true', inferschema='true').\
load('/home/oxclo/datafiles/wind2014/*.csv')


def date_and_hour(s):
    dt = parse(s.replace('?',' '))
    hour = dt.hour
    return (dt.strftime("%Y-%m-%d"), hour)

firstTuple = df2.rdd.sample(False, 0.1)
firstTuple = df2.rdd.map(lambda r: (r.Station_ID, r.Interval_End_Time, r.Wind_Velocity_Mtr_Sec, r.Ambient_Temperature_Deg_C))
print firstTuple.first()
filteredTuple = firstTuple.filter(lambda (s,time,wv, temperature): not (temperature==0.0 and wv==0.0))
filteredTuple = firstTuple.filter(lambda (s,time,wv, temperature): (bool(temperature) and bool(wv)))
secondTuple = filteredTuple.map(lambda (s,time,wv,temperature): (s,date_and_hour(time),wv, temperature))
secondTuple = secondTuple.map(lambda (s,(time,hour),wv,temperature): ((s,time,hour),(wv, temperature, 1)))
reducedTuple = secondTuple.reduceByKey(lambda (vw1, temperature1, count1), (vw2, temperature2, count2) : (vw1 + vw2, temperature1 + temperature2, count1 + count2))
reducedTuple = secondTuple.map(lambda ((s,d,h), (wv,t,c)): ((s,d,h), (wv/c,t/c)))

print reducedTuple.first()