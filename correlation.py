from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext

from numpy import array
from scipy import spatial


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

reducedTuple = df2.rdd.sample(False, 0.1)
reducedTuple = df2.rdd.map(lambda r: (r.Station_ID, r.Interval_End_Time, r.Wind_Velocity_Mtr_Sec, r.Ambient_Temperature_Deg_C))
reducedTuple = reducedTuple.filter(lambda (s,time,wv, temperature): not (temperature==0.0 and wv==0.0))
reducedTuple = reducedTuple.filter(lambda (s,time,wv, temperature): (bool(temperature) and bool(wv)))
reducedTuple = reducedTuple.map(lambda (s,time,wv,temperature): (s,date_and_hour(time),wv, temperature))
reducedTuple = reducedTuple.map(lambda (s,(time,hour),wv,temperature): ((s,time,hour),(wv, temperature, 1)))
reducedTuple = reducedTuple.reduceByKey(lambda (vw1, temperature1, count1), (vw2, temperature2, count2) : (vw1 + vw2, temperature1 + temperature2, count1 + count2))
reducedTuple = reducedTuple.map(lambda ((s,d,h), (wv,t,c)): ((s,d,h), (wv/c,t/c)))
reducedTuple = reducedTuple.map(lambda ((s,d,h),(wv,t)): ((d,h,s),(wv,t)))


df = sqlc2.read.format('com.databricks.spark.csv').\
options(header='true', inferschema='true').\
load('/home/oxclo/datafiles/incidents/sfpd.csv')


def date_and_hour(date, hour):
    dt = parse(date + " " + hour)
    hour = dt.hour
    return (dt.strftime("%Y-%m-%d"), hour, dt.strftime("%Y"))

def locate(l,index,locations):
	distance,i = index.query(l)
	return locations[i]    

firstTuple = df.rdd.sample(False, 0.1)
firstTuple = df.rdd.map(lambda r: (r.Date, r.Time, [r.X,r.Y]))
firstTuple = firstTuple.map(lambda (d,t,l): (date_and_hour(d,t), l) )
firstTuple = firstTuple.filter(lambda ((d,h,y),l): y == "2014")
firstTuple = firstTuple.map(lambda ((d,h,y),l): (d,h,l))


located = firstTuple.map(lambda (d, h, l): (locate(l, \
spatial.KDTree(array( \
[[37.7816834,-122.3887657],\
[37.7469112,-122.4821759],\
[37.7411022,-120.804151],\
[37.4834543,-122.3187302],\
[37.7576436,-122.3916382],\
[37.7970013,-122.4140409],\
[37.748496,-122.4567461],\
[37.7288155,-122.4210133],\
[37.5839487,-121.9499339],\
[37.7157156,-122.4145311],\
[37.7329613,-122.5051491],\
[37.7575891,-122.3923824],\
[37.7521169,-122.4497687]])),
["SF18", "SF04", "SF15", "SF17", "SF36", "SF37",\
"SF07", "SF11", "SF12", "SF14", "SF16", "SF19", "SF34"] ),d,h))

located = located.map(lambda (l,d,h): ((d,h,l),1))

located = located.reduceByKey(lambda a, b : a + b)

joined = located.join(reducedTuple)

print joined.first()

from pyspark.mllib.linalg import Vectors
from pyspark.mllib.stat import Statistics

vecs = joined.map(lambda ((d,h,s),(c,(w,t))): Vectors.dense([t,w,c]))

print(Statistics.corr(vecs))