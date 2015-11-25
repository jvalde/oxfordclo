
# use this command to start with yarn
# bin/spark-submit --master yarn-cluster wc.py "hdfs://localhost:54310/user/oxclo/books/*"

from pyspark import SparkContext, SparkConf
import sys
import unicodedata

conf = SparkConf().setAppName("wordCount")
sc = SparkContext(conf=conf)


books = sc.textFile(sys.argv[1])
split = books.flatMap(lambda line: line.split())
ascii = split.map(lambda u: str(unicodedata.normalize('NFKD', u).encode('ascii','ignore')))
stripped = ascii.map(lambda input: ''.join(filter(str.isalpha, input)))
numbered = stripped.map(lambda word: (word, 1))
wordcount = numbered.reduceByKey(lambda a,b: a+b)

for k,v in wordcount.collect():
  print k,v

sc.stop()