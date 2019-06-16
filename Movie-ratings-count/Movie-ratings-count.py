from pyspark import SparkConf, SparkContext
import collections

#Set up our context
#Set master node as local- runs on local machine, not on a cluster
conf = SparkConf().setMaster("local").setAppName("RatingsHistogram")

#sparkContext object
sc = SparkContext(conf = conf)

#Load the data
#Creating RDD using sc.textfile
#textfile breaks up the input file line-by-line so that every line of the text corresponds to one value in your RDD
lines = sc.textFile("file:///SparkCourse/ml-100k/u.data")#path of the data file ml-100k.data

#Extractig(map) the data we care about
#splitting based on whitespaces and exract field no. 2
ratings = lines.map(lambda x: x.split()[2])
result = ratings.countByValue()

#Sort according to key value(rating) and display results
sortedResults = collections.OrderedDict(sorted(result.items()))
for key, value in sortedResults.items():
    print("%s %i" % (key, value))
