from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("PopularHero")
sc = SparkContext(conf = conf)

#function that returns ID with the no. of co-occurences
def countCoOccurences(line):
    elements = line.split()
    return (int(elements[0]), len(elements) - 1)

def parseNames(line):
    fields = line.split('\"')
    return (int(fields[0]), fields[1].encode("utf8"))

names = sc.textFile("c:]/SparkCourse/marvel-names.txt")
namesRdd = names.map(parseNames)

lines = sc.textFile("c:/SparkCourse/marvel-graph.txt")

pairings = lines.map(countCoOccurences)
totalFriendsByCharacter = pairings.reduceByKey(lambda x, y : x + y)
#to find the total no. of co-occurences for each ID
flipped = totalFriendsByCharacter.map(lambda xy : (xy[1], xy[0]))

#returns (no. of occurences,ID)
mostPopular = flipped.max()

# Lookup the name corresponding to the most popular ID
mostPopularName = namesRdd.lookup(mostPopular[1])[0]

#print results
print(str(mostPopularName) + " is the most popular superhero, with " + \
    str(mostPopular[0]) + " co-appearances.")
