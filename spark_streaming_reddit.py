from pyspark.sql import SparkSession
from pyspark.streaming import StreamingContext
import json

spark = SparkSession.builder.appName("RedditStream").getOrCreate()
sc = spark.sparkContext
ssc = StreamingContext(sc, 5)

host = "localhost"
port = 9999
lines = ssc.socketTextStream(host, port)

def parse_json(line):
    try:
        data = json.loads(line)
        return (data["title"], data["text"])
    except:
        return ("", "")

posts = lines.map(parse_json)

words = posts.flatMap(lambda x: x[0].split(" "))
word_counts = words.map(lambda word: (word.lower(), 1)).reduceByKey(lambda a, b: a + b)
word_counts.pprint(10)

ssc.start()
ssc.awaitTermination()
