from pyspark.sql import SparkSession
from pyspark import SparkConf
from pyspark.sql.functions import explode
from pyspark.sql.functions import split
from pyspark.sql.functions import col
from pyspark.sql.functions import regexp_replace
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.functions import lower
import nlp
import nltk
import spacy
nlp = spacy.load("en_core_web_sm")
from nltk.corpus import stopwords


from pyspark.sql.types import ArrayType, StringType
from pyspark.sql.types import ArrayType, StringType, StructType, StructField

nltk.download('stopwords')
stop_words = stopwords.words('english')
stop_words = list(stop_words) + [".", "'", "!", "&", "%", "$", "#", ",", "*", "(", ")", "}", "{","I"]

conf = SparkConf().setAppName("MyNewApp")


spark = SparkSession \
        .builder \
        .appName("testing") \
        .master("local") \
        .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")
spark.conf.set("spark.sql.streaming.failOnDataLoss", "false")


lines = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "reddit-soccer") \
    .option("failOnDataLoss", "false")\
    .load() \
    .selectExpr("CAST(value AS STRING)")

############ PROCESSING

words = lines.select(
        explode(
            split(lines.value, ' ')
        ).alias('word')
    )

df_lower = words.select(lower(col("word")).alias('word'))
df_filtered = df_lower.filter(~col("word").isin (stop_words))
df_filtered1 = df_filtered.withColumn("word", regexp_replace(col("word"), "[^a-zA-Z0-9\\s]", ""))
df_filtered2 = df_filtered1.filter(~col("word").isin ("comment"))

wordCounts = df_filtered2.groupBy('word').count()
sortedCounts = wordCounts.orderBy(col("count").desc())
stop=[""]
df_filltered3 = sortedCounts.filter(~col("word").isin (stop))


def pos_tag_words(words):
    doc = nlp(words)
    return [(token.pos_) for token in doc]

udf_pos_tag_words = udf(pos_tag_words, StringType())

df_tagged = df_filltered3.withColumn("pos_tags", udf_pos_tag_words("word"))

df_final = df_tagged.filter(col("pos_tags").contains("[NOUN]") | col("pos_tags").contains("[PROPN]"))
df_final = df_final.drop("pos_tags")




##################### Write to console
# query = df_final.writeStream \
#     .outputMode("complete") \
#     .format("console") \
#     .start()

# query.awaitTermination()


################### Write to topic


query = df_final \
    .selectExpr("CAST(word AS STRING) AS key", "to_json(struct(*)) AS value")\
    .writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("topic", "word-count") \
    .option("checkpointLocation", "/Users/nikhileshamarnath/Desktop/Niki/Big Data/Assignment 3/Checkpoint") \
    .outputMode("complete")\
    .start()

query.awaitTermination()