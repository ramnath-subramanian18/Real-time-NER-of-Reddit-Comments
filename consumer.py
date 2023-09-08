from pyspark.sql import SparkSession
from pyspark import SparkConf
from pyspark.sql.functions import explode
from pyspark.sql.functions import split
import spacy
from pyspark.sql.functions import col
#from pyspark.sql.functions import regexp_replace
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.functions import lower
#import nltk
#from nltk import pos_tag, word_tokenize
#from nltk.internals import config_java

from pyspark.sql.types import ArrayType, StringType
from pyspark.sql.functions import array, col, udf
from pyspark.sql.types import ArrayType, StringType, StructType, StructField

# from pyspark.streaming import StreamingContext
# .set("spark.jars", "/Users/nikhileshamarnath/Downloads/kafka_clients/kafka-clients-3.4.0.jar" )

# stop_words = list(nlp.Defaults.stop_words)
# nltk.download('punkt')
# stop_words = list(stop_words) + [".", "'", "!", "&", "%", "$", "#", ",", "*", "(", ")", "}", "{","I"]
# nltk.download('averaged_perceptron_tagger')

# .config(conf=conf) \



# #@udf(returnType=StringType())
# def ner_test(word):
#     print("INNNNN")
#     # word1=list([word])
#     word1=["hello 1"]
#     #word1 = nltk.word_tokenize(word)
#     post = nltk.pos_tag(word1)
#     # print("PRINT")
#     #print(post)
#     return "test"

# my_udf = udf(ner_test, StringType())


conf = SparkConf().setAppName("MyApp")

spark = SparkSession \
        .builder \
        .appName("test") \
        .master("local") \
        .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

lines = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "reddit-comments") \
    .load() \
    .selectExpr("CAST(value AS STRING)")




query = lines.writeStream \
    .outputMode("append") \
    .format("console") \
    .start()

query.awaitTermination()



# words = lines.select(
#         # explode turns each item in an array into a separate row
#         explode(
#             split(lines.value, ' ')
#         ).alias('word')
#     )
# words=lines
# df_lower = words.select(lower(col("value")).alias('word_pos'))

# def pos_tagging(text):
#     tokens = word_tokenize(text)
#     pos_tags = nltk.pos_tag(tokens)
#     return pos_tags

# pos_tag_udf = udf(pos_tagging, ArrayType(StringType()))

# df_pos_tagged = df_lower.apply("pos_tags", pos_tag_udf(col("word_pos")))

# schema = StructType([
#     StructField("word", StringType(), True),
#     StructField("pos_tag", StringType(), True)
# ])

# @udf(returnType=ArrayType(schema))


# book_postags = pos_tag(["this iss a test string"])
# print("xxxxxxxxxxxx=====",book_postags)

# df_lower.printSchema()
# # df_new = df_lower.withColumn("tag", ner(col("word_pos")))
# df_new = df_lower.withColumn("postag",my_udf("word_pos"))
# df_new.printSchema()
# # df_new = df_lower


# df_new = df_lower.withColumn("tag",pos_tag(col("word_pos"))[0][1])
# dfData['SourceText'].apply(
#                  lamda row: [pos_tag(word_tokenize(row) for item in row])
#df_lower = words.withColumn(col, lower(col["word"]))
# df_tag_counts = df_pos_tagged.groupBy("pos_tags").count()

# output_schema = StructType([
#     StructField("word", StringType()),
#     StructField("count", LongType())
# ])

# words = lines.select(
#         # explode turns each item in an array into a separate row
#         explode(
#             split(lines.value, ' ')
#         ).alias('word')
#     )
# words_lower = words.select(lower("word"))

# df_lower = words.withColumn(col, lower(col["words"]))


# df_filtered = words.filter(~col("word").isin (stop_words))
# df_final = df_filtered.withColumn("words", regexp_replace(col("word"), "[^a-zA-Z0-9\\s]", ""))
# df_filtered2 = df_final.filter(~col("words").isin ("comment"))




# wordCounts = df_lower.groupBy('words').count()
# sortedCounts = wordCounts.orderBy(col("count").desc())


# query = lines.writeStream \
#     .outputMode("append") \
#     .format("console") \
#     .start()


# query.awaitTermination()

#################### Write to topic


# query = sortedCounts \
#     .selectExpr("CAST(words AS STRING) AS key", "to_json(struct(*)) AS value") \
#     .writeStream \
#     .format("kafka") \
#     .option("kafka.bootstrap.servers", "localhost:9092") \
#     .option("topic", "reddit_word_count") \
#     .option("checkpointLocation", "/Users/nikhileshamarnath/Desktop/Niki/Big\ Data/Assignment\ 3/Checkpoint") \
#     .outputMode("complete") \
#     .start()

# query.awaitTermination()
