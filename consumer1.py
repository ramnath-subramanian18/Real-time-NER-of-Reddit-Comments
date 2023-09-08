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


def consumer(kafka_topic,kafka_topic1,path):

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
        .option("subscribe", kafka_topic) \
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
    #sortedCounts = wordCounts.orderBy(col("count").desc())
    stop=[""]
    df_filltered3 = wordCounts.filter(~col("word").isin (stop))


    def pos_tag_words(words):
        doc = nlp(words)
        return [(token.pos_) for token in doc]

    udf_pos_tag_words = udf(pos_tag_words, StringType())

    df_tagged = df_filltered3.withColumn("pos_tags", udf_pos_tag_words("word"))

    df_final = df_tagged.filter(col("pos_tags").contains("[NOUN]") | col("pos_tags").contains("[PROPN]"))
    df_final = df_final.drop("pos_tags")




    ##################### Write to console
    # query = df_final.writeStream \
    #     .outputMode("update") \
    #     .format("console") \
    #     .start()

    # query.awaitTermination()


    ################### Write to topic


    query = df_final \
        .selectExpr("CAST(word AS STRING) AS key", "to_json(struct(*)) AS value")\
        .writeStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("topic", kafka_topic1) \
        .option("checkpointLocation", path) \
        .outputMode("update")\
        .start()

    query.awaitTermination()

path="/Users/nikhileshamarnath/Desktop/Niki/Big Data/Assignment 3/Checkpoint"
kalka_topic2="words-count"
kalka_topic1="soccer-reddit"
consumer(kalka_topic1,kalka_topic2,path)

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
