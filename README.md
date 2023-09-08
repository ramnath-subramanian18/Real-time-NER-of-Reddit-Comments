# Real-time-NER-of-Reddit-Comments
A Python application with Reddit Python SDK to stream subreddit comments and push to Kafka. 
Uses PySpark’s streaming feature to filter named entities from topic comments using spacy and push to LogStash. 
Created dashboard for visualization of the named entities in Kibana to analyze the trends in that subreddit.

This project demonstrates a simple pipeline for streaming data from Reddit to Elasticsearch via Kafka and Spark. The pipeline consists of three main components:

producer.py: A Python script that uses the Reddit API to fetch comments from a specified subreddit, and sends them to a Kafka topic.
consumer1.py: A Spark Streaming application that reads from the Kafka topic, processes the comments to generate word counts, and sends the results to a second Kafka topic.
logstash.conf: A Logstash configuration file that reads from the second Kafka topic and writes the data to Elasticsearch.
elasticsearch.yml: An Elasticsearch configuration file that disables security settings.

Setup:
1.	Create two Kafka topics:
a.	bin/kafka-topics.sh --create --topic soccer-reddit --bootstrap-server localhost:9092
b.	bin/kafka-topics.sh --create --topic words-count--bootstrap-server localhost:9092
2.	Edit producer.py to specify the subreddit you want to fetch comments from, and the name of the Kafka topic to send them to. 
3.	Edit consumer1.py to specify the name of the Kafka topic to read from, the name of the Kafka topic to write to, and the path to the checkpoint directory.
4.	Start the producer	
a.	Python producer.py
5.	Start the consumer 
spark-submit --driver-memory 2g --executor-memory 2g --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.1 consumer1.py
6.	Edit logstash.conf to specify the name of the Kafka topic to read from, and the name of the Elasticsearch index to write to. 
7.	Edit elasticsearch.yml to disable security settings.
8.	Start Logstash:
a.	bin/logstash -f logstash.conf
9.	Start Elasticsearch:
a.	bin/elasticsearch
	
The results can be viewed in Kibana by navigating to http://localhost:5601. The data should be automatically indexed and available for searching and visualization.
![image](https://user-images.githubusercontent.com/74395528/233898213-4840c542-d167-4d43-b469-80b0ec57a5a6.png)
