import praw
import json 
from kafka import KafkaProducer

def produce(subreddit_name,producer_name):
    reddit = praw.Reddit(client_id = '0PzRdwi1ASGVRNTgdaja1A', 
                        client_secret = 'BwX2RitRE32m1JVWzRvL4GhQE3yLcA', 
                        user_agent = 'NikiBigData', 
                        username = 'NikiBigData',
                        password = 'Family@383')

    producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
                            value_serializer=lambda x: json.dumps(x).encode('utf-8'))


    subred = reddit.subreddit(subreddit_name)


    for comment in subred.stream.comments(skip_existing=True):
        message = {'comment': comment.body}
        producer.send(producer_name, value=message)
        print(comment.body)

    producer.close()

subreddit_name="soccer"
kafka_topic="soccer-reddit"
produce(subreddit_name,kafka_topic)