import datetime

import praw
import json
from kafka import KafkaProducer


def main():
    # 1. Configure Reddit API
    reddit = praw.Reddit(
        client_id="nBROxkqXX7OBrfnBFPxLLw",
        client_secret="drO4H7CGeSg7XqTrfrTseCeqgAmGMA",
        user_agent="testrun1",
        username="cs651"
    )

    # 2. Initialize Kafka Producer
    producer = KafkaProducer(
        bootstrap_servers='localhost:9092',
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )

    # Kafka subject
    topic_name = "test_topic"

    # 3. Choose Subreddit
    subreddit = reddit.subreddit("investing")

    # 4.Read posts and send to Kafka
    #    set limit to 500
    for submission in subreddit.search("invest",limit=500):
        # 组装要发送的数据
        data = {
            "title": submission.title,  # 帖子标题
            "score": submission.score,  # Post upvotes
            "id": submission.id,  # post ID
            "url": submission.url,  # post URL
            "comms_num": submission.num_comments,  # comment number
            "created_utc": submission.created_utc,  # create timestamp
            "selftext": submission.selftext  # Post body
        }
        #timestamp_ms = convert_to_timestamp_ms(row["created_at"])

        # Send to Kafka
        #producer.send(topic_name, value=data, timestamp_ms=timestamp_ms)
        producer.send(topic_name, value=data)
        print(f"Send to Kafka: {data['title']}")

    # Push the remaining messages
    producer.flush()
    producer.close()


if __name__ == "__main__":
    main()
