import argparse
import time
import json
import praw
from kafka import KafkaProducer

def main():
    # 1. 使用 argparse 获取命令行参数
    parser = argparse.ArgumentParser(description="Reddit to Kafka producer")
    parser.add_argument("--subreddit", type=str, default="WallStreetBets", help="Name of the subreddit to crawl")
    parser.add_argument("--topic_name", type=str, default="investing", help="Name of the Kafka topic to produce messages to")
    parser.add_argument("--limit", type=int, default=100, help="Number of submissions to fetch")
    parser.add_argument("--keyword", type=str, required=False, help="Keyword to search for")
    args = parser.parse_args()

    # 2. 配置 Reddit API
    reddit = praw.Reddit(
        client_id="nBROxkqXX7OBrfnBFPxLLw",
        client_secret="drO4H7CGeSg7XqTrfrTseCeqgAmGMA",
        user_agent="testrun1",
        username="cs651"
    )

    # 3. 获取 subreddit、topic 名称，以及抓取数量 limit
    subreddit = reddit.subreddit(args.subreddit)
    topic_name = args.topic_name
    limit = args.limit
    keyword = args.keyword if args.keyword else ""

    # 4. 配置 Kafka Producer
    producer = KafkaProducer(
        bootstrap_servers=["localhost:9092"],
        value_serializer=lambda v: json.dumps(v).encode("utf-8")
    )

    processed_ids = set()

    # 5. 主循环：从指定 subreddit 中抓取内容并发送到 Kafka
    try:
        while True:
            if not keyword:
                submissions = subreddit.new(limit=limit)
            else:
                submissions = subreddit.search(keyword,limit=limit)

            for submission in submissions:
                if submission.id in processed_ids:
                    continue

                data = {
                    "title": submission.title,
                    "score": submission.score,
                    "id": submission.id,
                    "url": submission.url,
                    "comms_num": submission.num_comments,
                    "created_utc": submission.created_utc,
                    "selftext": submission.selftext
                }

                producer.send(topic_name, value=data)
                print(f"Sent to Kafka: {data['title']}")

                processed_ids.add(submission.id)

            producer.flush()
            time.sleep(20)

    except KeyboardInterrupt:
        print("Stopping the producer and closing the connection...")
    finally:
        producer.close()

if __name__ == "__main__":
    main()

