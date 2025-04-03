import argparse
import csv
import time
import json
from kafka import KafkaProducer

def main():
    parser = argparse.ArgumentParser(description="CSV file to Kafka producer")
    parser.add_argument("--topic_name", type=str, default="investing", help="Name of the Kafka topic to produce messages to")
    parser.add_argument("--limit", type=int, default=100, help="Number of submissions to fetch")
    args = parser.parse_args()

    bootstrap_servers = 'localhost:9092'
    topic_name = args.topic_name
    csv_path = 'archive/reddit_wsb.csv'
    limit = args.limit
    interval = 20

    producer = KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )

    processed_ids = set()
    sent_accumulator = 0
    while True:
        sent = 0
        with open(csv_path, newline='', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                if sent >= limit:
                    break

                if not row.get('body'):
                    continue

                if row['id'] in processed_ids:
                    continue

                message = {
                    "title": row['title'],
                    "score": int(row['score']) if row['score'] else None,
                    "id": row['id'],
                    "url": row['url'],
                    "comms_num": int(row['comms_num']) if row['comms_num'] else None,
                    "created_utc": row['timestamp'],
                    "selftext": row['body']
                }

                producer.send(topic_name, value=message)
                print(f"Sent ({sent_accumulator + 1}): {message['title']}")
                processed_ids.add(row['id'])
                sent += 1
                sent_accumulator += 1

        producer.flush()
        print(f"Finished sending {sent} messages. Waiting for {interval} seconds...\n")
        time.sleep(interval)

if __name__ == "__main__":
    main()

