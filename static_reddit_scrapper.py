import csv
import json
from kafka import KafkaProducer

bootstrap_servers = 'localhost:9092'
topic_name = 'test_topic'
csv_path = 'archive/reddit_wsb.csv'
max_messages = 50

producer = KafkaProducer(
    bootstrap_servers=bootstrap_servers,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

sent = 0
with open(csv_path, newline='', encoding='utf-8') as f:
    reader = csv.DictReader(f)
    for row in reader:
        if sent >= max_messages:
            break

        # Skip empty rows
        if not row.get('body'):
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
        print(f"Sent ({sent+1}): {message['title']}")
        sent += 1

producer.flush()
producer.close()
print(f"Finished sending {sent} messages.")
