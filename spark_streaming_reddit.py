import findspark
findspark.init(r"E:\downloads\spark-3.4.4-bin-hadoop3")

import time
import spacy
import torch.nn.functional as F
from transformers import BertTokenizer, BertForSequenceClassification

from transformers import pipeline
# Use the pre-trained FinBERT model for sentiment analysis
sentiment_pipeline = pipeline(
    task="sentiment-analysis",
    model="yiyanghkust/finbert-tone",
    device=0,
    batch_size=100
)
from kafka import KafkaProducer
producer_conf = {
    'bootstrap.servers': 'localhost:9092'
}
import argparse
import json
import findspark

findspark.init(r"E:\downloads\spark-3.4.4-bin-hadoop3")

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType


class KafkaPartitionQuery:
    def __init__(self, topic, partition):
        """
        :param bootstrap_servers: address of Kafka servers, e.g. "localhost:9092"
        :param topic: name of Topic to subscribe to
        :param partition: specify the partition number to read (int), if None then subscribe to the entire Topic
        """
        self.bootstrap_servers=producer_conf["bootstrap.servers"]
        self.topic = topic
        self.partition = partition
        self.spark = SparkSession.builder.appName("KafkaPartitionQuery").getOrCreate()

    def start(self):
        if self.partition is not None:
            # Use assign to subscribe only to the specified partition
            # Construct a JSON string, e.g. '{"investing": [0]}' or '{"investing": [1]}'
            assign_option = json.dumps({self.topic: [self.partition]})
            print(f"[INFO] Subscribe to a Topic using assign() '{self.topic}' 的分区 {self.partition}.")
            kafka_df = (
                self.spark.readStream
                .format("kafka")
                .option("kafka.bootstrap.servers", self.bootstrap_servers)
                .option("assign", assign_option)
                .option("startingOffsets", "earliest")
                .load()
            )
        else:
            # Subscribe to the entire Topic using subscribe
            print(f"[INFO] Use subscribe() to subscribe to the entire Topic. '{self.topic}'.")
            kafka_df = (
                self.spark.readStream
                .format("kafka")
                .option("kafka.bootstrap.servers", producer_conf["bootstrap.servers"])
                .option("subscribe", args.topic) #Get data from all partitions
                .option("startingOffsets", "earliest")
                .option("group.id", args.group_id)  # Attention: group.id is required for Kafka source
                .load()
            )
        raw_df = kafka_df.selectExpr("CAST(value AS STRING) as json_str")
        reddit_schema = StructType([
            StructField("title", StringType(), True),
            StructField("score", IntegerType(), True),
            StructField("id", StringType(), True),
            StructField("url", StringType(), True),
            StructField("comms_num", IntegerType(), True),
            StructField("created_utc", DoubleType(), True),
            StructField("selftext", StringType(), True)
        ])
        parsed_df = raw_df.select(from_json(col("json_str"), reddit_schema).alias("data")).select("data.*")

        query = (
            parsed_df.writeStream
            .outputMode("append")
            .foreachBatch(process_batch)
            .start()
        )
        query.awaitTermination()


intermediate_producer =  KafkaProducer(
        bootstrap_servers=[producer_conf["bootstrap.servers"]],
        value_serializer=lambda v: json.dumps(v).encode("utf-8")
)
intermediate_topic = "aggregated_results"

nlp = spacy.load("en_core_web_sm")
tokenizer = BertTokenizer.from_pretrained("yiyanghkust/finbert-tone")
model = BertForSequenceClassification.from_pretrained("yiyanghkust/finbert-tone")

# Accumulated state across all batches
global_state = {}

def send_aggregated_to_kafka():
    companies_sent = 0
    for company, info in global_state.items():
        # if info["Comment numbers"] < 10:
        #     continue
        message = {company: info}
        intermediate_producer.send("aggregated_results", message)
        companies_sent += 1
    if companies_sent > 0:
        intermediate_producer.flush()
        print(f"[INFO] Aggregated data for {companies_sent} companies sent to Kafka topic 'aggregated-results'.")
    else:
        print("[INFO] No company with sufficient data to send.")

def extract_companies_with_spacy(text):
    """Use spaCy to extract company names from the given text."""
    doc = nlp(text)
    companies = {ent.text.strip() for ent in doc.ents if ent.label_ == "ORG"}
    return list(companies)

def finbert_sentiment_analysis(text):
    """Use FinBERT model to analyze sentiment of the given text."""
    inputs = tokenizer(text, return_tensors="pt", truncation=True, max_length=512)
    outputs = model(**inputs)
    probs = F.softmax(outputs.logits, dim=-1)
    sentiment_labels = ["negative", "neutral", "positive"]
    return {label: probs[0][i].item() for i, label in enumerate(sentiment_labels)}

def compute_weight(score, comms_num, created_utc):
    """
    According to the score, comments number, and created time, compute a weighted score.
    The strategy is as follows:
    1) score weight: score ∈ [0, 500] maps to [1.0, 2.0], and a high score is also capped at 2.0;
    2) comms_num weight: comms_num ∈ [0, 100] is mapped to [1.0, 2.0], and a high comms_num is also capped at 2.0;
    3) time_decay: assume 7 days as a ‘half-life’, the newer the post, the smaller the decay.
       time_weight = 2^(- (current time - created_utc) / 604800 )
       created_utc is a UNIX timestamp in seconds, larger means newer.
    """
    score = score if score is not None else 0
    score_w = 1.0 + min(score, 500) / 500.0

    comms_num = comms_num if comms_num is not None else 0
    comms_w = 1.0 + min(comms_num, 100) / 100.0

    now_ts = time.time()
    diff_seconds = max(0, now_ts - (created_utc or 0))
    half_life = 7 * 24 * 3600  # seconds in 7 days
    time_w = 2 ** (- diff_seconds / half_life)

    total_weight = score_w * comms_w * time_w
    return total_weight

def classify_with_metadata(rows):
    """
    For each record, extract (company, [row, row, ...]) for each record.
    Where row contains title, selftext, score, comms_num, created_utc etc.
    Returns the dictionary: { ‘Apple’: [row1, row2], ‘Google’: [...], ... ,... }
    """
    company_map = {}
    for r in rows:
        title_str = r["title"] or ""
        selftext_str = r["selftext"] or ""
        combined_text = (title_str + " " + selftext_str).strip()
        if not combined_text:
            continue

        companies = extract_companies_with_spacy(combined_text)
        for c in companies:
            company_map.setdefault(c, []).append(r)
    return company_map

def aggregate_sentiments_with_weight(row_list):
    """
    Analyze sentiment for each row in the list and aggregate the sentiment counts with weight.
    Return a dictionary of sentiment counts: {"negative": x, "neutral": y, "positive": z}
    """
    sentiment_counts = {"negative": 0.0, "neutral": 0.0, "positive": 0.0}
    for r in row_list:
        title_str = r["title"] or ""
        selftext_str = r["selftext"] or ""
        combined_text = (title_str + " " + selftext_str).strip()
        result_probs = finbert_sentiment_analysis(combined_text)
        sentiment_label = max(result_probs, key=result_probs.get)
        w = compute_weight(
            score=r["score"],
            comms_num=r["comms_num"],
            created_utc=r["created_utc"]
        )
        sentiment_counts[sentiment_label] += w
    return sentiment_counts


def update_global_state(batch_results):
    """
    Integrate data to global_state：
    global_state Structure:
    {
      "Apple": {
          "Comment numbers": numbers of comments,
          "Sentiment counts": {"negative": ..., "neutral": ..., "positive": ...}
      },
      ...
    }
    """
    for company, data in batch_results.items():
        if company not in global_state:
            global_state[company] = {
                "Comment numbers": 0,
                "Sentiment counts": {"negative": 0.0, "neutral": 0.0, "positive": 0.0}
            }
        global_state[company]["Comment numbers"] += data["Comment numbers"]
        gs_counts = global_state[company]["Sentiment counts"]
        gs_counts["negative"] += data["Sentiment counts"]["negative"]
        gs_counts["neutral"] += data["Sentiment counts"]["neutral"]
        gs_counts["positive"] += data["Sentiment counts"]["positive"]


def process_batch(batch_df, batch_id):
    if batch_df.rdd.isEmpty():
        print(f"[batch {batch_id}] No data")
        return

    rows = batch_df.select("title", "selftext", "score", "comms_num", "created_utc").collect()
    texts = [((r["title"] or "") + " " + (r["selftext"] or "")).strip() for r in rows]
    docs = list(nlp.pipe(texts, batch_size=32, disable=["parser", "tagger"]))
    labels = sentiment_pipeline(texts, truncation=True, max_length=512)
    sentiments = [res["label"].lower() for res in labels]

    batch_results = {}
    for idx, r in enumerate(rows):
        weight = compute_weight(r["score"], r["comms_num"], r["created_utc"])
        label = sentiments[idx]
        for ent in docs[idx].ents:
            if ent.label_ == "ORG":
                comp = ent.text.strip()
                stats = batch_results.setdefault(comp, {"Comment numbers": 0, "Sentiment counts": {"negative": 0.0, "neutral": 0.0, "positive": 0.0}})
                stats["Comment numbers"] += 1
                stats["Sentiment counts"][label] += weight

    if batch_results:
        print(f"\n===== BATCH {batch_id} Weighted Results =====")
        for comp, info in sorted(batch_results.items(), key=lambda x: x[1]["Sentiment counts"]["positive"], reverse=True):
            print(f"\n{comp}:")
            print(" Comment numbers:", info["Comment numbers"])
            print(" Weighted Sentiment counts:", info["Sentiment counts"])

    update_global_state(batch_results)
    send_aggregated_to_kafka()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Spark Structured Streaming Kafka consumer with weighted sentiment analysis.")
    parser.add_argument("--topic", type=str, default="investing", help="Kafka topic to subscribe to.")
    parser.add_argument("--group_id", type=str, default="reddit-consumer-group", help="Kafka consumer group id.")
    parser.add_argument("--partition", type=int, default=None,help="Optional: specify partition number to assign (e.g. 0 or 1)")
    args = parser.parse_args()

    app = KafkaPartitionQuery(args.topic, args.partition)

    app.start()

