import json
import argparse
import openai
import os
from kafka import KafkaConsumer

openai.api_key = os.getenv("OPENAI_API_KEY")

REPORT_FILE = "output/accumulated_data.txt"
AI_REPORT_FILE = "output/AI_investing_report.txt"

# Use a global variable to store the state of the program
global_state = {}

def generate_investment_advice(sentiment_counts, total_comments, threshold_ratio=0.15):
    """
    According to the sentiment counts and total comments, generate investment advice:
    - If positive ratio is threshold higher than negative ratio, Buy
    - If positive ratio is threshold lower than negative ratio，Sell
    - else Hold
    """
    if total_comments < 10:
        return "Data insufficient for analysis"

    pos = sentiment_counts["positive"]
    neg = sentiment_counts["negative"]
    total_weight = pos + neg + sentiment_counts["neutral"]

    if total_weight == 0:
        return "Data insufficient for analysis"

    positive_ratio = pos / total_weight
    negative_ratio = neg / total_weight

    if positive_ratio - negative_ratio > threshold_ratio:
        return "Suggestions: Buy"
    elif negative_ratio - positive_ratio > threshold_ratio:
        return "Suggestions: Sell"
    else:
        return "Suggestions: Hold"

def consume_and_aggregate(topic):
    """
    Consume messages from a specified Kafka topic and cumulatively categorise data for the same company.
    Message format required:
 { "CompanyA": {"Comment numbers": x, "Sentiment counts": {"negative": n, "neutral": m, "positive": p}}, ... }
 Call poll() every 30 seconds to get new messages.
    """
    consumer = KafkaConsumer(
        topic,
        bootstrap_servers="localhost:9092",
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        value_deserializer=lambda m: json.loads(m.decode("utf-8"))
    )

    print(f"[INFO] Start consuming messages from topic '{topic}' every 30 seconds ...")
    try:
        while True:
            messages = consumer.poll(timeout_ms=90000)
            if messages:
                for tp, msgs in messages.items():
                    for message in msgs:
                        data = message.value  # For example：{"Apple": {...}, "Google": {...}}
                        for company, info in data.items():
                            if company not in global_state:
                                global_state[company] = {
                                    "Comment numbers": 0,
                                    "Sentiment counts": {"negative": 0.0, "neutral": 0.0, "positive": 0.0}
                                }
                            global_state[company]["Comment numbers"] += info["Comment numbers"]
                            for sentiment in ["negative", "neutral", "positive"]:
                                global_state[company]["Sentiment counts"][sentiment] += info["Sentiment counts"].get(sentiment, 0.0)
                print("[INFO] Consumed messages and updated global_state.")
                generate_final_report()
            else:
                print("[INFO] No new messages in the last 90 seconds.")
    except KeyboardInterrupt:
        print("[INFO] KeyboardInterrupt received. Exiting consumer loop.")

def generate_final_report():
    """
     - Generate a report based on the data accumulated in global_state:
     - Sort company data in descending order of positive sentiment
     - Filter companies with less than 10 reviews or no valid data
     - Call generate_investment_advice for each company to generate a buy/sell/hold recommendation
     Finally, write the report to REPORT_FILE and call the ChatGPT API to generate AI investment analysis report to AI_REPORT_FILE.
    """
    lines = []
    sorted_companies = sorted(
        global_state.items(),
        key=lambda x: x[1]["Sentiment counts"]["positive"],
        reverse=True
    )
    lines.append("\n========== ACCUMULATED REPORT (ALL BATCHES) WITH WEIGHTING ==========\n")
    has_any_company = False

    for company, info in sorted_companies:
        total_comments = info["Comment numbers"]
        sentiment_counts = info["Sentiment counts"]
        if total_comments < 10:
            continue
        if (sentiment_counts["negative"] == 0.0 and
            sentiment_counts["neutral"] == 0.0 and
            sentiment_counts["positive"] == 0.0):
            continue

        has_any_company = True
        advice = generate_investment_advice(sentiment_counts, total_comments)
        lines.append(f"Company: {company}\n")
        lines.append(f"  Comment numbers: {total_comments}\n")
        lines.append(f"  Weighted Sentiment counts: {sentiment_counts}\n")
        lines.append(f"  Investment Advice: {advice}\n\n")

    if not has_any_company:
        lines.append("No companies with >= 10 comments yet.\n\n")

    try:
        with open(REPORT_FILE, "w", encoding="utf-8") as f:
            f.writelines(lines)
        print(f"[INFO] Report data rewrite successfully to {REPORT_FILE}")
    except Exception as e:
        print(f"[ERROR] Failed writing report: {e}")
        return

    text_for_chatgpt = "".join(lines)
    try:
        response = openai.chat.completions.create(
            model="gpt-3.5-turbo",
            messages=[
                {"role": "system", "content": "You are a professional financial analyst."},
                {
                    "role": "user",
                    "content": (
                        "Here is our current investment data:\n\n"
                        + text_for_chatgpt +
                        "\n\nPlease generate a concise, professional investment analysis and advice."
                        +"Separate out which data is the company's stock data (real world company name) and tell which stocks to sell, buy, or hold.\n"
                    ),
                },
            ],
            temperature=0.7,
        )
        chatgpt_report = response.choices[0].message.content
        with open(AI_REPORT_FILE, "w", encoding="utf-8") as f:
            f.write("\n========== ChatGPT Investment Analysis ==========\n")
            f.write(chatgpt_report)
            f.write("\n\n")
        print(f"[INFO] AI financial report generated to {AI_REPORT_FILE}")
    except Exception as e:
        print(f"[ERROR] Failed calling ChatGPT API: {e}")

def main():
    parser = argparse.ArgumentParser(
        description="Kafka Consumer for Aggregated Investment Data with Final Report Generation"
    )
    parser.add_argument(
        "--topic", type=str, default="aggregated_results",
        help="Kafka topic to consume aggregated results from"
    )
    args = parser.parse_args()
    consume_and_aggregate(args.topic)

if __name__ == "__main__":
    main()
