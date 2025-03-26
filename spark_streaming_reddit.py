import findspark
findspark.init(r"E:\downloads\spark-3.4.4-bin-hadoop3")

import time
import spacy
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
import torch.nn.functional as F
from transformers import BertTokenizer, BertForSequenceClassification

import openai
import os
openai.api_key = os.getenv("OPENAI_API_KEY")

from transformers import pipeline
# 批量情感分析 pipeline（自动利用 GPU if available，batch_size=32）
sentiment_pipeline = pipeline(
    task="sentiment-analysis",
    model="yiyanghkust/finbert-tone",
    device=0,
    batch_size=100
)

REPORT_FILE = "output/accumulated_data.txt"

AI_REPORT_FILE = "output/AI_investing_report.txt"


nlp = spacy.load("en_core_web_sm")

tokenizer = BertTokenizer.from_pretrained("yiyanghkust/finbert-tone")
model = BertForSequenceClassification.from_pretrained("yiyanghkust/finbert-tone")

# 跨批次累积的全局状态 (公司 → 累计评论数 & 累计情感计数)
global_state = {}


def extract_companies_with_spacy(text):
    """使用 spaCy 对文本进行实体识别，提取 'ORG' 类型的公司名。"""
    doc = nlp(text)
    companies = {ent.text.strip() for ent in doc.ents if ent.label_ == "ORG"}
    return list(companies)


def finbert_sentiment_analysis(text):
    """对给定文本进行 FinBERT 情感分析，返回 negative, neutral, positive 的概率。"""
    inputs = tokenizer(text, return_tensors="pt", truncation=True, max_length=512)
    outputs = model(**inputs)
    probs = F.softmax(outputs.logits, dim=-1)
    sentiment_labels = ["negative", "neutral", "positive"]
    return {label: probs[0][i].item() for i, label in enumerate(sentiment_labels)}


def compute_weight(score, comms_num, created_utc):
    """
    根据 score、comms_num 以及帖子的发布时间对每条帖子计算一个加权系数。

    简化示例策略：
    1) score 权重：score ∈ [0, 500] 映射到 [1.0, 2.0]，过高的 score 也在 2.0 封顶；
    2) comms_num 权重：评论数 ∈ [0, 100] 映射到 [1.0, 2.0]，过高的评论数也在 2.0 封顶；
    3) 时间衰减：假设 7 天为一个“半衰期”，越新的帖子衰减越小。
       time_weight = 2^(- (当前时间 - created_utc) / 604800 )
       created_utc 是以秒为单位的 UNIX 时间戳，越大代表越新。
    """
    # 1) score 权重：最低 1.0，最高 2.0
    score = score if score is not None else 0
    score_w = 1.0 + min(score, 500) / 500.0

    # 2) comms_num 权重：最低 1.0，最高 2.0
    comms_num = comms_num if comms_num is not None else 0
    comms_w = 1.0 + min(comms_num, 100) / 100.0

    # 3) 时间衰减：7 天(604800 秒)为半衰期
    now_ts = time.time()
    # 如果 created_utc > now_ts, 代表将来时间，也做一个保护
    diff_seconds = max(0, now_ts - (created_utc or 0))
    half_life = 7 * 24 * 3600  # 7 天对应秒数
    time_w = 2 ** (- diff_seconds / half_life)

    total_weight = score_w * comms_w * time_w
    return total_weight


def classify_with_metadata(rows):
    """
    对每条记录，提取 (公司, [row, row, ...])。
    其中 row 包含 title, selftext, score, comms_num, created_utc 等。
    返回字典：{ "Apple": [row1, row2], "Google": [...], ... }
    """
    company_map = {}
    for r in rows:
        # 取出文本
        title_str = r["title"] or ""
        selftext_str = r["selftext"] or ""
        combined_text = (title_str + " " + selftext_str).strip()
        if not combined_text:
            continue

        # 用 spacy 找出本条记录涉及到的公司
        companies = extract_companies_with_spacy(combined_text)
        for c in companies:
            company_map.setdefault(c, []).append(r)
    return company_map


def aggregate_sentiments_with_weight(row_list):
    """
    对同一家公司的多条帖子进行情感分析，并基于 compute_weight 加权统计。
    返回一个字典，形如： {"negative": x, "neutral": y, "positive": z} (浮点数)
    """
    sentiment_counts = {"negative": 0.0, "neutral": 0.0, "positive": 0.0}
    for r in row_list:
        # 计算情感
        title_str = r["title"] or ""
        selftext_str = r["selftext"] or ""
        combined_text = (title_str + " " + selftext_str).strip()
        result_probs = finbert_sentiment_analysis(combined_text)
        sentiment_label = max(result_probs, key=result_probs.get)

        # 计算加权系数
        w = compute_weight(
            score=r["score"],
            comms_num=r["comms_num"],
            created_utc=r["created_utc"]
        )
        sentiment_counts[sentiment_label] += w
    return sentiment_counts


def generate_investment_advice(sentiment_counts,total_comments, threshold_ratio=0.15):
    """
    根据情感比例给出买/卖/持有建议：
    - 如果正面比例比负面高 0.2 以上，Buy
    - 如果负面比例比正面高 0.2 以上，Sell
    - 否则 Hold
    """

    if total_comments < 10:
        return "Data insufficient for analysis"

    pos = sentiment_counts["positive"]
    neg = sentiment_counts["negative"]
    total_weight = pos + neg + sentiment_counts["neutral"]

    # 防止除零
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

def update_global_state(batch_results):
    """
    将本批次公司统计合并到 global_state：
    global_state 结构:
    {
      "Apple": {
          "Comment numbers": <加权后帖子数？ or 实际帖子数？>,
          "Sentiment counts": {"negative": ..., "neutral": ..., "positive": ...}
      },
      ...
    }
    这里“Comment numbers”依旧累加帖子数(整形)，情感则累加加权计数(浮点)。
    """
    for company, data in batch_results.items():
        if company not in global_state:
            global_state[company] = {
                "Comment numbers": 0,
                "Sentiment counts": {"negative": 0.0, "neutral": 0.0, "positive": 0.0}
            }
        # 统计帖子总数（不加权）
        global_state[company]["Comment numbers"] += data["Comment numbers"]

        # 情感分数加权
        gs_counts = global_state[company]["Sentiment counts"]
        gs_counts["negative"] += data["Sentiment counts"]["negative"]
        gs_counts["neutral"] += data["Sentiment counts"]["neutral"]
        gs_counts["positive"] += data["Sentiment counts"]["positive"]


def generate_final_report():
    """
    将累计报告写入文件，并调用 ChatGPT API 生成投资报告。
    """

    # 准备要写入本地文件的文本内容
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
        if (sentiment_counts["negative"] == 0.0
            and sentiment_counts["neutral"] == 0.0
            and sentiment_counts["positive"] == 0.0):
            continue

        has_any_company = True
        advice = generate_investment_advice(sentiment_counts, total_comments)

        lines.append(f"Company: {company}\n")
        lines.append(f"  Comment numbers: {total_comments}\n")
        lines.append(f"  Weighted Sentiment counts: {sentiment_counts}\n")
        lines.append(f"  Investment Advice: {advice}\n\n")

    if not has_any_company:
        lines.append("No companies with >= 10 comments yet.\n\n")

    # ========== 1) 写入本地文件（追加写入） ==========
    try:
        with open(REPORT_FILE, "w", encoding="utf-8") as f:
            f.writelines(lines)
        print(f"[INFO] Report appended successfully to {REPORT_FILE}")
    except Exception as e:
        print(f"[ERROR] Failed writing report: {e}")
        return  # 如果写文件都失败了，就不再调用 API

    # ========== 2) 调用 ChatGPT API，获得更详细的投资报告 ==========
    # 组合成一个长字符串，作为 ChatGPT 的上下文
    text_for_chatgpt = "".join(lines)

    # 你需要在别处设置好 openai.api_key 或从环境变量读取
    # 例如: openai.api_key = "YOUR_OPENAI_API_KEY"
    try:
        response = openai.chat.completions.create(
            model="gpt-3.5-turbo",
            # 也可用 "gpt-4" 等，视情况和权限而定
            messages=[
                {"role": "system", "content": "You are a professional financial analyst."},
                {
                    "role": "user",
                    "content": (
                        "Here is our current investment data:\n\n"
                        + text_for_chatgpt
                        + "\n\nPlease generate a concise, professional investment analysis and advice."
                    ),
                },
            ],
            temperature=0.7,
        )
        chatgpt_report = response.choices[0].message.content

        with open(AI_REPORT_FILE, "a", encoding="utf-8") as f:
            f.write("\n========== ChatGPT Investment Analysis ==========\n")
            f.write(chatgpt_report)
            f.write("\n\n")
        print(f"[INFO] AI financial report generated to {AI_REPORT_FILE}")

    except Exception as e:
        print(f"[ERROR] Failed calling ChatGPT API: {e}")



def analyze_companies_with_weight(rows):
    """
    综合调用以上函数，对当前批次的记录进行加权情感分析。
    返回:
    {
      "Apple": {
        "Comment numbers": N(本批次公司帖子总数，不加权),
        "Sentiment counts": {"negative": x, "neutral": y, "positive": z} (加权浮点)
      }
    }
    """
    # 先根据实体把记录划分到各公司
    company_map = classify_with_metadata(rows)

    results = {}
    for company, row_list in company_map.items():
        sentiment_counts = aggregate_sentiments_with_weight(row_list)
        results[company] = {
            "Comment numbers": len(row_list),  # 实际帖子数
            "Sentiment counts": sentiment_counts
        }
    return results


# ================ #
#  创建 Spark 会话
# ================ #
spark = SparkSession.builder \
    .appName("RedditKafkaConsumerWeighted") \
    .getOrCreate()

# ================ #
#  从 Kafka 读数据
# ================ #
topic = "test_topic"
kafka_df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", topic)
    .option("startingOffsets", "earliest")  # 可改为 'latest'
    .load()
)

# 将二进制的 "value" 列转换为字符串
raw_df = kafka_df.selectExpr("CAST(value AS STRING) as json_str")

# 定义数据的 JSON Schema
reddit_schema = StructType([
    StructField("title", StringType(), True),
    StructField("score", IntegerType(), True),
    StructField("id", StringType(), True),
    StructField("url", StringType(), True),
    StructField("comms_num", IntegerType(), True),
    StructField("created_utc", DoubleType(), True),
    StructField("selftext", StringType(), True)
])

# 解析 JSON 数据并投影
parsed_df = raw_df \
    .select(from_json(col("json_str"), reddit_schema).alias("data")) \
    .select("data.*")


# def process_batch(batch_df, batch_id):
#     """foreachBatch 的回调函数，用于处理每个微批。"""
#     if batch_df.rdd.isEmpty():
#         print(f"[batch {batch_id}] >>> No data in this batch.")
#         return
#
#     # 收集当前批次相关字段到 Driver 侧
#     # 包括 title, selftext, score, comms_num, created_utc
#     rows = batch_df.select(
#         "title", "selftext", "score", "comms_num", "created_utc"
#     ).collect()
#
#     # 分析该批次
#     batch_results = analyze_companies_with_weight(rows)
#
#     if not batch_results:
#         print(f"[batch {batch_id}] >>> 未识别到任何公司实体。")
#     else:
#         print(f"\n===== BATCH {batch_id} Analysis Results (Current Batch with Weight) =====")
#         # 按正面加权计数做个简单排序展示
#         sorted_companies = sorted(
#             batch_results.items(),
#             key=lambda x: x[1]["Sentiment counts"]["positive"],
#             reverse=True
#         )
#         for company, data in sorted_companies:
#             print(f"\n{company}:")
#             print("  Comment numbers:", data["Comment numbers"])
#             print("  Weighted Sentiment counts:", data["Sentiment counts"])
#
#     # 更新全局状态，并输出“跨批次”综合报告
#     update_global_state(batch_results)
#     generate_final_report()


def process_batch(batch_df, batch_id):
    if batch_df.rdd.isEmpty():
        print(f"[batch {batch_id}] No data")
        return

    # 拉取当前批所需字段
    rows = batch_df.select("title", "selftext", "score", "comms_num", "created_utc").collect()

    # 拼接文本列表
    texts = [( (r["title"] or "") + " " + (r["selftext"] or "") ).strip() for r in rows]

    # 批量 NER
    docs = list(nlp.pipe(texts, batch_size=32, disable=["parser","tagger"]))

    # 批量情感分类（返回标签："negative"/"neutral"/"positive"）
    labels = sentiment_pipeline(texts, truncation=True, max_length=512)
    sentiments = [res["label"].lower() for res in labels]

    # 构造公司 → 加权统计
    batch_results = {}
    for idx, r in enumerate(rows):

        weight = compute_weight(r["score"], r["comms_num"], r["created_utc"])
        label = sentiments[idx]

        for ent in docs[idx].ents:
            if ent.label_ == "ORG":
                comp = ent.text.strip()
                stats = batch_results.setdefault(comp, {"Comment numbers": 0, "Sentiment counts": {"negative":0.0, "neutral":0.0, "positive":0.0}})
                stats["Comment numbers"] += 1
                stats["Sentiment counts"][label] += weight

    # 打印本批次结果
    if batch_results:
        print(f"\n===== BATCH {batch_id} Weighted Results =====")
        for comp, info in sorted(batch_results.items(), key=lambda x: x[1]["Sentiment counts"]["positive"], reverse=True):
            # print(f"\n{comp}: \nCount={info['Comment numbers']}, \nWeighted Sentiments={info['Sentiment counts']}")
            print(f"\n{comp}:")
            print(" Comment numbers:", info["Comment numbers"])
            print(" Weighted Sentiment counts:", info["Sentiment counts"])

    # 更新全局状态并输出累计报告
    update_global_state(batch_results)
    generate_final_report()


# 启动流式查询
query = (
    parsed_df.writeStream
    .outputMode("append")
    .foreachBatch(process_batch)
    .start()
)

query.awaitTermination()
