# cs651-project

Author: Mingchen Cai

Static datasource (in directory archive): https://www.kaggle.com/datasets/gpreda/reddit-wallstreetsbets-posts/data
Dynamic datasource: Reddit API

Check your if you have the following libraries installed in your local environment, 
if not, install them (run pip install):

     argparse
    
     time
    
     json
    
     praw
    
     kafka
    
     os
    
     openai
    
     spacy
    
     torch
    
     findspark
    
     pyspark.sql
    
     csv
    
     transformers(BertTokenizer, BertForSequenceClassification)

In addition, before running the code, especially spark_streaming_reddit.py, 
make sure to have the following libraries installed in your spark/jars directory:

    spark-sql-kafka-0-10_2.12-3.2.0.jar

    kafka-clients-2.6.0.jar

    commons-pool2-2.9.0.jar

    spark-token-provider-kafka-0-10_2.12-3.2.0.jar

    spark-streaming-kafka-0-10_2.12-3.1.1.jar

They are all available in https://mvnrepository.com/.

Kafka configuration:
The kafka location is "localhost:9092" if you configure it in a different way, please change the code in the corresponding place.
1. Start Zookeeper: `bin/zookeeper-server-start.sh config/zookeeper.properties`
2. Start Kafka: `bin/kafka-server-start.sh config/server.properties`
3. Create a topic named "reddit" with 2 partition and replication factor of 1: 
`bin/kafka-topics.sh --create --topic reddit --bootstrap-server localhost:9092 --replication-factor 1 --partitions 2`
4. Create a consumer group named "investing" with 1 partition:
`bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --create --group investing --topic reddit --partitions 1`
5. Run the producers:dynamic data: `python reddit_scrapper.py` static data: `python static_reddit_scrapper.py` to get the data from Reddit and send it to Kafka.
6. Run the consumers: `python spark_streaming_reddit.py --partition 0` and `python spark_streaming_reddit.py --partition 1`
at the same time. or just run `python spark_streaming_reddit.py` to just run 1 partition at the same time.
7. Run the final consumer `python report_generator.py` to get the final result in the output directory.

You also should configure your openai api key in your local environment and name it as **OPENAI_API_KEY**.


Sample of output is shown in the output directory.

**Disclaimer: This report does not constitute actual financial advice**

