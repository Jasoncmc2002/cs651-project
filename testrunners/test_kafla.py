# kafka_example.py
from kafka import KafkaProducer, KafkaConsumer


class KafkaConfig:
    """
    管理 Kafka 的配置信息，如服务器地址、Topic 等
    """
    def __init__(self, bootstrap_servers='localhost:9092', topic='test_topic'):
        self._bootstrap_servers = bootstrap_servers
        self._topic = topic

    @property
    def bootstrap_servers(self):
        return self._bootstrap_servers

    @property
    def topic(self):
        return self._topic


class MyProducer:
    """
    用于发送消息到 Kafka 的生产者
    """
    def __init__(self, config: KafkaConfig):
        self.producer = KafkaProducer(bootstrap_servers=config.bootstrap_servers)
        self.topic = config.topic

    def send_message(self, message: str):
        """
        发送消息到指定的 Kafka Topic
        """
        self.producer.send(self.topic, value=message.encode('utf-8'))
        # 同步等待消息发送完成（可选）
        self.producer.flush()
        print(f"发送消息: '{message}' 到 topic: {self.topic}")


class MyConsumer:
    """
    用于从 Kafka 消费消息并打印的消费者
    """
    def __init__(self, config: KafkaConfig):
        self.consumer = KafkaConsumer(
            config.topic,
            bootstrap_servers=config.bootstrap_servers,
            auto_offset_reset='earliest',  # 从最早的可用消息开始消费
            enable_auto_commit=True,        # 自动提交偏移量
            group_id='my-group-id'          # 消费者组 ID
        )

    def consume_messages(self):
        """
        不断轮询 Kafka 中的消息，并打印出来
        """
        print(f"开始消费 topic: {self.consumer.subscription()}")
        for msg in self.consumer:
            print(f"收到消息: {msg.value.decode('utf-8')}")


def main():
    """
    演示如何使用上述三个类进行 Kafka 消息的生产与消费
    """
    # 初始化 Kafka 配置
    config = KafkaConfig(bootstrap_servers='localhost:9092', topic='test_topic')

    # 创建生产者并发送一条消息
    producer = MyProducer(config)
    producer.send_message("Hello Kafka")

    # 创建消费者并开始消费消息
    consumer = MyConsumer(config)
    consumer.consume_messages()


if __name__ == '__main__':
    main()
