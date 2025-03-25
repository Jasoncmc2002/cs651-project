
from kafka import KafkaProducer, KafkaConsumer

class KafkaConfig:

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

    def __init__(self, config: KafkaConfig):
        self.producer = KafkaProducer(bootstrap_servers=config.bootstrap_servers)
        self.topic = config.topic

    def send_message(self, message: str):

        self.producer.send(self.topic, value=message.encode('utf-8'))
        # 同步等待消息发送完成（可选）
        self.producer.flush()
        print(f"发送消息: '{message}' 到 topic: {self.topic}")


class MyConsumer:

    def __init__(self, config: KafkaConfig):
        self.consumer = KafkaConsumer(
            config.topic,
            bootstrap_servers=config.bootstrap_servers,
            auto_offset_reset='earliest',  # 从最早的可用消息开始消费
            enable_auto_commit=True,        # 自动提交偏移量
            group_id='my-group-id'          # 消费者组 ID
        )

    def consume_messages(self):

        print(f"开始消费 topic: {self.consumer.subscription()}")
        for msg in self.consumer:
            print(f"收到消息: {msg.value.decode('utf-8')}")


def main():

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
