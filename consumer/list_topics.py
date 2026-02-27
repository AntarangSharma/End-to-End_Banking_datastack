from kafka import KafkaConsumer
consumer = KafkaConsumer(bootstrap_servers='localhost:29092')
print(consumer.topics())
