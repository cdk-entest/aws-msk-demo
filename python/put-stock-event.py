# haimtran on 22/07/2023
# generate sensor data to kafka topic 

from kafka import KafkaProducer
import json 
import datetime
import random

# bootstrapserver
BOOTSTRAP_SERVERS ="SSL_BOOTSTRAP_BROKERS_GO_HERE"


# topic name 
TOPIC = "stock-topic"

# create producer
producer = KafkaProducer(
    bootstrap_servers=BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    key_serializer=str.encode,
    retry_backoff_ms=500,
    request_timeout_ms=20000,
    security_protocol='SSL')

# send message to a kafka topic 
while True:
    # create stock event
    event = {
        'event_time': datetime.datetime.now().isoformat(),
        'ticker': random.choice(['AAPL', 'AMZN', 'MSFT', 'INTC', 'TBV']),
        'price': round(random.random() * 100, 2)
    }
    # send event
    future = producer.send(
        topic=TOPIC, 
        value=event, 
        key=event['ticker']
    )
    # flush event
    producer.flush()
    # record metadata 
    record_metadata = future.get(timeout=10)
    # print 
    print('sent event to kafka topic {0} parition {1} offset {2} \n'.format(
        record_metadata.topic, 
        record_metadata.partition, 
        record_metadata.offset
    ))