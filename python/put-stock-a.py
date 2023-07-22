# haimtran 20/07/2023
# use confuent kafka client 

import datetime
import random
import time 
import json 
from confluent_kafka import Producer 

# bootstrapserver
BOOTSTRAP_SERVERS ="SSL_BOOTSTRAP_BROKERS_GO_HERE"

# topic name 
TOPIC = "stock-topic"

# callback delivery function 
def delivery_report(error, message):
    """
    """
    if error is not None:
        print("GOOD")
    else:
        print(message)


# producer 
producer = Producer({
    'bootstrap.servers': BOOTSTRAP_SERVERS, 
    'security.protocol': 'SSL'
})

# send event to topic 
while True:
  # create event 
  event = {
        'event_time': datetime.datetime.now().isoformat(),
        'ticker': random.choice(['AAPL', 'AMZN', 'MSFT', 'INTC', 'TBV']),
        'price': round(random.random() * 100, 2)
    }
  # convert dict to byte 
  data = json.dumps(event, indent=2).encode("utf-8")
  # send event to topic 
  producer.produce(TOPIC, data, callback=delivery_report)
  time.sleep(1)