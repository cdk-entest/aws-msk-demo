# haimtran 20/07/2023
# use confuent kafka client
import socket
import datetime
import random
import time
import json
from confluent_kafka import Producer
from aws_msk_iam_sasl_signer import MSKAuthTokenProvider

# bootstrapserver
# BOOTSTRAP_SERVERS ="SSL_BOOTSTRAP_BROKERS_GO_HERE"
BOOTSTRAP_SERVERS = ".c1.kafka-serverless.ap-southeast-1.amazonaws.com:9098"

# topic name
REGION = "ap-southeast-1"
TOPIC = "stock-topic"


def oauth_cb(oauth_config=None):
    auth_token, expiry_ms = MSKAuthTokenProvider.generate_auth_token(REGION)
    # Note that this library expects oauth_cb to return expiry time in seconds since epoch, while the token generator returns expiry in ms
    print(auth_token)
    return auth_token, expiry_ms / 1000


# callback delivery function
def delivery_report(error, message):
    """ """
    if error is not None:
        print("GOOD")
    else:
        print(message)


# producer
producer = Producer(
    {
        "bootstrap.servers": BOOTSTRAP_SERVERS,
        "client.id": socket.gethostname(),
        "security.protocol": "SASL_SSL",
        "sasl.mechanisms": "OAUTHBEARER",
        "oauth_cb": oauth_cb,
    }
)

# send event to topic
while True:
    # create event
    event = {
        # 'event_time': datetime.datetime.now().isoformat(),
        "event_time": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f"),
        "ticker": random.choice(["AAPL", "AMZN", "MSFT", "INTC", "TBV"]),
        "price": round(random.random() * 100, 2),
    }
    # convert dict to byte
    data = json.dumps(event, indent=2).encode("utf-8")
    # send event to topic
    producer.produce(TOPIC, data, callback=delivery_report)
    time.sleep(1)
    producer.flush()
