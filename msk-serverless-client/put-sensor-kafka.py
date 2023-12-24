from kafka import KafkaProducer
from kafka.errors import KafkaError
import socket
import time
from aws_msk_iam_sasl_signer import MSKAuthTokenProvider


BOOTSTRAP_SERVERS = ".c1.kafka-serverless.us-east-1.amazonaws.com:9098"
REGION = "us-east-1"
TOPIC = "sensor-topic"

class MSKTokenProvider():
    def token(self):
        token, _ = MSKAuthTokenProvider.generate_auth_token(REGION)
        return token

tp = MSKTokenProvider()

producer = KafkaProducer(
    bootstrap_servers=BOOTSTRAP_SERVERS,
    security_protocol='SASL_SSL',
    sasl_mechanism='OAUTHBEARER',
    sasl_oauth_token_provider=tp,
    client_id=socket.gethostname(),
)

while True:
    try:
        inp=input("Hello me")
        producer.send(TOPIC, inp.encode())
        producer.flush()
        print("Produced!")
        time.sleep(1)
    except Exception:
        print("Failed to send message:", e)

producer.close()