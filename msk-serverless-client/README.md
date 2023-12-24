---
author: haimtran
title: getting started with msk serverless
description: getting started with msk serverless
publishedDate: 20/07/2022
date: 2022-20-07
---

## Introduction

- Stack to creat MSK serverless
- Pub/sub using terminal
- Pub/sub using python client
- Integrate with Flink application

> [!IMPORTANT]  
> Ensure that the client and cluster are in the same region

> [!IMPORTANT]  
> Using IAM authentication and client lib for MSK serverless

## Serverless Cluster

First create a net work stack

```ts
import { aws_ec2, Stack, StackProps } from "aws-cdk-lib";
import { Construct } from "constructs";

interface VpcProps extends StackProps {
  cidr: string;
  name: string;
}

export class NetworkStack extends Stack {
  public readonly vpc: aws_ec2.Vpc;
  public readonly MskSecurityGroup: aws_ec2.SecurityGroup;

  constructor(scope: Construct, id: string, props: VpcProps) {
    super(scope, id, props);

    const vpc = new aws_ec2.Vpc(this, `${props.name}-Vpc`, {
      vpcName: props.name,
      maxAzs: 3,
      enableDnsHostnames: true,
      enableDnsSupport: true,
      ipAddresses: aws_ec2.IpAddresses.cidr(props.cidr),
      // aws nat gateway service not instance
      natGatewayProvider: aws_ec2.NatProvider.gateway(),
      // can be less than num az default 1 natgw/zone
      natGateways: 1,
      // which public subet have the natgw
      // natGatewaySubnets: {
      //   subnetType: aws_ec2.SubnetType.PRIVATE_WITH_EGRESS,
      // },
      subnetConfiguration: [
        {
          // cdk add igw and route tables
          name: "PublicSubnet",
          cidrMask: 24,
          subnetType: aws_ec2.SubnetType.PUBLIC,
        },
        {
          // cdk add nat and route tables
          name: "PrivateSubnetNat",
          cidrMask: 24,
          subnetType: aws_ec2.SubnetType.PRIVATE_WITH_EGRESS,
        },
      ],
    });

    const MskSecurityGroup = new aws_ec2.SecurityGroup(
      this,
      "MskSecurityGroup",
      {
        securityGroupName: "MskSecurityGroup",
        vpc: vpc,
      }
    );

    MskSecurityGroup.addIngressRule(
      MskSecurityGroup,
      aws_ec2.Port.allTraffic(),
      "self reference security group"
    );

    vpc.addInterfaceEndpoint("STSVpcEndpoint", {
      service: aws_ec2.InterfaceVpcEndpointAwsService.STS,
      open: true,
      subnets: {
        subnetType: aws_ec2.SubnetType.PRIVATE_WITH_EGRESS,
      },
      securityGroups: [MskSecurityGroup],
    });

    this.vpc = vpc;
    this.MskSecurityGroup = MskSecurityGroup;
  }
}
```

Then create a cluster

```ts
import { Stack, StackProps, aws_ec2, aws_msk } from "aws-cdk-lib";
import { Construct } from "constructs";

interface MskServerlessProps extends StackProps {
  clusterName: string;
  securityGroup: aws_ec2.SecurityGroup;
  vpc: aws_ec2.Vpc;
}

export class MSKServerlessStack extends Stack {
  constructor(scope: Construct, id: string, props: MskServerlessProps) {
    super(scope, id, props);

    const subnetIds = props.vpc.publicSubnets.map((net) => net.subnetId);
    const securityGroupId = props.securityGroup.securityGroupId;

    new aws_msk.CfnServerlessCluster(this, "MSKServerlessDemo", {
      clusterName: "ServerlessDemo",
      clientAuthentication: {
        sasl: {
          iam: {
            enabled: true,
          },
        },
      },
      vpcConfigs: [
        {
          subnetIds: subnetIds,
          securityGroups: [securityGroupId],
        },
      ],
    });
  }
}
```

## Setup Client

Setup permissions for client with the following policy, this policy apply for both

- EC2 for demo
- Zeppline notebook

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "kafka-cluster:Connect",
        "kafka-cluster:AlterCluster",
        "kafka-cluster:DescribeCluster"
      ],
      "Resource": ["arn:aws:kafka:ap-southeast-1:1111222233334444:cluster/*"]
    },
    {
      "Effect": "Allow",
      "Action": [
        "kafka-cluster:*Topic*",
        "kafka-cluster:WriteData",
        "kafka-cluster:ReadData"
      ],
      "Resource": ["arn:aws:kafka:ap-southeast-1:1111222233334444:topic/*"]
    },
    {
      "Effect": "Allow",
      "Action": ["kafka-cluster:AlterGroup", "kafka-cluster:DescribeGroup"],
      "Resource": ["arn:aws:kafka:ap-southeast-1:1111222233334444:group/*"]
    }
  ]
}
```

## Pub Sub Client

```bash
# export endpoint
export ENDPOINT=""
export TOPIC=sensor-topic

# create a topic
bin/kafka-topics.sh \
--create --bootstrap-server $ENDPOINT \
--command-config bin/client.properties \
--replication-factor 3 --partitions 1 \
--topic $TOPIC

# list topic
bin/kafka-topics.sh \
--list --bootstrap-server $ENDPOINT \
--command-config bin/client.properties

# describe topic
bin/kafka-topics.sh \
--describe --bootstrap-server  $ENDPOINT \
--command-config bin/client.properties \
--topic $TOPIC

# pub a topic
bin/kafka-console-producer.sh --broker-list \
$ENDPOINT \
--producer.config bin/client.properties \
--topic $TOPIC


# sub a topic
bin/kafka-console-consumer.sh \
--bootstrap-server $ENDPOINT \
--consumer.config bin/client.properties \
--topic  $TOPIC \
--from-beginning
```

## Python Client

Install dependencies

```txt
confluent-kafka==2.2.0
kafka-python==2.0.2
aws-msk-iam-sasl-signer-python
```

First option, let create a Confluent consumer and producer

```py
# haimtran 20/07/2023
# use confuent kafka client

import socket
import datetime
import random
import time
import json
from confluent_kafka import Producer, Consumer
from aws_msk_iam_sasl_signer import MSKAuthTokenProvider

# bootstrapserver with iam auth
BOOTSTRAP_SERVERS = ""

# topic name
REGION = "us-east-1"
TOPIC = "sensor-topic"


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


def get_data():
    # consumer
    consumer = Consumer(
        {
            # "debug": "all",
            "bootstrap.servers": BOOTSTRAP_SERVERS,
            "client.id": socket.gethostname(),
            "security.protocol": "SASL_SSL",
            "sasl.mechanisms": "OAUTHBEARER",
            "oauth_cb": oauth_cb,
            "group.id": "mygroup",
            "auto.offset.reset": "earliest"
        }
    )
    consumer.subscribe(['sensor-topic'])
    while True:
        message = consumer.poll(5)
        if message is None:
            continue
        if message.error():
            print("consumer error")
        #
        print(message.value())


def send_data():
    # producer
    producer = Producer(
        {
            "bootstrap.servers": BOOTSTRAP_SERVERS,
            "client.id": socket.gethostname(),
            "security.protocol": "SASL_SSL",
            "sasl.mechanisms": "OAUTHBEARER",
            "oauth_cb": oauth_cb
        }
    )
    print(producer)

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
        try:
            producer.produce(TOPIC, data, callback=delivery_report)
            producer.flush()
            time.sleep(1)
        except:
            print('not able to send message')


if __name__ == "__main__":
    # oauth_cb()
    send_data()
    # get_data()
```

Second option, let create a Kafka producer

```py
from kafka import KafkaProducer
from kafka.errors import KafkaError
import socket
import time
from aws_msk_iam_sasl_signer import MSKAuthTokenProvider


BOOTSTRAP_SERVERS = ""
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
```
