# export endpoint
export ENDPOINT="IAM_BOOTSTRAP_BROKERS_GO_HERE"

# create a topic 
bin/kafka-topics.sh \
--create --bootstrap-server $ENDPOINT \
--command-config bin/client.properties \
--replication-factor 3 --partitions 1 \
--topic stock-topic

# list topic 
bin/kafka-topics.sh \
--list --bootstrap-server $ENDPOINT \
--command-config bin/client.properties

# describe topic
bin/kafka-topics.sh \
--describe --bootstrap-server  $ENDPOINT \
--command-config bin/client.properties \
--topic stock-topic

# pub a topic
bin/kafka-console-producer.sh --broker-list \
$ENDPOINT \
--producer.config bin/client.properties \
--topic stock-topic


# sub a topic
bin/kafka-console-consumer.sh \
--bootstrap-server $ENDPOINT \
--consumer.config bin/client.properties \
--topic stock-topic \
--from-beginning
