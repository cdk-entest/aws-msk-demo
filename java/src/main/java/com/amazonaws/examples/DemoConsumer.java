package com.amazonaws.examples;

import java.util.*;
import java.util.Properties;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.ConsumerRecord;

/**
 * Hello world!
 *
 */
public class DemoConsumer {
  public static void main(String[] args) {
    if (args.length < 2) {
      System.out.println("USAGE: com.amazonaws.examples.Consumer bootstrap-brokers topic-name");
    }
    String brokers = args[0];
    String topicName = args[1];

    Properties props = new Properties();
    props.put("bootstrap.servers", brokers);

    // Set acknowledgements for producer requests.
    props.put("acks", "all");

    // If the request fails, the producer can automatically retry,
    props.put("retries", 0);

    // Specify buffer size in config
    props.put("batch.size", 16384);

    // Reduce the no of requests less than 0
    props.put("linger.ms", 1);

    // The buffer.memory controls the total amount of memory available to the producer for
    // buffering.
    props.put("buffer.memory", 33554432);

    props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

    props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

    props.put("security.protocol", "SASL_SSL");
    props.put("sasl.mechanism", "AWS_MSK_IAM");
    props.put("sasl.jaas.config", "software.amazon.msk.auth.iam.IAMLoginModule required;");
    props.put("sasl.client.callback.handler.class",
        "software.amazon.msk.auth.iam.IAMClientCallbackHandler");

    props.put("group.id", "foobar");
    props.put("enable.auto.commit", "false");
    props.put("auto.offset.reset", "earliest");

    Consumer<String, String> consumer = new KafkaConsumer<String, String>(props);

    consumer.subscribe(Collections.singletonList(topicName));

    while (true) {
      ConsumerRecords<String, String> records = consumer.poll(500);
      for (ConsumerRecord record : records) {
        System.out.println(record.value());
      }
    }
  }
}
