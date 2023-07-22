package com.amazonaws.examples;

import java.util.*;
import java.util.Properties;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.clients.producer.Callback;

/**
 * Hello world!
 *
 */
public class DemoProducer {
  public static void main(String[] args) {
    if (args.length < 2) {
      System.out.println("USAGE: com.amazonaws.examples.App bootstrap-brokers topic-name");
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

    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");

    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

    props.put("security.protocol", "SASL_SSL");
    props.put("sasl.mechanism", "AWS_MSK_IAM");
    props.put("sasl.jaas.config", "software.amazon.msk.auth.iam.IAMLoginModule required;");
    props.put("sasl.client.callback.handler.class",
        "software.amazon.msk.auth.iam.IAMClientCallbackHandler");
    props.put("min.insync.replicas", 1);

    Producer<String, String> producer = new KafkaProducer<String, String>(props);

    Scanner scanner = new Scanner(System.in);
    while (true) {
      System.out.print(">");
      String input = scanner.nextLine();
      ProducerRecord<String, String> record =
          new ProducerRecord<String, String>(topicName, input, input);
      producer.send(record, new TestCallback());
      producer.flush();
    }
  }


  private static class TestCallback implements Callback {
    public void onCompletion(RecordMetadata recordMetadata, Exception e) {
      if (e != null) {
        System.out.println("Error while producing message to topic :" + recordMetadata);
        e.printStackTrace();
      } else {
        String message = String.format("sent message to topic:%s partition:%s  offset:%s",
            recordMetadata.topic(), recordMetadata.partition(), recordMetadata.offset());
        System.out.println(message);
      }
    }
  }

}
