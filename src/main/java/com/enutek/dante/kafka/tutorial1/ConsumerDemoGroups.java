package com.enutek.dante.kafka.tutorial1;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ConsumerDemoGroups {

    public static void main(String[] args) {

        Logger logger = LoggerFactory.getLogger(ConsumerDemoGroups.class.getName());

        Properties properties = new Properties();

        String bootstrapServer = "127.0.0.1:9092";

        String groupId = "my-fifth-application";

        String topic = "first_topic";

        /**
         * consumer configs
         */

        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        /**
         * consumer
         */

        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);

        /**
         * subscribe consumer to our topics
         */

        consumer.subscribe(Arrays.asList(topic));

        /**
         * poll for new data
         */

        while (true) {

            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100)); // new in kafka 2.0.0 uses a duratioon use to accept a long


            for (ConsumerRecord<String, String> records1 : records) { // loop through each record and print info

                logger.info("Key: " + records1.key() + ", Value: " + records1.value());

                logger.info("Partition: " +  records1.partition() + ", Offset:" + records1.offset());


            }


        }








    }
}
