package com.enutek.dante.kafka.tutorial1;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Properties;

public class ConsumerDemoAssignSeek {

    public static void main(String[] args) {

        Logger logger = LoggerFactory.getLogger(ConsumerDemoAssignSeek.class.getName());

        Properties properties = new Properties();

        String bootstrapServer = "127.0.0.1:9092";

        String groupId = "my-seven-application";

        String topic = "first_topic";

        /**
         * consumer configs
         */

        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        /**
         * consumer
         */

        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);

        /**
         * subscribe consumer to our topics
         *
         */

        // none

        /**
         * assign & seek are mostl used to replay data or fetch a specific message
         *
         */

        //assign
        TopicPartition partitionToReadFrom = new TopicPartition(topic,0);
        long offsetToReadFrom = 15l;

        consumer.assign(Arrays.asList(partitionToReadFrom));

        //seek
        consumer.seek(partitionToReadFrom, offsetToReadFrom);

        int numberOfMessagesToRead = 5;
        boolean keeoOnReading = true;
        int numberOfMessagesReadSoFar = 0;

        /**
         * poll for new data
         */

        while (keeoOnReading) {

            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100)); // new in kafka 2.0.0 uses a duratioon use to accept a long


            for (ConsumerRecord<String, String> records1 : records) { // loop through each record and print info

                numberOfMessagesReadSoFar += 1;

                logger.info("Key: " + records1.key() + ", Value: " + records1.value());

                logger.info("Partition: " +  records1.partition() + ", Offset:" + records1.offset());

                if (numberOfMessagesReadSoFar >= numberOfMessagesToRead) {


                    keeoOnReading = false;
                    break;
                }

            }


        }

        logger.info("exiting the application");








    }
}
