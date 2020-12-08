package com.enutek.dante.kafka.tutorial1;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class ProducerDemoKeys {


    public static void main(String[] args) throws ExecutionException, InterruptedException {

        final Logger logger = LoggerFactory.getLogger(ProducerDemoKeys.class);



        //producer properties


        String bootstrapServers = "127.0.0.1:9092";

        //producer properties
        Properties  properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName()); // what type of value you're send to kafka in bytes // string serializer needed // key will be a string serialzier
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // kafka producer

        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

        for (int i = 0; i < 10; i++) {


            String topic = "first_topic";
            String value = "hello worldl" + Integer.toString(i);
            String key = "id_ " + Integer.toString(i);


            //saad@arthurlawrence.net


            //producer record

            ProducerRecord<String, String> record = new ProducerRecord<String, String>(topic, key, value); //

            logger.info("Key: " + key); // log the key
            /*
            a partition is like a load balancer and keys are distributed randomly

            at first run and this is just a test of a concept
            same key alwayes go to the smae partition

            partitions 1 includes: 0,1,5,6,9
            partition 0 includes: 2,3,4,7,8


             */

            // send data - asynchronous
            producer.send(record, new Callback() {

                public void onCompletion(RecordMetadata recordMetadata, Exception e) {

                    //execute when a record is successfully sent or an exception is thrwn

                    if (e == null) {

                        // the reword was successfully sent

                        logger.info("received new metadta. \n " +
                                "topic:  " + recordMetadata.topic() + "\n" +
                                "Partition: " + recordMetadata.partition() + "\n" +
                                "Offset: " + recordMetadata.offset() + "\n" +
                                "Timestamp: " + recordMetadata.timestamp());

                    } else {
                        logger.error("error while produceing", e);

                    }
                }
            }).get(); // block the send to make it synchronous, dont do in production
        }
        producer.flush();
        producer.close();



    }
}
