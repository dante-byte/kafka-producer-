package com.enutek.dante.kafka.tutorial1;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoKeys {


    public static void main(String[] args) {

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

            //saad@arthurlawrence.net


            //producer record

            ProducerRecord<String, String> record =
                    new ProducerRecord<String, String>("first_topic", "hello-world-test" + Integer.toString(i));
            // send data
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
            });
        }
        producer.flush();
        producer.close();



    }
}
