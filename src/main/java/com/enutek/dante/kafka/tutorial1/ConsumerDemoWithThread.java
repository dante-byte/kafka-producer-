package com.enutek.dante.kafka.tutorial1;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class ConsumerDemoWithThread {

    public static void main(String[] args) {

        new ConsumerDemoWithThread().run();








    }//

    private ConsumerDemoWithThread(){


    }

    private void run(){

        Logger logger = LoggerFactory.getLogger(ConsumerDemoWithThread.class.getName());

        String bootstrapServer = "127.0.0.1:9092";

        String groupId = "my-sixth-application";

        String topic = "first_topic";


        //latch for dealing with mutiple threads
        CountDownLatch latch = new CountDownLatch(1);

        // create consumer runnable
        logger.info("Creating the consumer thread ");

        Runnable myConsumerRunnable = new ConsumerRunnable(
                bootstrapServer,
                groupId,
                topic,
                latch
        );


        //start thread
        Thread myThread = new Thread(myConsumerRunnable);
        myThread.start();

        //shut down hook
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {

            logger.info("caught shutdown hook");
            ((ConsumerRunnable)myConsumerRunnable).shutdown();
            try {
                latch.await();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            logger.info("application has exicited");
        }

        ));

        try {
            latch.await();
        } catch (InterruptedException e) {
            logger.error("application got interrupted",e);

        } finally {
            logger.info("application is closing ");
        }


    }





    public class ConsumerRunnable implements Runnable { // create a consumer thread

        private CountDownLatch latch;
        private KafkaConsumer<String,String> consumer;
        private Logger logger = LoggerFactory.getLogger(ConsumerRunnable.class.getName());








        public ConsumerRunnable(String bootstrapServer, String groupId, String topic, CountDownLatch latch) {

            this.latch = latch; // use to shutdown application & pass on latch

            // properties start config  --

            Properties properties = new Properties();

            properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
            properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
            properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

            // nproperteis end --

            // 1. create consumer
            consumer = new KafkaConsumer<String, String>(properties);

            // 2. subscribe to consumer to our topics

            consumer.subscribe(Arrays.asList(topic));
        }


        @Override
        public void run() {

            try {


                while (true) {

                    ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100)); // new in kafka 2.0.0 uses a duratioon use to accept a long


                    for (ConsumerRecord<String, String> records1 : records) { // loop through each record and print info

                        logger.info("Key: " + records1.key() + ", Value: " + records1.value());

                        logger.info("Partition: " + records1.partition() + ", Offset:" + records1.offset());


                    }


                }
            } catch (WakeupException e) {

                logger.info("received shut down siginal");


            } finally {

                consumer.close();
                // tellls main code we are done with consumer
                latch.countDown();
            }



        }

        public void shutdown() {


            consumer.wakeup(); // method to interrup  consumer.poll() it will throw the exception wakeup exception


        }

    }


}