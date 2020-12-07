package com.enutek.dante.kafka.tutorial1;

import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class ProducerDemo {


    public static void main(String[] args) {

        // create producer properties


        String bootstrapServers = "127.0.0.1:9092";

        Properties  properties = new Properties();
        properties.setProperty("bootstrap.servers", bootstrapServers);

        properties.setProperty("key.serializer", StringSerializer.class.getName()); // what type of value you're send to kafka in bytes // string serializer needed // key will be a string serialzier
        properties.setProperty("value.serializer", StringSerializer.class.getName());


    }
}
