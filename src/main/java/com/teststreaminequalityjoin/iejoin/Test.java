package com.teststreaminequalityjoin.iejoin;

import com.correctness.iejoin.BoltTestKaka;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.kafka.spout.*;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.io.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class Test {
    public static void main(String[] args) throws Exception {

    new Test().fileTest();

    }


    public void fileTest() throws Exception{
        BufferedReader bufferedReader= new BufferedReader(new FileReader(new File("")));
        String line=null;
        long id= 0l;
        while((line = bufferedReader.readLine()) != null){

            id++;
        }
        System.out.print("===="+id);

    }



    public void testKafka() throws InvalidTopologyException, AuthorizationException, AlreadyAliveException {
        Config config = new Config();
        TopologyBuilder builder = new TopologyBuilder();
        AtomicLong id= new AtomicLong();
        String groupId = "kafka-reader-group";

        KafkaSpoutConfig<String, String> kafkaSpoutConfigForStreamR = KafkaSpoutConfig.builder("192.168.122.159:9092,192.168.122.231:9092", "selfjoin")
                .setProp(ConsumerConfig.GROUP_ID_CONFIG,groupId)
                .setProp(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class)
                .setProp(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class)
                .setProcessingGuarantee(KafkaSpoutConfig.ProcessingGuarantee.AT_LEAST_ONCE)
                .setRecordTranslator(record -> {
                    String[] splitValues = record.value().split(","); // Split record.value() based on a delimiter, adjust it as needed
                    String value1 = splitValues[5]; // Extract the first value
                    String value2 = splitValues[11]; // Extract the second value
                    id.getAndIncrement();
                    //String value3 = splitValues[2];
                    return new Values(value1, value2, id, System.currentTimeMillis());
                }, new Fields("distance", "trip", "id", "Time"), "StreamR")
                .setFirstPollOffsetStrategy(FirstPollOffsetStrategy.UNCOMMITTED_LATEST)
                .build();

        KafkaSpoutConfig<String, String> kafkaSpoutConfigForStreamS = KafkaSpoutConfig.builder("192.168.122.159:9092,192.168.122.231:9092", "selfjoin")

                .setProcessingGuarantee(KafkaSpoutConfig.ProcessingGuarantee.AT_LEAST_ONCE)
                .setRecordTranslator(record -> {
                    String[] splitValues = record.value().split(","); // Split record.value() based on a delimiter, adjust it as needed
                    String value1 = splitValues[0]; // Extract the first value
                    String value2 = splitValues[1]; // Extract the second value
                    String value3 = splitValues[2];
                    return new Values(value1, value2, value3, System.currentTimeMillis());
                }, new Fields("Value1", "Value2", "Value3", "Time"), "StreamS")
                .setFirstPollOffsetStrategy(FirstPollOffsetStrategy.UNCOMMITTED_LATEST)
                .build();
        builder.setSpout("kafkaSpoutTopicR", new KafkaSpout<>(kafkaSpoutConfigForStreamR), 1);
        builder.setSpout("kafkaSpoutTopicS", new KafkaSpout<>(kafkaSpoutConfigForStreamS), 1);
        builder.setBolt("TestKafka", new BoltTestKaka()).shuffleGrouping("kafkaSpoutTopicR","StreamR").shuffleGrouping("kafkaSpoutTopicS","StreamS");

        config.setNumWorkers(4);
        StormSubmitter.submitTopology("Test", config, builder.createTopology());


    }

}
