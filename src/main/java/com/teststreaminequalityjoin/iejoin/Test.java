package com.teststreaminequalityjoin.iejoin;

import com.correctness.iejoin.BoltTestKaka;
import com.fasterxml.jackson.annotation.JacksonAnnotation;
import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.kafka.spout.*;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

public class Test {
    public static void main(String[] args) throws Exception {
        new Test().testKafka();

    }


    public void testKafka() throws InvalidTopologyException, AuthorizationException, AlreadyAliveException {
        Config config = new Config();
        TopologyBuilder builder = new TopologyBuilder();
        KafkaSpoutConfig<String, String> kafkaSpoutConfigForStreamR = KafkaSpoutConfig.builder("192.168.122.159:9092,192.168.122.231:9092", "test")

                .setProcessingGuarantee(KafkaSpoutConfig.ProcessingGuarantee.AT_LEAST_ONCE)
                .setRecordTranslator(record -> {
                    String[] splitValues = record.value().split(","); // Split record.value() based on a delimiter, adjust it as needed
                    String value1 = splitValues[0]; // Extract the first value
                    String value2 = splitValues[1]; // Extract the second value
                    String value3 = splitValues[2];
                    return new Values(value1, value2, value3, System.currentTimeMillis());
                }, new Fields("Value1", "Value2", "Value3", "Time"), "StreamR")
                .setFirstPollOffsetStrategy(FirstPollOffsetStrategy.UNCOMMITTED_LATEST)
                .build();

        KafkaSpoutConfig<String, String> kafkaSpoutConfigForStreamS = KafkaSpoutConfig.builder("192.168.122.159:9092,192.168.122.231:9092", "test1")

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
//    public static KafkaSpoutRetryService getRetryService() {
////        return new KafkaSpoutRetryExponentialBackoff(KafkaSpoutRetryExponentialBackoff.TimeInterval.microSeconds(0),
////                KafkaSpoutRetryExponentialBackoff.TimeInterval.milliSeconds(2), Integer.MAX_VALUE,
////                KafkaSpoutRetryExponentialBackoff.TimeInterval.seconds(10));
////    }
}
