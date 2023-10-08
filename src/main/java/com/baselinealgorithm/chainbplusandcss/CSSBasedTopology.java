package com.baselinealgorithm.chainbplusandcss;

import com.configurationsandconstants.iejoinandbaseworks.Configuration;
import com.configurationsandconstants.iejoinandbaseworks.Constants;
import com.stormiequality.inputdata.RackPowerSpout;
import com.stormiequality.inputdata.SplitBolt;
import com.stormiequality.inputdata.Spout;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.kafka.spout.*;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class CSSBasedTopology {
    public static void main(String[] args) throws Exception {



//        KafkaSpoutConfig<String, String> kafkaSpoutConfigForStreamR = KafkaSpoutConfig.builder(args[0], args[1])
//                .setRetry(getRetryService())
//                .setProcessingGuarantee(KafkaSpoutConfig.ProcessingGuarantee.AT_LEAST_ONCE)
//                .setRecordTranslator(record -> new Values( record.value(), System.currentTimeMillis()),
//                        new Fields( "Tuple", "Time"),"StreamR")
//                .setFirstPollOffsetStrategy(FirstPollOffsetStrategy.UNCOMMITTED_LATEST)
//                .build();
//
//        KafkaSpoutConfig<String, String> kafkaSpoutConfigForStreamS = KafkaSpoutConfig.builder(args[0], args[2])
//                .setRetry(getRetryService())
//                .setProcessingGuarantee(KafkaSpoutConfig.ProcessingGuarantee.AT_LEAST_ONCE)
//                .setRecordTranslator(record -> new Values( record.value(), System.currentTimeMillis()),
//                        new Fields( "Tuple", "Time"), "StreamS")
//                .setFirstPollOffsetStrategy(FirstPollOffsetStrategy.UNCOMMITTED_LATEST)
//                .build();



        Config config= new Config();
        Map<String, Object> map= Configuration.configurationConstantForStreamIDs();
        config.setNumWorkers(20);
        config.registerSerialization(java.util.BitSet.class);
        config.registerSerialization(java.util.HashSet.class);
        config.registerSerialization(java.util.concurrent.atomic.AtomicLong.class);
        List<String> workerChildopts = new ArrayList<>();
        workerChildopts.add("-Xmx2g");
        workerChildopts.add("-Xss8m");
        config.put(Config.TOPOLOGY_WORKER_CHILDOPTS, workerChildopts);
        config.setNumAckers(4);
        TopologyBuilder builder= new TopologyBuilder();
    // builder.setSpout("streamspout", new Spout(1000));
        builder.setSpout("streamspout", new RackPowerSpout());
       // builder.setSpout("streamspout", new RackPowerSpout());
       builder.setBolt("testBolt", new SplitBolt(),2).fieldsGrouping("streamspout", "StreamR", new Fields("ID")).fieldsGrouping("streamspout", "StreamS", new Fields("ID")).setNumTasks(5);
//        builder.setBolt(Constants.LEFT_PREDICATE_CSS_TREE_BOLT, new LeftPredicateCSSTreeBolt()).fieldsGrouping("testBolt", (String) map.get("LeftSmallerPredicateTuple"),new Fields(Constants.TUPLE_ID)).
//                fieldsGrouping("testBolt", (String) map.get("RightSmallerPredicateTuple"),new Fields(Constants.TUPLE_ID));
//        builder.setBolt(Constants.RIGHT_PREDICATE_CSS_TREE_BOLT, new RightPredicateCSSTreeBolt()).fieldsGrouping("testBolt", (String) map.get("LeftGreaterPredicateTuple"),new Fields(Constants.TUPLE_ID)).
//                fieldsGrouping("testBolt", (String) map.get("RightGreaterPredicateTuple"),new Fields(Constants.TUPLE_ID));
//
//        builder.setBolt(Constants.MUTABLE_PART_EVALUATION_BOLT,new JoinerCSSTreeMutablePart((String)map.get("LeftPredicateSourceStreamIDHashSet"), (String)map.get("RightPredicateSourceStreamIDHashSet")),4).fieldsGrouping(Constants.LEFT_PREDICATE_CSS_TREE_BOLT,(String)map.get("LeftPredicateSourceStreamIDHashSet"), new Fields(Constants.TUPLE_ID)).
//                fieldsGrouping(Constants.LEFT_PREDICATE_CSS_TREE_BOLT,(String)map.get("RightPredicateSourceStreamIDHashSet"), new Fields(Constants.TUPLE_ID)).
//                fieldsGrouping(Constants.RIGHT_PREDICATE_CSS_TREE_BOLT,(String)map.get("LeftPredicateSourceStreamIDHashSet"), new Fields(Constants.TUPLE_ID)).
//                fieldsGrouping(Constants.RIGHT_PREDICATE_CSS_TREE_BOLT,(String)map.get("RightPredicateSourceStreamIDHashSet"), new Fields(Constants.TUPLE_ID)).setNumTasks(10);
//
//        builder.setBolt("RecordMutable", new RecordmutableJoin()).shuffleGrouping(Constants.MUTABLE_PART_EVALUATION_BOLT,"LeftMutableResults").
//                shuffleGrouping(Constants.MUTABLE_PART_EVALUATION_BOLT,"RightMutableResults");

        builder.setBolt(Constants.LEFT_PREDICATE_IMMUTABLE_CSS,new LeftPredicateImmutableCSSBolt(),1)
                //shuffleGrouping(Constants.LEFT_PREDICATE_CSS_TREE_BOLT, (String) map.get("LeftSmallerPredicateTuple")).
               // shuffleGrouping(Constants.LEFT_PREDICATE_CSS_TREE_BOLT, (String) map.get("RightSmallerPredicateTuple"))
                //shuffleGrouping(Constants.LEFT_PREDICATE_CSS_TREE_BOLT,"LeftCheckForMerge").
              //  shuffleGrouping(Constants.LEFT_PREDICATE_CSS_TREE_BOLT,"RightCheckForMerge")
                .allGrouping("streamspout","StreamR" ).allGrouping("streamspout","StreamS").setNumTasks(1);

        builder.setBolt(Constants.RIGHT_PREDICATE_IMMUTABLE_CSS,new RightPredicateImmutableCSSBolt(),1).
                //.shuffleGrouping(Constants.RIGHT_PREDICATE_CSS_TREE_BOLT, (String) map.get("LeftGreaterPredicateTuple")).
               // shuffleGrouping(Constants.RIGHT_PREDICATE_CSS_TREE_BOLT, (String) map.get("RightGreaterPredicateTuple")).
               // shuffleGrouping(Constants.RIGHT_PREDICATE_CSS_TREE_BOLT,"LeftCheckForMerge").
                //shuffleGrouping(Constants.RIGHT_PREDICATE_CSS_TREE_BOLT,"RightCheckForMerge").
                allGrouping("streamspout","StreamR" ).allGrouping("streamspout","StreamS").setNumTasks(1);

        builder.setBolt(Constants.IMMUTABLE_HASH_SET_EVALUATION,new JoinerCSSTreeBolt("LeftPredicate", "RightPredicate") ).
                fieldsGrouping(Constants.LEFT_PREDICATE_IMMUTABLE_CSS,"LeftPredicate", new Fields(Constants.TUPLE_ID)).
                fieldsGrouping(Constants.LEFT_PREDICATE_IMMUTABLE_CSS,"RightPredicate", new Fields(Constants.TUPLE_ID)).
                fieldsGrouping(Constants.RIGHT_PREDICATE_IMMUTABLE_CSS,"LeftPredicate", new Fields(Constants.TUPLE_ID)).
                fieldsGrouping(Constants.RIGHT_PREDICATE_IMMUTABLE_CSS,"RightPredicate", new Fields(Constants.TUPLE_ID)).setNumTasks(1);

//        builder.setBolt(Constants.MERGE_BOLT_EVALUATION_CSS,new RecordCSSTreeMergeOperation("LeftMergeBitSet", "RightMergeBitSet"),1 ).
//                fieldsGrouping(Constants.LEFT_PREDICATE_IMMUTABLE_CSS,"LeftMergeBitSet", new Fields(Constants.TUPLE_ID)).
//                fieldsGrouping(Constants.RIGHT_PREDICATE_IMMUTABLE_CSS,"RightMergeBitSet", new Fields(Constants.TUPLE_ID)).setNumTasks(2);
     StormSubmitter.submitTopology("CrossJoin", config, builder.createTopology());

//        LocalCluster cluster = new LocalCluster();
//        cluster.submitTopology("CrossJoin", config, builder.createTopology());
    }
    public static KafkaSpoutRetryService getRetryService() {
        return new KafkaSpoutRetryExponentialBackoff(KafkaSpoutRetryExponentialBackoff.TimeInterval.microSeconds(0),
                KafkaSpoutRetryExponentialBackoff.TimeInterval.milliSeconds(2), Integer.MAX_VALUE,
                KafkaSpoutRetryExponentialBackoff.TimeInterval.seconds(10));
    }
}
