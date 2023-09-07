package com.experiment.selfjoin;

import clojure.lang.Cons;
import com.baselinealgorithm.chainbplusandcss.*;
import com.baselinealgorithm.chainindexbplustree.JoinerBoltBplusTree;
import com.baselinealgorithm.chainindexbplustree.LeftStreamPredicateBplusTree;
import com.baselinealgorithm.chainindexbplustree.RightStreamPredicateBplusTree;
import com.baselinealgorithm.chainindexrbst.JoinerBoltBST;
import com.baselinealgorithm.chainindexrbst.LeftPredicateBoltBST;
import com.baselinealgorithm.chainindexrbst.RightPredicateBoltBST;
import com.configurationsandconstants.iejoinandbaseworks.Configuration;
import com.configurationsandconstants.iejoinandbaseworks.Constants;
import com.experiment.selfjoin.bplustree.LeftStreamPredicateBplusTreeSelfJoin;
import com.experiment.selfjoin.bplustree.RecordBolt;
import com.experiment.selfjoin.bplustree.RightStreamPredicateBplusTreeSelfJoin;
import com.experiment.selfjoin.broadcasthashjoin.*;
import com.experiment.selfjoin.csstree.*;
import com.experiment.selfjoin.iejoinproposed.*;
import com.experiment.selfjoin.redblacktree.LeftPredicateBoltBSTSelfJoin;
import com.experiment.selfjoin.redblacktree.RightPredicateBoltBSTSelfJoin;
import com.proposed.iejoinandbplustreebased.IEJoinWithLinkedList;
import com.proposed.iejoinandbplustreebased.JoinerBoltForBitSetOperation;
import com.proposed.iejoinandbplustreebased.MutableBPlusTreeBolt;
import com.proposed.iejoinandbplustreebased.PermutationBolt;
import com.stormiequality.test.Spout;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.kafka.spout.FirstPollOffsetStrategy;
import org.apache.storm.kafka.spout.KafkaSpout;
import org.apache.storm.kafka.spout.KafkaSpoutConfig;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import redis.clients.jedis.Jedis;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class Topology {
    public static void main(String[] args) throws Exception {

        new Topology().BCHJ();

    }

    public void ChainedBST() throws Exception {
        Config config = new Config();
        Map<String, Object> map = Configuration.configurationConstantForStreamIDs();
        config.setNumWorkers(6);
        config.registerSerialization(java.util.BitSet.class);
        config.registerSerialization(java.util.HashSet.class);
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("testSpout", new Spout(1000));
        builder.setBolt("testBolt", new SplitBolt())
                .fieldsGrouping("testSpout", "StreamR", new Fields("ID"));


        builder.setBolt(Constants.LEFT_PREDICATE_BPLUS_TREE_AND_RBS_BOLT, new LeftPredicateBoltBSTSelfJoin()).fieldsGrouping("testBolt", (String) map.get("LeftGreaterPredicateTuple"), new Fields(Constants.TUPLE_ID));

        builder.setBolt(Constants.RIGHT_PREDICATE_BPLUS_TREE_AND_RBST_BOLT, new RightPredicateBoltBSTSelfJoin()).fieldsGrouping("testBolt", (String) map.get("RightSmallerPredicateTuple"), new Fields(Constants.TUPLE_ID));
        builder.setBolt(Constants.HASH_SET_EVALUATION, new JoinerBoltBST(Constants.LEFT_PREDICATE_BOLT, Constants.RIGHT_PREDICATE_BOLT)).fieldsGrouping(Constants.LEFT_PREDICATE_BPLUS_TREE_AND_RBS_BOLT, Constants.LEFT_PREDICATE_BOLT, new Fields(Constants.TUPLE_ID)).
                fieldsGrouping(Constants.RIGHT_PREDICATE_BPLUS_TREE_AND_RBST_BOLT, Constants.RIGHT_PREDICATE_BOLT, new Fields(Constants.TUPLE_ID));

        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("Storm", config, builder.createTopology());
    }

    public void IEJoin() throws Exception {

        Config config = new Config();
        Map<String, Object> map = Configuration.configurationConstantForStreamIDs();
        config.setNumWorkers(10);
        config.registerSerialization(java.util.BitSet.class);
        config.registerSerialization(java.util.HashSet.class);
        config.registerSerialization(java.util.concurrent.atomic.AtomicLong.class);
        List<String> workerChildopts = new ArrayList<>();
        workerChildopts.add("-Xmx2g");
        workerChildopts.add("-Xss8m");
        config.put(Config.TOPOLOGY_WORKER_CHILDOPTS, workerChildopts);
        config.setNumAckers(4);
        TopologyBuilder builder = new TopologyBuilder();
        final int[] id = {0};
        String groupId = "kafka-reader-group";
        // Kafka Spout for reading tuples:

        KafkaSpoutConfig<String, String> kafkaSpoutConfigForStreamR = KafkaSpoutConfig.builder("192.168.122.160:9093,192.168.122.231:9094", "selfjoin")
                .setProp(ConsumerConfig.GROUP_ID_CONFIG, groupId)
                .setProp(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class)
                .setProp(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class)
                .setProcessingGuarantee(KafkaSpoutConfig.ProcessingGuarantee.AT_LEAST_ONCE)
                .setRecordTranslator(record -> {
                    String[] splitValues = record.value().split(","); // Split record.value() based on a delimiter, adjust it as needed
                    double value1, value2 = 0;
                    //  int id=0;
                    try {
                        value1 = Double.parseDouble(splitValues[5]);
                        value2 = Double.parseDouble(splitValues[11]);
                        //    id= Integer.parseInt(splitValues[splitValues.length - 2]);


                    } catch (NumberFormatException e) {
                        value1 = 0;
                        value2 = 0;
                        //  id=0;
                    }
                    long kafkaTime = Long.parseLong(splitValues[splitValues.length - 1]);
                    // Extract the second value
                    id[0]++;
                    //String value3 = splitValues[2];

                    return new Values((int) Math.round(value1), (int) Math.round(value2), id[0], kafkaTime, System.currentTimeMillis());
                }, new Fields("distance", "amount", "ID", "kafkaTime", "Time"), "StreamR")
                .setFirstPollOffsetStrategy(FirstPollOffsetStrategy.UNCOMMITTED_LATEST)
                .build();
        builder.setSpout(Constants.KAFKA_SPOUT, new KafkaSpout<>(kafkaSpoutConfigForStreamR), 1);
        // builder.setSpout(Constants.KAFKA_SPOUT, new Spout(1000));
        builder.setBolt(Constants.SPLIT_BOLT, new SplitBolt())
                .fieldsGrouping(Constants.KAFKA_SPOUT, "StreamR", new Fields("ID"));
        builder.setBolt(Constants.LEFT_PREDICATE_BOLT, new MutableBPlusTree(">", (String) map.get("LeftBatchPermutation")))
                .fieldsGrouping(Constants.SPLIT_BOLT, (String) map.get("LeftGreaterPredicateTuple"), new Fields(Constants.TUPLE_ID));//fieldsGrouping("testBolt", (String) map.get("RightSmallerPredicateTuple"), new Fields(Constants.TUPLE_ID));
        builder.setBolt(Constants.RIGHT_PREDICATE_BOLT, new MutableBPlusTree("<", (String) map.get("RightBatchPermutation")))
                .fieldsGrouping(Constants.SPLIT_BOLT, (String) map.get("RightSmallerPredicateTuple"), new Fields(Constants.TUPLE_ID));//.fieldsGrouping("testBolt", (String) map.get("RightGreaterPredicateTuple"), new Fields(Constants.TUPLE_ID));
//        builder.setBolt(Constants.BIT_SET_EVALUATION_BOLT, new JoinerBoltForBitSetOperation()).fieldsGrouping(Constants.LEFT_PREDICATE_BOLT, (String) map.get("LeftPredicateSourceStreamIDBitSet"), new Fields(Constants.TUPLE_ID)).
//                fieldsGrouping(Constants.RIGHT_PREDICATE_BOLT, (String) map.get("RightPredicateSourceStreamIDBitSet"), new Fields(Constants.TUPLE_ID)).setNumTasks(50);

        builder.setBolt(Constants.BIT_SET_EVALUATION_BOLT, new MutableJoiner()).fieldsGrouping(Constants.LEFT_PREDICATE_BOLT, (String) map.get("LeftPredicateSourceStreamIDBitSet"), new Fields(Constants.TUPLE_ID)).
                fieldsGrouping(Constants.RIGHT_PREDICATE_BOLT, (String) map.get("RightPredicateSourceStreamIDBitSet"), new Fields(Constants.TUPLE_ID)).setNumTasks(50);

//
        builder.setBolt(Constants.BITSET_RECORD_BOLT, new ResultBoltBitOperation()).
                shuffleGrouping(Constants.BIT_SET_EVALUATION_BOLT, (String) map.get("LeftPredicateSourceStreamIDBitSet"))
                .shuffleGrouping(Constants.BIT_SET_EVALUATION_BOLT, (String) map.get("RightPredicateSourceStreamIDBitSet")).
                shuffleGrouping(Constants.BIT_SET_EVALUATION_BOLT, (String) map.get("Results"));


        builder.setBolt(Constants.OFFSET_AND_IE_JOIN_BOLT_ID, new IEJoin())
                .allGrouping(Constants.KAFKA_SPOUT, "StreamR")
                .directGrouping(Constants.LEFT_PREDICATE_BOLT, (String) map.get("LeftBatchPermutation"))
                .directGrouping(Constants.RIGHT_PREDICATE_BOLT, (String) map.get("RightBatchPermutation"))
                .directGrouping(Constants.LEFT_PREDICATE_BOLT, (String) map.get("MergingFlag"))
                .directGrouping(Constants.RIGHT_PREDICATE_BOLT, (String) map.get("MergingFlag"));
        //.allGrouping(Constants.KAFKA_SPOUT, "RightStream").setNumTasks(10);
        // Record bolt
        builder.setBolt(Constants.IEJOIN_BOLT_RESULT, new ResultBoltIEJoinOperation()).shuffleGrouping(Constants.OFFSET_AND_IE_JOIN_BOLT_ID, (String) map.get("MergingTuplesRecord"))
                .shuffleGrouping(Constants.OFFSET_AND_IE_JOIN_BOLT_ID, (String) map.get("MergingTupleEvaluation")).shuffleGrouping(Constants.OFFSET_AND_IE_JOIN_BOLT_ID, (String) map.get("IEJoinResult"));
//        LocalCluster cluster = new LocalCluster();
//        cluster.submitTopology("Storm", config, builder.createTopology());
        StormSubmitter.submitTopology("selfjoinIE", config, builder.createTopology());
    }

    public void BplusTree() throws Exception {
        Config config = new Config();
        Map<String, Object> map = Configuration.configurationConstantForStreamIDs();
        config.setNumWorkers(10);
        config.registerSerialization(java.util.BitSet.class);
        config.registerSerialization(java.util.HashSet.class);
        config.registerSerialization(java.util.concurrent.atomic.AtomicLong.class);
        List<String> workerChildopts = new ArrayList<>();
        workerChildopts.add("-Xmx2g");
        workerChildopts.add("-Xss8m");
        config.put(Config.TOPOLOGY_WORKER_CHILDOPTS, workerChildopts);
        config.setNumAckers(4);
        TopologyBuilder builder = new TopologyBuilder();
        final int[] id = {0};
        String groupId = "kafka-reader-group";
        // Kafka Spout for reading tuples:

        KafkaSpoutConfig<String, String> kafkaSpoutConfigForStreamR = KafkaSpoutConfig.builder("192.168.122.160:9093,192.168.122.231:9094", "selfjoin")
                .setProp(ConsumerConfig.GROUP_ID_CONFIG, groupId)
                .setProp(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class)
                .setProp(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class)
                .setProcessingGuarantee(KafkaSpoutConfig.ProcessingGuarantee.AT_LEAST_ONCE)
                .setRecordTranslator(record -> {
                    String[] splitValues = record.value().split(","); // Split record.value() based on a delimiter, adjust it as needed
                    double value1, value2 = 0;
                    //  int id=0;
                    try {
                        value1 = Double.parseDouble(splitValues[5]);
                        value2 = Double.parseDouble(splitValues[11]);
                        //    id= Integer.parseInt(splitValues[splitValues.length - 2]);


                    } catch (NumberFormatException e) {
                        value1 = 0;
                        value2 = 0;
                        //  id=0;
                    }
                    long kafkaTime = Long.parseLong(splitValues[splitValues.length - 1]);
                    // Extract the second value
                    id[0]++;
                    //String value3 = splitValues[2];

                    return new Values((int) Math.round(value1), (int) Math.round(value2), id[0], kafkaTime, System.currentTimeMillis());
                }, new Fields("distance", "amount", "ID", "kafkaTime", "Time"), "StreamR")
                .setFirstPollOffsetStrategy(FirstPollOffsetStrategy.UNCOMMITTED_LATEST)
                .build();
        builder.setSpout(Constants.KAFKA_SPOUT, new KafkaSpout<>(kafkaSpoutConfigForStreamR), 1);

    //    builder.setSpout(Constants.KAFKA_SPOUT, new Spout(1000), 1);
        builder.setBolt("SplitBolt", new SplitBolt())
                .fieldsGrouping(Constants.KAFKA_SPOUT, "StreamR", new Fields("ID"));//fieldsGrouping("testSpout", "RightStream", new Fields("ID"));
        builder.setBolt(Constants.LEFT_PREDICATE_BPLUS_TREE_AND_RBS_BOLT, new LeftStreamPredicateBplusTreeSelfJoin()).fieldsGrouping("SplitBolt", (String) map.get("LeftGreaterPredicateTuple"), new Fields(Constants.TUPLE_ID));

        builder.setBolt(Constants.RIGHT_PREDICATE_BPLUS_TREE_AND_RBST_BOLT, new RightStreamPredicateBplusTreeSelfJoin()).fieldsGrouping("SplitBolt", (String) map.get("RightSmallerPredicateTuple"), new Fields(Constants.TUPLE_ID));
        builder.setBolt(Constants.HASH_SET_EVALUATION, new JoinerBoltBplusTree(Constants.LEFT_PREDICATE_BOLT, Constants.RIGHT_PREDICATE_BOLT)).fieldsGrouping(Constants.LEFT_PREDICATE_BPLUS_TREE_AND_RBS_BOLT, Constants.LEFT_PREDICATE_BOLT, new Fields(Constants.TUPLE_ID)).
                fieldsGrouping(Constants.RIGHT_PREDICATE_BPLUS_TREE_AND_RBST_BOLT, Constants.RIGHT_PREDICATE_BOLT, new Fields(Constants.TUPLE_ID)).setNumTasks(50);
        builder.setBolt("DataRecordingBolt", new RecordBolt()).fieldsGrouping(Constants.HASH_SET_EVALUATION,"Record",new Fields("ID"));
       StormSubmitter.submitTopology("selfjoinChainedIndex", config, builder.createTopology());
//        LocalCluster cluster = new LocalCluster();
//        cluster.submitTopology("Storm", config, builder.createTopology());
    }

    public void CSSTree() throws Exception {
        Config config = new Config();
        Map<String, Object> map = Configuration.configurationConstantForStreamIDs();
        config.setNumWorkers(10);
        config.registerSerialization(java.util.BitSet.class);
        config.registerSerialization(java.util.HashSet.class);
        config.registerSerialization(java.util.concurrent.atomic.AtomicLong.class);
        List<String> workerChildopts = new ArrayList<>();
        workerChildopts.add("-Xmx2g");
        workerChildopts.add("-Xss8m");
        config.put(Config.TOPOLOGY_WORKER_CHILDOPTS, workerChildopts);
        config.setNumAckers(4);
        //config.setDebug(true);
        TopologyBuilder builder = new TopologyBuilder();
        final int[] id = {0};
        String groupId = "kafka-reader-group";
        // Kafka Spout for reading tuples:

        KafkaSpoutConfig<String, String> kafkaSpoutConfigForStreamR = KafkaSpoutConfig.builder("192.168.122.160:9093,192.168.122.231:9094", "selfjoin")
                .setProp(ConsumerConfig.GROUP_ID_CONFIG, groupId)
                .setProp(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class)
                .setProp(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class)
                .setProcessingGuarantee(KafkaSpoutConfig.ProcessingGuarantee.AT_LEAST_ONCE)
                .setRecordTranslator(record -> {
                    String[] splitValues = record.value().split(","); // Split record.value() based on a delimiter, adjust it as needed
                    double value1, value2 = 0;
                    //  int id=0;
                    try {
                        value1 = Double.parseDouble(splitValues[5]);
                        value2 = Double.parseDouble(splitValues[11]);
                        //    id= Integer.parseInt(splitValues[splitValues.length - 2]);


                    } catch (NumberFormatException e) {
                        value1 = 0;
                        value2 = 0;
                        //  id=0;
                    }
                    long kafkaTime = Long.parseLong(splitValues[splitValues.length - 1]);
                    // Extract the second value
                    id[0]++;
                    //String value3 = splitValues[2];

                    return new Values((int) Math.round(value1), (int) Math.round(value2), id[0], kafkaTime, System.currentTimeMillis());
                }, new Fields("distance", "amount", "ID", "kafkaTime", "Time"), "StreamR")
                .setFirstPollOffsetStrategy(FirstPollOffsetStrategy.UNCOMMITTED_LATEST)
                .build();
        builder.setSpout(Constants.KAFKA_SPOUT, new KafkaSpout<>(kafkaSpoutConfigForStreamR), 1);
       // builder.setSpout(Constants.KAFKA_SPOUT, new Spout(1000));
        builder.setBolt("testBolt", new SplitBolt())
                .fieldsGrouping(Constants.KAFKA_SPOUT, "StreamR", new Fields("ID"));//fieldsGrouping("testSpout", "RightStream", new Fields("ID"));
        builder.setBolt(Constants.LEFT_PREDICATE_CSS_TREE_BOLT, new LeftPredicateCSSTreeBoltSelfJoin()).fieldsGrouping("testBolt", (String) map.get("LeftGreaterPredicateTuple"), new Fields(Constants.TUPLE_ID));
        builder.setBolt(Constants.RIGHT_PREDICATE_CSS_TREE_BOLT, new RightPredicateCSSTreeBoltSelfJoin()).fieldsGrouping("testBolt", (String) map.get("RightSmallerPredicateTuple"), new Fields(Constants.TUPLE_ID));
        builder.setBolt(Constants.MUTABLE_PART_EVALUATION_BOLT, new JoinerCSSTreeBolt((String) map.get("LeftPredicateSourceStreamIDHashSet"), (String) map.get("RightPredicateSourceStreamIDHashSet"))).fieldsGrouping(Constants.LEFT_PREDICATE_CSS_TREE_BOLT, (String) map.get("LeftPredicateSourceStreamIDHashSet"), new Fields(Constants.TUPLE_ID)).
                fieldsGrouping(Constants.RIGHT_PREDICATE_CSS_TREE_BOLT, (String) map.get("RightPredicateSourceStreamIDHashSet"), new Fields(Constants.TUPLE_ID)).setNumTasks(50);
        builder.setBolt(Constants.HASHSET_RECORD_BOLT, new RecordMutablePart()).
                shuffleGrouping(Constants.MUTABLE_PART_EVALUATION_BOLT, (String) map.get("LeftPredicateSourceStreamIDBitSet"))
                .shuffleGrouping(Constants.MUTABLE_PART_EVALUATION_BOLT, (String) map.get("RightPredicateSourceStreamIDBitSet")).
                shuffleGrouping(Constants.MUTABLE_PART_EVALUATION_BOLT, (String) map.get("Results"));


        builder.setBolt(Constants.LEFT_PREDICATE_IMMUTABLE_CSS, new ImmutableCSSPartUpdated()).
                  allGrouping(Constants.KAFKA_SPOUT, "StreamR").
                shuffleGrouping(Constants.LEFT_PREDICATE_CSS_TREE_BOLT, (String) map.get("LeftGreaterPredicateTuple")).
                shuffleGrouping(Constants.LEFT_PREDICATE_CSS_TREE_BOLT, "LeftCheckForMerge").
                shuffleGrouping(Constants.RIGHT_PREDICATE_CSS_TREE_BOLT, (String) map.get("RightSmallerPredicateTuple")).
                shuffleGrouping(Constants.RIGHT_PREDICATE_CSS_TREE_BOLT, "RightCheckForMerge");
               // .allGrouping(Constants.KAFKA_SPOUT, "StreamR");

        builder.setBolt("RecordImmutablePart", new RecordImmutablePart()).shuffleGrouping(Constants.LEFT_PREDICATE_IMMUTABLE_CSS, "MergingTime").shuffleGrouping(Constants.LEFT_PREDICATE_IMMUTABLE_CSS, "QueueMergeTime");
//


//        builder.setBolt(Constants.LEFT_PREDICATE_IMMUTABLE_CSS, new LeftPredicateImmutableCSSBoltSelfJoin()).shuffleGrouping(Constants.LEFT_PREDICATE_CSS_TREE_BOLT, (String) map.get("LeftGreaterPredicateTuple")).
//
//                shuffleGrouping(Constants.LEFT_PREDICATE_CSS_TREE_BOLT, "LeftCheckForMerge")
//                .allGrouping("testBolt", "LeftStreamR");
//
//        builder.setBolt(Constants.RIGHT_PREDICATE_IMMUTABLE_CSS, new RightPredicateImmutableCSSBoltSelfJoin()).shuffleGrouping(Constants.RIGHT_PREDICATE_CSS_TREE_BOLT, (String) map.get("RightSmallerPredicateTuple")).
//
//                shuffleGrouping(Constants.RIGHT_PREDICATE_CSS_TREE_BOLT, "RightCheckForMerge").allGrouping("testBolt", "RightStreamR");
//
////
//        builder.setBolt(Constants.IMMUTABLE_HASH_SET_EVALUATION, new JoinerCSSTreeImmutableTupleBolt("LeftPredicate", "RightPredicate")).
//                fieldsGrouping(Constants.LEFT_PREDICATE_IMMUTABLE_CSS, "LeftPredicate", new Fields(Constants.TUPLE_ID)).
//                fieldsGrouping(Constants.RIGHT_PREDICATE_IMMUTABLE_CSS, "RightPredicate", new Fields(Constants.TUPLE_ID));
//
//        builder.setBolt(Constants.MERGE_BOLT_EVALUATION_CSS, new JoinerCSSTreeImmutableTupleQueueDuringMerging("LeftMergeHashSet", "RightMergeHashSet")).
//                fieldsGrouping(Constants.LEFT_PREDICATE_IMMUTABLE_CSS, "LeftMergeHashSet", new Fields(Constants.TUPLE_ID)).
//                fieldsGrouping(Constants.RIGHT_PREDICATE_IMMUTABLE_CSS, "RightMergeHashSet", new Fields(Constants.TUPLE_ID));
//
//        builder.setBolt("MergeTimeBolt", new MergingTimeEvaluation("LeftMergingTuplesCSSCreation", "RightMergingTuplesCSSCreation")).
//                shuffleGrouping(Constants.LEFT_PREDICATE_IMMUTABLE_CSS, "LeftMergingTuplesCSSCreation").
//                shuffleGrouping(Constants.RIGHT_PREDICATE_IMMUTABLE_CSS, "RightMergingTuplesCSSCreation");
//
//        builder.setBolt("Record", new RecordImmutablePart()).

//                shuffleGrouping("MergeTimeBolt","MergingTime");
    StormSubmitter.submitTopology("selfjoinCSS", config, builder.createTopology());
//
//        LocalCluster cluster = new LocalCluster();
//        cluster.submitTopology("Storm", config, builder.createTopology());
    }
    public void BCHJ() throws Exception{
        Config config = new Config();
        Map<String, Object> map = Configuration.configurationConstantForStreamIDs();
        config.setNumWorkers(10);
        config.registerSerialization(java.util.BitSet.class);
        config.registerSerialization(java.util.HashSet.class);
        config.registerSerialization(java.util.concurrent.atomic.AtomicLong.class);
        List<String> workerChildopts = new ArrayList<>();
        workerChildopts.add("-Xmx2g");
        workerChildopts.add("-Xss8m");
        config.put(Config.TOPOLOGY_WORKER_CHILDOPTS, workerChildopts);
        config.setNumAckers(4);

        TopologyBuilder builder = new TopologyBuilder();
        final int[] id = {0};
        String groupId = "kafka-reader-group";
        // Kafka Spout for reading tuples:

        KafkaSpoutConfig<String, String> kafkaSpoutConfigForStreamR = KafkaSpoutConfig.builder("192.168.122.160:9093,192.168.122.231:9094", "selfjoin")
                .setProp(ConsumerConfig.GROUP_ID_CONFIG, groupId)
                .setProp(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class)
                .setProp(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class)
                .setProcessingGuarantee(KafkaSpoutConfig.ProcessingGuarantee.AT_LEAST_ONCE)
                .setRecordTranslator(record -> {
                    String[] splitValues = record.value().split(","); // Split record.value() based on a delimiter, adjust it as needed
                    double value1, value2 = 0;
                    //  int id=0;
                    try {
                        value1 = Double.parseDouble(splitValues[5]);
                        value2 = Double.parseDouble(splitValues[11]);
                        //    id= Integer.parseInt(splitValues[splitValues.length - 2]);


                    } catch (NumberFormatException e) {
                        value1 = 0;
                        value2 = 0;
                        //  id=0;
                    }
                    long kafkaTime = Long.parseLong(splitValues[splitValues.length - 1]);
                    // Extract the second value
                    id[0]++;
                    //String value3 = splitValues[2];

                    return new Values((int) Math.round(value1), (int) Math.round(value2), id[0], kafkaTime, System.currentTimeMillis());
                }, new Fields("distance", "amount", "ID", "kafkaTime", "Time"), "StreamR")
                .setFirstPollOffsetStrategy(FirstPollOffsetStrategy.UNCOMMITTED_LATEST)
                .build();
        builder.setSpout(Constants.KAFKA_SPOUT, new KafkaSpout<>(kafkaSpoutConfigForStreamR), 1);


       // builder.setSpout(Constants.KAFKA_SPOUT, new Spout(1000), 1);
        builder.setBolt("SplitBolt", new BroadCastHashJoinSplitBolt())
                .fieldsGrouping(Constants.KAFKA_SPOUT, "StreamR", new Fields("ID"));
        builder.setBolt("LeftPredicate", new LeftPredicateEvaluation()).allGrouping("SplitBolt","LeftInsertion").fieldsGrouping("SplitBolt","LeftSearch", new Fields(Constants.TUPLE_ID)).setNumTasks(10);
        builder.setBolt("RightPredicate", new RightPredicateEvaluation()).allGrouping("SplitBolt","RightInsertion").fieldsGrouping("SplitBolt","RightSearch", new Fields(Constants.TUPLE_ID)).setNumTasks(10);
        builder.setBolt("Joiner", new Joiner()).fieldsGrouping("LeftPredicate","Left", new Fields(Constants.TUPLE_ID)).
                fieldsGrouping("RightPredicate","Right",new Fields(Constants.TUPLE_ID)).setNumTasks(30);
        builder.setBolt("LeftPredicateResult", new LeftPredicateEvaluationRecord()).shuffleGrouping("LeftPredicate","LeftRecord");
        builder.setBolt("RightPredicateResult", new RightPredicateEvaluationRecord()).shuffleGrouping("RightPredicate","RightRecord");
        builder.setBolt("JoinerRecord", new JoinerRecord()).shuffleGrouping("Joiner","JoinerStream");
       StormSubmitter.submitTopology("selfjoinChainedIndex", config, builder.createTopology());
//          LocalCluster cluster = new LocalCluster();
//          cluster.submitTopology("Storm", config, builder.createTopology());
    }
    public void redisDB(){

            Jedis jedis = new Jedis("192.168.122.1", 6379);
            String response = jedis.ping();

            if (response.equalsIgnoreCase("PONG")) {
                System.out.println("Redis server is running.");
            } else {
                System.out.println("Redis server is not running.");
            }

            jedis.close();

    }
}
