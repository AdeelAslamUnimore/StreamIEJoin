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
import com.experiment.selfjoin.bplustree.RightStreamPredicateBplusTreeSelfJoin;
import com.experiment.selfjoin.csstree.LeftPredicateCSSTreeBoltSelfJoin;
import com.experiment.selfjoin.csstree.LeftPredicateImmutableCSSBoltSelfJoin;
import com.experiment.selfjoin.csstree.RightPredicateCSSTreeBoltSelfJoin;
import com.experiment.selfjoin.csstree.RightPredicateImmutableCSSBoltSelfJoin;
import com.experiment.selfjoin.iejoinproposed.IEJoin;
import com.experiment.selfjoin.iejoinproposed.MutableBPlusTree;
import com.experiment.selfjoin.iejoinproposed.ResultBoltBitOperation;
import com.experiment.selfjoin.iejoinproposed.ResultBoltIEJoinOperation;
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
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.kafka.spout.FirstPollOffsetStrategy;
import org.apache.storm.kafka.spout.KafkaSpout;
import org.apache.storm.kafka.spout.KafkaSpoutConfig;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

public class Topology {
    public static void main(String[] args) throws Exception {
        new Topology().CSSTree();

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
                .fieldsGrouping("testSpout", "LeftStreamTuples", new Fields("ID"));


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
        TopologyBuilder builder = new TopologyBuilder();
        AtomicLong id = new AtomicLong();
        String groupId = "kafka-reader-group";
        // Kafka Spout for reading tuples:

        KafkaSpoutConfig<String, String> kafkaSpoutConfigForStreamR = KafkaSpoutConfig.builder("192.168.122.159:9092,192.168.122.231:9092", "selfjoin")
                .setProp(ConsumerConfig.GROUP_ID_CONFIG, groupId)
                .setProp(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class)
                .setProp(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class)
                .setProcessingGuarantee(KafkaSpoutConfig.ProcessingGuarantee.AT_LEAST_ONCE)
                .setRecordTranslator(record -> {
                    String[] splitValues = record.value().split(","); // Split record.value() based on a delimiter, adjust it as needed
                    String value1 = splitValues[5]; // Extract the first value
                    String value2 = splitValues[11];
                    long kafkaTime= Long.parseLong(splitValues[splitValues.length-1]);
                    // Extract the second value
                    id.getAndIncrement();
                    //String value3 = splitValues[2];
                    return new Values(value1, value2, id, kafkaTime, System.currentTimeMillis());
                }, new Fields("distance", "amount", "ID","KafkaTime", "Time"), "StreamR")
                .setFirstPollOffsetStrategy(FirstPollOffsetStrategy.UNCOMMITTED_LATEST)
                .build();


        builder.setSpout(Constants.KAFKA_SPOUT, new KafkaSpout<>(kafkaSpoutConfigForStreamR), 1);
        builder.setBolt(Constants.SPLIT_BOLT, new SplitBolt())
                .fieldsGrouping(Constants.KAFKA_SPOUT, "StreamR", new Fields("ID"));
        builder.setBolt(Constants.LEFT_PREDICATE_BOLT, new MutableBPlusTree(">", (String) map.get("LeftBatchPermutation")))
                .fieldsGrouping(Constants.SPLIT_BOLT, (String) map.get("LeftGreaterPredicateTuple"), new Fields(Constants.TUPLE_ID));//fieldsGrouping("testBolt", (String) map.get("RightSmallerPredicateTuple"), new Fields(Constants.TUPLE_ID));
        builder.setBolt(Constants.RIGHT_PREDICATE_BOLT, new MutableBPlusTree("<", (String) map.get("RightBatchPermutation")))
                .fieldsGrouping(Constants.SPLIT_BOLT, (String) map.get("RightSmallerPredicateTuple"), new Fields(Constants.TUPLE_ID));//.fieldsGrouping("testBolt", (String) map.get("RightGreaterPredicateTuple"), new Fields(Constants.TUPLE_ID));
        builder.setBolt(Constants.BIT_SET_EVALUATION_BOLT, new JoinerBoltForBitSetOperation()).fieldsGrouping(Constants.LEFT_PREDICATE_BOLT, (String) map.get("LeftPredicateSourceStreamIDBitSet"), new Fields(Constants.TUPLE_ID)).
                fieldsGrouping(Constants.RIGHT_PREDICATE_BOLT, (String) map.get("RightPredicateSourceStreamIDBitSet"), new Fields(Constants.TUPLE_ID)).setNumTasks(10);


        builder.setBolt(Constants.BITSET_RECORD_BOLT, new ResultBoltBitOperation()).
                shuffleGrouping(Constants.BIT_SET_EVALUATION_BOLT, (String) map.get("LeftPredicateSourceStreamIDBitSet"))
                .shuffleGrouping(Constants.BIT_SET_EVALUATION_BOLT, (String) map.get("RightPredicateSourceStreamIDBitSet")).
                shuffleGrouping(Constants.BIT_SET_EVALUATION_BOLT, (String) map.get("Results"));


        builder.setBolt(Constants.OFFSET_AND_IE_JOIN_BOLT_ID, new IEJoin())
                .directGrouping(Constants.LEFT_PREDICATE_BOLT, (String) map.get("LeftBatchPermutation"))
                .directGrouping(Constants.RIGHT_PREDICATE_BOLT, (String) map.get("RightBatchPermutation"))
                .directGrouping(Constants.LEFT_PREDICATE_BOLT, (String) map.get("MergingFlag"))
                .directGrouping(Constants.RIGHT_PREDICATE_BOLT, (String) map.get("MergingFlag"))
                .allGrouping(Constants.KAFKA_SPOUT, "StreamR");//.allGrouping(Constants.KAFKA_SPOUT, "RightStream").setNumTasks(10);
        // Record bolt
        builder.setBolt(Constants.IEJOIN_BOLT_RESULT, new ResultBoltIEJoinOperation()).shuffleGrouping(Constants.OFFSET_AND_IE_JOIN_BOLT_ID, (String) map.get("MergingTuplesRecord"))
                .shuffleGrouping(Constants.OFFSET_AND_IE_JOIN_BOLT_ID, (String) map.get("MergingTupleEvaluation")).shuffleGrouping(Constants.OFFSET_AND_IE_JOIN_BOLT_ID, (String) map.get("IEJoinResult"));
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("selfjoin", config, builder.createTopology());
    }

    public void BplusTree() throws Exception {
        Config config = new Config();
        Map<String, Object> map = Configuration.configurationConstantForStreamIDs();
        config.setNumWorkers(6);
        config.registerSerialization(java.util.BitSet.class);
        config.registerSerialization(java.util.HashSet.class);
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("testSpout", new Spout(1000));
        builder.setBolt("testBolt", new SplitBolt())
                .fieldsGrouping("testSpout", "LeftStreamTuples", new Fields("ID")).fieldsGrouping("testSpout", "RightStream", new Fields("ID"));
        builder.setBolt(Constants.LEFT_PREDICATE_BPLUS_TREE_AND_RBS_BOLT, new LeftStreamPredicateBplusTreeSelfJoin()).fieldsGrouping("testBolt", (String) map.get("LeftGreaterPredicateTuple"), new Fields(Constants.TUPLE_ID));

        builder.setBolt(Constants.RIGHT_PREDICATE_BPLUS_TREE_AND_RBST_BOLT, new RightStreamPredicateBplusTreeSelfJoin()).fieldsGrouping("testBolt", (String) map.get("RightSmallerPredicateTuple"), new Fields(Constants.TUPLE_ID));
        builder.setBolt(Constants.HASH_SET_EVALUATION, new JoinerBoltBplusTree(Constants.LEFT_PREDICATE_BOLT, Constants.RIGHT_PREDICATE_BOLT)).fieldsGrouping(Constants.LEFT_PREDICATE_BPLUS_TREE_AND_RBS_BOLT, Constants.LEFT_PREDICATE_BOLT, new Fields(Constants.TUPLE_ID)).
                fieldsGrouping(Constants.RIGHT_PREDICATE_BPLUS_TREE_AND_RBST_BOLT, Constants.RIGHT_PREDICATE_BOLT, new Fields(Constants.TUPLE_ID));

        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("Storm", config, builder.createTopology());
    }

    public void CSSTree() throws Exception {
        Config config = new Config();
        Map<String, Object> map = Configuration.configurationConstantForStreamIDs();
        config.setNumWorkers(6);
        config.registerSerialization(java.util.BitSet.class);
        config.registerSerialization(java.util.HashSet.class);
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("testSpout", new Spout(1000));
        builder.setBolt("testBolt", new SplitBolt())
                .fieldsGrouping("testSpout", "LeftStreamTuples", new Fields("ID")).fieldsGrouping("testSpout", "RightStream", new Fields("ID"));
        builder.setBolt(Constants.LEFT_PREDICATE_CSS_TREE_BOLT, new LeftPredicateCSSTreeBoltSelfJoin()).fieldsGrouping("testBolt", (String) map.get("LeftGreaterPredicateTuple"), new Fields(Constants.TUPLE_ID));
        builder.setBolt(Constants.RIGHT_PREDICATE_CSS_TREE_BOLT, new RightPredicateCSSTreeBoltSelfJoin()).fieldsGrouping("testBolt", (String) map.get("RightSmallerPredicateTuple"), new Fields(Constants.TUPLE_ID));
        builder.setBolt(Constants.MUTABLE_PART_EVALUATION_BOLT, new JoinerCSSTreeBolt((String) map.get("LeftPredicateSourceStreamIDHashSet"), (String) map.get("RightPredicateSourceStreamIDHashSet"))).fieldsGrouping(Constants.LEFT_PREDICATE_CSS_TREE_BOLT, (String) map.get("LeftPredicateSourceStreamIDHashSet"), new Fields(Constants.TUPLE_ID)).
                fieldsGrouping(Constants.RIGHT_PREDICATE_CSS_TREE_BOLT, (String) map.get("RightPredicateSourceStreamIDHashSet"), new Fields(Constants.TUPLE_ID));

        builder.setBolt(Constants.LEFT_PREDICATE_IMMUTABLE_CSS, new LeftPredicateImmutableCSSBoltSelfJoin()).shuffleGrouping(Constants.LEFT_PREDICATE_CSS_TREE_BOLT, (String) map.get("LeftGreaterPredicateTuple")).

                shuffleGrouping(Constants.LEFT_PREDICATE_CSS_TREE_BOLT, "LeftCheckForMerge")
                .shuffleGrouping("testSpout", "LeftStreamTuples");

        builder.setBolt(Constants.RIGHT_PREDICATE_IMMUTABLE_CSS, new RightPredicateImmutableCSSBoltSelfJoin()).shuffleGrouping(Constants.RIGHT_PREDICATE_CSS_TREE_BOLT, (String) map.get("RightSmallerPredicateTuple")).

                shuffleGrouping(Constants.RIGHT_PREDICATE_CSS_TREE_BOLT, "RightCheckForMerge").shuffleGrouping("testSpout", "LeftStreamTuples");

        builder.setBolt(Constants.IMMUTABLE_HASH_SET_EVALUATION, new JoinerCSSTreeBolt("LeftPredicate", "RightPredicate")).
                fieldsGrouping(Constants.LEFT_PREDICATE_IMMUTABLE_CSS, "LeftPredicate", new Fields(Constants.TUPLE_ID)).
                fieldsGrouping(Constants.RIGHT_PREDICATE_IMMUTABLE_CSS, "RightPredicate", new Fields(Constants.TUPLE_ID));

        builder.setBolt(Constants.MERGE_BOLT_EVALUATION_CSS, new JoinerCSSTreeBolt("LeftMergeBitSet", "RightMergeBitSet")).
                fieldsGrouping(Constants.LEFT_PREDICATE_IMMUTABLE_CSS, "LeftMergeBitSet", new Fields(Constants.TUPLE_ID)).
                fieldsGrouping(Constants.RIGHT_PREDICATE_IMMUTABLE_CSS, "RightMergeBitSet", new Fields(Constants.TUPLE_ID));

        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("Storm", config, builder.createTopology());
    }
}
