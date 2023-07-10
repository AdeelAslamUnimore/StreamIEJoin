package com.baselinealgorithm.chainindexbplustree;

import com.configurationsandconstants.iejoinandbaseworks.Configuration;
import com.configurationsandconstants.iejoinandbaseworks.Constants;
import com.stormiequality.test.SplitBolt;
import com.stormiequality.test.Spout;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

import java.util.Map;

public class ChainIndexBPlusTree {
    public static void main(String[] args) throws Exception {
        Config config= new Config();
        Map<String, Object> map= Configuration.configurationConstantForStreamIDs();
        config.setNumWorkers(6);
        config.registerSerialization(java.util.BitSet.class);
        config.registerSerialization(java.util.HashSet.class);
        TopologyBuilder builder= new TopologyBuilder();
        builder.setSpout("testSpout", new Spout(1000));
        builder.setBolt("testBolt", new SplitBolt())
                .fieldsGrouping("testSpout", "LeftStreamTuples", new Fields("ID")).fieldsGrouping("testSpout", "RightStream", new Fields("ID"));
        builder.setBolt(Constants.LEFT_PREDICATE_BPLUS_TREE_AND_RBS_BOLT, new LeftStreamPredicateBplusTree()).fieldsGrouping("testBolt",(String) map.get("LeftSmallerPredicateTuple"), new Fields(Constants.TUPLE_ID)).
                fieldsGrouping("testBolt",(String) map.get("RightSmallerPredicateTuple"), new Fields(Constants.TUPLE_ID));

        builder.setBolt(Constants.RIGHT_PREDICATE_BPLUS_TREE_AND_RBST_BOLT, new RightStreamPredicateBplusTree()).fieldsGrouping("testBolt",(String) map.get("LeftGreaterPredicateTuple"), new Fields(Constants.TUPLE_ID)).
                fieldsGrouping("testBolt",(String) map.get("RightGreaterPredicateTuple"), new Fields(Constants.TUPLE_ID));
        builder.setBolt(Constants.HASH_SET_EVALUATION, new JoinerBoltBplusTree(Constants.LEFT_PREDICATE_BOLT, Constants.RIGHT_PREDICATE_BOLT)).fieldsGrouping(Constants.LEFT_PREDICATE_BPLUS_TREE_AND_RBS_BOLT,Constants.LEFT_PREDICATE_BOLT, new Fields(Constants.TUPLE_ID)).
                fieldsGrouping(Constants.RIGHT_PREDICATE_BPLUS_TREE_AND_RBST_BOLT, Constants.RIGHT_PREDICATE_BOLT, new Fields(Constants.TUPLE_ID));

        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("Storm", config, builder.createTopology());
    }
}
