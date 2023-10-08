package com.baselinealgorithm.chainindexbplustree;

import com.configurationsandconstants.iejoinandbaseworks.Configuration;
import com.configurationsandconstants.iejoinandbaseworks.Constants;
import com.stormiequality.inputdata.RackPowerSpout;
import com.stormiequality.inputdata.SplitBolt;
import com.stormiequality.inputdata.Spout;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class ChainIndexBPlusTree {
    public static void main(String[] args) throws Exception {
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
        config.setNumAckers(4);;
        TopologyBuilder builder= new TopologyBuilder();
       // builder.setSpout("streamSpout", new Spout(1000));
         builder.setSpout("streamspout", new RackPowerSpout());
        builder.setBolt("testBolt", new SplitBolt(),5)
                .fieldsGrouping("streamspout", "StreamR", new Fields("ID")).fieldsGrouping("streamspout", "StreamS", new Fields("ID")).setNumTasks(10);
        builder.setBolt(Constants.LEFT_PREDICATE_BPLUS_TREE_AND_RBS_BOLT, new LeftStreamPredicateBplusTree()).fieldsGrouping("testBolt",(String) map.get("LeftSmallerPredicateTuple"), new Fields(Constants.TUPLE_ID)).
                fieldsGrouping("testBolt",(String) map.get("RightSmallerPredicateTuple"), new Fields(Constants.TUPLE_ID));

        builder.setBolt(Constants.RIGHT_PREDICATE_BPLUS_TREE_AND_RBST_BOLT, new RightStreamPredicateBplusTree()).fieldsGrouping("testBolt",(String) map.get("LeftGreaterPredicateTuple"), new Fields(Constants.TUPLE_ID)).
                fieldsGrouping("testBolt",(String) map.get("RightGreaterPredicateTuple"), new Fields(Constants.TUPLE_ID));
        builder.setBolt(Constants.HASH_SET_EVALUATION, new JoinerBoltBplusTree(Constants.LEFT_PREDICATE_BOLT, Constants.RIGHT_PREDICATE_BOLT), 5).fieldsGrouping(Constants.LEFT_PREDICATE_BPLUS_TREE_AND_RBS_BOLT,Constants.LEFT_PREDICATE_BOLT, new Fields(Constants.TUPLE_ID))
                .fieldsGrouping(Constants.LEFT_PREDICATE_BPLUS_TREE_AND_RBS_BOLT,Constants.RIGHT_PREDICATE_BOLT, new Fields(Constants.TUPLE_ID)).
                fieldsGrouping(Constants.RIGHT_PREDICATE_BPLUS_TREE_AND_RBST_BOLT, Constants.LEFT_PREDICATE_BOLT, new Fields(Constants.TUPLE_ID)).
                fieldsGrouping(Constants.RIGHT_PREDICATE_BPLUS_TREE_AND_RBST_BOLT, Constants.RIGHT_PREDICATE_BOLT, new Fields(Constants.TUPLE_ID)).setNumTasks(10);
        builder.setBolt("Results", new RecordChainIndex()).shuffleGrouping(Constants.HASH_SET_EVALUATION, "Record");

        StormSubmitter.submitTopology("CrossJoin", config, builder.createTopology());
//
//        LocalCluster cluster = new LocalCluster();
//        cluster.submitTopology("Storm", config, builder.createTopology());
    }
}
