package com.proposed.iejoinandbplustreebased;

import com.configurationsandconstants.iejoinandbaseworks.Configuration;
import com.configurationsandconstants.iejoinandbaseworks.Constants;
import com.stormiequality.test.SplitBolt;
import com.stormiequality.test.Spout;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

import java.util.Map;

public class ProposedModelTopology {
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
        builder.setBolt(Constants.LEFT_PREDICATE_BOLT, new MutableBPlusTreeBolt("<",(String)map.get("LeftBatchPermutation"),(String)map.get("LeftBatchOffset")))
                .fieldsGrouping("testBolt", (String) map.get("LeftSmallerPredicateTuple"), new Fields(Constants.TUPLE_ID)).fieldsGrouping("testBolt", (String) map.get("RightSmallerPredicateTuple"), new Fields(Constants.TUPLE_ID));
        builder.setBolt(Constants.RIGHT_PREDICATE_BOLT, new MutableBPlusTreeBolt(">",(String)map.get("RightBatchPermutation"),(String)map.get("RightBatchOffset")))
                .fieldsGrouping("testBolt", (String) map.get("LeftGreaterPredicateTuple"), new Fields(Constants.TUPLE_ID)).fieldsGrouping("testBolt", (String) map.get("RightGreaterPredicateTuple"), new Fields(Constants.TUPLE_ID));
        builder.setBolt(Constants.BIT_SET_EVALUATION_BOLT, new JoinerBoltForBitSetOperation()).fieldsGrouping(Constants.LEFT_PREDICATE_BOLT, (String)map.get("LeftPredicateSourceStreamIDBitSet"),new Fields(Constants.TUPLE_ID)).
                fieldsGrouping(Constants.RIGHT_PREDICATE_BOLT,(String)map.get("RightPredicateSourceStreamIDBitSet"),new Fields(Constants.TUPLE_ID)).setNumTasks(1);
        builder.setBolt(Constants.PERMUTATION_COMPUTATION_BOLT_ID, new PermutationBolt()).directGrouping(Constants.LEFT_PREDICATE_BOLT, (String) map.get("LeftBatchPermutation")).  directGrouping(Constants.RIGHT_PREDICATE_BOLT, (String) map.get("RightBatchPermutation")).setNumTasks(2);
        builder.setBolt(Constants.OFFSET_AND_IE_JOIN_BOLT_ID, new IEJoinWithLinkedList())
                .directGrouping(Constants.PERMUTATION_COMPUTATION_BOLT_ID, (String) map.get("LeftBatchPermutation"))
                 .directGrouping(Constants.PERMUTATION_COMPUTATION_BOLT_ID, (String) map.get("RightBatchPermutation"))
                .directGrouping(Constants.LEFT_PREDICATE_BOLT,(String) map.get("LeftBatchOffset"))
                .directGrouping(Constants.RIGHT_PREDICATE_BOLT, (String) map.get("RightBatchOffset")).
                directGrouping(Constants.LEFT_PREDICATE_BOLT,(String) map.get("MergingFlag") )
                .directGrouping(Constants.RIGHT_PREDICATE_BOLT,(String) map.get("MergingFlag") )
                .allGrouping("testSpout", "LeftStreamTuples").allGrouping("testSpout", "RightStream").setNumTasks(10);

        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("Storm", config, builder.createTopology());
    }

}
