package com.proposed.iejoinandbplustreebased;

import com.stormiequality.join.IEJoinComputationBolt;
import com.stormiequality.test.SplitBolt;
import com.stormiequality.test.Spout;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

public class ProposedModelTopology {
    public static void main(String[] args) throws Exception {
        Config config= Configuration.configurationConstantForStreamIDs();
        config.setNumWorkers(6);
        config.registerSerialization(java.util.BitSet.class);
        config.registerSerialization(java.util.HashSet.class);
        TopologyBuilder builder= new TopologyBuilder();
        builder.setSpout("testSpout", new Spout(500));
        builder.setBolt("testBolt", new SplitBolt())
                .fieldsGrouping("testSpout", "LeftStreamTuples", new Fields("ID")).fieldsGrouping("testSpout", "RightStream", new Fields("ID"));
        builder.setBolt(Constants.LEFT_PREDICATE_BOLT, new MutableBPlusTreeBolt("<",(String)config.get("LeftBatchPermutation"),(String)config.get("LeftBatchOffset")))
                .fieldsGrouping("testSpout", (String) config.get("LeftSmallerPredicateTuple"), new Fields("ID")).fieldsGrouping("testSpout", (String) config.get("RightSmallerPredicateTuple"), new Fields("ID"));
        builder.setBolt(Constants.RIGHT_PREDICATE_BOLT, new MutableBPlusTreeBolt(">",(String)config.get("RightBatchPermutation"),(String)config.get("RightBatchOffset")))
                .fieldsGrouping("testSpout", (String) config.get("LeftGreaterPredicateTuple"), new Fields("ID")).fieldsGrouping("testSpout", (String) config.get("RightGreaterPredicateTuple"), new Fields("ID"));
        builder.setBolt(Constants.BIT_SET_EVALUATION_BOLT, new JoinerBoltForBitSetOperation()).fieldsGrouping(Constants.LEFT_PREDICATE_BOLT, (String)config.get("LeftPredicateSourceStreamIDBitSet"),new Fields(Constants.TUPLE_ID)).
                fieldsGrouping(Constants.RIGHT_PREDICATE_BOLT,(String)config.get("RightPredicateSourceStreamIDBitSet"),new Fields(Constants.TUPLE_ID)).setNumTasks(10);
        builder.setBolt(Constants.PERMUTATION_COMPUTATION_BOLT_ID, new PermutationBolt()).directGrouping(Constants.LEFT_PREDICATE_BOLT, (String) config.get("LeftBatchPermutation")).  directGrouping(Constants.RIGHT_PREDICATE_BOLT, (String) config.get("RightBatchPermutation")).setNumTasks(2);
        builder.setBolt(Constants.OFFSET_AND_IE_JOIN_BOLT_ID, new IEJoinBolt())
                .directGrouping(Constants.PERMUTATION_COMPUTATION_BOLT_ID, (String) config.get("LeftBatchPermutation"))
                 .directGrouping(Constants.PERMUTATION_COMPUTATION_BOLT_ID, (String) config.get("RightBatchPermutation"))
                .directGrouping(Constants.LEFT_PREDICATE_BOLT,(String) config.get("LeftBatchOffset"))
                .directGrouping(Constants.RIGHT_PREDICATE_BOLT, (String) config.get("RightBatchOffset")).
                directGrouping(Constants.LEFT_PREDICATE_BOLT,(String) config.get("MergingFlag") )
                .allGrouping("testSpout", "LeftStreamTuples").allGrouping("testSpout", "RightStream").setNumTasks(10);

        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("Storm", config, builder.createTopology());
    }

}
