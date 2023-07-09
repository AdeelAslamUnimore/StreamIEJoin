package com.baselinealgorithm.chainbplusandcss;

import com.configurationsandconstants.iejoinandbaseworks.Configuration;
import com.configurationsandconstants.iejoinandbaseworks.Constants;
import com.stormiequality.test.SplitBolt;
import com.stormiequality.test.Spout;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

import java.util.Map;

public class CSSBasedTopology {
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
        builder.setBolt(Constants.LEFT_PREDICATE_CSS_TREE_BOLT, new LeftPredicateCSSTreeBolt()).fieldsGrouping("testBolt", (String) map.get("LeftSmallerPredicateTuple"),new Fields(Constants.TUPLE_ID)).
                fieldsGrouping("testBolt", (String) map.get("RightSmallerPredicateTuple"),new Fields(Constants.TUPLE_ID));
        builder.setBolt(Constants.RIGHT_PREDICATE_CSS_TREE_BOLT, new RightPredicateCSSTreeBolt()).fieldsGrouping("testBolt", (String) map.get("LeftGreaterPredicateTuple"),new Fields(Constants.TUPLE_ID)).
                fieldsGrouping("testBolt", (String) map.get("RightGreaterPredicateTuple"),new Fields(Constants.TUPLE_ID));
        builder.setBolt(Constants.MUTABLE_PART_EVALUATION_BOLT,new JoinerCSSTreeBolt((String)map.get("LeftPredicateSourceStreamIDHashSet"), (String)map.get("RightPredicateSourceStreamIDHashSet"))).fieldsGrouping(Constants.LEFT_PREDICATE_CSS_TREE_BOLT,(String)map.get("LeftPredicateSourceStreamIDHashSet"), new Fields(Constants.TUPLE_ID)).
                fieldsGrouping(Constants.RIGHT_PREDICATE_CSS_TREE_BOLT,(String)map.get("RightPredicateSourceStreamIDHashSet"), new Fields(Constants.TUPLE_ID));

        builder.setBolt(Constants.LEFT_PREDICATE_IMMUTABLE_CSS,new LeftPredicateImmutableCSSBolt()).shuffleGrouping(Constants.LEFT_PREDICATE_CSS_TREE_BOLT, (String) map.get("LeftSmallerPredicateTuple")).
                shuffleGrouping(Constants.LEFT_PREDICATE_CSS_TREE_BOLT, (String) map.get("RightSmallerPredicateTuple")).
                shuffleGrouping(Constants.LEFT_PREDICATE_CSS_TREE_BOLT,"LeftCheckForMerge").
                shuffleGrouping(Constants.LEFT_PREDICATE_CSS_TREE_BOLT,"RightCheckForMerge").shuffleGrouping("testSpout","LeftStreamTuples" ).shuffleGrouping("testSpout","RightStream");

        builder.setBolt(Constants.RIGHT_PREDICATE_IMMUTABLE_CSS,new RightPredicateImmutableCSSBolt()).shuffleGrouping(Constants.RIGHT_PREDICATE_CSS_TREE_BOLT, (String) map.get("LeftGreaterPredicateTuple")).
                shuffleGrouping(Constants.RIGHT_PREDICATE_CSS_TREE_BOLT, (String) map.get("RightGreaterPredicateTuple")).
                shuffleGrouping(Constants.RIGHT_PREDICATE_CSS_TREE_BOLT,"LeftCheckForMerge").
                shuffleGrouping(Constants.RIGHT_PREDICATE_CSS_TREE_BOLT,"RightCheckForMerge").shuffleGrouping("testSpout","LeftStreamTuples" ).shuffleGrouping("testSpout","RightStream");

        builder.setBolt(Constants.IMMUTABLE_HASH_SET_EVALUATION,new JoinerCSSTreeBolt("LeftPredicate", "RightPredicate") ).
                fieldsGrouping(Constants.LEFT_PREDICATE_IMMUTABLE_CSS,"LeftPredicate", new Fields(Constants.TUPLE_ID)).
                fieldsGrouping(Constants.RIGHT_PREDICATE_IMMUTABLE_CSS,"RightPredicate", new Fields(Constants.TUPLE_ID));

        builder.setBolt(Constants.MERGE_BOLT_EVALUATION_CSS,new JoinerCSSTreeBolt("LeftMergeBitSet", "RightMergeBitSet") ).
                fieldsGrouping(Constants.LEFT_PREDICATE_IMMUTABLE_CSS,"LeftMergeBitSet", new Fields(Constants.TUPLE_ID)).
                fieldsGrouping(Constants.RIGHT_PREDICATE_IMMUTABLE_CSS,"RightMergeBitSet", new Fields(Constants.TUPLE_ID));

        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("Storm", config, builder.createTopology());
    }
}
