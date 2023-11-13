package com.proposed.iejoinandbplustreebased;

import com.configurationsandconstants.iejoinandbaseworks.Configuration;
import com.configurationsandconstants.iejoinandbaseworks.Constants;
import com.stormiequality.inputdata.CoolingInfrastructureSpout;
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

public class ProposedModelTopology {
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
        config.setNumAckers(4);
        TopologyBuilder builder= new TopologyBuilder();
      builder.setSpout("streamspout", new Spout(1000));
     //  builder.setSpout("streamspout", new RackPowerSpout());
      // builder.setSpout("rightStreamSpout", new CoolingInfrastructureSpout());

        builder.setBolt("SplitBolt", new SplitBolt(), 5)
                .fieldsGrouping("streamspout", "StreamR", new Fields("ID")).fieldsGrouping("streamspout", "StreamS", new Fields("ID")).setNumTasks(10);
        builder.setBolt(Constants.LEFT_PREDICATE_BOLT, new MutableBPlusTreeBolt("<",(String)map.get("LeftBatchPermutation"),(String)map.get("LeftBatchOffset")))
                .fieldsGrouping("SplitBolt", (String) map.get("LeftSmallerPredicateTuple"), new Fields(Constants.TUPLE_ID)).fieldsGrouping("SplitBolt", (String) map.get("RightSmallerPredicateTuple"), new Fields(Constants.TUPLE_ID));
        builder.setBolt(Constants.RIGHT_PREDICATE_BOLT, new MutableBPlusTreeBolt(">",(String)map.get("RightBatchPermutation"),(String)map.get("RightBatchOffset")))
                .fieldsGrouping("SplitBolt", (String) map.get("LeftGreaterPredicateTuple"), new Fields(Constants.TUPLE_ID)).fieldsGrouping("SplitBolt", (String) map.get("RightGreaterPredicateTuple"), new Fields(Constants.TUPLE_ID));
        builder.setBolt(Constants.BIT_SET_EVALUATION_BOLT, new JoinerBoltForBitSetOperation(),5).fieldsGrouping(Constants.LEFT_PREDICATE_BOLT, (String)map.get("LeftPredicateSourceStreamIDBitSet"),new Fields(Constants.TUPLE_ID))
                .fieldsGrouping(Constants.LEFT_PREDICATE_BOLT, (String)map.get("RightPredicateSourceStreamIDBitSet"),new Fields(Constants.TUPLE_ID)).fieldsGrouping(Constants.RIGHT_PREDICATE_BOLT, (String)map.get("LeftPredicateSourceStreamIDBitSet"),new Fields(Constants.TUPLE_ID)).
                fieldsGrouping(Constants.RIGHT_PREDICATE_BOLT,(String)map.get("RightPredicateSourceStreamIDBitSet"),new Fields(Constants.TUPLE_ID)).setNumTasks(40);

//                builder.setBolt(Constants.BIT_SET_EVALUATION_BOLT, new JoinerBoltForBitSetOperationWithoutTupleResilence(),5).fieldsGrouping(Constants.LEFT_PREDICATE_BOLT, (String)map.get("LeftPredicateSourceStreamIDBitSet"),new Fields(Constants.TUPLE_ID))
//                .fieldsGrouping(Constants.LEFT_PREDICATE_BOLT, (String)map.get("RightPredicateSourceStreamIDBitSet"),new Fields(Constants.TUPLE_ID)).fieldsGrouping(Constants.RIGHT_PREDICATE_BOLT, (String)map.get("LeftPredicateSourceStreamIDBitSet"),new Fields(Constants.TUPLE_ID)).
//                fieldsGrouping(Constants.RIGHT_PREDICATE_BOLT,(String)map.get("RightPredicateSourceStreamIDBitSet"),new Fields(Constants.TUPLE_ID)).setNumTasks(10);


        builder.setBolt("RecordBitSetJoin", new RecordBitSetJoin()).shuffleGrouping(Constants.BIT_SET_EVALUATION_BOLT,(String) map.get("LeftPredicateSourceStreamIDBitSet")).shuffleGrouping(Constants.BIT_SET_EVALUATION_BOLT,(String) map.get("RightPredicateSourceStreamIDBitSet"));

        builder.setBolt(Constants.PERMUTATION_COMPUTATION_BOLT_ID, new PermutationBolt(),1).directGrouping(Constants.LEFT_PREDICATE_BOLT, (String) map.get("LeftBatchPermutation")).  directGrouping(Constants.RIGHT_PREDICATE_BOLT, (String) map.get("RightBatchPermutation")).setNumTasks(2);
        builder.setBolt(Constants.OFFSET_AND_IE_JOIN_BOLT_ID, new IEJoinWithLinkedListUpdated("Duration","Revenue","Time","Cost"),2)
                .directGrouping(Constants.PERMUTATION_COMPUTATION_BOLT_ID, (String) map.get("LeftBatchPermutation"))
                 .directGrouping(Constants.PERMUTATION_COMPUTATION_BOLT_ID, (String) map.get("RightBatchPermutation"))
                .directGrouping(Constants.LEFT_PREDICATE_BOLT,(String) map.get("LeftBatchOffset"))
                .directGrouping(Constants.RIGHT_PREDICATE_BOLT, (String) map.get("RightBatchOffset")).
                directGrouping(Constants.LEFT_PREDICATE_BOLT,(String) map.get("MergingFlag") )
                .directGrouping(Constants.RIGHT_PREDICATE_BOLT,(String) map.get("MergingFlag") ).directGrouping(Constants.PERMUTATION_COMPUTATION_BOLT_ID,"WindowCount")
                .allGrouping("streamspout", "StreamR").allGrouping("streamspout", "StreamS").setNumTasks(10);

   //  builder.setBolt("RecordIEJoin", new RecordsIEJoin()).shuffleGrouping(Constants.OFFSET_AND_IE_JOIN_BOLT_ID,(String) map.get("IEJoinResult")).shuffleGrouping(Constants.OFFSET_AND_IE_JOIN_BOLT_ID,(String) map.get("MergingTupleEvaluation"));
      StormSubmitter.submitTopology("CrossJoin", config, builder.createTopology());
//       LocalCluster cluster = new LocalCluster();
//        cluster.submitTopology("Storm", config, builder.createTopology());
    }

}
