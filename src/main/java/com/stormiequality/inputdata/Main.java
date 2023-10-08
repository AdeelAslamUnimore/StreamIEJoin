package com.stormiequality.inputdata;

import com.correctness.iejoin.SpoutCorrectness;
import com.stormiequality.benchmark.JoinBoltBTree;
import com.stormiequality.join.*;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

public class Main {
    public static void main(String[] args) throws Exception {

        Config config = new Config();

        config.setNumWorkers(9);
        TopologyBuilder builder = new TopologyBuilder();
//        Kryo kryo = new Kryo();
//        kryo.register(java.util.BitSet.class);
//        kryo.register(com.stormiequality.BTree.Node.class);
        config.registerSerialization(java.util.BitSet.class);
//        config.registerSerialization(long[].class);
//        config.registerSerialization(com.stormiequality.BTree.Node.class);
//        config.registerSerialization(com.stormiequality.BTree.Key.class);
       // config.setMessageTimeoutSecs(30);

        // config.registerSerialization(java.util.BitSet.class);
        //  config.setKryoFactory(KryoSerliazation.class);
        /////////
        // new Main().ProposedCombined(builder,config);


        new Main().Correctness(builder,config);


//        builder.setSpout("testSpout", new Spout());
//        builder.setBolt("testBolt", new SplitBolt())
//                .fieldsGrouping("testSpout", "LeftStreamTuples", new Fields("ID")).fieldsGrouping("testSpout", "RightStream", new Fields("ID"));
//        builder.setBolt("testLeftPredicateBolt", new JoinBoltForBPlusTree(8, 10000, "<", "Predicate1", "StreamForIEJoin1")).fieldsGrouping("testBolt", "LeftPredicate", new Fields("ID"));
//        builder.setBolt("testRightPredicateBolt", new JoinBoltForBPlusTree(8, 10000, ">", "Predicate2", "StreamForIEJoin2")).fieldsGrouping("testBolt", "RightPredicate", new Fields("ID"));
//        builder.setBolt("BPlusTreeEvaluation", new JoinerForBitSet()).fieldsGrouping("testLeftPredicateBolt", "Predicate1", new Fields("ID")).
//                fieldsGrouping("testRightPredicateBolt", "Predicate2", new Fields("ID")).setNumTasks(10);
//        builder.setBolt("IEJoin", new JoinBoltForIEJoin(), 6).customGrouping("testLeftPredicateBolt", "StreamForIEJoin1", new RoundRobinGrouping()).
//                customGrouping("testRightPredicateBolt", "StreamForIEJoin2", new RoundRobinGrouping()).allGrouping("testSpout", "LeftStreamTuples").allGrouping("testSpout", "RightStream").setNumTasks(11);
//       StormSubmitter.submitTopology("ieandbtreejoin", config, builder.createTopology());
//        LocalCluster cluster = new LocalCluster();
//        cluster.submitTopology("Storm", config, builder.createTopology());
    }




    public void BPlusTree(TopologyBuilder builder, Config config) throws Exception{
        builder.setSpout("testSpout", new Spout(10000));

        builder.setBolt("ForEvaluation", new SplitBolt())
                .fieldsGrouping("testSpout", "LeftStreamTuples", new Fields("ID")).fieldsGrouping("testSpout", "RightStream", new Fields("ID"));

        builder.setBolt("testBolt", new SplitBolt())
                .fieldsGrouping("testSpout", "LeftStreamTuples", new Fields("ID")).fieldsGrouping("testSpout", "RightStream", new Fields("ID"));
        builder.setBolt("testLeftPredicateBolt", new JoinBoltBTree(8,20000,"<","Predicate1")).customGrouping("testBolt", "LeftPredicate",new CustomizedRoundRobinForBPlusPartitioning()).allGrouping("ForEvaluation","LeftPredicate");//.setNumTasks(10);
        builder.setBolt("testRightPredicateBolt", new JoinBoltBTree(8, 20000, ">", "Predicate2")).customGrouping("testBolt", "RightPredicate", new CustomizedRoundRobinForBPlusPartitioning()).allGrouping("ForEvaluation","RightPredicate");//.setNumTasks(10);
//        builder.setBolt("BPlusTreeEvaluation", new JoinerForBitSet()).fieldsGrouping("testLeftPredicateBolt", "Predicate1", new Fields("ID")).
//                fieldsGrouping("testRightPredicateBolt", "Predicate2", new Fields("ID"));//.setNumTasks(10);
        StormSubmitter.submitTopology("ieandbtreejoin", config, builder.createTopology());
//        LocalCluster cluster = new LocalCluster();
//        cluster.submitTopology("Storm", config, builder.createTopology());
    }


    public void ProposedCombined(TopologyBuilder builder, Config config) throws Exception{
        builder.setSpout("testSpout", new Spout(50000));
        builder.setBolt("testBolt", new SplitBolt())
                .fieldsGrouping("testSpout", "LeftStreamTuples", new Fields("ID")).fieldsGrouping("testSpout", "RightStream", new Fields("ID"));
        builder.setBolt("testLeftPredicateBolt", new JoinBoltForBPlusTreeTest(8, 100000, "<", "Predicate1", "Predicate1T","PermutationLeft","PermutationLeftComputation","PermutationRightComputation","LeftStreamOffset","IEJoin")).fieldsGrouping("testBolt", "LeftPredicate", new Fields("ID"));
        builder.setBolt("testRightPredicateBolt", new JoinBoltForBPlusTreeTest(8, 100000, ">", "Predicate2", "Predicate2T","PermutationRight","PermutationLeftComputation","PermutationRightComputation","RightStreamOffset","IEJoin")).fieldsGrouping("testBolt", "RightPredicate", new Fields("ID"));
        builder.setBolt("BPlusTreeEvaluation", new JoinerForBitSet("Predicate1","Predicate2")).fieldsGrouping("testLeftPredicateBolt", "Predicate1", new Fields("ID")).
                fieldsGrouping("testRightPredicateBolt", "Predicate2", new Fields("ID")).setNumTasks(10);

//        builder.setBolt("Permutation",new IEJoinPermutationBolt("IEJoin", 100)).directGrouping("testLeftPredicateBolt","PermutationLeft").
//                directGrouping("testRightPredicateBolt","PermutationRight").setNumTasks(2);
//
        builder.setBolt("PermutationLeftComputation",new IEJoinPermutationBolt("LeftStreamID","IEJoin", 50000)).directGrouping("testLeftPredicateBolt","PermutationLeft").directGrouping("testRightPredicateBolt","PermutationRight");
        builder.setBolt("PermutationRightComputation",new IEJoinPermutationBolt("RightStreamID","IEJoin", 50000))
              .directGrouping("testLeftPredicateBolt","PermutationLeft").  directGrouping("testRightPredicateBolt","PermutationRight");
        builder.setBolt("IEJoin", new IEJoinComputationBolt(100000,1000000)).directGrouping("PermutationLeftComputation","LeftStreamID").
                directGrouping("PermutationRightComputation","RightStreamID").directGrouping("testLeftPredicateBolt","LeftStreamOffset")
             .directGrouping("testRightPredicateBolt","RightStreamOffset").allGrouping("testSpout", "LeftStreamTuples").allGrouping("testSpout", "RightStream").setNumTasks(10);
      //  StormSubmitter.submitTopology("ieandbtreejoin", config, builder.createTopology());

//        builder.setBolt("IEJoin", new IEJoinComputationBolt()).directGrouping("testLeftPredicateBolt","LeftStreamOffset")
//                .directGrouping("testRightPredicateBolt","RightStreamOffset").directGrouping("PermutationLeftComputation","LeftStream").directGrouping("PermutationRightComputation","RightStream").setNumTasks(10);


//        builder.setBolt("testLeftPredicateBolt", new JoinBoltForBPlusTree(8, 20000, "<", "Predicate1", "Predicate1T","StreamForIEJoin1")).fieldsGrouping("testBolt", "LeftPredicate", new Fields("ID"));
//        builder.setBolt("testRightPredicateBolt", new JoinBoltForBPlusTree(8, 20000, ">", "Predicate2", "Predicate2T","StreamForIEJoin2")).fieldsGrouping("testBolt", "RightPredicate", new Fields("ID"));
//        builder.setBolt("BPlusTreeEvaluation", new JoinerForBitSet("Predicate1","Predicate2")).fieldsGrouping("testLeftPredicateBolt", "Predicate1", new Fields("ID")).
//                fieldsGrouping("testRightPredicateBolt", "Predicate2", new Fields("ID")).setNumTasks(10);
//        builder.setBolt("BPlusTreeEvaluationT", new JoinerForBitSet("Predicate1T", "Predicate2T")).fieldsGrouping("testLeftPredicateBolt", "Predicate1T", new Fields("ID")).
//                fieldsGrouping("testRightPredicateBolt", "Predicate2T", new Fields("ID")).setNumTasks(10);
//        builder.setBolt("IEJoin", new JoinBoltForIEJoin(200, 20000)).customGrouping("testLeftPredicateBolt", "StreamForIEJoin1", new RoundRobinGrouping()).
//                customGrouping("testRightPredicateBolt", "StreamForIEJoin2", new RoundRobinGrouping());//.allGrouping("testSpout", "LeftStreamTuples").allGrouping("testSpout", "RightStream").setNumTasks(10);
     //  StormSubmitter.submitTopology("ieandbtreejoin", config, builder.createTopology());
//        builder.setBolt("IEJoin", new JoinBoltForIEJoin(200, 20000),6).directGrouping("testLeftPredicateBolt", "StreamForIEJoin1").
//                directGrouping("testRightPredicateBolt", "StreamForIEJoin2");//.allGrouping("testSpout", "LeftStreamTuples").allGrouping("testSpout", "RightStream").setNumTasks(10);
        //
                LocalCluster cluster = new LocalCluster();
                cluster.submitTopology("Storm", config, builder.createTopology());
//        builder.setBolt("BPlusTreeEvaluation", new JoinerForBitSet("Predicate1","Predicate2")).fieldsGrouping("testLeftPredicateBolt", "Predicate1", new Fields("ID")).
//                fieldsGrouping("testRightPredicateBolt", "Predicate2", new Fields("ID")).setNumTasks(10);
//        builder.setBolt("BPlusTreeEvaluationT", new JoinerForBitSet("Predicate1T", "Predicate2T")).fieldsGrouping("testLeftPredicateBolt", "Predicate1T", new Fields("ID")).
//                fieldsGrouping("testRightPredicateBolt", "Predicate2T", new Fields("ID")).setNumTasks(10);
    }

    public void Correctness(TopologyBuilder builder, Config config) throws Exception {
        builder.setSpout("testSpout", new SpoutCorrectness());
        builder.setBolt("testBolt", new SplitBolt())
                .fieldsGrouping("testSpout", "LeftStreamTuples", new Fields("ID")).fieldsGrouping("testSpout", "RightStream", new Fields("ID"));
        builder.setBolt("testLeftPredicateBolt", new JoinBoltForBPlusTreeTest(4, 10, "<", "Predicate1", "Predicate1T","PermutationLeft","PermutationLeftComputation","PermutationRightComputation","LeftStreamOffset","IEJoin")).fieldsGrouping("testBolt", "LeftPredicate", new Fields("ID"));
        builder.setBolt("testRightPredicateBolt", new JoinBoltForBPlusTreeTest(4, 10, ">", "Predicate2", "Predicate2T","PermutationRight","PermutationLeftComputation","PermutationRightComputation","RightStreamOffset","IEJoin")).fieldsGrouping("testBolt", "RightPredicate", new Fields("ID"));
        builder.setBolt("BPlusTreeEvaluation", new JoinerForBitSet("Predicate1","Predicate2")).fieldsGrouping("testLeftPredicateBolt", "Predicate1", new Fields("ID")).
              fieldsGrouping("testRightPredicateBolt", "Predicate2", new Fields("ID")).setNumTasks(10);

        LocalCluster cluster = new LocalCluster();
         cluster.submitTopology("Storm", config, builder.createTopology());
    }

}
       // builder.setBolt("Search", new ) /home/adeel/Data/StormUpdated/apache-storm-2.4.0/lib/


