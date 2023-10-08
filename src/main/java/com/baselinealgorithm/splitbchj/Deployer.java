package com.baselinealgorithm.splitbchj;

import com.baselinealgorithm.chainindexbplustree.JoinerBoltBplusTree;
import com.baselinealgorithm.chainindexbplustree.LeftStreamPredicateBplusTree;
import com.baselinealgorithm.chainindexbplustree.RecordChainIndex;
import com.baselinealgorithm.chainindexbplustree.RightStreamPredicateBplusTree;
import com.configurationsandconstants.iejoinandbaseworks.Configuration;
import com.configurationsandconstants.iejoinandbaseworks.Constants;
import com.stormiequality.inputdata.RackPowerSpout;
import com.stormiequality.inputdata.SplitBolt;
import com.stormiequality.inputdata.Spout;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class Deployer {

    public static void main(String[] args) throws Exception {
        Config config= new Config();
        Map<String, Object> map= Configuration.configurationConstantForStreamIDs();
        String leftStreamSmaller = (String) map.get("LeftSmallerPredicateTuple");
        String rightStreamSmaller = (String) map.get("RightSmallerPredicateTuple");
        String leftStreamGreater = (String) map.get("LeftGreaterPredicateTuple");
        String rightStreamGreater = (String) map.get("RightGreaterPredicateTuple");
        config.setNumWorkers(8);
        config.registerSerialization(java.util.BitSet.class);
        config.registerSerialization(java.util.HashSet.class);
        config.registerSerialization(java.util.concurrent.atomic.AtomicLong.class);
        List<String> workerChildopts = new ArrayList<>();
        workerChildopts.add("-Xmx4g");
        workerChildopts.add("-Xss8m");
        config.put(Config.TOPOLOGY_WORKER_CHILDOPTS, workerChildopts);
        config.setNumAckers(4);

        TopologyBuilder builder= new TopologyBuilder();
       builder.setSpout("streamspout", new Spout(1000));
      //  builder.setSpout("streamspout", new RackPowerSpout());
        builder.setBolt("splitbolt", new SplitBolt(),1).fieldsGrouping("streamspout", "StreamR", new Fields("ID")).fieldsGrouping("streamspout", "StreamS", new Fields("ID")).setNumTasks(4);
        builder.setBolt("LeftStream", new LeftPredicateEvaluation(leftStreamSmaller,rightStreamSmaller,
                "LeftForSearchSmaller","rightForSearchSmaller"), 2).shuffleGrouping("splitbolt",leftStreamSmaller).
                allGrouping("splitbolt","LeftForSearchSmaller").shuffleGrouping("splitbolt",rightStreamSmaller).
                allGrouping("splitbolt", "rightForSearchSmaller").setNumTasks(5);
        builder.setBolt("RightStream", new RightPredicateEvaluation(leftStreamGreater,rightStreamGreater,
                "LeftForSearchGreater","rightForSearchGreater"), 2).shuffleGrouping("splitbolt",leftStreamGreater).
                allGrouping("splitbolt","LeftForSearchGreater").shuffleGrouping("splitbolt",rightStreamGreater).
                allGrouping("splitbolt", "rightForSearchGreater").setNumTasks(5);
        builder.setBolt("joiner", new Joiner(), 2 ).fieldsGrouping("LeftStream","StreamR", new Fields("ID")).
                fieldsGrouping("LeftStream","StreamR", new Fields("ID")).fieldsGrouping("LeftStream", "StreamS", new Fields("ID")).
                fieldsGrouping("RightStream", "StreamR", new Fields("ID")). fieldsGrouping("LeftStream", "StreamS", new Fields("ID"))
                .setNumTasks(4);
        builder.setBolt("recordsWriting", new RecordBolt()).shuffleGrouping("joiner","Record");
      StormSubmitter.submitTopology("CrossJoin", config, builder.createTopology());
////
//        LocalCluster cluster = new LocalCluster();
//        cluster.submitTopology("Storm", config, builder.createTopology());
    }

    public void splitJoin() throws Exception {
        Config config= new Config();
        Map<String, Object> map= Configuration.configurationConstantForStreamIDs();
        String leftStreamSmaller = (String) map.get("LeftSmallerPredicateTuple");
        String rightStreamSmaller = (String) map.get("RightSmallerPredicateTuple");
        String leftStreamGreater = (String) map.get("LeftGreaterPredicateTuple");
        String rightStreamGreater = (String) map.get("RightGreaterPredicateTuple");
        config.setNumWorkers(8);
        config.registerSerialization(java.util.BitSet.class);
        config.registerSerialization(java.util.HashSet.class);
        config.registerSerialization(java.util.concurrent.atomic.AtomicLong.class);
        List<String> workerChildopts = new ArrayList<>();
        workerChildopts.add("-Xmx2g");
        workerChildopts.add("-Xss8m");
        config.put(Config.TOPOLOGY_WORKER_CHILDOPTS, workerChildopts);
        config.setNumAckers(4);;
        TopologyBuilder builder= new TopologyBuilder();
        // builder.setSpout("streamspout", new Spout(1000));
        builder.setSpout("streamspout", new RackPowerSpout());
        builder.setBolt("splitbolt", new SplitBolt(),1).fieldsGrouping("streamspout", "StreamR", new Fields("ID")).fieldsGrouping("streamspout", "StreamS", new Fields("ID")).setNumTasks(4);
        builder.setBolt("LeftStream", new LeftPredicateEvaluation(leftStreamSmaller,rightStreamSmaller,
                "LeftForSearchSmaller","rightForSearchSmaller")).shuffleGrouping("splitbolt",leftStreamSmaller).
                allGrouping("splitbolt","LeftForSearchSmaller").shuffleGrouping("splitbolt",rightStreamSmaller).
                allGrouping("splitbolt", "rightForSearchSmaller");
        builder.setBolt("RightStream", new RightPredicateEvaluation(leftStreamGreater,rightStreamGreater,
                "LeftForSearchGreater","rightForSearchGreater")).shuffleGrouping("splitbolt",leftStreamGreater).
                allGrouping("splitbolt","LeftForSearchGreater").shuffleGrouping("splitbolt",rightStreamGreater).
                allGrouping("splitbolt", "rightForSearchGreater");
        builder.setBolt("joiner", new Joiner() ).fieldsGrouping("LeftStream","StreamR", new Fields("ID")).
                fieldsGrouping("LeftStream","StreamR", new Fields("ID")).fieldsGrouping("LeftStream", "StreamS", new Fields("ID")).
                fieldsGrouping("RightStream", "StreamR", new Fields("ID")). fieldsGrouping("LeftStream", "StreamS", new Fields("ID"))
                .setNumTasks(1);
        builder.setBolt("recordsWriting", new RecordBolt()).shuffleGrouping("joiner","Record");
        StormSubmitter.submitTopology("CrossJoin", config, builder.createTopology());
    }


}
