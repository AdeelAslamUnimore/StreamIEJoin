package com.proposed.iejoinandbplustreebased;

import com.stormiequality.test.SplitBolt;
import com.stormiequality.test.Spout;
import org.apache.storm.Config;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

public class ProposedModelTopology {
    public static void main(String[] args) {
        Config config= Configuration.configurationConstantForStreamIDs();
        config.setNumWorkers(6);
        config.registerSerialization(java.util.BitSet.class);
        config.registerSerialization(java.util.HashSet.class);
        TopologyBuilder builder= new TopologyBuilder();
        builder.setSpout("testSpout", new Spout(500));
        builder.setBolt("testBolt", new SplitBolt())
                .fieldsGrouping("testSpout", "LeftStreamTuples", new Fields("ID")).fieldsGrouping("testSpout", "RightStream", new Fields("ID"));

    }

}
