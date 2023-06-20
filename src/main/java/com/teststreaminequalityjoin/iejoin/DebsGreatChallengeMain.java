package com.teststreaminequalityjoin.iejoin;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

public class DebsGreatChallengeMain {
    public static final int TOP_N = 10;
    public static int PROFIT_WINDOW = 15 * 60; // in seconds
    public static int EMPTY_TAXIS_WINDOW = 30 * 60; // in seconds
    public static String INPUT_FILE = "/home/adeelaslam/Input/sorted_data.csv";
    public static String OUTPUT_FILE = "/home/adeelaslam/OutputResultForScheduler/rankings.output";

    public static void main(String[] args) throws Exception {
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("data", new DataGenerator(INPUT_FILE), 1);
        builder.setBolt("profit", new ProfitBolt(PROFIT_WINDOW), 10)
                .fieldsGrouping(
                        "data",
                        DataGenerator.PROFIT_STREAM_ID,
                        new Fields("pickupCell")
                );

        builder.setBolt("empty_taxis", new EmptyTaxisBolt(EMPTY_TAXIS_WINDOW), 10)
                .fieldsGrouping(
                        "data",
                        DataGenerator.EMPTY_TAXIS_STREAM_ID,
                        new Fields("taxiID")
                );


        builder.setBolt("empty_taxis_counter", new EmptyTaxisCounterBolt(), 10)
                .fieldsGrouping("empty_taxis", new Fields("cell"));


        // joiner
        builder.setBolt("profitability", new ProfitablityBolt(), 10)
                .fieldsGrouping("profit",
                        ProfitBolt.OUT_STREAM_ID,
                        new Fields("windowID")
                )
                .fieldsGrouping(
                        "empty_taxis_counter",
                        EmptyTaxisCounterBolt.OUT_STREAM_ID,
                        new Fields("windowID")
                );

        builder.setBolt("rankings", new RankingBolt(TOP_N)).globalGrouping("profitability");

        //builder.setBolt("to_file", new DataWriter(OUTPUT_FILE)).globalGrouping("rankings");

        Config conf = new Config();
   //     conf.setTopologyStrategy(org.apache.storm.scheduler.resource.strategies.scheduling.DefaultResourceAwareStrategy.class);
           conf.setNumWorkers(20);
            StormSubmitter.submitTopology("DEBS-Challenge", conf, builder.createTopology());


    }
    }
