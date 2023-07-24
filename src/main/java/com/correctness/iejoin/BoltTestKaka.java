package com.correctness.iejoin;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.util.Map;

public class BoltTestKaka extends BaseRichBolt {
    private BufferedWriter writer;
    @Override
    public void prepare(Map<String, Object> map, TopologyContext topologyContext, OutputCollector outputCollector) {
        try{
            writer= new BufferedWriter(new FileWriter(new File("/home/adeel/Data/Results/result.csv")));
        }catch (Exception e){

        }
    }

    @Override
    public void execute(Tuple tuple) {
        try{
            this.writer.write(tuple+"\n");
            this.writer.flush();
        }catch (Exception e){

        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }
}
