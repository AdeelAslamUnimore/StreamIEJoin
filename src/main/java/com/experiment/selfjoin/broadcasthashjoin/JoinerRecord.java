package com.experiment.selfjoin.broadcasthashjoin;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.util.Map;

public class JoinerRecord extends BaseRichBolt {
    private BufferedWriter bufferedWriter;
    @Override
    public void prepare(Map<String, Object> map, TopologyContext topologyContext, OutputCollector outputCollector) {
        try{
            this.bufferedWriter= new BufferedWriter(new FileWriter(new File("/home/adeel/Data/Results/JoinRecord.csv")));
            this.bufferedWriter.write("ID,StartTime,EndTime,TaskID,HostName \n");
            this.bufferedWriter.flush();
        }catch (Exception e){

        }
    }

    @Override
    public void execute(Tuple tuple) {
        try{
            this.bufferedWriter.write(tuple.getValue(0)+","+tuple.getValue(1)+","+tuple.getValue(2)+","+tuple.getValue(3)+","+tuple.getValue(4)+"\n");
            this.bufferedWriter.flush();
        }catch (Exception e){

        }

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }
}
