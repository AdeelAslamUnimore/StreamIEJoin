package com.experiment.selfjoin.bplustree;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Map;

public class RecordBolt extends BaseRichBolt{
        private int counterRecord;
        private BufferedWriter bufferedWriter;
        private StringBuilder stringBuilder;
    @Override
    public void prepare(Map<String, Object> map, TopologyContext topologyContext, OutputCollector outputCollector) {
        try{
            this.counterRecord=0;
            this.bufferedWriter= new BufferedWriter(new FileWriter(new File("/home/adeel/Data/Results/Joiner.csv")));
            this.bufferedWriter.write("LeftStreamID, beforeTime,AfterTime,taskID,HostName,\n");
            this.bufferedWriter.flush();
            this.stringBuilder= new StringBuilder();

        }catch (Exception e){
            e.printStackTrace();
        }
    }

    @Override
    public void execute(Tuple tuple) {
        counterRecord++;
        stringBuilder.append(tuple.getValue(0)+","+tuple.getValue(1)+","+tuple.getValue(2)+","+tuple.getValue(3)+","+tuple.getValue(4)+"\n");
        if(counterRecord==1000){
            try {
                this.bufferedWriter.write(stringBuilder.toString());
                this.bufferedWriter.flush();
                this.stringBuilder= new StringBuilder();
                counterRecord=0;
            } catch (IOException e) {
                e.printStackTrace();
            }

        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }
}
