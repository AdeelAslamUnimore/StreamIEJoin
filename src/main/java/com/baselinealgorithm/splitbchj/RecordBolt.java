package com.baselinealgorithm.splitbchj;

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

public class RecordBolt extends BaseRichBolt {
    private int counter;
    private StringBuilder stringBuilder;
    private BufferedWriter bufferedWriter;
    @Override
    public void prepare(Map<String, Object> map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.counter=0;
        this.stringBuilder= new StringBuilder();
        try{
            this.bufferedWriter= new BufferedWriter(new FileWriter(new File("/home/adeel/Data/Results/Record.csv")));
            this.bufferedWriter.write("ID,KafkaTime,KafkaSpoutTime,tupleArrivalTime,TupleEvaluationTime,taskID,hostName,JoinerTime,JoinerEvaluationTime,TaskID,HostName \n");
            this.bufferedWriter.flush();
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    @Override
    public void execute(Tuple tuple) {
        //System.out.println("I am here");
        this.counter++;
        this.stringBuilder.append(tuple.getValue(0)+","+tuple.getValue(1)+","+tuple.getValue(2)+","+tuple.getValue(3)+","+
                tuple.getValue(4)+","+tuple.getValue(5)+","+ tuple.getValue(6)+","+tuple.getValue(7)+","+tuple.getValue(8)+","+
                tuple.getValue(9)+","+tuple.getValue(10)+"\n");
        if(counter==1000){
            try {
                this.bufferedWriter.write(stringBuilder.toString());
                this.stringBuilder= new StringBuilder();
                this.counter=0;
            } catch (IOException e) {
                e.printStackTrace();
            }

        }

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }
}
