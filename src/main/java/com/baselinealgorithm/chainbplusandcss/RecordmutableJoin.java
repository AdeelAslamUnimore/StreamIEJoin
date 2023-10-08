package com.baselinealgorithm.chainbplusandcss;

import com.configurationsandconstants.iejoinandbaseworks.Configuration;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.util.Map;

public class RecordmutableJoin extends BaseRichBolt {
    private BufferedWriter leftBufferedWriter;
    private BufferedWriter rightBufferedWriter;
    private int leftCounter;
    private int rightCounter;
    private String leftStreamID;
    private String rightStreamID;
    private StringBuilder leftRecordStringBuilder;
    private StringBuilder rightRecordStringBuilder;



    @Override
    public void prepare(Map<String, Object> map, TopologyContext topologyContext, OutputCollector outputCollector) {
        try{
            this.leftCounter=0;
            this.rightCounter=0;
            leftRecordStringBuilder= new StringBuilder();
            rightRecordStringBuilder= new StringBuilder();
            this.leftBufferedWriter= new BufferedWriter(new FileWriter(new File("/home/adeel/Data/Results//LeftStream.csv")));
            this.rightBufferedWriter= new BufferedWriter(new FileWriter(new File("/home/adeel/Data/Results//RightStream.csv")));
            this.leftBufferedWriter.write("TUPLE_ID,KAFKA_TIME,KAFKA_SPOUT_TIME, SPLIT_BOLT_TIME, TASK_ID_FOR_SPLIT_BOLT, HOST_NAME_FOR_SPLIT_BOLT,TupleArrivalTime,TupleEvaluationTime,TaskID,HostName,EvaluationStartTime,EvaluationEndTime,TaskID,HostName\n");
           this.rightBufferedWriter.write("TUPLE_ID,KAFKA_TIME,KAFKA_SPOUT_TIME, SPLIT_BOLT_TIME, TASK_ID_FOR_SPLIT_BOLT, HOST_NAME_FOR_SPLIT_BOLT,TupleArrivalTime,TupleEvaluationTime,TaskID,HostName,EvaluationStartTime,EvaluationEndTime,TaskID,HostName\n");

        }catch (Exception e){
            e.printStackTrace();
        }
    }

    @Override
    public void execute(Tuple tuple) {
        if(tuple.getSourceStreamId().equals("LeftMutableResults")){
            this.leftCounter++;
            this.leftRecordStringBuilder.append(tuple.getValue(0)+","+tuple.getValue(1)+","+tuple.getValue(2)+","+tuple.getValue(3)+","+
                    tuple.getValue(4)+","+tuple.getValue(5)+","+tuple.getValue(6)+","+tuple.getValue(7)+","+tuple.getValue(8)+","+tuple.getValue(9)+","+
                    tuple.getValue(10)+","+tuple.getValue(11)+","+tuple.getValue(12)+","+tuple.getValue(13)+"\n");
        }
        if(tuple.getSourceStreamId().equals("RightMutableResults")){
            this.rightCounter++;
            this.rightRecordStringBuilder.append(tuple.getValue(0)+","+tuple.getValue(1)+","+tuple.getValue(2)+","+tuple.getValue(3)+","+
                    tuple.getValue(4)+","+tuple.getValue(5)+","+tuple.getValue(6)+","+tuple.getValue(7)+","+tuple.getValue(8)+","+tuple.getValue(9)+","+
                    tuple.getValue(10)+","+tuple.getValue(11)+","+tuple.getValue(12)+","+tuple.getValue(13)+"\n");
        }
        if (rightCounter == 1000) {
            rightCounter=0;
            try{

                this.rightBufferedWriter.write(rightRecordStringBuilder.toString());
                this.rightBufferedWriter.flush();
                this.rightRecordStringBuilder= new StringBuilder();
            }catch (Exception e){

            }
        }
        if(leftCounter==1000){
            leftCounter=0;
            try{

                this.leftBufferedWriter.write(leftRecordStringBuilder.toString());
                this.leftBufferedWriter.flush();
                this.leftRecordStringBuilder= new StringBuilder();
            }catch (Exception e){

            }
        }

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }
}
