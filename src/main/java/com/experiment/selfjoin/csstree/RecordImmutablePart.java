package com.experiment.selfjoin.csstree;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.util.Map;

public class RecordImmutablePart extends BaseRichBolt {
    private BufferedWriter bufferedWriterTimeMerge;
    private BufferedWriter bufferedWriterQueueMerge;
    private String leftStreamTime;
    private String rightStreamTime;
    private String leftStreamQueue;
    private String rightStreamQueue;
      @Override
    public void prepare(Map<String, Object> map, TopologyContext topologyContext, OutputCollector outputCollector) {
       try{
           this.bufferedWriterTimeMerge= new BufferedWriter(new FileWriter(new File("/home/adeel/Data/Results/MergeTime.csv")));
           this.bufferedWriterTimeMerge.write("L.StartTime,L.EndTime,L.Machine,L.TaskID,L.StreamID, R.StartTime,R.EndTime,R.Machine,R.TaskID,R.StreamID, \n");

           this.bufferedWriterQueueMerge= new BufferedWriter(new FileWriter(new File("/home/adeel/Data/Results/QueueMerge.csv")));
           this.bufferedWriterQueueMerge.write("L.StartTime,L.EndTime,L.Machine,L.TaskID,L.StreamID, R.StartTime,R.EndTime,R.Machine,R.TaskID,R.StreamID, \n");
       }catch (Exception e){
           e.printStackTrace();
       }
    }

    @Override
    public void execute(Tuple tuple) {
        try{
            if(tuple.getSourceStreamId().equals("MergingTime")) {
                if (tuple.getStringByField("StreamID").equals("LeftStream")) {
                    leftStreamTime = tuple.getValue(0) + "," + tuple.getValue(1) + "," + tuple.getValue(2) + "," + tuple.getValue(3)  + "," + tuple.getValue(4);
                }
                if (tuple.getStringByField("StreamID").equals("RightStream")) {
                    rightStreamTime = tuple.getValue(0) + "," + tuple.getValue(1) + "," + tuple.getValue(2) + "," + tuple.getValue(3) + "," + tuple.getValue(4);
                }
                if (leftStreamTime != null && rightStreamTime!= null) {
                    this.bufferedWriterTimeMerge.write(leftStreamTime + "," + rightStreamTime + "\n");
                    this.bufferedWriterTimeMerge.flush();
                    leftStreamTime=null;
                    rightStreamTime=null;
                }
            }
        }catch (Exception e){
            e.printStackTrace();
        }

        try{
            if(tuple.getSourceStreamId().equals("QueueMergeTime")) {

                if (tuple.getStringByField("StreamID").equals("LeftStream")) {
                    leftStreamQueue = tuple.getValue(0) + "," + tuple.getValue(1) + "," + tuple.getValue(2) + "," + tuple.getValue(3)  + "," + tuple.getValue(4);
                }
                if (tuple.getStringByField("StreamID").equals("RightStream")) {
                    rightStreamQueue= tuple.getValue(0) + "," + tuple.getValue(1) + "," + tuple.getValue(2) + "," + tuple.getValue(3)  + "," + tuple.getValue(4);
                }
                if (leftStreamQueue != null && rightStreamQueue != null) {
                    this.bufferedWriterQueueMerge.write(leftStreamQueue + "," + rightStreamQueue + "\n");
                    this.bufferedWriterQueueMerge.flush();
                    leftStreamQueue=null;
                    rightStreamQueue=null;
                }
            }
        }catch (Exception e){
            e.printStackTrace();
        }


    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }
}
