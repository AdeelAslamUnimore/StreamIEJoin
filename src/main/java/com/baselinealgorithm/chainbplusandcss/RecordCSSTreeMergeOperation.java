package com.baselinealgorithm.chainbplusandcss;

import com.configurationsandconstants.iejoinandbaseworks.Configuration;
import com.configurationsandconstants.iejoinandbaseworks.Constants;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.io.*;
import java.net.InetAddress;
import java.util.BitSet;
import java.util.Map;

public class RecordCSSTreeMergeOperation extends BaseRichBolt {
    private BitSet leftHashSet;
    private BitSet rightHashSet;
    private String leftStreamID;
    private String rightStreamID;
    private OutputCollector outputCollector;
    private String result;
    private int taskID;
    private String hostName;
    private BufferedWriter leftMergeWriter;
    private BufferedWriter rightMergeWriter;

    public RecordCSSTreeMergeOperation(String leftStreamID, String rightStreamID){
        Map<String, Object> map = Configuration.configurationConstantForStreamIDs();
        this.leftStreamID=leftStreamID;
        this.rightStreamID=rightStreamID;
        this.result= (String) map.get("Results");
    }

    @Override
    public void prepare(Map<String, Object> map, TopologyContext topologyContext, OutputCollector outputCollector) {
        taskID= topologyContext.getThisTaskId();
        this.outputCollector=outputCollector;
        try{
            hostName= InetAddress.getLocalHost().getHostName();
            leftMergeWriter= new BufferedWriter(new FileWriter(new File("/home/adeel/Data/Results//leftMergeWriter.csv")));
            rightMergeWriter= new BufferedWriter(new FileWriter(new File("/home/adeel/Data/Results//rightMergeWriter.csv")));
           leftMergeWriter.write( "LeftMergeTimeS, completeTime,System.currentTimeMillis(), taskID, hostName , mergeTaskID, mergeHostName\n");
            rightMergeWriter.write( "RightMergeTimeS, completeTime,System.currentTimeMillis(), taskID, hostName, mergeTaskId, mergeHostName \n");
            leftMergeWriter.flush();
            rightMergeWriter.flush();
        }catch (Exception e){

        }
    }

    @Override
    public void execute(Tuple tuple) {

        if(tuple.getSourceStreamId().equals(leftStreamID)) {

           leftHashSet = convertToObject(tuple.getBinaryByField(Constants.LEFT_HASH_SET));
            //System.out.println("LeftHashSet"+leftHashSet);
            try {
                if (leftMergeWriter != null) {
                    this.leftMergeWriter.write(tuple.getValue(2) + "," + tuple.getValue(3) + "," + tuple.getValue(4) + "," + tuple.getValue(5) + "," + tuple.getValue(6) + "," + taskID + "," + hostName + "\n");
                    leftMergeWriter.flush();
                }
                }catch(Exception e){
                    e.printStackTrace();
                }

        }
        if(tuple.getSourceStreamId().equals(rightStreamID)){
            rightHashSet=convertToObject(tuple.getBinaryByField(Constants.RIGHT_HASH_SET));
            //
            try{
                if(rightMergeWriter!=null) {
                    this.rightMergeWriter.write(tuple.getValue(2) + "," + tuple.getValue(3) + "," + tuple.getValue(4) + "," + tuple.getValue(5) + "," + tuple.getValue(6) + "," + taskID + "," + hostName + "\n");
                    rightMergeWriter.flush();
                }
            }catch (Exception e){
                e.printStackTrace();
            }

        }


    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {


    }

    private BitSet convertToObject(byte[] byteData) {
        try (ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(byteData);
             ObjectInputStream objectInputStream = new ObjectInputStream(byteArrayInputStream)) {
            Object object = objectInputStream.readObject();
            if (object instanceof BitSet) {
                return (BitSet) object;
            } else {
                throw new IllegalArgumentException("Invalid object type after deserialization");
            }
        } catch (ClassNotFoundException | IOException e) {
            e.printStackTrace();
            // Handle the exception appropriately
        }
        return null; // Return null or handle the failure case accordingly
    } }
