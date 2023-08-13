package com.experiment.selfjoin.csstree;

import com.configurationsandconstants.iejoinandbaseworks.Configuration;
import com.configurationsandconstants.iejoinandbaseworks.Constants;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.net.InetAddress;
import java.util.HashSet;
import java.util.Map;

public class MergingTimeEvaluation extends BaseRichBolt {
    private HashSet<Integer> leftHashSet;
    private HashSet<Integer> rightHashSet;
    private String leftStreamID;
    private String rightStreamID;
    private OutputCollector outputCollector;
    private int taskID;
    private String hostName;
    private String leftStreamRecord;
    private String rightStreamRecord;
    public MergingTimeEvaluation(String leftStreamID, String rightStreamID){
        Map<String, Object> map = Configuration.configurationConstantForStreamIDs();
        this.leftStreamID=leftStreamID;
        this.rightStreamID=rightStreamID;

    }

    @Override
    public void prepare(Map<String, Object> map, TopologyContext topologyContext, OutputCollector outputCollector) {
        taskID= topologyContext.getThisTaskId();
        this.outputCollector=outputCollector;
        try{
            hostName= InetAddress.getLocalHost().getHostName();
        }catch (Exception e){

        }
    }

    @Override
    public void execute(Tuple tuple) {

        if(tuple.getSourceStreamId().equals(leftStreamID)) {

         //   leftHashSet = convertByteArrayToHashSet(tuple.getBinaryByField(Constants.LEFT_HASH_SET));
            leftStreamRecord=tuple.getValue(0)+","+tuple.getValue(1)+","+tuple.getValue(2)+","+tuple.getValue(3);
        }
        if(tuple.getSourceStreamId().equals(rightStreamID)){
          //  rightHashSet=convertByteArrayToHashSet(tuple.getBinaryByField(Constants.RIGHT_HASH_SET));
            rightStreamRecord=tuple.getValue(0)+","+tuple.getValue(1)+","+tuple.getValue(2)+","+tuple.getValue(3);

        }
        if(leftStreamRecord!=null&&rightStreamRecord!=null){
           // leftHashSet.retainAll(rightHashSet);
            long timeAfterCalculation= System.currentTimeMillis();

            this.outputCollector.emit("MergingTime", tuple, new Values(leftStreamRecord, rightStreamRecord));
            this.outputCollector.ack(tuple);
            leftStreamRecord=null;
            rightStreamRecord=null;
        }


    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

        outputFieldsDeclarer.declareStream("MergingTime", new Fields("LeftStreamRecord","RightStreamRecord"));


    }
    public static HashSet<Integer> convertByteArrayToHashSet(byte[] byteArray) {
        HashSet<Integer> hashSet = null;
        try {
            ByteArrayInputStream bis = new ByteArrayInputStream(byteArray);
            ObjectInputStream ois = new ObjectInputStream(bis);
            hashSet = (HashSet<Integer>) ois.readObject();
        } catch (IOException | ClassNotFoundException e) {
            e.printStackTrace();
        }
        return hashSet;
    }
}
