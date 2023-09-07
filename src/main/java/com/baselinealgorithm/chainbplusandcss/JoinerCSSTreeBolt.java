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

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.net.InetAddress;
import java.util.BitSet;
import java.util.HashSet;
import java.util.Map;

public class JoinerCSSTreeBolt extends BaseRichBolt {
    private BitSet leftHashSet;
    private BitSet rightHashSet;
    private String leftStreamID;
    private String rightStreamID;
    private OutputCollector outputCollector;
    private String result;
    private int taskID;
    private String hostName;
    public JoinerCSSTreeBolt(String leftStreamID, String rightStreamID){
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
        }catch (Exception e){

        }
    }

    @Override
    public void execute(Tuple tuple) {
        int streamID=(Integer) tuple.getValue(1);
        long time=System.currentTimeMillis();
        if(tuple.getSourceStreamId().equals(leftStreamID)) {
            leftHashSet = convertToObject(tuple.getBinaryByField(Constants.LEFT_HASH_SET));
         //System.out.println("LeftHashSet"+leftHashSet);
        }
        if(tuple.getSourceStreamId().equals(rightStreamID)){
            rightHashSet=convertToObject(tuple.getBinaryByField(Constants.RIGHT_HASH_SET));
          //  System.out.println("rightStreamID"+rightHashSet);
        }
        if(leftHashSet!=null&&rightHashSet!=null){
            leftHashSet.and(rightHashSet);
            long timeAfterCalculation= System.currentTimeMillis();
            leftHashSet=null;
            rightHashSet=null;
            this.outputCollector.emit(result, tuple, new Values( time, timeAfterCalculation,streamID,taskID,hostName));
            this.outputCollector.ack(tuple);
        }
        this.outputCollector.emit(tuple.getSourceStreamId(), tuple, new Values(tuple
                .getValue(1), tuple.getValue(2),tuple.getValue(3),tuple.getValue(4),tuple.getValue(5),
                tuple.getValue(6),tuple.getValue(7),tuple.getValue(8), tuple.getValue(9),tuple.getValue(10)));
        this.outputCollector.ack(tuple);

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        Fields greaterFields= new Fields(  Constants.TUPLE_ID, Constants.KAFKA_TIME,
                Constants.KAFKA_SPOUT_TIME, Constants.SPLIT_BOLT_TIME, Constants.TASK_ID_FOR_SPLIT_BOLT, Constants.HOST_NAME_FOR_SPLIT_BOLT, "TupleArrivalTime",
                Constants.GREATER_PREDICATE_EVALUATION_TIME_BOLT, Constants.MUTABLE_BOLT_TASK_ID,Constants.MUTABLE_BOLT_MACHINE);
        outputFieldsDeclarer.declareStream(leftStreamID, greaterFields);
        Fields lesserFields= new Fields(  Constants.TUPLE_ID,Constants.KAFKA_TIME,
                Constants.KAFKA_SPOUT_TIME, Constants.SPLIT_BOLT_TIME, Constants.TASK_ID_FOR_SPLIT_BOLT, Constants.HOST_NAME_FOR_SPLIT_BOLT,"TupleArrivalTime",
                Constants.LESSER_PREDICATE_EVALUATION_TIME_BOLT, Constants.MUTABLE_BOLT_TASK_ID,Constants.MUTABLE_BOLT_MACHINE);
        outputFieldsDeclarer.declareStream(rightStreamID, lesserFields);
        outputFieldsDeclarer.declareStream(result, new Fields("Time","timeAfterCalculation", "streamright","TaskID","HostName"));


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
    }
}
