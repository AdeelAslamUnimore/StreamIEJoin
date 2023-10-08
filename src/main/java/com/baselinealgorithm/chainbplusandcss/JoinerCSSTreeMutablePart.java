package com.baselinealgorithm.chainbplusandcss;


import com.configurationsandconstants.iejoinandbaseworks.Constants;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.io.BufferedWriter;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

public class JoinerCSSTreeMutablePart extends BaseRichBolt {
    private HashSet<Integer> leftHashSet;
    private HashSet<Integer> rightHashSet;
    private String leftStreamID;
    private String rightStreamID;
    //  Take Results
    private long leftTupleArrivalTime;
    private long rightTupleArrivalTime;
    // HashMap
  //  private Map<String, HashSet> resultsForProvence;

    // Buffered Writer
   private OutputCollector collector;
   private int taskID;
   private String hostName;

    public JoinerCSSTreeMutablePart(String leftStreamID, String rightStreamID){
        this.leftStreamID=leftStreamID;
        this.rightStreamID=rightStreamID;
    }

    @Override
    public void prepare(Map<String, Object> map, TopologyContext topologyContext, OutputCollector outputCollector) {
       // this.resultsForProvence= new HashMap<>();
        this.collector= outputCollector;
        this.taskID= topologyContext.getThisTaskId();
        try {
            this.hostName= InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void execute(Tuple tuple) {
        long tupleArrivalTime=System.currentTimeMillis();
        if(tuple.getSourceStreamId().equals(leftStreamID)) {
            if(leftHashSet!=null){
                leftHashSet.retainAll(convertByteArrayToHashSet(tuple.getBinaryByField(Constants.LEFT_HASH_SET)));
                leftHashSet=null;
                this.collector.emit("LeftMutableResults",new Values(tuple.getValue(1),tuple.getValue(2),tuple.getValue(3),
                        tuple.getValue(4), tuple.getValue(5), tuple.getValue(6),tuple.getValue(7),
                        tuple.getValue(8), tuple.getValue(9), tuple.getValue(10), tupleArrivalTime,System.currentTimeMillis(), taskID,hostName));

            }else{
                leftHashSet = convertByteArrayToHashSet(tuple.getBinaryByField(Constants.LEFT_HASH_SET));
            }

//            leftHashSet = convertByteArrayToHashSet(tuple.getBinaryByField(Constants.LEFT_HASH_SET));
//            if(resultsForProvence.containsKey(tuple.getStringByField(Constants.TUPLE_ID))){
//                leftHashSet.retainAll(resultsForProvence.get(tuple.getStringByField(Constants.TUPLE_ID)));
//                resultsForProvence.remove(tuple.getStringByField(Constants.TUPLE_ID));
//                this.collector.emit("LeftMutableResults",new Values(tuple.getValue(1),tuple.getValue(2),tuple.getValue(3),
//                        tuple.getValue(4), tuple.getValue(5), tuple.getValue(6),tuple.getValue(7),
//                        tuple.getValue(8), tuple.getValue(9), tuple.getValue(10), tupleArrivalTime,System.currentTimeMillis(), taskID,hostName));
//            }
//            else{
//                resultsForProvence.put(tuple.getStringByField(Constants.TUPLE_ID),leftHashSet);
//                leftHashSet=null;
//            }
        }
        if(tuple.getSourceStreamId().equals(rightStreamID)){
            if(rightHashSet!=null){
                rightHashSet.retainAll(convertByteArrayToHashSet(tuple.getBinaryByField(Constants.RIGHT_HASH_SET)));
                rightHashSet=null;
                this.collector.emit("RightMutableResults",new Values(tuple.getValue(1),tuple.getValue(2),tuple.getValue(3),
                        tuple.getValue(4), tuple.getValue(5), tuple.getValue(6),tuple.getValue(7),
                        tuple.getValue(8), tuple.getValue(9), tuple.getValue(10), tupleArrivalTime,System.currentTimeMillis(), taskID,hostName));

            }else{
                rightHashSet = convertByteArrayToHashSet(tuple.getBinaryByField(Constants.RIGHT_HASH_SET));
            }

//            rightHashSet = convertByteArrayToHashSet(tuple.getBinaryByField(Constants.RIGHT_HASH_SET));
//            if(resultsForProvence.containsKey(tuple.getStringByField(Constants.TUPLE_ID))){
//                rightHashSet.retainAll(resultsForProvence.get(tuple.getStringByField(Constants.TUPLE_ID)));
//                resultsForProvence.remove(tuple.getStringByField(Constants.TUPLE_ID));
//                this.collector.emit("RightMutableResults",new Values(tuple.getValue(1),tuple.getValue(2),tuple.getValue(3),
//                        tuple.getValue(4), tuple.getValue(5), tuple.getValue(6),tuple.getValue(7),
//                        tuple.getValue(8), tuple.getValue(9), tuple.getValue(10), tupleArrivalTime,System.currentTimeMillis(), taskID,hostName));
//
//            }
//            else{
//                resultsForProvence.put(tuple.getStringByField(Constants.TUPLE_ID),rightHashSet);
//                rightHashSet=null;
//            }
        }


    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declareStream("LeftMutableResults", new Fields(Constants.TUPLE_ID,Constants.KAFKA_TIME,Constants.KAFKA_SPOUT_TIME, Constants.SPLIT_BOLT_TIME, Constants.TASK_ID_FOR_SPLIT_BOLT, Constants.HOST_NAME_FOR_SPLIT_BOLT,
                "TupleArrivalTime","TupleEvaluationTime","HostName", "TaskID", "EvaluationStartTime", "EvaluationEndTime","TaskIDForBitSetEvaluation", "HostNameForEvaluation"));
        outputFieldsDeclarer.declareStream("RightMutableResults", new Fields(Constants.TUPLE_ID,Constants.KAFKA_TIME,Constants.KAFKA_SPOUT_TIME, Constants.SPLIT_BOLT_TIME, Constants.TASK_ID_FOR_SPLIT_BOLT, Constants.HOST_NAME_FOR_SPLIT_BOLT,
                "TupleArrivalTime","TupleEvaluationTime","HostName", "TaskID", "EvaluationStartTime", "EvaluationEndTime","TaskIDForBitSetEvaluation", "HostNameForEvaluation"));

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