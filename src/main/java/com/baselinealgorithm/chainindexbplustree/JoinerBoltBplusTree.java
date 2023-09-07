package com.baselinealgorithm.chainindexbplustree;

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
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

public class JoinerBoltBplusTree extends BaseRichBolt {
    private BitSet leftHashSet;
    private BitSet rightHashSet;
    private String leftStreamID;
    private String rightStreamID;
    private OutputCollector collector;
    private int taskID;
    private String hostName;
    private HashMap<Integer, BitSet> hashMap;
    public JoinerBoltBplusTree(String leftStreamID, String rightStreamID){
        this.leftStreamID=leftStreamID;
        this.rightStreamID=rightStreamID;
    }

    @Override
    public void prepare(Map<String, Object> map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.hashMap= new HashMap<>();
        try{

            taskID=topologyContext.getThisTaskId();
            hostName= InetAddress.getLocalHost().getHostName();
            this.collector= outputCollector;
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    @Override
    public void execute(Tuple tuple) {



        long beforeTime=System.currentTimeMillis();
        if(tuple.getSourceStreamId().equals(leftStreamID)) {
            leftHashSet = convertToObject(tuple.getBinaryByField(Constants.LEFT_HASH_SET));
            if(hashMap.containsKey(tuple.getIntegerByField(Constants.TUPLE_ID))){
                hashMap.get(tuple.getIntegerByField(Constants.TUPLE_ID)).and(leftHashSet);
                hashMap.remove(tuple.getIntegerByField(Constants.TUPLE_ID));
                try{
                    this.collector.emit ("Record", new Values(tuple.getIntegerByField(Constants.TUPLE_ID),beforeTime, System.currentTimeMillis(),taskID,hostName));
                    this.collector.ack(tuple);
                }catch (Exception e){
                    e.printStackTrace();
                }
            } else{
                hashMap.put(tuple.getIntegerByField(Constants.TUPLE_ID),leftHashSet);
            }
        }
        if(tuple.getSourceStreamId().equals(rightStreamID)){
            rightHashSet=convertToObject(tuple.getBinaryByField(Constants.RIGHT_HASH_SET));
            if(hashMap.containsKey(tuple.getIntegerByField(Constants.TUPLE_ID))){
                hashMap.get(tuple.getIntegerByField(Constants.TUPLE_ID)).and(rightHashSet);
                hashMap.remove(tuple.getIntegerByField(Constants.TUPLE_ID));
                try{
                    this.collector.emit ("Record", new Values(tuple.getIntegerByField(Constants.TUPLE_ID),beforeTime, System.currentTimeMillis(),taskID,hostName));
                    this.collector.ack(tuple);
                }catch (Exception e){
                    e.printStackTrace();
                }
            } else{
                hashMap.put(tuple.getIntegerByField(Constants.TUPLE_ID),rightHashSet);
            }
        }


//        if(leftHashSet!=null&&rightHashSet!=null){
//            leftHashSet.retainAll(rightHashSet);
//         long afterTime=System.currentTimeMillis();
//         counterRecord++;
//         this.stringBuilder.append(leftStreamID+","+rightStreamID+","+beforeTime+","+ afterTime+","+taskID+","+hostName+"\n");
//     if(counterRecord==100){
//             try {
//                 bufferedWriter.write(stringBuilder.toString());
//                 bufferedWriter.flush();
//                 counterRecord=0;
//                 this.stringBuilder= new StringBuilder();
//             } catch (IOException e) {
//                 e.printStackTrace();
//             }
//            leftHashSet=null;
//            rightHashSet=null;
//         }

      //  }

   }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declareStream("Record", new Fields("ID","BeforeTime", "AfterTime", "TaskID","Machine") );

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
    }}