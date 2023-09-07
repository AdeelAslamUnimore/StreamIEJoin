package com.experiment.selfjoin.broadcasthashjoin;

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
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

public class Joiner extends BaseRichBolt {
    private HashMap<Integer, BitSet> map;
    private OutputCollector collector;
    private  int taskID;
    private String hostName;
    @Override
    public void prepare(Map<String, Object> map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.map= new HashMap<>();
        this.collector= outputCollector;
        taskID= topologyContext.getThisTaskId();
        try{
            hostName= InetAddress.getLocalHost().getHostName();
        }catch (Exception e){

        }
    }

    @Override
    public void execute(Tuple tuple) {
        long startTime=System.currentTimeMillis();
        if(map.containsKey(tuple.getIntegerByField(Constants.TUPLE_ID))){
            byte[] byteArrayForBitSet = tuple.getBinaryByField(Constants.BYTE_ARRAY);
            BitSet  predicate1BitSet = convertToObject(byteArrayForBitSet);
            map.get(tuple.getIntegerByField(Constants.TUPLE_ID)).and(predicate1BitSet);
             map.remove(tuple.getIntegerByField(Constants.TUPLE_ID));
             try {
                 this.collector.emit("JoinerStream",new Values(tuple.getIntegerByField(Constants.TUPLE_ID), startTime, System.currentTimeMillis(), taskID, hostName));
             }
             catch (IllegalArgumentException e){
                 System.out.println(tuple.getIntegerByField(Constants.TUPLE_ID));
             }
        }
        else{
            byte[] byteArrayForBitSet = tuple.getBinaryByField(Constants.BYTE_ARRAY);
            BitSet  predicate1BitSet = convertToObject(byteArrayForBitSet);

            map.put(tuple.getIntegerByField(Constants.TUPLE_ID),predicate1BitSet);
        }

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
    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declareStream("JoinerStream", new Fields("ID","startTime","EndTime", "TaskID", "HostName"));
    }
}
