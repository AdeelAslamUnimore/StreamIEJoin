package com.baselinealgorithm.splitbchj;

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
import java.util.Map;

public class Joiner extends BaseRichBolt {
    private HashMap<Integer, BitSet> streamRMap;
    private HashMap<Integer, BitSet> streamSMap;
    private int taskID;
    private String hostName;
    private OutputCollector collector;
    @Override
    public void prepare(Map<String, Object> map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.streamRMap= new HashMap<>();
        this.streamSMap= new HashMap<>();
        this.collector=outputCollector;
        taskID=topologyContext.getThisTaskId();
        try{
            this.hostName= InetAddress.getLocalHost().getHostName();
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    @Override
    public void execute(Tuple tuple) {

        long tupleArrivalTime= System.currentTimeMillis();
        if(tuple.getSourceStreamId().equals("StreamR")){

            if(streamRMap.containsKey(tuple.getIntegerByField("ID"))){

              convertToObject(tuple.getBinaryByField("BitArray")).and(streamRMap.get(tuple.getIntegerByField("ID")));
                streamRMap.remove(tuple.getIntegerByField("ID"));
                this.collector.emit("Record", new Values(tuple.getIntegerByField("ID"),tuple.getValue(2),tuple.getValue(3),
                        tuple.getValue(4), tuple.getValue(5), tuple.getValue(6), tuple.getValue(7),tupleArrivalTime,System.currentTimeMillis(),taskID,hostName));
            }else{
                streamRMap.put(tuple.getIntegerByField("ID"), convertToObject(tuple.getBinaryByField("BitArray")));
            }
        }
        if(tuple.getSourceStreamId().equals("StreamS")){
            if(streamSMap.containsKey(tuple.getIntegerByField("ID"))){
                convertToObject(tuple.getBinaryByField("BitArray")).and(streamSMap.get(tuple.getIntegerByField("ID")));
                streamSMap.remove(tuple.getIntegerByField("ID"));
                this.collector.emit("Record", new Values(tuple.getIntegerByField("ID"),tuple.getValue(2),tuple.getValue(3),
                        tuple.getValue(4), tuple.getValue(5), tuple.getValue(6), tuple.getValue(7),tupleArrivalTime,System.currentTimeMillis(),taskID,hostName));

            }else{
                streamSMap.put(tuple.getIntegerByField("ID"), convertToObject(tuple.getBinaryByField("BitArray")));
            }
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
       outputFieldsDeclarer.declareStream("Record", new Fields("ID","KafkaTime",
               "KafkaSpoutTime", "tupleArrivalTime", "TupleEvaluationTime", "taskID",
               "hostName", "JoinerTime", "JoinerEvaluationTime","TaskID", "HostName"));

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
