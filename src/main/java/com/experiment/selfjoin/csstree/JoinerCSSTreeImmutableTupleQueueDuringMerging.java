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
import java.util.BitSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

public class JoinerCSSTreeImmutableTupleQueueDuringMerging extends BaseRichBolt {
    private HashSet<Integer> leftHashSet;
    private HashSet<Integer> rightHashSet;
    private HashSet<Integer> hashSet;
    private String leftStreamID;
    private String rightStreamID;
    private OutputCollector outputCollector;
    private int taskID;
    private String hostName;
    private String leftStreamRecord;
    private String rightStreamRecord;
    private HashMap<Integer, HashSet> hashMapHashSet;
    public JoinerCSSTreeImmutableTupleQueueDuringMerging(String leftStreamID, String rightStreamID){
        Map<String, Object> map = Configuration.configurationConstantForStreamIDs();
        this.leftStreamID=leftStreamID;
        this.rightStreamID=rightStreamID;

    }

    @Override
    public void prepare(Map<String, Object> map, TopologyContext topologyContext, OutputCollector outputCollector) {
        taskID= topologyContext.getThisTaskId();
        this.outputCollector=outputCollector;
        this.hashMapHashSet= new HashMap<>();
        try{
            hostName= InetAddress.getLocalHost().getHostName();
        }catch (Exception e){

        }
    }

    @Override
    public void execute(Tuple tuple) {

        if(tuple.getSourceStreamId().equals(leftStreamID)) {

            long time=System.currentTimeMillis();
            leftHashSet = convertByteArrayToHashSet(tuple.getBinaryByField(Constants.HASH_SET));
            leftStreamRecord=tuple.getValue(1)+","+tuple.getValue(2)+","+tuple.getValue(3)+","+tuple.getValue(4)+","+time;
            this.outputCollector.emit("MergingQueueTuple", tuple, new Values(leftStreamRecord,System.currentTimeMillis(), tuple.getValue(1), taskID, hostName,"Left"));
            this.outputCollector.ack(tuple);
        }
        if(tuple.getSourceStreamId().equals(rightStreamID)){

            long time=System.currentTimeMillis();
            rightHashSet=convertByteArrayToHashSet(tuple.getBinaryByField(Constants.HASH_SET));
            rightStreamRecord=tuple.getValue(1)+","+tuple.getValue(2)+","+tuple.getValue(3)+","+tuple.getValue(4)+","+time;
            this.outputCollector.emit("MergingQueueTuple", tuple, new Values( rightStreamRecord, System.currentTimeMillis(), tuple.getValue(1), taskID, hostName,"Right"));
            this.outputCollector.ack(tuple);
        }
//     if(leftHashSet!=null&&rightHashSet!=null) {
//
//         this.outputCollector.emit("MergingQueueTuple", tuple, new Values(leftStreamRecord, rightStreamRecord, System.currentTimeMillis(), tuple.getValue(1), taskID, hostName));
//         this.outputCollector.ack(tuple);
//     }
//          //  System.out.println(tuple+"=========");
//            leftHashSet.retainAll(rightHashSet);
//            long timeAfterCalculation= System.currentTimeMillis();
//            leftHashSet=null;
//            rightHashSet=null;
//            this.outputCollector.emit("MergingQueueTuple", tuple, new Values(leftStreamRecord, rightStreamRecord,timeAfterCalculation,streamID,taskID,hostName));
//            this.outputCollector.ack(tuple);
//        }


//       int  streamID=(Integer) tuple.getValue(1);
//
//        byte[] byteArrayForHashSet = tuple.getBinaryByField(Constants.HASH_SET);
//        hashSet = convertByteArrayToHashSet(byteArrayForHashSet);
//        if(hashMapHashSet.containsKey(streamID)){
//            System.out.println(streamID);
//            hashSet.retainAll(hashMapHashSet.get(streamID));
//            long timeAfterCalculation= System.currentTimeMillis();
//            hashMapHashSet.remove(streamID);
//            this.outputCollector.emit("MergingQueueTuple", tuple, new Values( leftStreamRecord, rightStreamRecord,timeAfterCalculation,streamID,taskID,hostName));
//            this.outputCollector.ack(tuple);
//        }else{
//            hashMapHashSet.put(streamID,hashSet);
//            //this.outputCollector.emit(result, tuple, new Values( time, 0l,streamID,taskID,hostName));
//
//        }


    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

        outputFieldsDeclarer.declareStream("MergingQueueTuple", new Fields("RightStreamRecord","timeAfterCalculation", "streamID","TaskID","HostName","stream"));


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
