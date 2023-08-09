package com.experiment.selfjoin.iejoinproposed;

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
import java.util.Map;

public class MutableJoiner extends BaseRichBolt {
    private BitSet predicate1BitSet = null;
    private BitSet predicate2BitSet = null;
    private String leftPredicateSourceStreamID = null;
    private String rightPredicateSourceStreamID = null;
    private OutputCollector outputCollector;
    private String result;
    private int taskID;
    private String hostName;
    private int streamIDLeft;
    private int streamIDRight;
    private HashMap<Integer, BitSet> hashMapBitSet;
    private int streamID;
    public MutableJoiner() {
        Map<String, Object> map = Configuration.configurationConstantForStreamIDs();
        this.leftPredicateSourceStreamID = (String) map.get("LeftPredicateSourceStreamIDBitSet");
        this.rightPredicateSourceStreamID = (String) map.get("RightPredicateSourceStreamIDBitSet");
        this.result= (String) map.get("Results");

    }

    @Override
    public void prepare(Map<String, Object> map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.outputCollector = outputCollector;
        taskID= topologyContext.getThisTaskId();
        hashMapBitSet= new HashMap<>();
        try{
            hostName= InetAddress.getLocalHost().getHostName();
        }catch (Exception e){

        }
//            this.leftPredicateSourceStreamID= (String) map.get("LeftPredicateSourceStreamIDBitSet");
//            this.rightPredicateSourceStreamID= (String) map.get("RightPredicateSourceStreamIDBitSet");

    }

    /**
     * Takes two input stream as byte array convert them back to bit array and perform AND, OR, other operation for
     * Completion of results
     * This operation is performed for every new tuple from upstream processing task.
     *
     * @param tuple is a byte array;
     */
    @Override
    public void execute(Tuple tuple) {
        streamID=(Integer) tuple.getValue(1);
        long time=System.currentTimeMillis();
        byte[] byteArrayForBitSet = tuple.getBinaryByField(Constants.BYTE_ARRAY);
        predicate1BitSet = convertToObject(byteArrayForBitSet);
        if(hashMapBitSet.containsKey(streamID)){
            predicate1BitSet.and(hashMapBitSet.get(streamID));
            long timeAfterCalculation= System.currentTimeMillis();
            hashMapBitSet.remove(streamID);
            this.outputCollector.emit(result, tuple, new Values( time, timeAfterCalculation,streamID,taskID,hostName));
            this.outputCollector.ack(tuple);
        }else{
            hashMapBitSet.put(streamID,predicate1BitSet);
            //this.outputCollector.emit(result, tuple, new Values( time, 0l,streamID,taskID,hostName));

        }

        this.outputCollector.emit(tuple.getSourceStreamId(), tuple, new Values(tuple
                .getValue(1), tuple.getValue(2),tuple.getValue(3),tuple.getValue(4),tuple.getValue(5),
                tuple.getValue(6),tuple.getValue(7),tuple.getValue(8), tuple.getValue(9),tuple.getValue(10)));
        this.outputCollector.ack(tuple);


//
//
//        if (tuple.getSourceStreamId().equals(leftPredicateSourceStreamID)) {
//            streamIDLeft= (Integer) tuple.getValue(1);
//            byte[] byteArrayPredicateLeftBitSet = tuple.getBinaryByField(Constants.BYTE_ARRAY);
//            predicate1BitSet = convertToObject(byteArrayPredicateLeftBitSet);
//            this.outputCollector.emit(leftPredicateSourceStreamID, tuple, new Values(tuple
//                    .getValue(1), tuple.getValue(2),tuple.getValue(3),tuple.getValue(4),tuple.getValue(5),
//                    tuple.getValue(6),tuple.getValue(7),tuple.getValue(8),tuple.getValue(9),tuple.getValue(10)));
//
//        }
//        if (tuple.getSourceStreamId().equals(rightPredicateSourceStreamID)) {
//            streamIDRight= (Integer) tuple.getValue(1);
//            byte[] byteArrayPredicateRightArray = tuple.getBinaryByField(Constants.BYTE_ARRAY);
//            predicate2BitSet = convertToObject(byteArrayPredicateRightArray);
//
//            this.outputCollector.emit(rightPredicateSourceStreamID, tuple, new Values(tuple
//                    .getValue(1), tuple.getValue(2),tuple.getValue(3),tuple.getValue(4),tuple.getValue(5),
//                    tuple.getValue(6),tuple.getValue(7),tuple.getValue(8), tuple.getValue(9),tuple.getValue(10)));
//
//        }
//        if ((predicate1BitSet != null) && (predicate2BitSet != null)) {
//            predicate2BitSet.and(predicate1BitSet);
//            long time= System.currentTimeMillis();
//            this.outputCollector.emit(result, tuple, new Values( time, streamIDLeft,streamIDRight,taskID,hostName));
//
//            predicate1BitSet = null;
//            predicate2BitSet = null;
//        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        Fields greaterFields= new Fields(  Constants.TUPLE_ID, Constants.KAFKA_TIME,
                Constants.KAFKA_SPOUT_TIME, Constants.SPLIT_BOLT_TIME, Constants.TASK_ID_FOR_SPLIT_BOLT, Constants.HOST_NAME_FOR_SPLIT_BOLT, "TupleArrivalTime",
                Constants.GREATER_PREDICATE_EVALUATION_TIME_BOLT, Constants.MUTABLE_BOLT_TASK_ID,Constants.MUTABLE_BOLT_MACHINE);
        outputFieldsDeclarer.declareStream(leftPredicateSourceStreamID, greaterFields);
        Fields lesserFields= new Fields(  Constants.TUPLE_ID,Constants.KAFKA_TIME,
                Constants.KAFKA_SPOUT_TIME, Constants.SPLIT_BOLT_TIME, Constants.TASK_ID_FOR_SPLIT_BOLT, Constants.HOST_NAME_FOR_SPLIT_BOLT,"TupleArrivalTime",
                Constants.LESSER_PREDICATE_EVALUATION_TIME_BOLT, Constants.MUTABLE_BOLT_TASK_ID,Constants.MUTABLE_BOLT_MACHINE);
        outputFieldsDeclarer.declareStream(rightPredicateSourceStreamID, lesserFields);
        outputFieldsDeclarer.declareStream(result, new Fields("Time","timeAfterCalculation", "streamright","TaskID","HostName"));


    }

    // Convert the byte array from source stream to original array
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
