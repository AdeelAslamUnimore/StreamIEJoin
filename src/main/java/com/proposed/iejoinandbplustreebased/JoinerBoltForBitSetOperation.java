package com.proposed.iejoinandbplustreebased;

import clojure.lang.Cons;
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
import java.util.HashMap;
import java.util.Map;

public class JoinerBoltForBitSetOperation extends BaseRichBolt {
    private BitSet predicateBitSetLeft = null;
    private BitSet predicateBitSetRight = null;
    private String leftPredicateSourceStreamID = null;
    private String rightPredicateSourceStreamID = null;
    private OutputCollector outputCollector;
    private String result;
    private int taskID;
    private String hostName;
    private int streamIDLeft;
    private int streamIDRight;
    ///
    private HashMap<String,BitSet> mapBitSetLeftStream;
    //
    private HashMap<String,BitSet> mapBitSetRightStream;

    //BitSet for Test
    private BitSet bitSetLeft;
    //Bitset for Right
    private BitSet bitSetRight;
    // private LeftID
    private String leftID;
    // private RightID
    private String rightID;
    // The buffered writer left stream
//    BufferedWriter leftStreamWriter;
//    // The buffered writer right stream
//    BufferedWriter rightStreamWriter;
//     private StringBuilder stringBuilderLeft;
//    private StringBuilder stringBuilderRight;
//     private int counterLeft;
//     private int counterRight;



    public JoinerBoltForBitSetOperation() {
        Map<String, Object> map = Configuration.configurationConstantForStreamIDs();
        this.leftPredicateSourceStreamID = (String) map.get("LeftPredicateSourceStreamIDBitSet");
        this.rightPredicateSourceStreamID = (String) map.get("RightPredicateSourceStreamIDBitSet");
       // this.result= (String) map.get("Results");

    }

    @Override
    public void prepare(Map<String, Object> map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.outputCollector = outputCollector; // Assign the output collector.

        taskID = topologyContext.getThisTaskId(); // Get the current task ID.

        try {
            hostName = InetAddress.getLocalHost().getHostName(); // Get the host name of the local machine.
            mapBitSetLeftStream = new HashMap<>(); // Initialize a HashMap for bit sets on the left stream.
            mapBitSetRightStream = new HashMap<>(); // Initialize a HashMap for bit sets on the right stream.
        } catch (Exception e) {
            e.printStackTrace(); // Print any exceptions that occur.
        }
//        try {
////            //  this.counterForRecord=0;
////            this.stringBuilderLeft = new StringBuilder();
////            this.stringBuilderRight = new StringBuilder();
////            leftStreamWriter = new BufferedWriter(new FileWriter(new File("/home/adeel/Data/Results/leftStream"+taskID+".csv")));
////            rightStreamWriter = new BufferedWriter(new FileWriter(new File("/home/adeel/Data/Results/rightStream"+taskID+".csv")));
////            leftStreamWriter.write(Constants.TUPLE_ID + "," +
////                    Constants.KAFKA_TIME + "," + Constants.KAFKA_SPOUT_TIME + "," + Constants.SPLIT_BOLT_TIME + "," + Constants.TASK_ID_FOR_SPLIT_BOLT + "," + Constants.HOST_NAME_FOR_SPLIT_BOLT + "," + "TupleArrivalTime" + "," +
////                    Constants.GREATER_PREDICATE_EVALUATION_TIME_BOLT + "," + Constants.MUTABLE_BOLT_TASK_ID + "," + Constants.MUTABLE_BOLT_MACHINE + ",EvaluationStartTime , EvaluationEndTime,TasksID, HostName \n");
////            leftStreamWriter.flush();
////            rightStreamWriter.write(Constants.TUPLE_ID + "," +
////                    Constants.KAFKA_TIME + "," + Constants.KAFKA_SPOUT_TIME + "," + Constants.SPLIT_BOLT_TIME + "," + Constants.TASK_ID_FOR_SPLIT_BOLT + "," + Constants.HOST_NAME_FOR_SPLIT_BOLT + "," + "TupleArrivalTime" + "," +
////                    Constants.GREATER_PREDICATE_EVALUATION_TIME_BOLT + "," + Constants.MUTABLE_BOLT_TASK_ID + "," + Constants.MUTABLE_BOLT_MACHINE + ",EvaluationStartTime,EvaluationEndTime,TasksID, HostName \n");
////
////            rightStreamWriter.flush();
////        }
////        catch (Exception e){
////            e.printStackTrace();
////        }


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
     testEvaluationData(tuple);
      //  executeHashBased(tuple);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
      Fields fields= new Fields(  Constants.TUPLE_ID, Constants.KAFKA_TIME,
                Constants.KAFKA_SPOUT_TIME, Constants.SPLIT_BOLT_TIME, Constants.TASK_ID_FOR_SPLIT_BOLT, Constants.HOST_NAME_FOR_SPLIT_BOLT, "TupleArrivalTime",
                Constants.GREATER_PREDICATE_EVALUATION_TIME_BOLT, Constants.MUTABLE_BOLT_TASK_ID,Constants.MUTABLE_BOLT_MACHINE, "startEvalutionTime","EndEvaluationTime","tmp_ID","TaskID","MachineID");
        outputFieldsDeclarer.declareStream(leftPredicateSourceStreamID, fields);
        Fields rightFields= new Fields(  Constants.TUPLE_ID,Constants.KAFKA_TIME,
                Constants.KAFKA_SPOUT_TIME, Constants.SPLIT_BOLT_TIME, Constants.TASK_ID_FOR_SPLIT_BOLT, Constants.HOST_NAME_FOR_SPLIT_BOLT,"TupleArrivalTime",
                Constants.LESSER_PREDICATE_EVALUATION_TIME_BOLT, Constants.MUTABLE_BOLT_TASK_ID,Constants.MUTABLE_BOLT_MACHINE,"startEvalutionTime","EndEvaluationTime","tmp_ID","TaskID","MachineID");
        outputFieldsDeclarer.declareStream(rightPredicateSourceStreamID, rightFields);


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
    public void testEvaluationData(Tuple tuple){
        if (tuple.getSourceStreamId().equals(leftPredicateSourceStreamID)){

            long tupleStartTime = System.currentTimeMillis();

            // Extract the byte array representing the left stream predicate bit set.
            byte[] byteArrayPredicateLeftBitSet = tuple.getBinaryByField(Constants.BYTE_ARRAY);

            // Convert the byte array to a BitSet representing the left stream predicate.
            BitSet predicateBitSetLeftStream = convertToObject(byteArrayPredicateLeftBitSet);
            if(bitSetLeft!=null){
                predicateBitSetLeftStream.and(bitSetLeft);
                bitSetLeft=null;
                this.outputCollector.emit(leftPredicateSourceStreamID, new Values(tuple
                        .getValue(1), tuple.getValue(2), tuple.getValue(3), tuple.getValue(4), tuple.getValue(5),
                        tuple.getValue(6), tuple.getValue(7), tuple.getValue(8), tuple.getValue(9), tuple.getValue(10),
                        tupleStartTime, System.currentTimeMillis(), leftID, taskID, hostName));


            }
            else{
                bitSetLeft= predicateBitSetLeftStream;
                leftID=tuple.getStringByField(Constants.TUPLE_ID);
            }
        }
        if (tuple.getSourceStreamId().equals(rightPredicateSourceStreamID)){

            long tupleStartTime = System.currentTimeMillis();

            // Extract the byte array representing the left stream predicate bit set.
            byte[] byteArrayPredicateRightBitSet = tuple.getBinaryByField(Constants.BYTE_ARRAY);

            // Convert the byte array to a BitSet representing the left stream predicate.
            BitSet predicateBitSetRightStream = convertToObject(byteArrayPredicateRightBitSet);
            if(bitSetRight!=null){
                predicateBitSetRightStream.and(bitSetRight);
                bitSetRight=null;
                this.outputCollector.emit(rightPredicateSourceStreamID, new Values(tuple
                        .getValue(1), tuple.getValue(2), tuple.getValue(3), tuple.getValue(4), tuple.getValue(5),
                        tuple.getValue(6), tuple.getValue(7), tuple.getValue(8), tuple.getValue(9), tuple.getValue(10),
                        tupleStartTime, System.currentTimeMillis(), rightID, taskID, hostName));

            }
            else{
                bitSetRight= predicateBitSetRightStream;
                rightID=tuple.getStringByField(Constants.TUPLE_ID);
            }
        }
    }

    public void executeHashBased(Tuple tuple){
        if (tuple.getSourceStreamId().equals(leftPredicateSourceStreamID)) {
            // Check if the left stream predicate for the tuple exists in the bit set map.
            if (mapBitSetLeftStream.containsKey(tuple.getStringByField(Constants.TUPLE_ID))) {
                // Record the start time for processing the tuple.
                long tupleStartTime = System.currentTimeMillis();

                // Extract the byte array representing the left stream predicate bit set.
                byte[] byteArrayPredicateLeftBitSet = tuple.getBinaryByField(Constants.BYTE_ARRAY);

                // Convert the byte array to a BitSet representing the left stream predicate.
                BitSet predicateBitSetLeftStream = convertToObject(byteArrayPredicateLeftBitSet);

                // Perform a bitwise AND operation with the stored predicate bit set.
                predicateBitSetLeftStream.and(mapBitSetLeftStream.get(tuple.getStringByField(Constants.TUPLE_ID)));

                // Remove the predicate bit set from the map.
                mapBitSetLeftStream.remove(tuple.getStringByField(Constants.TUPLE_ID));

                // Emit the tuple to the left predicate stream with updated values.
                this.outputCollector.emit(leftPredicateSourceStreamID, new Values(tuple
                        .getValue(1), tuple.getValue(2), tuple.getValue(3), tuple.getValue(4), tuple.getValue(5),
                        tuple.getValue(6), tuple.getValue(7), tuple.getValue(8), tuple.getValue(9), tuple.getValue(10),
                        tupleStartTime, System.currentTimeMillis(),tuple.getStringByField(Constants.TUPLE_ID), taskID, hostName));

                // Acknowledge the tuple processing.
                this.outputCollector.ack(tuple);
            } else {
                // If the predicate does not exist in the map, add it to the map.
                byte[] byteArrayPredicateLeftBitSet = tuple.getBinaryByField(Constants.BYTE_ARRAY);
                BitSet predicateBitSetLeftStream = convertToObject(byteArrayPredicateLeftBitSet);
                mapBitSetLeftStream.put(tuple.getStringByField(Constants.TUPLE_ID), predicateBitSetLeftStream);
            }
        }

        if (tuple.getSourceStreamId().equals(rightPredicateSourceStreamID)) {
            // Check if the right stream predicate for the tuple exists in the bit set map.
            if (mapBitSetRightStream.containsKey(tuple.getStringByField(Constants.TUPLE_ID))) {
                // Record the start time for processing the tuple.
                long tupleStartTime = System.currentTimeMillis();

                // Extract the byte array representing the right stream predicate bit set.
                byte[] byteArrayPredicateRightBitSet = tuple.getBinaryByField(Constants.BYTE_ARRAY);

                // Convert the byte array to a BitSet representing the right stream predicate.
                BitSet predicateBitSetRightStream = convertToObject(byteArrayPredicateRightBitSet);

                // Perform a bitwise AND operation with the stored predicate bit set.
                predicateBitSetRightStream.and(mapBitSetRightStream.get(tuple.getStringByField(Constants.TUPLE_ID)));

                // Remove the predicate bit set from the map.
                mapBitSetRightStream.remove(tuple.getStringByField(Constants.TUPLE_ID));

                // Emit the tuple to the right predicate stream with updated values.
                this.outputCollector.emit(rightPredicateSourceStreamID, new Values(tuple
                        .getValue(1), tuple.getValue(2), tuple.getValue(3), tuple.getValue(4), tuple.getValue(5),
                        tuple.getValue(6), tuple.getValue(7), tuple.getValue(8), tuple.getValue(9), tuple.getValue(10),
                        tupleStartTime, System.currentTimeMillis(), tuple.getStringByField(Constants.TUPLE_ID), taskID, hostName));

                // Acknowledge the tuple processing.
                this.outputCollector.ack(tuple);
            } else {
                // If the predicate does not exist in the map, add it to the map.
                byte[] byteArrayPredicateRightBitSet = tuple.getBinaryByField(Constants.BYTE_ARRAY);
                BitSet predicateBitSetRightStream = convertToObject(byteArrayPredicateRightBitSet);
                mapBitSetRightStream.put(tuple.getStringByField(Constants.TUPLE_ID), predicateBitSetRightStream);
            }

        }
    }
}
