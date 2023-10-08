package com.proposed.iejoinandbplustreebased;

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

public class JoinerBoltForBitSetOperationWithoutTupleResilence extends BaseRichBolt {
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
    private BitSet leftBitSet;
    private BitSet rightBitSet;
    public JoinerBoltForBitSetOperationWithoutTupleResilence() {
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
          //  this.leftBitSet= new BitSet();
            //this.rightBitSet= new BitSet();
        } catch (Exception e) {
            e.printStackTrace(); // Print any exceptions that occur.
        }


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
        if (tuple.getSourceStreamId().equals(leftPredicateSourceStreamID)) {
            // Check if the left stream predicate for the tuple exists in the bit set map.

            long tupleStartTime= System.currentTimeMillis();
            byte[] byteArrayPredicateLeftBitSet = tuple.getBinaryByField(Constants.BYTE_ARRAY);
            if(leftBitSet==null) {
                leftBitSet = convertToObject(byteArrayPredicateLeftBitSet);
            }
            else{
                leftBitSet.and(convertToObject(byteArrayPredicateLeftBitSet));
                leftBitSet= null;
                this.outputCollector.emit(leftPredicateSourceStreamID, new Values(tuple
                        .getValue(1), tuple.getValue(2), tuple.getValue(3), tuple.getValue(4), tuple.getValue(5),
                        tuple.getValue(6), tuple.getValue(7), tuple.getValue(8), tuple.getValue(9), tuple.getValue(10),
                        tupleStartTime, System.currentTimeMillis(), taskID, hostName));

                // Acknowledge the tuple processing.
                this.outputCollector.ack(tuple);
            }


                // Emit the tuple to the left predicate stream with updated values.

            }


        if (tuple.getSourceStreamId().equals(rightPredicateSourceStreamID)) {
            // Check if the right stream predicate for the tuple exists in the bit set map.
            long tupleStartTime= System.currentTimeMillis();
            byte[] byteArrayPredicateLeftBitSet = tuple.getBinaryByField(Constants.BYTE_ARRAY);
            if(rightBitSet==null) {
                rightBitSet = convertToObject(byteArrayPredicateLeftBitSet);
            }
            else{
                rightBitSet.and(convertToObject(byteArrayPredicateLeftBitSet));
                rightBitSet= null;
                this.outputCollector.emit(rightPredicateSourceStreamID, new Values(tuple
                        .getValue(1), tuple.getValue(2), tuple.getValue(3), tuple.getValue(4), tuple.getValue(5),
                        tuple.getValue(6), tuple.getValue(7), tuple.getValue(8), tuple.getValue(9), tuple.getValue(10),
                        tupleStartTime, System.currentTimeMillis(), taskID, hostName));

                // Acknowledge the tuple processing.
                this.outputCollector.ack(tuple);
            }


            // Emit the tuple to the right predicate stream with updated values.

            }




    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        Fields fields= new Fields(  Constants.TUPLE_ID, Constants.KAFKA_TIME,
                Constants.KAFKA_SPOUT_TIME, Constants.SPLIT_BOLT_TIME, Constants.TASK_ID_FOR_SPLIT_BOLT, Constants.HOST_NAME_FOR_SPLIT_BOLT, "TupleArrivalTime",
                Constants.GREATER_PREDICATE_EVALUATION_TIME_BOLT, Constants.MUTABLE_BOLT_TASK_ID,Constants.MUTABLE_BOLT_MACHINE, "startEvalutionTime","EndEvaluationTime","TaskID","MachineID");
        outputFieldsDeclarer.declareStream(leftPredicateSourceStreamID, fields);
        Fields rightFields= new Fields(  Constants.TUPLE_ID,Constants.KAFKA_TIME,
                Constants.KAFKA_SPOUT_TIME, Constants.SPLIT_BOLT_TIME, Constants.TASK_ID_FOR_SPLIT_BOLT, Constants.HOST_NAME_FOR_SPLIT_BOLT,"TupleArrivalTime",
                Constants.LESSER_PREDICATE_EVALUATION_TIME_BOLT, Constants.MUTABLE_BOLT_TASK_ID,Constants.MUTABLE_BOLT_MACHINE,"startEvalutionTime","EndEvaluationTime","TaskID","MachineID");
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
}
