package com.experiment.selfjoin.iejoinproposed;

import com.configurationsandconstants.iejoinandbaseworks.Configuration;
import com.configurationsandconstants.iejoinandbaseworks.Constants;
import com.stormiequality.BTree.BPlusTreeWithTmpForPermutation;
import com.stormiequality.BTree.Node;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.net.InetAddress;
import java.util.BitSet;
import java.util.List;
import java.util.Map;

public class MutableBPlusTree extends BaseRichBolt {
    //left stream for MutableBPlusTree
    private BPlusTreeWithTmpForPermutation bPlusTree;
    // mergeIntervalCount;
    private int mergeIntervalCount;
    // mergeIntervalDefinedByTheUser
    private int mergeIntervalDefinedByUser;
    // merge operation
    private String mergeOperationStreamID;
    // Operator that needs to be performed
    private String operator;
    // permutation computation stream ID
    private String permutationComputationStreamID;
    // Output collector
    private OutputCollector outputCollector;
    //InputStreamStreamID;
    private String leftStreamGreater;
    // leftPredicateID
    private String rightStreamSmaller;
    // Down stream tasks
    private List<Integer> downStreamTasksForIEJoin;
    // BitSet predicate Evaluation
    private String leftPredicateBitSetSteamID;
    // Bitset left predicate Evaluation
    private String rightPredicateBitSetStreamID = null;
    // // tmp ID for the computation of Permutation ID
    private int tmpIDForPermutationForStreamR;
    // ID for Downstream processing task;
    private int idForDownStreamTasksForIEJoin;
    // Machine ID
    private String hostName;
    // Tasl ID
    private int taskID;
    //
    private long tupleArrivalTime;

    public MutableBPlusTree(String operator, String permutationComputationStreamID) {
        this.operator = operator;
        this.mergeIntervalDefinedByUser = Constants.MUTABLE_WINDOW_SIZE;
        this.permutationComputationStreamID = permutationComputationStreamID;
        Map<String, Object> map = Configuration.configurationConstantForStreamIDs();
        this.leftStreamGreater = (String) map.get("LeftGreaterPredicateTuple");
        this.rightStreamSmaller = (String) map.get("RightSmallerPredicateTuple");
        this.mergeOperationStreamID = (String) map.get("MergingFlag");
        this.leftPredicateBitSetSteamID = (String) map.get("LeftPredicateSourceStreamIDBitSet");
        this.rightPredicateBitSetStreamID = (String) map.get("RightPredicateSourceStreamIDBitSet");

    }

    @Override
    public void prepare(Map<String, Object> map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.bPlusTree = new BPlusTreeWithTmpForPermutation(Constants.ORDER_OF_B_PLUS_TREE);
        this.outputCollector = outputCollector;
        this.mergeIntervalCount = 0;
        this.downStreamTasksForIEJoin = topologyContext.getComponentTasks(Constants.OFFSET_AND_IE_JOIN_BOLT_ID);
        this.idForDownStreamTasksForIEJoin = 0;
        try {
            this.hostName = InetAddress.getLocalHost().getHostName();
            this.taskID = topologyContext.getThisTaskId();
        } catch (Exception e) {

        }
    }

    @Override
    public void execute(Tuple tuple) {
        //executeMethodSync(tuple);
//
        if (operator.equals(">")) {
            this.mergeIntervalCount++;
            tupleArrivalTime = System.currentTimeMillis();
            greaterPredicateEvaluation(tuple);
        }
        if (operator.equals("<")) {
            this.mergeIntervalCount++;
            tupleArrivalTime = System.currentTimeMillis();
            lessPredicateEvaluation(tuple);
        }
        if (mergeIntervalCount == mergeIntervalDefinedByUser) {

            mergingOfMutableStructureForImmutableDataStructure(tuple);

        }

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        // Left
        outputFieldsDeclarer.declareStream(leftPredicateBitSetSteamID, new Fields(Constants.BYTE_ARRAY, Constants.TUPLE_ID,
                Constants.KAFKA_TIME, Constants.KAFKA_SPOUT_TIME, Constants.SPLIT_BOLT_TIME, Constants.TASK_ID_FOR_SPLIT_BOLT, Constants.HOST_NAME_FOR_SPLIT_BOLT, "TupleArrivalTime",
                Constants.GREATER_PREDICATE_EVALUATION_TIME_BOLT, Constants.MUTABLE_BOLT_TASK_ID, Constants.MUTABLE_BOLT_MACHINE));
        //Right part of predicate
        outputFieldsDeclarer.declareStream(rightPredicateBitSetStreamID, new Fields(Constants.BYTE_ARRAY, Constants.TUPLE_ID,
                Constants.KAFKA_TIME, Constants.KAFKA_SPOUT_TIME, Constants.SPLIT_BOLT_TIME, Constants.TASK_ID_FOR_SPLIT_BOLT, Constants.HOST_NAME_FOR_SPLIT_BOLT, "TupleArrivalTime",
                Constants.LESSER_PREDICATE_EVALUATION_TIME_BOLT, Constants.MUTABLE_BOLT_TASK_ID, Constants.MUTABLE_BOLT_MACHINE));
        //Permutation
        outputFieldsDeclarer.declareStream(permutationComputationStreamID, new Fields(Constants.TUPLE, Constants.PERMUTATION_TUPLE_IDS, Constants.BATCH_COMPLETION_FLAG, Constants.MERGING_TIME));
        //Merge
        outputFieldsDeclarer.declareStream(mergeOperationStreamID, new Fields(Constants.MERGING_OPERATION_FLAG, Constants.MERGING_TIME));

    }

    public synchronized byte[] convertToByteArray(Serializable object) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (ObjectOutputStream objectOutputStream = new ObjectOutputStream(baos)) {
            objectOutputStream.writeObject(object);
            objectOutputStream.flush();
        }
        return baos.toByteArray();
    }

    // convert integer for moving to the network;
    private static byte[] convertToByteArray(List<Integer> integerList) {
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        for (Integer num : integerList) {
            outputStream.write(num.byteValue());
        }
        return outputStream.toByteArray();
    }

    public void greaterPredicateEvaluation(Tuple tuple) {
        if (tuple.getSourceStreamId().equals(leftStreamGreater)) {
            tmpIDForPermutationForStreamR++;
            bPlusTree.insert(tuple.getIntegerByField(Constants.TUPLE), tuple.getIntegerByField(Constants.TUPLE_ID), tmpIDForPermutationForStreamR);
            BitSet bitSet = bPlusTree.lessThenSpecificValue(tuple.getIntegerByField(Constants.TUPLE));
            if (bitSet != null) {
                try {
                    byte[] bytArrayRBitSet = convertToByteArray(bitSet);
                    //Emit logic here tuple emitting
                    this.outputCollector.emit(leftPredicateBitSetSteamID, tuple, new Values(bytArrayRBitSet, tuple.getIntegerByField(Constants.TUPLE_ID),
                            tuple.getLongByField(Constants.KAFKA_TIME), tuple.getLongByField(Constants.KAFKA_SPOUT_TIME), tuple.getLongByField(Constants.SPLIT_BOLT_TIME), tuple.getValueByField(Constants.TASK_ID_FOR_SPLIT_BOLT),
                            tuple.getStringByField(Constants.HOST_NAME_FOR_SPLIT_BOLT), tupleArrivalTime, System.currentTimeMillis(), this.taskID, hostName));
                    this.outputCollector.ack(tuple);
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        // rightStream: this rightStream depicts the right tuple from right part of predicate


    }

    private void lessPredicateEvaluation(Tuple tuple) {
        if (tuple.getSourceStreamId().equals(rightStreamSmaller)) {
            tmpIDForPermutationForStreamR++;
            // Inserting the tuple into the BTree location;
            bPlusTree.insert(tuple.getIntegerByField(Constants.TUPLE), tuple.getIntegerByField(Constants.TUPLE_ID), tmpIDForPermutationForStreamR);
            // Evaluating Tuples from right stream BPlus Tree.
            BitSet bitSet = bPlusTree.greaterThenSpecificValue(tuple.getIntegerByField(Constants.TUPLE));
            if (bitSet != null) {
                try {
                    byte[] bytArrayRBitSet = convertToByteArray(bitSet);
                    // Only emitting the bit array  tuple is due to acking mechanisim
                    this.outputCollector.emit(rightPredicateBitSetStreamID, tuple, new Values(bytArrayRBitSet, tuple.getIntegerByField(Constants.TUPLE_ID),
                            tuple.getLongByField(Constants.KAFKA_TIME), tuple.getLongByField(Constants.KAFKA_SPOUT_TIME), tuple.getLongByField(Constants.SPLIT_BOLT_TIME), tuple.getValueByField(Constants.TASK_ID_FOR_SPLIT_BOLT),
                            tuple.getStringByField(Constants.HOST_NAME_FOR_SPLIT_BOLT), tupleArrivalTime, System.currentTimeMillis(), this.taskID, hostName));
                    this.outputCollector.ack(tuple);
                    // Emitting Logic for tuples

                } catch (IOException e) {

                }
            }

        }

    }

    public synchronized void mergingOfMutableStructureForImmutableDataStructure(Tuple tuple) {
        // Collector for merge operation.

        this.outputCollector.emitDirect(downStreamTasksForIEJoin.get(idForDownStreamTasksForIEJoin), mergeOperationStreamID, tuple, new Values(true, System.currentTimeMillis()));
        this.outputCollector.ack(tuple);
        // Left most node for stream R
        Node batch = bPlusTree.leftMostNode();
        // Left Most Node for Stream S
        emitTuplePermutation(batch, tuple, permutationComputationStreamID, downStreamTasksForIEJoin.get(idForDownStreamTasksForIEJoin));
        tmpIDForPermutationForStreamR = 0;
        idForDownStreamTasksForIEJoin++;
        bPlusTree = new BPlusTreeWithTmpForPermutation(Constants.ORDER_OF_B_PLUS_TREE);
        mergeIntervalCount = 0;
        if (idForDownStreamTasksForIEJoin >= downStreamTasksForIEJoin.size()) {
            idForDownStreamTasksForIEJoin = 0;
        }

    }

    public synchronized void emitTuplePermutation(Node node, Tuple tuple, String streamID, int downStreamTaskID) {
        while (node != null) {

            for (int i = 0; i < node.getKeys().size(); i++) {
                // Emitting tuples to downStream Task for tuple
                for (int j : node.getKeys().get(i).getValues()) {
                    this.outputCollector.emitDirect(downStreamTaskID, streamID, tuple, new Values(node.getKeys().get(i).getKey(), j, false, 0l));
                    // this.outputCollector.emitDirect(downStreamTaskID, streamID, tuple, new Values(node.getKeys().get(i).getKey(), convertToByteArray(node.getKeys().get(i).getValues()), false, 0l));
                    this.outputCollector.ack(tuple);
                }
            }
            node = node.getNext();
        }
        // Flag tuple that indicates the completeness of batch

        this.outputCollector.emitDirect(downStreamTaskID, streamID, tuple, new Values(0, 0, true, System.currentTimeMillis()));
        this.outputCollector.ack(tuple);
    }

    public synchronized void executeMethodSync(Tuple tuple){
        if (operator.equals(">")) {
            this.mergeIntervalCount++;
            tupleArrivalTime = System.currentTimeMillis();
            greaterPredicateEvaluation(tuple);
        }
        if (operator.equals("<")) {
            this.mergeIntervalCount++;
            tupleArrivalTime = System.currentTimeMillis();
            lessPredicateEvaluation(tuple);
        }
        if (mergeIntervalCount == mergeIntervalDefinedByUser) {

            mergingOfMutableStructureForImmutableDataStructure(tuple);

        }

    }
}
