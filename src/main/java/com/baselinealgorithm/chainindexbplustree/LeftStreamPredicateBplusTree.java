package com.baselinealgorithm.chainindexbplustree;

import com.configurationsandconstants.iejoinandbaseworks.Configuration;
import com.configurationsandconstants.iejoinandbaseworks.Constants;
import com.stormiequality.BTree.BPlusTreeUpdated;
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
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Map;

public class LeftStreamPredicateBplusTree extends BaseRichBolt {
    private LinkedList<BPlusTreeUpdated> leftPredicateLinkedList = null;
    private LinkedList<BPlusTreeUpdated> rightPredicateLinkedList = null;
    private int treeRemovalThresholdUserDefined;
    private int treeArchiveThresholdDuration;
    private int treeArchiveThresholdTime;
    private int treeArchiveUserDefined;
    private int tupleRemovalCountForLocal;
    private OutputCollector outputCollector;
    private Map<String, Object> map;
    private String leftStreamSmaller;
    private String rightStreamSmaller;
    private long tupleArrivalTime;
    private int taskID;
    private String hostName;

    // Constructor parameter for tuples
    public LeftStreamPredicateBplusTree() {
        map = Configuration.configurationConstantForStreamIDs();
        treeRemovalThresholdUserDefined = Constants.TUPLE_REMOVAL_THRESHOLD;
        treeArchiveUserDefined = Constants.TUPLE_ARCHIVE_THRESHOLD;
        this.leftStreamSmaller = (String) map.get("LeftSmallerPredicateTuple");
        this.rightStreamSmaller = (String) map.get("RightSmallerPredicateTuple");
    }

    @Override
    public void prepare(Map<String, Object> map, TopologyContext topologyContext, OutputCollector outputCollector) {
        leftPredicateLinkedList = new LinkedList<>();
        rightPredicateLinkedList = new LinkedList<>();
        this.outputCollector = outputCollector;
        this.taskID= topologyContext.getThisTaskId();
        try{
            this.hostName= InetAddress.getLocalHost().getHostName();
        }catch (Exception e){

        }
    }

    @Override
    public void execute(Tuple tuple) {
        this.tupleArrivalTime=System.currentTimeMillis();
        tupleRemovalCountForLocal++;

        //Left Stream Tuple means Insert in Duration and Search in Time
        if (tuple.getSourceStreamId().equals(leftStreamSmaller)) {
            try {
                leftPredicateEvaluation(tuple);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        if (tuple.getSourceStreamId().equals(rightStreamSmaller)) {
            try {
                rightPredicateEvaluation(tuple);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        if (tupleRemovalCountForLocal == treeRemovalThresholdUserDefined) {
            rightPredicateLinkedList.remove(rightPredicateLinkedList.getFirst());
            leftPredicateLinkedList.remove(leftPredicateLinkedList.getFirst());
            tupleRemovalCountForLocal=0;
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declareStream(Constants.LEFT_PREDICATE_BOLT, new Fields(Constants.LEFT_HASH_SET, Constants.TUPLE_ID,
               Constants.KAFKA_TIME, Constants.KAFKA_SPOUT_TIME, Constants.SPLIT_BOLT_TIME, Constants.TASK_ID_FOR_SPLIT_BOLT,  Constants.HOST_NAME_FOR_SPLIT_BOLT, "TupleArrivalTimeChainIndex", "TupleComputationTime", "LeftPredicateTaskID","LeftPredicateHostID" ));
        outputFieldsDeclarer.declareStream(Constants.RIGHT_PREDICATE_BOLT, new Fields(Constants.RIGHT_HASH_SET, Constants.TUPLE_ID,
                Constants.KAFKA_TIME, Constants.KAFKA_SPOUT_TIME, Constants.SPLIT_BOLT_TIME, Constants.TASK_ID_FOR_SPLIT_BOLT,  Constants.HOST_NAME_FOR_SPLIT_BOLT, "TupleArrivalTimeChainIndex", "TupleComputationTime", "LeftPredicateTaskID","LeftPredicateHostID" ));

    }

    public void leftPredicateEvaluation(Tuple tuple) throws IOException {

        if (!leftPredicateLinkedList.isEmpty()) {
            //New insertion only active sub index structure that always exist on the right that is last index of linkedList
            BPlusTreeUpdated currentBPlusTreeDuration = leftPredicateLinkedList.getLast();
            currentBPlusTreeDuration.insert(tuple.getIntegerByField(Constants.TUPLE), tuple.getIntegerByField(Constants.TUPLE_ID));
            treeArchiveThresholdDuration++;
            //Archive period achieve
            if (treeArchiveThresholdDuration >= treeArchiveUserDefined) {
                treeArchiveThresholdDuration = 0;
                // New Object of BPlus
                BPlusTreeUpdated bPlusTree = new BPlusTreeUpdated(Constants.ORDER_OF_B_PLUS_TREE);
                //Added to the linked list
                leftPredicateLinkedList.add(bPlusTree);
            }
        } else {
            // When the linkedlist is empty:
            BPlusTreeUpdated bPlusTree = new BPlusTreeUpdated(Constants.ORDER_OF_B_PLUS_TREE);
            treeArchiveThresholdDuration++;
            bPlusTree.insert(tuple.getIntegerByField(Constants.TUPLE), tuple.getIntegerByField(Constants.TUPLE_ID));
            leftPredicateLinkedList.add(bPlusTree);
        }
        //Search of inequality condition and insertion into the hashset

        for (BPlusTreeUpdated bPlusTree : rightPredicateLinkedList) {

            BitSet bitSet=  bPlusTree.greaterThenSpecificValue(tuple.getIntegerByField(Constants.TUPLE_ID));
            if (bitSet != null) {

                outputCollector.emit(Constants.LEFT_PREDICATE_BOLT, new Values(convertToByteArray(bitSet), tuple.getIntegerByField(Constants.TUPLE_ID)+"L",
                        tuple.getLongByField(Constants.KAFKA_TIME), tuple.getLongByField(Constants.KAFKA_SPOUT_TIME), tuple.getLongByField(Constants.SPLIT_BOLT_TIME),  tuple.getIntegerByField(Constants.TASK_ID_FOR_SPLIT_BOLT),  tuple.getStringByField(Constants.HOST_NAME_FOR_SPLIT_BOLT), tupleArrivalTime, System.currentTimeMillis(), taskID,hostName ));
                outputCollector.ack(tuple);
            }
            //EmitLogic tomorrow
        }


    }
    public synchronized byte[] convertToByteArray(Serializable object) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (ObjectOutputStream objectOutputStream = new ObjectOutputStream(baos)) {
            objectOutputStream.writeObject(object);
            objectOutputStream.flush();
        }
        return baos.toByteArray();
    }

    public void rightPredicateEvaluation(Tuple tuple) throws IOException {
        if (!rightPredicateLinkedList.isEmpty()) {
            //Only last index for insertion
            BPlusTreeUpdated currentBPlusTreeDuration = rightPredicateLinkedList.getLast();
            currentBPlusTreeDuration.insert(tuple.getIntegerByField(Constants.TUPLE), tuple.getIntegerByField(Constants.TUPLE_ID));
            treeArchiveThresholdTime++;
            //Checking Archive period
            if (treeArchiveThresholdTime >= treeArchiveUserDefined) {
                treeArchiveThresholdTime = 0;
                BPlusTreeUpdated bPlusTree = new BPlusTreeUpdated(Constants.ORDER_OF_B_PLUS_TREE);
                rightPredicateLinkedList.add(bPlusTree);
            }
        } else {
            BPlusTreeUpdated bPlusTree = new BPlusTreeUpdated(Constants.ORDER_OF_B_PLUS_TREE);
            treeArchiveThresholdTime++;
            bPlusTree.insert(tuple.getIntegerByField(Constants.TUPLE), tuple.getIntegerByField(Constants.TUPLE_ID));
            rightPredicateLinkedList.add(bPlusTree);
        }

        for (BPlusTreeUpdated bPlusTree : leftPredicateLinkedList) {
            BitSet bitSet = bPlusTree.lessThenSpecificValue(tuple.getIntegerByField(Constants.TUPLE));
            if (bitSet != null)
              //  hashSet.addAll(integerHashSet);
            outputCollector.emit(Constants.RIGHT_PREDICATE_BOLT, new Values(convertToByteArray(bitSet), tuple.getIntegerByField(Constants.TUPLE_ID)+"R", tuple.getLongByField(Constants.KAFKA_TIME), tuple.getLongByField(Constants.KAFKA_SPOUT_TIME), tuple.getLongByField(Constants.SPLIT_BOLT_TIME),  tuple.getIntegerByField(Constants.TASK_ID_FOR_SPLIT_BOLT),  tuple.getStringByField(Constants.HOST_NAME_FOR_SPLIT_BOLT), tupleArrivalTime, System.currentTimeMillis(), taskID,hostName ));
            outputCollector.ack(tuple);
        }

    }

    public static byte[] convertHashSetToByteArray(HashSet<Integer> hashSet) {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        try {
            ObjectOutputStream oos = new ObjectOutputStream(bos);
            oos.writeObject(hashSet);
            oos.flush();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return bos.toByteArray();
    }
}
