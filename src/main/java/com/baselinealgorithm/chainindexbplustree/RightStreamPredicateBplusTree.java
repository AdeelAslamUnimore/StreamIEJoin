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

public class RightStreamPredicateBplusTree extends BaseRichBolt {
    private LinkedList<BPlusTreeUpdated> leftBPlusTreeLinkedList = null;
    private LinkedList<BPlusTreeUpdated> rightBPlusTreeLinkedList = null;
    private int treeRemovalThresholdUserDefined;
    private int treeArchiveThresholdRevenue;
    private int treeArchiveThresholdCost;
    private int treeArchiveUserDefined;
    private int tupleRemovalCountForLocal;
    private String leftStreamGreater;
    private String rightStreamGreater;
    private OutputCollector outputCollector;
    private Map<String, Object> map;
    private long tupleArrivalTime;
    private int taskID;
    private String hostName;

    public RightStreamPredicateBplusTree() {
        map = Configuration.configurationConstantForStreamIDs();
        treeRemovalThresholdUserDefined = Constants.TUPLE_REMOVAL_THRESHOLD;
        treeArchiveUserDefined = Constants.TUPLE_ARCHIVE_THRESHOLD;
        this.leftStreamGreater = (String) map.get("LeftGreaterPredicateTuple");
        this.rightStreamGreater = (String) map.get("RightGreaterPredicateTuple");
    }

    @Override
    public void prepare(Map<String, Object> map, TopologyContext topologyContext, OutputCollector outputCollector) {
        leftBPlusTreeLinkedList = new LinkedList<>();
        rightBPlusTreeLinkedList = new LinkedList<>();
        this.outputCollector = outputCollector;
        this.taskID=topologyContext.getThisTaskId();
        try{
            this.hostName= InetAddress.getLocalHost().getHostName();
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    @Override
    public void execute(Tuple tuple) {
        tupleArrivalTime=System.currentTimeMillis();
        tupleRemovalCountForLocal++;
        if (tuple.getSourceStreamId().equals(leftStreamGreater)) {
            if (!leftBPlusTreeLinkedList.isEmpty()) {
                BPlusTreeUpdated currentBplusTreeDuration = leftBPlusTreeLinkedList.getLast();
                currentBplusTreeDuration.insert(tuple.getIntegerByField(Constants.TUPLE), tuple.getIntegerByField(Constants.TUPLE_ID));
                treeArchiveThresholdRevenue++;
                if (treeArchiveThresholdRevenue >= treeArchiveUserDefined) {
                    treeArchiveThresholdRevenue = 0;
                    BPlusTreeUpdated bPlusTree = new BPlusTreeUpdated(Constants.ORDER_OF_B_PLUS_TREE);
                    leftBPlusTreeLinkedList.add(bPlusTree);
                }
            } else {
                BPlusTreeUpdated bPlusTree = new BPlusTreeUpdated(Constants.ORDER_OF_B_PLUS_TREE);
                treeArchiveThresholdRevenue++;
                bPlusTree.insert(tuple.getIntegerByField(Constants.TUPLE), tuple.getIntegerByField(Constants.TUPLE_ID));
                leftBPlusTreeLinkedList.add(bPlusTree);
            }
            for (BPlusTreeUpdated bPlusTree : rightBPlusTreeLinkedList) {
               BitSet hashSetGreater = bPlusTree.lessThenSpecificValue(tuple.getIntegerByField(Constants.TUPLE));
                //EmitLogic tomorrow
                if(hashSetGreater!=null) {
                    try {
                        outputCollector.emit(Constants.LEFT_PREDICATE_BOLT, new Values(convertToByteArray(hashSetGreater), tuple.getIntegerByField(Constants.TUPLE_ID)+"L",
                                tuple.getLongByField(Constants.KAFKA_TIME), tuple.getLongByField(Constants.KAFKA_SPOUT_TIME), tuple.getLongByField(Constants.SPLIT_BOLT_TIME), tuple.getIntegerByField(Constants.TASK_ID_FOR_SPLIT_BOLT)
                                , tuple.getStringByField( Constants.HOST_NAME_FOR_SPLIT_BOLT),tupleArrivalTime, System.currentTimeMillis(), taskID, hostName ));
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                    outputCollector.ack(tuple);
                }
            }


        }
        if (tuple.getSourceStreamId().equals(rightStreamGreater)) {
            if (!rightBPlusTreeLinkedList.isEmpty()) {
                BPlusTreeUpdated currentBplusTreeDuration = rightBPlusTreeLinkedList.getLast();
                currentBplusTreeDuration.insert(tuple.getIntegerByField(Constants.TUPLE), tuple.getIntegerByField(Constants.TUPLE_ID));
                treeArchiveThresholdCost++;
                if (treeArchiveThresholdCost >= treeArchiveUserDefined) {
                    treeArchiveThresholdCost = 0;
                    BPlusTreeUpdated bPlusTree = new BPlusTreeUpdated(Constants.ORDER_OF_B_PLUS_TREE);
                    rightBPlusTreeLinkedList.add(bPlusTree);
                }
            } else {
                BPlusTreeUpdated bPlusTree = new BPlusTreeUpdated(Constants.ORDER_OF_B_PLUS_TREE);
                treeArchiveThresholdCost++;
                bPlusTree.insert(tuple.getIntegerByField(Constants.TUPLE), tuple.getIntegerByField(Constants.TUPLE_ID));
                rightBPlusTreeLinkedList.add(bPlusTree);
            }

            for (BPlusTreeUpdated bPlusTree : leftBPlusTreeLinkedList) {
                BitSet hashSetsLess = bPlusTree.greaterThenSpecificValue(tuple.getIntegerByField(Constants.TUPLE));
                if(hashSetsLess!=null) {
                    try {
                        outputCollector.emit(Constants.RIGHT_PREDICATE_BOLT, new Values(convertToByteArray(hashSetsLess), tuple.getIntegerByField(Constants.TUPLE_ID)+"R",
                               tuple.getLongByField( Constants.KAFKA_TIME), tuple.getLongByField( Constants.KAFKA_SPOUT_TIME),  tuple.getLongByField(Constants.SPLIT_BOLT_TIME), tuple.getIntegerByField(Constants.TASK_ID_FOR_SPLIT_BOLT),  tuple.getStringByField(Constants.HOST_NAME_FOR_SPLIT_BOLT), tupleArrivalTime, System.currentTimeMillis(), taskID,hostName));
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                    outputCollector.ack(tuple);
                }
            }

        }
        if (tupleRemovalCountForLocal >= treeRemovalThresholdUserDefined) {
            rightBPlusTreeLinkedList.remove(rightBPlusTreeLinkedList.getFirst());
            leftBPlusTreeLinkedList.remove(leftBPlusTreeLinkedList.getFirst());
            tupleRemovalCountForLocal= 0;

        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declareStream(Constants.LEFT_PREDICATE_BOLT, new Fields(Constants.LEFT_HASH_SET,Constants.TUPLE_ID
                ,
                Constants.KAFKA_TIME, Constants.KAFKA_SPOUT_TIME, Constants.SPLIT_BOLT_TIME, Constants.TASK_ID_FOR_SPLIT_BOLT,  Constants.HOST_NAME_FOR_SPLIT_BOLT, "TupleArrivalTimeChainIndex", "TupleComputationTime", "RightPredicateTaskID","RightPredicateHostID"));

        outputFieldsDeclarer.declareStream(Constants.RIGHT_PREDICATE_BOLT, new Fields(Constants.RIGHT_HASH_SET,Constants.TUPLE_ID
                ,
                Constants.KAFKA_TIME, Constants.KAFKA_SPOUT_TIME, Constants.SPLIT_BOLT_TIME, Constants.TASK_ID_FOR_SPLIT_BOLT,  Constants.HOST_NAME_FOR_SPLIT_BOLT, "TupleArrivalTimeChainIndex", "TupleComputationTime", "RightPredicateTaskID","RightPredicateHostID"));

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
    public synchronized byte[] convertToByteArray(Serializable object) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (ObjectOutputStream objectOutputStream = new ObjectOutputStream(baos)) {
            objectOutputStream.writeObject(object);
            objectOutputStream.flush();
        }
        return baos.toByteArray();
    }
}
