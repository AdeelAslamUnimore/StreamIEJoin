package com.baselinealgorithm.chainindexbplustree;

import com.configurationsandconstants.iejoinandbaseworks.Configuration;
import com.configurationsandconstants.iejoinandbaseworks.Constants;
import com.stormiequality.BTree.BPlusTree;
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
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Map;

public class LeftStreamPredicateBplusTree extends BaseRichBolt {
    private LinkedList<BPlusTree> leftPredicateLinkedList = null;
    private LinkedList<BPlusTree> rightPredicateLinkedList = null;
    private int treeRemovalThresholdUserDefined;
    private int treeArchiveThresholdDuration;
    private int treeArchiveThresholdTime;
    private int treeArchiveUserDefined;
    private int tupleRemovalCountForLocal;
    private OutputCollector outputCollector;
    private Map<String, Object> map;
    private String leftStreamSmaller;
    private String rightStreamSmaller;

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
    }

    @Override
    public void execute(Tuple tuple) {

        tupleRemovalCountForLocal++;

        //Left Stream Tuple means Insert in Duration and Search in Time
        if (tuple.getSourceStreamId().equals(leftStreamSmaller)) {
            leftPredicateEvaluation(tuple);
        }
        if (tuple.getSourceStreamId().equals(rightStreamSmaller)) {
            rightPredicateEvaluation(tuple);
        }
        if (tupleRemovalCountForLocal >= treeRemovalThresholdUserDefined) {
            rightPredicateLinkedList.remove(rightPredicateLinkedList.getLast());
            leftPredicateLinkedList.remove(leftPredicateLinkedList.getLast());
            tupleRemovalCountForLocal=0;
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declareStream(Constants.LEFT_PREDICATE_BOLT, new Fields(Constants.LEFT_HASH_SET, Constants.TUPLE_ID));
    }

    public void leftPredicateEvaluation(Tuple tuple) {

        if (!leftPredicateLinkedList.isEmpty()) {
            //New insertion only active sub index structure that always exist on the right that is last index of linkedList
            BPlusTree currentBPlusTreeDuration = leftPredicateLinkedList.getFirst();
            currentBPlusTreeDuration.insert(tuple.getIntegerByField(Constants.TUPLE), tuple.getIntegerByField(Constants.TUPLE_ID));
            treeArchiveThresholdDuration++;
            //Archive period achieve
            if (treeArchiveThresholdDuration >= treeArchiveUserDefined) {
                treeArchiveThresholdDuration = 0;
                // New Object of BPlus
                BPlusTree bPlusTree = new BPlusTree(Constants.ORDER_OF_B_PLUS_TREE);
                //Added to the linked list
                leftPredicateLinkedList.add(bPlusTree);
            }
        } else {
            // When the linkedlist is empty:
            BPlusTree bPlusTree = new BPlusTree(Constants.ORDER_OF_B_PLUS_TREE);
            treeArchiveThresholdDuration++;
            bPlusTree.insert(tuple.getIntegerByField(Constants.TUPLE), tuple.getIntegerByField(Constants.TUPLE_ID));
            leftPredicateLinkedList.add(bPlusTree);
        }
        //Search of inequality condition and insertion into the hashset

        for (BPlusTree bPlusTree : rightPredicateLinkedList) {
            HashSet<Integer> integerHashSet = bPlusTree.greaterThenSpecificValueHashSet(tuple.getIntegerByField(Constants.TUPLE_ID));
            if (integerHashSet != null) {

                outputCollector.emit(Constants.LEFT_PREDICATE_BOLT, new Values(convertHashSetToByteArray(integerHashSet), tuple.getIntegerByField(Constants.TUPLE_ID)));
                outputCollector.ack(tuple);
            }
            //EmitLogic tomorrow
        }


    }

    public void rightPredicateEvaluation(Tuple tuple) {
        if (!rightPredicateLinkedList.isEmpty()) {
            //Only last index for insertion
            BPlusTree currentBPlusTreeDuration = rightPredicateLinkedList.getFirst();
            currentBPlusTreeDuration.insert(tuple.getIntegerByField(Constants.TUPLE), tuple.getIntegerByField(Constants.TUPLE_ID));
            treeArchiveThresholdTime++;
            //Checking Archive period
            if (treeArchiveThresholdTime >= treeArchiveUserDefined) {
                treeArchiveThresholdTime = 0;
                BPlusTree bPlusTree = new BPlusTree(Constants.ORDER_OF_B_PLUS_TREE);
                rightPredicateLinkedList.add(bPlusTree);
            }
        } else {
            BPlusTree bPlusTree = new BPlusTree(Constants.ORDER_OF_B_PLUS_TREE);
            treeArchiveThresholdTime++;
            bPlusTree.insert(tuple.getIntegerByField(Constants.TUPLE), tuple.getIntegerByField(Constants.TUPLE_ID));
            rightPredicateLinkedList.add(bPlusTree);
        }

        for (BPlusTree bPlusTree : leftPredicateLinkedList) {
            HashSet<Integer> integerHashSet = bPlusTree.smallerThenSpecificValueHashSet(tuple.getIntegerByField(Constants.TUPLE));
            if (integerHashSet != null)
              //  hashSet.addAll(integerHashSet);
            outputCollector.emit(Constants.LEFT_PREDICATE_BOLT, new Values(convertHashSetToByteArray(integerHashSet), tuple.getIntegerByField(Constants.TUPLE_ID)));
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
