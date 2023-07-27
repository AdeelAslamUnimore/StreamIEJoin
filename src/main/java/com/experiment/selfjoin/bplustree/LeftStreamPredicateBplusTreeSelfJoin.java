package com.experiment.selfjoin.bplustree;

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

public class LeftStreamPredicateBplusTreeSelfJoin extends BaseRichBolt {
    private LinkedList<BPlusTree> leftPredicateLinkedList = null;
    private int treeRemovalThresholdUserDefined;
    private int treeArchiveThresholdDuration;
    private int treeArchiveThresholdTime;
    private int treeArchiveUserDefined;
    private int tupleRemovalCountForLocal;
    private OutputCollector outputCollector;
    private Map<String, Object> map;
    private String leftStreamGreater;


    // Constructor parameter for tuples
    public LeftStreamPredicateBplusTreeSelfJoin() {
        map = Configuration.configurationConstantForStreamIDs();
        treeRemovalThresholdUserDefined = Constants.TUPLE_REMOVAL_THRESHOLD;
        treeArchiveUserDefined = Constants.TUPLE_ARCHIVE_THRESHOLD;
        this.leftStreamGreater = (String) map.get("LeftGreaterPredicateTuple");

    }

    @Override
    public void prepare(Map<String, Object> map, TopologyContext topologyContext, OutputCollector outputCollector) {
        leftPredicateLinkedList = new LinkedList<>();
        this.outputCollector = outputCollector;
    }

    @Override
    public void execute(Tuple tuple) {

        tupleRemovalCountForLocal++;

        //Left Stream Tuple means Insert in Duration and Search in Time
        if (tuple.getSourceStreamId().equals(leftStreamGreater)) {
            leftPredicateEvaluation(tuple);
        }
        if (tupleRemovalCountForLocal >= treeRemovalThresholdUserDefined) {
            leftPredicateLinkedList.remove(leftPredicateLinkedList.getFirst());
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
            BPlusTree currentBPlusTreeDuration = leftPredicateLinkedList.getLast();
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

        for (BPlusTree bPlusTree : leftPredicateLinkedList) {
            HashSet<Integer> integerHashSet = bPlusTree.smallerThenSpecificValueHashSet(tuple.getIntegerByField(Constants.TUPLE_ID));
            if (integerHashSet != null) {

                outputCollector.emit(Constants.LEFT_PREDICATE_BOLT, new Values(convertHashSetToByteArray(integerHashSet), tuple.getIntegerByField(Constants.TUPLE_ID)));
                outputCollector.ack(tuple);
            }

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
