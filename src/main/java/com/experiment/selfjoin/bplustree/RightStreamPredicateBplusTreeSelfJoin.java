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

public class RightStreamPredicateBplusTreeSelfJoin extends BaseRichBolt {
    private LinkedList<BPlusTree> rightBPlusTreeLinkedList = null;
    private int treeRemovalThresholdUserDefined;
    private int treeArchiveThresholdRevenue;
    private int treeArchiveThresholdCost;
    private int treeArchiveUserDefined;
    private int tupleRemovalCountForLocal;
    private String rightStreamSmaller;
    private OutputCollector outputCollector;
    private Map<String, Object> map;

    public RightStreamPredicateBplusTreeSelfJoin() {
        map = Configuration.configurationConstantForStreamIDs();
        treeRemovalThresholdUserDefined = Constants.TUPLE_REMOVAL_THRESHOLD;
        treeArchiveUserDefined = Constants.TUPLE_ARCHIVE_THRESHOLD;
        this.rightStreamSmaller = (String) map.get("RightSmallerPredicateTuple");
    }

    @Override
    public void prepare(Map<String, Object> map, TopologyContext topologyContext, OutputCollector outputCollector) {

        rightBPlusTreeLinkedList = new LinkedList<>();
        this.outputCollector = outputCollector;
    }

    @Override
    public void execute(Tuple tuple) {
        tupleRemovalCountForLocal++;
        if (tuple.getSourceStreamId().equals(rightStreamSmaller)) {
            if (!rightBPlusTreeLinkedList.isEmpty()) {
                BPlusTree currentBplusTreeDuration = rightBPlusTreeLinkedList.getLast();
                currentBplusTreeDuration.insert(tuple.getIntegerByField(Constants.TUPLE), tuple.getIntegerByField(Constants.TUPLE_ID));
                treeArchiveThresholdRevenue++;
                if (treeArchiveThresholdRevenue >= treeArchiveUserDefined) {
                    treeArchiveThresholdRevenue = 0;
                    BPlusTree bPlusTree = new BPlusTree(Constants.ORDER_OF_B_PLUS_TREE);
                    rightBPlusTreeLinkedList.add(bPlusTree);
                }
            } else {
                BPlusTree bPlusTree = new BPlusTree(Constants.ORDER_OF_B_PLUS_TREE);
                treeArchiveThresholdRevenue++;
                bPlusTree.insert(tuple.getIntegerByField(Constants.TUPLE), tuple.getIntegerByField(Constants.TUPLE_ID));
                rightBPlusTreeLinkedList.add(bPlusTree);
            }
            for (BPlusTree bPlusTree : rightBPlusTreeLinkedList) {
                HashSet<Integer> hashSetGreater = bPlusTree.greaterThenSpecificValueHashSet(tuple.getIntegerByField(Constants.TUPLE));

                if(hashSetGreater!=null) {
                    outputCollector.emit(Constants.RIGHT_PREDICATE_BOLT, new Values(convertHashSetToByteArray(hashSetGreater), tuple.getIntegerByField(Constants.TUPLE_ID)));
                    outputCollector.ack(tuple);
                }
            }




        }
        if (tupleRemovalCountForLocal >= treeRemovalThresholdUserDefined) {
            rightBPlusTreeLinkedList.remove(rightBPlusTreeLinkedList.getFirst());

            tupleRemovalCountForLocal= 0;

        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declareStream(Constants.RIGHT_PREDICATE_BOLT, new Fields(Constants.RIGHT_HASH_SET,Constants.TUPLE_ID));

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
