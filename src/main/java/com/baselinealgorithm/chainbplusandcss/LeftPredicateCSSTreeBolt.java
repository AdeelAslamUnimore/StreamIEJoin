package com.baselinealgorithm.chainbplusandcss;

import com.configurationsandconstants.iejoinandbaseworks.Configuration;
import com.configurationsandconstants.iejoinandbaseworks.Constants;
import com.stormiequality.BTree.BPlusTree;
import com.stormiequality.BTree.Key;
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
import java.util.HashSet;
import java.util.Map;

public class LeftPredicateCSSTreeBolt extends BaseRichBolt {
    private BPlusTree leftStreamBPlusTree = null;
    private BPlusTree rightStreamBPlusTree = null;
    private int archiveCount;
    private int leftStreamArchiveCount;
    private int rightStreamArchiveCount;
    private int orderOfTreeBothBPlusTreeAndCSSTree;
    private int tupleRemovalCountForLocal;
    private OutputCollector outputCollector;
    private String leftStreamSmaller;
    private String rightStreamSmaller;
    private String leftPredicateSourceStreamIDHashSet;

    public LeftPredicateCSSTreeBolt() {
        this.archiveCount = Constants.MUTABLE_WINDOW_SIZE;
        this.orderOfTreeBothBPlusTreeAndCSSTree = Constants.ORDER_OF_CSS_TREE;
        Map<String, Object> map = Configuration.configurationConstantForStreamIDs();
        this.leftStreamSmaller = (String) map.get("LeftSmallerPredicateTuple");
        this.rightStreamSmaller = (String) map.get("RightSmallerPredicateTuple");
        this.leftPredicateSourceStreamIDHashSet = (String) map.get("LeftPredicateSourceStreamIDHashSet");
    }

    @Override
    public void prepare(Map<String, Object> map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.leftStreamBPlusTree = new BPlusTree(orderOfTreeBothBPlusTreeAndCSSTree);
        this.rightStreamBPlusTree = new BPlusTree(orderOfTreeBothBPlusTreeAndCSSTree);
        this.leftStreamArchiveCount = 0;
        this.rightStreamArchiveCount = 0;
        this.outputCollector = outputCollector;
    }

    @Override
    public void execute(Tuple tuple) {
        tupleRemovalCountForLocal++;
        if (tuple.getSourceStreamId().equals(leftStreamSmaller)) {
            leftStreamSmallerPredicateEvaluation(tuple, outputCollector);
        }
        if (tuple.getSourceStreamId().equals(rightStreamSmaller)) {
            rightStreamSmallerPredicateEvaluation(tuple, outputCollector);
        }


    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declareStream(this.leftPredicateSourceStreamIDHashSet, new Fields(Constants.LEFT_HASH_SET, Constants.TUPLE_ID));
        // outputFieldsDeclarer.declareStream(this.rightStreamSmaller,new Fields(Constants.RIGHT_HASH_SET,Constants.TUPLE_ID));

    }

    private void rightStreamSmallerPredicateEvaluation(Tuple tuple, OutputCollector outputCollector) {
        // Insertion
        rightStreamBPlusTree.insert(tuple.getIntegerByField(Constants.TUPLE), tuple.getIntegerByField(Constants.TUPLE_ID));
        rightStreamArchiveCount++;
        HashSet<Integer> hashSetIds = leftStreamBPlusTree.lessThenSpecificValueHash(tuple.getIntegerByField(Constants.TUPLE));
        if (hashSetIds != null) {
            // emitting Logic
            outputCollector.emit(this.leftPredicateSourceStreamIDHashSet, tuple, new Values(convertHashSetToByteArray(hashSetIds), tuple.getIntegerByField(Constants.TUPLE_ID)));
            outputCollector.ack(tuple);
        }
        //Results form BPlus Tree searching

        //Archive Interval achieve Then merge the active BPlus Tree into CSS Tree
        if (rightStreamArchiveCount >= archiveCount) {
//            rightStreamArchiveCount = 0;
//            Node node = rightStreamBPlusTree.leftMostNode();
//            CSSTree cssTree = new CSSTree(orderOfTreeBothBPlusTreeAndCSSTree);
//            while (node != null) {
//                for (Key key : node.getKeys()) {
//                    cssTree.insertBulkUpdate(key.getKey(), key.getValues());
//                }
//
//                node = node.getNext();
//            }
            //Insert into the linkedList

        }
    }

    private void leftStreamSmallerPredicateEvaluation(Tuple tuple, OutputCollector outputCollector) {
        leftStreamArchiveCount++;
        leftStreamBPlusTree.insert(tuple.getIntegerByField(Constants.TUPLE), tuple.getIntegerByField(Constants.TUPLE_ID));
        HashSet<Integer> hashSetIds = rightStreamBPlusTree.greaterThenSpecificValueHashSet(tuple.getIntegerByField(Constants.TUPLE));
        //Results form BPlus Tree
        if (hashSetIds != null) {
            outputCollector.emit(this.leftPredicateSourceStreamIDHashSet, tuple, new Values(convertHashSetToByteArray(hashSetIds), tuple.getIntegerByField(Constants.TUPLE_ID)));
            outputCollector.ack(tuple);
        }
        // Results from immutable B+ CSS Tree

        /// Writer Emittier of tuples

        //Archive Interval achieve Then merge the active BPlus Tree into CSS Tree
        if (leftStreamArchiveCount >= archiveCount) {
//            leftStreamArchiveCount = 0;
////            Node node = leftStreamBPlusTree.leftMostNode();
////            CSSTree cssTree = new CSSTree(orderOfTreeBothBPlusTreeAndCSSTree);
////            while (node != null) {
////                for (Key key : node.getKeys()) {
////                    cssTree.insertBulkUpdate(key.getKey(), key.getValues());
////                }
////
////                node = node.getNext();
////            }
            //Insert into the linkedList

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
