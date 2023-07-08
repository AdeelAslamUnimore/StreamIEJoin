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
import java.util.LinkedList;
import java.util.Map;

public class RightPredicateCSSTreeBolt extends BaseRichBolt {
    private BPlusTree leftStreamBPlusTree = null;
    private BPlusTree rightStreamBPlusTree = null;
    private int archiveCount;
    private int revenueArchiveCount;
    private int costArchiveCount;
    private int tupleRemovalCount;
    private int tupleRemovalCountForLocal;
    private int orderOfTreeBothBPlusTreeAndCSSTree;
    private OutputCollector outputCollector;
    private String leftStreamGreater;
    private String rightStreamGreater;
    private String rightStreamHashSetEmitterStreamID;

    public RightPredicateCSSTreeBolt() {
        this.archiveCount = Constants.MUTABLE_WINDOW_SIZE;
        //   this.tupleRemovalCount= Constants.IMMUTABLE_WINDOW_SIZE;
        this.orderOfTreeBothBPlusTreeAndCSSTree = Constants.ORDER_OF_CSS_TREE;
        Map<String, Object> map = Configuration.configurationConstantForStreamIDs();
        this.leftStreamGreater = (String) map.get("LeftGreaterPredicateTuple");
        this.rightStreamGreater = (String) map.get("RightGreaterPredicateTuple");
        this.rightStreamHashSetEmitterStreamID = (String) map.get("RightPredicateSourceStreamIDHashSet");
    }

    @Override
    public void prepare(Map<String, Object> map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.leftStreamBPlusTree = new BPlusTree(orderOfTreeBothBPlusTreeAndCSSTree);
        this.rightStreamBPlusTree = new BPlusTree(orderOfTreeBothBPlusTreeAndCSSTree);
        this.revenueArchiveCount = 0;
        this.costArchiveCount = 0;
        this.outputCollector = outputCollector;
    }

    @Override
    public void execute(Tuple tuple) {
        if (tuple.getSourceStreamId().equals(leftStreamGreater)) {
            leftPredicateEvaluation(tuple, outputCollector);
        }
        if (tuple.getSourceStreamId().equals(rightStreamGreater)) {
            rightPredicateEvaluation(tuple, outputCollector);
        }


    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declareStream(this.rightStreamHashSetEmitterStreamID, new Fields(Constants.RIGHT_HASH_SET, Constants.TUPLE_ID));

    }

    private void rightPredicateEvaluation(Tuple tuple, OutputCollector outputCollector) {
        // Insertion
        rightStreamBPlusTree.insert(tuple.getIntegerByField(Constants.TUPLE), tuple.getIntegerByField(Constants.TUPLE_ID));
        costArchiveCount++;
        HashSet<Integer> hashSetIds = leftStreamBPlusTree.lessThenSpecificValueHash(tuple.getIntegerByField(Constants.TUPLE));
        //Results form BPlus Tree searching
        if (hashSetIds != null) {
            // emitting Logic
            outputCollector.emit(this.rightStreamHashSetEmitterStreamID, tuple, new Values(convertHashSetToByteArray(hashSetIds), tuple.getIntegerByField(Constants.TUPLE_ID)));
            outputCollector.ack(tuple);
        }
        //Archive Interval achieve Then merge the active BPlus Tree into CSS Tree
        if (costArchiveCount >= archiveCount) {
//            costArchiveCount=0;
////            com.stormiequality.BTree.Node node= rightStreamBPlusTree.leftMostNode();
////            CSSTree cssTree= new CSSTree(orderOfTreeBothBPlusTreeAndCSSTree);
////            while(node!=null){
////                for(com.stormiequality.BTree.Key key: node.getKeys()){
////                    cssTree.insertBulkUpdate(key.getKey(),key.getValues());
////                }
////
////                node= node.getNext();
////            }
////            //Insert into the linkedList
////

        }
    }

    private void leftPredicateEvaluation(Tuple tuple, OutputCollector outputCollector) {
        leftStreamBPlusTree.insert(tuple.getIntegerByField(Constants.TUPLE), tuple.getIntegerByField(Constants.TUPLE_ID));
        revenueArchiveCount++;
        HashSet<Integer> hashSetIds = rightStreamBPlusTree.greaterThenSpecificValueHashSet(tuple.getIntegerByField(Constants.TUPLE));
        if (hashSetIds != null) {
            // emitting Logic
            outputCollector.emit(this.rightStreamHashSetEmitterStreamID, tuple, new Values(convertHashSetToByteArray(hashSetIds), tuple.getIntegerByField(Constants.TUPLE_ID)));
            outputCollector.ack(tuple);
        }
        //Results form BPlus Tree
        // Results from immutable B+ CSS Tree

        //Archive Interval achieve Then merge the active BPlus Tree into CSS Tree
        if (revenueArchiveCount >= archiveCount) {
//            revenueArchiveCount=0;
//            Node node= leftStreamBPlusTree.leftMostNode();
//            CSSTree cssTree= new CSSTree(orderOfTreeBothBPlusTreeAndCSSTree);
//            while(node!=null){
//                for(Key key: node.getKeys()){
//                    cssTree.insertBulkUpdate(key.getKey(),key.getValues());
//                }
//
//                node= node.getNext();
//            }
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
