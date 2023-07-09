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
import java.util.List;
import java.util.Map;

public class LeftPredicateCSSTreeBolt extends BaseRichBolt {
    private BPlusTree leftStreamBPlusTree = null;
    private BPlusTree rightStreamBPlusTree = null;
    private int archiveCount;
    private int counter;
    private int orderOfTreeBothBPlusTreeAndCSSTree;

    private OutputCollector outputCollector;
    private String leftStreamSmaller;
    private String rightStreamSmaller;
    private String leftPredicateSourceStreamIDHashSet;

    public LeftPredicateCSSTreeBolt() {
        // Map that defines the constants StreamID For Every bolt
        Map<String, Object> map = Configuration.configurationConstantForStreamIDs();
        // Left Stream ID from UP Stream Component Bolt
        this.leftStreamSmaller = (String) map.get("LeftSmallerPredicateTuple");
        // Right Stream ID from UP Stream Component Bolt
        this.rightStreamSmaller = (String) map.get("RightSmallerPredicateTuple");
        // Stream ID for emitting
        this.leftPredicateSourceStreamIDHashSet = (String) map.get("LeftPredicateSourceStreamIDHashSet");
    }

    @Override
    public void prepare(Map<String, Object> map, TopologyContext topologyContext, OutputCollector outputCollector) {
        // For Mutable part initialization

        this.leftStreamBPlusTree = new BPlusTree(orderOfTreeBothBPlusTreeAndCSSTree);
        this.rightStreamBPlusTree = new BPlusTree(orderOfTreeBothBPlusTreeAndCSSTree);
        // Output collector
        this.outputCollector = outputCollector;
    }

    @Override
    public void execute(Tuple tuple) {
        counter++; // Counter for mutable to immutable merge
        if (tuple.getSourceStreamId().equals(leftStreamSmaller)) {
            // Stream from left StreamID; per
            leftStreamSmallerPredicateEvaluation(tuple, outputCollector);
        }
        if (tuple.getSourceStreamId().equals(rightStreamSmaller)) {
            // Stream From right StreamID
            rightStreamSmallerPredicateEvaluation(tuple, outputCollector);
        }
        if (counter >= Constants.MUTABLE_WINDOW_SIZE) {
            // When merging counter reached:
            // Just 2 flags to downstream to holds data for compeletion of tuples;
            // One flag for left and other for right
            this.outputCollector.emit("LeftCheckForMerge", new Values(true));
            this.outputCollector.emit("RightCheckForMerge", new Values(true));
            // start merging: Node from left
            Node nodeForLeft = leftStreamBPlusTree.leftMostNode();
            // data merging to CSS
            dataMergingToCSS(nodeForLeft, tuple, this.leftStreamSmaller);
            // start merging node from right
            Node nodeForRight = rightStreamBPlusTree.leftMostNode();
            // data for merging
            dataMergingToCSS(nodeForRight, tuple, this.rightStreamSmaller);
            counter = 0; // reinitialize counter
            // Re initialization of BTrees:
            leftStreamBPlusTree = new BPlusTree(Constants.ORDER_OF_B_PLUS_TREE);
            rightStreamBPlusTree = new BPlusTree(Constants.ORDER_OF_B_PLUS_TREE);
        }


    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        // Declaring for hashset evaluation
        outputFieldsDeclarer.declareStream(this.leftPredicateSourceStreamIDHashSet, new Fields(Constants.LEFT_HASH_SET, Constants.TUPLE_ID));
        // Two stream same as upstream as downstream for merging
        outputFieldsDeclarer.declareStream(this.leftStreamSmaller, new Fields(Constants.BATCH_CSS_TREE_KEY, Constants.BATCH_CSS_TREE_VALUES, Constants.BATCH_CSS_FLAG));
        outputFieldsDeclarer.declareStream(this.rightStreamSmaller, new Fields(Constants.BATCH_CSS_TREE_KEY, Constants.BATCH_CSS_TREE_VALUES, Constants.BATCH_CSS_FLAG));
        // Flags holding tmp data during merge
        outputFieldsDeclarer.declareStream("LeftCheckForMerge", new Fields("mergeFlag"));
        outputFieldsDeclarer.declareStream("RightCheckForMerge", new Fields("mergeFlag"));

    }

    private void rightStreamSmallerPredicateEvaluation(Tuple tuple, OutputCollector outputCollector) {
// Insert data into BPlus tree:
        rightStreamBPlusTree.insert(tuple.getIntegerByField(Constants.TUPLE), tuple.getIntegerByField(Constants.TUPLE_ID));
// Search tuple from other field If Tuple from R it evaluates against S and Same as for R
        HashSet<Integer> hashSetIds = leftStreamBPlusTree.smallerThenSpecificValueHashSet(tuple.getIntegerByField(Constants.TUPLE));
        if (hashSetIds != null) {
            // emitting Logic
            // Emit for hash set evaluation.
            outputCollector.emit(this.leftPredicateSourceStreamIDHashSet, tuple, new Values(convertHashSetToByteArray(hashSetIds), tuple.getIntegerByField(Constants.TUPLE_ID)));
            outputCollector.ack(tuple);
        }


    }

    // Right Stream evaluation R<S
    // Here in R<S means from all greater and vice versa for other
    private void leftStreamSmallerPredicateEvaluation(Tuple tuple, OutputCollector outputCollector) {
        // Tuple from S stream evaluates
        leftStreamBPlusTree.insert(tuple.getIntegerByField(Constants.TUPLE), tuple.getIntegerByField(Constants.TUPLE_ID));
        HashSet<Integer> hashSetIds = rightStreamBPlusTree.greaterThenSpecificValueHashSet(tuple.getIntegerByField(Constants.TUPLE));
        //Results form BPlus Tree
        if (hashSetIds != null) {
            outputCollector.emit(this.leftPredicateSourceStreamIDHashSet, tuple, new Values(convertHashSetToByteArray(hashSetIds), tuple.getIntegerByField(Constants.TUPLE_ID)));
            outputCollector.ack(tuple);
        }
    }

    // Simply iterating from from left most node to the end for iteration
    public void dataMergingToCSS(Node node, Tuple tuple, String streamID) {
// node to end of all leaf nodes log n for finding the left node from root and linear scna to end n
        while (node != null) {
            for (Key key : node.getKeys()) {
                outputCollector.emit(streamID, tuple, new Values(key.getKey(), convertToByteArray(key.getValues()), false));
                outputCollector.ack(tuple);
            }
            node = node.getNext();
        }
        // last tuple that indicate the completeness of all tuples with true flag
        outputCollector.emit(streamID, tuple, new Values(0, 0, true));
        outputCollector.ack(tuple);


    }
    // Byte array for Hash Set
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

    // Byte array conversion because While emitting we are emitting values as bytes array to downstream
    // A key has multple values these values are byte array
    private static byte[] convertToByteArray(List<Integer> integerList) {
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        for (Integer num : integerList) {
            outputStream.write(num.byteValue());
        }
        return outputStream.toByteArray();
    }


}
