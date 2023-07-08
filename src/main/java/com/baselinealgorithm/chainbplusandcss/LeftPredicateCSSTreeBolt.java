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

        this.outputCollector = outputCollector;
    }

    @Override
    public void execute(Tuple tuple) {
        counter++;
        if (tuple.getSourceStreamId().equals(leftStreamSmaller)) {
            leftStreamSmallerPredicateEvaluation(tuple, outputCollector);
        }
        if (tuple.getSourceStreamId().equals(rightStreamSmaller)) {
            rightStreamSmallerPredicateEvaluation(tuple, outputCollector);
        }
        if (counter >= Constants.MUTABLE_WINDOW_SIZE) {
            this.outputCollector.emit("LeftCheckForMerge",new Values(true));
            this.outputCollector.emit("RightCheckForMerge",new Values(true));
            Node nodeForLeft = leftStreamBPlusTree.leftMostNode();
            dataMergingToCSS(nodeForLeft, tuple, this.leftStreamSmaller);
            Node nodeForRight = rightStreamBPlusTree.leftMostNode();
            dataMergingToCSS(nodeForRight, tuple, this.rightStreamSmaller);
            counter = 0;
            leftStreamBPlusTree = new BPlusTree(Constants.ORDER_OF_B_PLUS_TREE);
            rightStreamBPlusTree = new BPlusTree(Constants.ORDER_OF_B_PLUS_TREE);
        }


    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declareStream(this.leftPredicateSourceStreamIDHashSet, new Fields(Constants.LEFT_HASH_SET, Constants.TUPLE_ID));
        outputFieldsDeclarer.declareStream(this.leftStreamSmaller, new Fields(Constants.BATCH_CSS_TREE_KEY, Constants.BATCH_CSS_TREE_VALUES, Constants.BATCH_CSS_FLAG));
        outputFieldsDeclarer.declareStream(this.rightStreamSmaller, new Fields(Constants.BATCH_CSS_TREE_KEY, Constants.BATCH_CSS_TREE_VALUES, Constants.BATCH_CSS_FLAG));
        outputFieldsDeclarer.declareStream("LeftCheckForMerge",new Fields("mergeFlag"));
        outputFieldsDeclarer.declareStream("RightCheckForMerge", new Fields("mergeFlag"));

    }

    private void rightStreamSmallerPredicateEvaluation(Tuple tuple, OutputCollector outputCollector) {

        rightStreamBPlusTree.insert(tuple.getIntegerByField(Constants.TUPLE), tuple.getIntegerByField(Constants.TUPLE_ID));

        HashSet<Integer> hashSetIds = leftStreamBPlusTree.smallerThenSpecificValueHashSet(tuple.getIntegerByField(Constants.TUPLE));
        if (hashSetIds != null) {
            // emitting Logic
            outputCollector.emit(this.leftPredicateSourceStreamIDHashSet, tuple, new Values(convertHashSetToByteArray(hashSetIds), tuple.getIntegerByField(Constants.TUPLE_ID)));
            outputCollector.ack(tuple);
        }


    }

    private void leftStreamSmallerPredicateEvaluation(Tuple tuple, OutputCollector outputCollector) {

        leftStreamBPlusTree.insert(tuple.getIntegerByField(Constants.TUPLE), tuple.getIntegerByField(Constants.TUPLE_ID));
        HashSet<Integer> hashSetIds = rightStreamBPlusTree.greaterThenSpecificValueHashSet(tuple.getIntegerByField(Constants.TUPLE));
        //Results form BPlus Tree
        if (hashSetIds != null) {
            outputCollector.emit(this.leftPredicateSourceStreamIDHashSet, tuple, new Values(convertHashSetToByteArray(hashSetIds), tuple.getIntegerByField(Constants.TUPLE_ID)));
            outputCollector.ack(tuple);
        }
    }

    public void dataMergingToCSS(Node node, Tuple tuple, String streamID) {

        while (node != null) {
            for (Key key : node.getKeys()) {
                outputCollector.emit(streamID, tuple, new Values(key.getKey(), convertToByteArray(key.getValues()), false));
                outputCollector.ack(tuple);
            }
        }

        outputCollector.emit(streamID, tuple, new Values(0, 0, true));
        outputCollector.ack(tuple);


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

    private static byte[] convertToByteArray(List<Integer> integerList) {
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        for (Integer num : integerList) {
            outputStream.write(num.byteValue());
        }
        return outputStream.toByteArray();
    }


}
