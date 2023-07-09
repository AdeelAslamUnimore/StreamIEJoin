package com.baselinealgorithm.chainbplusandcss;

import com.configurationsandconstants.iejoinandbaseworks.Configuration;
import com.configurationsandconstants.iejoinandbaseworks.Constants;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.*;

public class LeftPredicateImmutableCSSBolt extends BaseRichBolt {
    private LinkedList<CSSTree> leftStreamLinkedListCSSTree = null;
    private LinkedList<CSSTree> rightStreamLinkedListCSSTree = null;
    private String leftStreamSmaller;
    private String rightStreamSmaller;

    private CSSTree leftStreamCSSTree = null;
    private CSSTree rightStreamCSSTree = null;
    private boolean leftStreamMergeSmaller = false;
    private boolean rightStreamMergeSmaller = false;
    private Queue<Tuple> leftStreamSmallerQueueMerge = null;
    private Queue<Tuple> rightStreamSmallerQueueMerge = null;

    private boolean leftBooleanTupleRemovalCounter = false;
    private boolean rightBooleanTupleRemovalCounter = false;
    private static int tupleRemovalCount = 0;

    private OutputCollector outputCollector;

    public LeftPredicateImmutableCSSBolt() {
        Map<String, Object> map = Configuration.configurationConstantForStreamIDs();
        this.leftStreamSmaller = (String) map.get("LeftSmallerPredicateTuple");
        this.rightStreamSmaller = (String) map.get("RightSmallerPredicateTuple");
    }

    @Override
    public void prepare(Map<String, Object> map, TopologyContext topologyContext, OutputCollector outputCollector) {
        leftStreamLinkedListCSSTree = new LinkedList<>();
        rightStreamLinkedListCSSTree = new LinkedList<>();
        leftStreamCSSTree = new CSSTree(Constants.ORDER_OF_CSS_TREE);
        rightStreamCSSTree = new CSSTree(Constants.ORDER_OF_CSS_TREE);
        // Merge operation
        this.leftStreamSmallerQueueMerge = new LinkedList<>();
        this.rightStreamSmallerQueueMerge = new LinkedList<>();

        this.outputCollector = outputCollector;

    }

    @Override
    public void execute(Tuple tuple) {
        if (tuple.getSourceStreamId().equals("LeftStreamTuples")) {
            if (leftStreamMergeSmaller) {
                this.leftStreamSmallerQueueMerge.offer(tuple);
            }
            HashSet<Integer> leftHashSet = probingResultsSmaller(tuple, rightStreamLinkedListCSSTree);
           if(leftHashSet!=null) {

               outputCollector.emit("LeftPredicate", tuple, new Values(convertHashSetToByteArray(leftHashSet), tuple.getIntegerByField("ID")));
               outputCollector.ack(tuple);
           }

        }
        if (tuple.getSourceStreamId().equals("RightStream")) {
            if (rightStreamMergeSmaller) {
                this.rightStreamSmallerQueueMerge.offer(tuple);
            }
            HashSet<Integer> rightHashSet = probingResultsGreater(tuple, leftStreamLinkedListCSSTree);
            if(rightHashSet!=null) {

                outputCollector.emit("LeftPredicate", tuple, new Values(convertHashSetToByteArray(rightHashSet), tuple.getIntegerByField("ID")));
                outputCollector.ack(tuple);
            }
        }

        if (tuple.getSourceStreamId().equals(leftStreamSmaller)) {
            leftInsertionTuplesSmaller(tuple, leftStreamLinkedListCSSTree, leftStreamMergeSmaller, leftStreamSmallerQueueMerge);
        }
        if (tuple.getSourceStreamId().equals(rightStreamSmaller)) {
            rightInsertionTupleSmaller(tuple, rightStreamLinkedListCSSTree,  rightStreamMergeSmaller, rightStreamSmallerQueueMerge);
        }
        if (tuple.getSourceStreamId().equals("LeftCheckForMerge")) {
            //this.leftStreamSmallerQueueMerge.offer(tuple);
            leftStreamMergeSmaller = true;
        }
        if (tuple.getSourceStreamId().equals("RightCheckForMerge")) {
            // this.rightStreamSmallerQueueMerge.offer(tuple);
            rightStreamMergeSmaller = true;
        }
        /// Change For Both Right Stream and Left Stream depend upon which sliding window you are using
        if (leftBooleanTupleRemovalCounter && rightBooleanTupleRemovalCounter) {
            rightStreamLinkedListCSSTree.remove(rightStreamLinkedListCSSTree.getLast());
            leftStreamLinkedListCSSTree.remove(leftStreamLinkedListCSSTree.getLast());
            tupleRemovalCount = tupleRemovalCount - Constants.MUTABLE_WINDOW_SIZE;
            leftBooleanTupleRemovalCounter = false;
            rightBooleanTupleRemovalCounter = false;

        }

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declareStream("LeftPredicate", new Fields(Constants.LEFT_HASH_SET, Constants.TUPLE_ID));
        outputFieldsDeclarer.declareStream("LeftMergeBitSet", new Fields(Constants.LEFT_HASH_SET, Constants.TUPLE_ID));

    }

    public HashSet<Integer> probingResultsSmaller(Tuple tuple, LinkedList<CSSTree> linkedListCSSTree) {
        HashSet<Integer> leftHashSet = new HashSet<>();
        if (!linkedListCSSTree.isEmpty()) {
            tupleRemovalCount++;
            for (CSSTree cssTree : linkedListCSSTree) {

                leftHashSet.addAll(cssTree.searchGreater(tuple.getIntegerByField("Duration")));


            }
            return leftHashSet;

        }
        return null;

    }

    public HashSet<Integer> probingResultsGreater(Tuple tuple, LinkedList<CSSTree> linkedListCSSTree) {

        HashSet<Integer> rightHashSet = new HashSet<>();
        if (!linkedListCSSTree.isEmpty()) {
            tupleRemovalCount++;
            for (CSSTree cssTree : linkedListCSSTree) {

                rightHashSet.addAll(cssTree.searchSmaller(tuple.getIntegerByField("Time")));

            }
            return rightHashSet;

        }
        return null;
    }

    public void leftInsertionTuplesSmaller(Tuple tuple, LinkedList<CSSTree> linkedListCSSTree,  boolean flagOfMerge, Queue<Tuple> queuesOfTuplesDuringMerge) {
        if (tuple.getBooleanByField(Constants.BATCH_CSS_FLAG)) {
            linkedListCSSTree.add(leftStreamCSSTree);

            if (flagOfMerge) {
                int i = 0;
                for (Tuple tuples : queuesOfTuplesDuringMerge) {
                    i = i + 1;
                    HashSet hashSet = leftStreamCSSTree.searchGreater(tuples.getIntegerByField("Duration"));
                    outputCollector.emit("LeftMergeBitSet", new Values(convertHashSetToByteArray(hashSet), i));

                }
                leftStreamSmallerQueueMerge = new LinkedList<>();
                leftStreamMergeSmaller = false;
            }
            leftStreamCSSTree = new CSSTree(Constants.ORDER_OF_CSS_TREE);
            tupleRemovalCount = tupleRemovalCount + Constants.MUTABLE_WINDOW_SIZE;
            if (tupleRemovalCount >= Constants.IMMUTABLE_CSS_PART_REMOVAL) {
                this.leftBooleanTupleRemovalCounter = true;
                //  leftStreamLinkedListCSSTree.remove(leftStreamLinkedListCSSTree.getLast());
            }
        } else {

            leftStreamCSSTree.insertBulkUpdate(tuple.getIntegerByField(Constants.BATCH_CSS_TREE_KEY),
                    convertToIntegerList(tuple.getBinaryByField(Constants.BATCH_CSS_TREE_VALUES)));
        }
    }

    public void rightInsertionTupleSmaller(Tuple tuple, LinkedList<CSSTree> linkedListCSSTree, boolean flagOfMerge, Queue<Tuple> queuesOfTuplesDuringMerge) {
        HashSet<Integer> leftHashSet = new HashSet<>();
        if (tuple.getBooleanByField(Constants.BATCH_CSS_FLAG)) {
            linkedListCSSTree.add(rightStreamCSSTree);
            if (flagOfMerge) {
                int i = 0;
                for (Tuple tuples : queuesOfTuplesDuringMerge) {
                    i = i + 1;
                    HashSet hashSet = rightStreamCSSTree.searchSmaller(tuples.getIntegerByField("Time"));
                    outputCollector.emit("LeftMergeBitSet", new Values(convertHashSetToByteArray(hashSet), i));

                }
                rightStreamSmallerQueueMerge = new LinkedList<>();
                rightStreamMergeSmaller = false;
            }
            rightStreamCSSTree = new CSSTree(Constants.ORDER_OF_CSS_TREE);
            tupleRemovalCount = tupleRemovalCount + Constants.MUTABLE_WINDOW_SIZE;
            if (tupleRemovalCount >= Constants.IMMUTABLE_CSS_PART_REMOVAL) {
                this.rightBooleanTupleRemovalCounter = true;
            }
        } else {

            rightStreamCSSTree.insertBulkUpdate(tuple.getIntegerByField(Constants.BATCH_CSS_TREE_KEY),
                    convertToIntegerList(tuple.getBinaryByField(Constants.BATCH_CSS_TREE_VALUES)));

        }
    }

    private static List<Integer> convertToIntegerList(byte[] byteArray) {
        List<Integer> integerList = new ArrayList<>();
        ByteArrayInputStream inputStream = new ByteArrayInputStream(byteArray);
        int nextByte;
        while ((nextByte = inputStream.read()) != -1) {
            integerList.add(nextByte);
        }
        return integerList;
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
