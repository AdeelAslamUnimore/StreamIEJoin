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

public class RightPredicateImmutableCSSBolt extends BaseRichBolt {
    private LinkedList<CSSTree> leftStreamLinkedListCSSTree = null;
    private LinkedList<CSSTree> rightStreamLinkedListCSSTree = null;
    private String leftStreamGreater;
    private String rightStreamGreater;
    private HashSet<Integer> leftStreamTuplesHashSet = null;
    private HashSet<Integer> rightStreamTuplesHashSet = null;
    private CSSTree leftStreamCSSTree = null;
    private CSSTree rightStreamCSSTree = null;
    private boolean leftStreamMergeGreater = false;
    private boolean rightStreamMergeGreater = false;
    private Queue<Tuple> leftStreamGreaterQueueMerge = null;
    private Queue<Tuple> rightStreamGreaterQueueMerge = null;
    private static int tupleRemovalCount = 0;

    private boolean leftBooleanTupleRemovalCounter = false;
    private boolean rightBooleanTupleRemovalCounter = false;
    private OutputCollector outputCollector;

    public RightPredicateImmutableCSSBolt() {
        Map<String, Object> map = Configuration.configurationConstantForStreamIDs();
        this.leftStreamGreater = (String) map.get("LeftGreaterPredicateTuple");
        this.rightStreamGreater = (String) map.get("RightGreaterPredicateTuple");
    }

    @Override
    public void prepare(Map<String, Object> map, TopologyContext topologyContext, OutputCollector outputCollector) {
        leftStreamLinkedListCSSTree = new LinkedList<>();
        rightStreamLinkedListCSSTree = new LinkedList<>();
        leftStreamCSSTree = new CSSTree(Constants.ORDER_OF_CSS_TREE);
        rightStreamCSSTree = new CSSTree(Constants.ORDER_OF_CSS_TREE);
        this.leftStreamGreaterQueueMerge = new LinkedList<>();
        this.rightStreamGreaterQueueMerge = new LinkedList<>();
        this.outputCollector = outputCollector;

    }

    @Override
    public void execute(Tuple tuple) {
        if (tuple.getSourceStreamId().equals("LeftStreamTuples")) {
            if (leftStreamMergeGreater) {
                leftStreamGreaterQueueMerge.offer(tuple);
            }
            HashSet<Integer> leftHashSet = probingResultsGreater(tuple, rightStreamLinkedListCSSTree);
            if (leftHashSet != null) {
                outputCollector.emit("RightPredicate", tuple, new Values(convertHashSetToByteArray(leftHashSet), tuple.getIntegerByField("ID")));
                outputCollector.ack(tuple);

            }
        }
        if (tuple.getSourceStreamId().equals("RightStream")) {
            if (rightStreamMergeGreater) {
                rightStreamGreaterQueueMerge.offer(tuple);
            }
            HashSet<Integer> rightHashSet = probingResultsSmaller(tuple, leftStreamLinkedListCSSTree);
            if (rightHashSet != null) {

                outputCollector.emit("RightPredicate", tuple, new Values(convertHashSetToByteArray(rightHashSet), tuple.getIntegerByField("ID")));
                outputCollector.ack(tuple);
            }
        }

        if (tuple.getSourceStreamId().equals(leftStreamGreater)) {
            leftInsertionTupleGreater(tuple, leftStreamLinkedListCSSTree, leftStreamMergeGreater, leftStreamGreaterQueueMerge);
        }
        if (tuple.getSourceStreamId().equals(rightStreamGreater)) {
            rightInsertionTupleGreater(tuple, rightStreamLinkedListCSSTree, rightStreamMergeGreater, rightStreamGreaterQueueMerge);
        }
        if (tuple.getSourceStreamId().equals("LeftCheckForMerge")) {

            leftStreamMergeGreater = true;
        }
        if (tuple.getSourceStreamId().equals("RightCheckForMerge")) {

            rightStreamMergeGreater = true;
        }
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

        outputFieldsDeclarer.declareStream("RightPredicate", new Fields(Constants.RIGHT_HASH_SET, Constants.TUPLE_ID));
        outputFieldsDeclarer.declareStream("RightMergeBitSet", new Fields(Constants.RIGHT_HASH_SET, Constants.TUPLE_ID));

    }

    public HashSet<Integer> probingResultsGreater(Tuple tuple, LinkedList<CSSTree> linkedListCSSTree) {
        HashSet<Integer> hashSet = new HashSet<>();
        if (!linkedListCSSTree.isEmpty()) {
            tupleRemovalCount++;
            for (CSSTree cssTree : linkedListCSSTree) {
                hashSet.addAll(cssTree.searchGreater(tuple.getIntegerByField("Revenue")));

            }
            return hashSet;

        }
        return null;
    }

    public HashSet<Integer> probingResultsSmaller(Tuple tuple, LinkedList<CSSTree> linkedListCSSTree) {
        HashSet<Integer> hashSet = new HashSet<>();
        if (!linkedListCSSTree.isEmpty()) {
            tupleRemovalCount++;
            for (CSSTree cssTree : linkedListCSSTree) {
                hashSet.addAll(cssTree.searchSmaller(tuple.getIntegerByField("Cost")));

            }
            return hashSet;

        }
        return null;
    }

    public void leftInsertionTupleGreater(Tuple tuple, LinkedList<CSSTree> linkedListCSSTree, boolean flagOfMerge, Queue<Tuple> queuesOfTuplesDuringMerge) {

        if (tuple.getBooleanByField(Constants.BATCH_CSS_FLAG)) {
            linkedListCSSTree.add(leftStreamCSSTree);
            if (flagOfMerge) {
                int i = 0;
                for (Tuple tuples : queuesOfTuplesDuringMerge) {
                    HashSet hashSet = leftStreamCSSTree.searchSmaller(tuples.getIntegerByField("Revenue"));
                    i = i + 1;
                    outputCollector.emit("RightMergeBitSet", new Values(convertHashSetToByteArray(hashSet), i));
                }
                leftStreamGreaterQueueMerge = new LinkedList<>();
                leftStreamMergeGreater = false;
            }
            leftStreamCSSTree = new CSSTree(Constants.ORDER_OF_CSS_TREE);
            tupleRemovalCount = tupleRemovalCount + Constants.MUTABLE_WINDOW_SIZE;
            if (tupleRemovalCount >= Constants.IMMUTABLE_CSS_PART_REMOVAL) {
                this.leftBooleanTupleRemovalCounter = true;
            }
        } else {

            leftStreamCSSTree.insertBulkUpdate(tuple.getIntegerByField(Constants.BATCH_CSS_TREE_KEY),
                    convertToIntegerList(tuple.getBinaryByField(Constants.BATCH_CSS_TREE_VALUES)));
        }
    }

    public void rightInsertionTupleGreater(Tuple tuple, LinkedList<CSSTree> linkedListCSSTree, boolean flagOfMerge, Queue<Tuple> queuesOfTuplesDuringMerge) {

        if (tuple.getBooleanByField(Constants.BATCH_CSS_FLAG)) {
            linkedListCSSTree.add(rightStreamCSSTree);
            if (flagOfMerge) {
                int i = 0;
                for (Tuple tuples : queuesOfTuplesDuringMerge) {
                    i = i + 1;
                    HashSet hashSet = rightStreamCSSTree.searchGreater(tuples.getIntegerByField("Cost"));
                    outputCollector.emit("RightMergeBitSet", new Values(convertHashSetToByteArray(hashSet), i));
                }
                rightStreamGreaterQueueMerge = new LinkedList<>();
                rightStreamMergeGreater = false;
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

    private static List<Integer> convertToIntegerList(byte[] byteArray) {
        List<Integer> integerList = new ArrayList<>();
        ByteArrayInputStream inputStream = new ByteArrayInputStream(byteArray);
        int nextByte;
        while ((nextByte = inputStream.read()) != -1) {
            integerList.add(nextByte);
        }
        return integerList;
    }
}
