package com.baselinealgorithm.chainbplusandcss;

import com.configurationsandconstants.iejoinandbaseworks.Configuration;
import com.configurationsandconstants.iejoinandbaseworks.Constants;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;

import java.util.HashSet;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;

public class LeftPredicateImmutableCSSBolt extends BaseRichBolt {
    private LinkedList<CSSTree> leftStreamLinkedListCSSTree = null;
    private LinkedList<CSSTree> rightStreamLinkedListCSSTree = null;
    private String leftStreamSmaller;
    private String rightStreamSmaller;
    private HashSet<Integer> leftStreamTuplesHashSet = null;
    private HashSet<Integer> rightStreamTuplesHashSet = null;
    private CSSTree leftStreamCSSTree = null;
    private CSSTree rightStreamCSSTree = null;
    private boolean leftStreamMergeSmaller = false;
    private boolean rightStreamMergeSmaller = false;
    private Queue<Tuple> leftStreamSmallerQueueMerge = null;
    private Queue<Tuple> rightStreamSmallerQueueMerge = null;
    private static int tupleRemovalCount = 0;

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
        this.leftStreamSmallerQueueMerge = new LinkedList<>();
        this.rightStreamSmallerQueueMerge = new LinkedList<>();

    }

    @Override
    public void execute(Tuple tuple) {
        if (tuple.getSourceStreamId().equals("this.leftStreamSmaller")) {
            tupleRemovalCount++;
            leftStreamTuplesHashSet = probingResultsSmaller(tuple, rightStreamLinkedListCSSTree);
        }
        if (tuple.getSourceStreamId().equals("this.rightStreamSmaller")) {
            tupleRemovalCount++;
            rightStreamTuplesHashSet = probingResultsGreater(tuple, leftStreamLinkedListCSSTree);
        }

        if (tuple.getSourceStreamId().equals(leftStreamSmaller)) {
            insertionTuplesSmaller(tuple, leftStreamLinkedListCSSTree, leftStreamCSSTree, leftStreamMergeSmaller, leftStreamSmallerQueueMerge);
        }
        if (tuple.getSourceStreamId().equals(rightStreamSmaller)) {
            insertionTuplesGreater(tuple, rightStreamLinkedListCSSTree, rightStreamCSSTree, rightStreamMergeSmaller, rightStreamSmallerQueueMerge);
        }
        if (tuple.getSourceStreamId().equals("LeftCheckForMerge")) {
            this.leftStreamSmallerQueueMerge.offer(tuple);
            leftStreamMergeSmaller = true;
        }
        if (tuple.getSourceStreamId().equals("RightCheckForMerge")) {
            this.rightStreamSmallerQueueMerge.offer(tuple);
            rightStreamMergeSmaller = true;
        }
        /// Change For Both Right Stream and Left Stream depend upon which sliding window you are using
        if (tupleRemovalCount > Constants.IMMUTABLE_WINDOW_SIZE) {
            tupleRemovalCount = tupleRemovalCount - 2 * Constants.MUTABLE_WINDOW_SIZE;
            rightStreamLinkedListCSSTree.remove(rightStreamLinkedListCSSTree.getLast());
            leftStreamLinkedListCSSTree.remove(leftStreamLinkedListCSSTree.getLast());
        }

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }

    public HashSet<Integer> probingResultsSmaller(Tuple tuple, LinkedList<CSSTree> linkedListCSSTree) {
        HashSet<Integer> leftHashSet = new HashSet<>();
        if (!linkedListCSSTree.isEmpty()) {
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
            for (CSSTree cssTree : linkedListCSSTree) {
                rightHashSet.addAll(cssTree.searchSmaller(tuple.getIntegerByField("Time")));
            }
            return rightHashSet;
        }
        return null;
    }

    public void insertionTuplesSmaller(Tuple tuple, LinkedList<CSSTree> linkedListCSSTree, CSSTree cssTree, boolean flagOfMerge, Queue<Tuple> queuesOfTuplesDuringMerge) {
        HashSet<Integer> leftHashSet = new HashSet<>();
        if (tuple.getBooleanByField("Flag")) {
            linkedListCSSTree.add(cssTree);
            if (flagOfMerge) {
                for (Tuple tuples : queuesOfTuplesDuringMerge) {

                    leftHashSet.addAll(cssTree.searchGreater(tuples.getIntegerByField("Duration")));
                }
                leftStreamSmallerQueueMerge = new LinkedList<>();
                leftStreamMergeSmaller = false;
            }
            leftStreamCSSTree = new CSSTree(Constants.ORDER_OF_CSS_TREE);
        } else {
            cssTree.insert(tuple.getIntegerByField(""), tuple.getIntegerByField(""));
        }
    }

    public void insertionTuplesGreater(Tuple tuple, LinkedList<CSSTree> linkedListCSSTree, CSSTree cssTree, boolean flagOfMerge, Queue<Tuple> queuesOfTuplesDuringMerge) {
        HashSet<Integer> leftHashSet = new HashSet<>();
        if (tuple.getBooleanByField("Flag")) {
            linkedListCSSTree.add(cssTree);
            if (flagOfMerge) {
                for (Tuple tuples : queuesOfTuplesDuringMerge) {

                    leftHashSet.addAll(cssTree.searchSmaller(tuples.getIntegerByField("Time")));
                }
                rightStreamSmallerQueueMerge = new LinkedList<>();
                rightStreamMergeSmaller = false;
            }
            rightStreamCSSTree = new CSSTree(Constants.ORDER_OF_CSS_TREE);
        } else {
            cssTree.insert(tuple.getIntegerByField(""), tuple.getIntegerByField(""));
        }
    }
}
