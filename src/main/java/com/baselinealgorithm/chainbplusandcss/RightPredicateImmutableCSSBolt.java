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

    }

    @Override
    public void execute(Tuple tuple) {
        if (tuple.getSourceStreamId().equals("this.leftStreamSmaller")) {
            tupleRemovalCount++;
            leftStreamTuplesHashSet = probingResultsSmaller(tuple, rightStreamLinkedListCSSTree);
            if(leftStreamMergeGreater){
                leftStreamGreaterQueueMerge.offer(tuple);
            }
        }
        if (tuple.getSourceStreamId().equals("this.rightStreamSmaller")) {
            tupleRemovalCount++;
            rightStreamTuplesHashSet = probingResultsGreater(tuple, leftStreamLinkedListCSSTree);
            if(rightStreamMergeGreater){
                rightStreamGreaterQueueMerge.offer(tuple);
            }
        }

        if (tuple.getSourceStreamId().equals(leftStreamGreater)) {
            insertionTuplesSmaller(tuple, leftStreamLinkedListCSSTree, leftStreamCSSTree, leftStreamMergeGreater, leftStreamGreaterQueueMerge);
        }
        if (tuple.getSourceStreamId().equals(rightStreamGreater)) {
            insertionTuplesGreater(tuple, rightStreamLinkedListCSSTree, rightStreamCSSTree, rightStreamMergeGreater, rightStreamGreaterQueueMerge);
        }
        if (tuple.getSourceStreamId().equals("LeftCheckForMerge")) {

            leftStreamMergeGreater = true;
        }
        if (tuple.getSourceStreamId().equals("RightCheckForMerge")) {

            rightStreamMergeGreater = true;
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
                leftStreamGreaterQueueMerge = new LinkedList<>();
                leftStreamMergeGreater = false;
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
                rightStreamGreaterQueueMerge = new LinkedList<>();
                rightStreamMergeGreater = false;
            }
            rightStreamCSSTree = new CSSTree(Constants.ORDER_OF_CSS_TREE);
        } else {
            cssTree.insert(tuple.getIntegerByField(""), tuple.getIntegerByField(""));
        }
    }
}
