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
    // Two linked that holds the linked list of two relation for example R.Duration and S.Time
    private LinkedList<CSSTree> leftStreamLinkedListCSSTree = null;
    private LinkedList<CSSTree> rightStreamLinkedListCSSTree = null;
    // The two stream ids from upstream processing elements
    private String leftStreamSmaller;
    private String rightStreamSmaller;
    // Signature for CSS tree that used as an object
    private CSSTree leftStreamCSSTree = null;
    private CSSTree rightStreamCSSTree = null;
    // Two boolean that are used as a flag for merge operation one for R other for S
    private boolean leftStreamMergeSmaller = false;
    private boolean rightStreamMergeSmaller = false;
    // Two queues that maintian two stream of tuples during merge and evaluate the tuples of CSS tree that is newly added to the linked list during merge
    private Queue<Tuple> leftStreamSmallerQueueMerge = null;
    private Queue<Tuple> rightStreamSmallerQueueMerge = null;
    // Two boolean for counter that indicate true and flag that tuple removal counter for both is above the threshold
    private boolean leftBooleanTupleRemovalCounter = false;
    private boolean rightBooleanTupleRemovalCounter = false;
    // Tuple removal counter
    private static int tupleRemovalCount = 0;
    // Output collector
    private OutputCollector outputCollector;

    public LeftPredicateImmutableCSSBolt() {
        // These are the initialization of upstream streamIDS
        Map<String, Object> map = Configuration.configurationConstantForStreamIDs();
        this.leftStreamSmaller = (String) map.get("LeftSmallerPredicateTuple");
        this.rightStreamSmaller = (String) map.get("RightSmallerPredicateTuple");
    }

    @Override
    public void prepare(Map<String, Object> map, TopologyContext topologyContext, OutputCollector outputCollector) {
        // both linked list and CSS trees are initilized
        leftStreamLinkedListCSSTree = new LinkedList<>();
        rightStreamLinkedListCSSTree = new LinkedList<>();
        leftStreamCSSTree = new CSSTree(Constants.ORDER_OF_CSS_TREE);
        rightStreamCSSTree = new CSSTree(Constants.ORDER_OF_CSS_TREE);
        // Merge operation
        // Updated  queue during merge
        this.leftStreamSmallerQueueMerge = new LinkedList<>();
        this.rightStreamSmallerQueueMerge = new LinkedList<>();

        this.outputCollector = outputCollector;

    }

    @Override
    public void execute(Tuple tuple) {
        // Check tuple from left stream R.Duration
        if (tuple.getSourceStreamId().equals("LeftStreamTuples")) {
            // IF MERGE FLAG is ON then associated queue must be updated.
            if (leftStreamMergeSmaller) {
                this.leftStreamSmallerQueueMerge.offer(tuple);
            }
            // Probing the results
            HashSet<Integer> leftHashSet = probingResultsSmaller(tuple, rightStreamLinkedListCSSTree);
           if(leftHashSet!=null) {
               // Only emit whole hashSet if linked list contains the tuples;
               // emit for hashset evaluation/
               outputCollector.emit("LeftPredicate", tuple, new Values(convertHashSetToByteArray(leftHashSet), tuple.getIntegerByField("ID")));
               outputCollector.ack(tuple);
           }

        }
        // // Check tuple from left stream S.Duration
        if (tuple.getSourceStreamId().equals("RightStream")) {
            if (rightStreamMergeSmaller) {
                // IF MERGE FLAG is ON then associated queue must be updated.
                this.rightStreamSmallerQueueMerge.offer(tuple);
            }
            HashSet<Integer> rightHashSet = probingResultsGreater(tuple, leftStreamLinkedListCSSTree);
            if(rightHashSet!=null) {
                outputCollector.emit("LeftPredicate", tuple, new Values(convertHashSetToByteArray(rightHashSet), tuple.getIntegerByField("ID")));
                outputCollector.ack(tuple);
            }
        }
        // During merge flag that just indicate update for inserting tuples in queue
        if (tuple.getSourceStreamId().equals("LeftCheckForMerge")) {
            //this.leftStreamSmallerQueueMerge.offer(tuple);
            leftStreamMergeSmaller = true;
        }
        if (tuple.getSourceStreamId().equals("RightCheckForMerge")) {
            // this.rightStreamSmallerQueueMerge.offer(tuple);
            rightStreamMergeSmaller = true;
        }
        // leftStreamSmaller If the Merging TUples arrives from R.Duration
        if (tuple.getSourceStreamId().equals(leftStreamSmaller)) {
            leftInsertionTuplesSmaller(tuple);
        }
        //rightStream If merging tuples arrives from S.Time
        if (tuple.getSourceStreamId().equals(rightStreamSmaller)) {
            rightInsertionTupleSmaller(tuple);
        }


        /// Change For Both Right Stream and Left Stream depend upon which sliding window you are using
        if (leftBooleanTupleRemovalCounter && rightBooleanTupleRemovalCounter) {
          //removing tuples only remove last CSS TREE from both R an S such as R.Duration and S.Time
            rightStreamLinkedListCSSTree.remove(rightStreamLinkedListCSSTree.getLast());
            leftStreamLinkedListCSSTree.remove(leftStreamLinkedListCSSTree.getLast());
            tupleRemovalCount = tupleRemovalCount - Constants.MUTABLE_WINDOW_SIZE; // Reintilize counter with just substract one length of CSS structure;
            // set removal boolean to false again
            leftBooleanTupleRemovalCounter = false;
            rightBooleanTupleRemovalCounter = false;

        }

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declareStream("LeftPredicate", new Fields(Constants.LEFT_HASH_SET, Constants.TUPLE_ID));
        outputFieldsDeclarer.declareStream("LeftMergeBitSet", new Fields(Constants.LEFT_HASH_SET, Constants.TUPLE_ID));

    }
    // Probing results insert tuple R.Duration search Greater to all for every new tuple.
    public HashSet<Integer> probingResultsSmaller(Tuple tuple, LinkedList<CSSTree> linkedListCSSTree) {
        HashSet<Integer> leftHashSet = new HashSet<>();
        // Iterate all linked list
        if (!linkedListCSSTree.isEmpty()) {
            tupleRemovalCount++;
            // Probe to all linked list
            for (CSSTree cssTree : linkedListCSSTree) {
                leftHashSet.addAll(cssTree.searchGreater(tuple.getIntegerByField("Duration")));

            }
            return leftHashSet;

        }
        return null;

    }
    // Probing results insert tuple S.Time search Greater to all for every new tuple.
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
    // Insertion R.Duration insert into the CSS Tree
    public void leftInsertionTuplesSmaller(Tuple tuple) {
        if (tuple.getBooleanByField(Constants.BATCH_CSS_FLAG)) { // If tuples arrive and have flag true;
            leftStreamLinkedListCSSTree.add(leftStreamCSSTree);
                // Evaluating tuples during merge
            if (leftStreamMergeSmaller) {
                int i = 0;
                for (Tuple tuples : leftStreamSmallerQueueMerge) {
                    i = i + 1;
                    HashSet hashSet = leftStreamCSSTree.searchGreater(tuples.getIntegerByField("Duration"));
                    outputCollector.emit("LeftMergeBitSet", new Values(convertHashSetToByteArray(hashSet), i));

                }
                // flush the queue that maintain that tuples
                leftStreamSmallerQueueMerge = new LinkedList<>();
                leftStreamMergeSmaller = false; // set associated flag to false
            }
            // create new CSS tree object
            leftStreamCSSTree = new CSSTree(Constants.ORDER_OF_CSS_TREE);
            // updated the tuple removal counter by newly added tuples from immutable part
            tupleRemovalCount = tupleRemovalCount + Constants.MUTABLE_WINDOW_SIZE;
            // If tuple removal counter approach to limit then its associated boolean is on the structure is removed only when both R.Duration and S.Duration or union of both tuples reaches to the limit
            if (tupleRemovalCount >= Constants.IMMUTABLE_CSS_PART_REMOVAL) {
                this.leftBooleanTupleRemovalCounter = true;
                //  leftStreamLinkedListCSSTree.remove(leftStreamLinkedListCSSTree.getLast());
            }
        } else {
            // leftStreamCSSTree is used for inserting tuples first all upstream tuples are inserted first into css tree
            leftStreamCSSTree.insertBulkUpdate(tuple.getIntegerByField(Constants.BATCH_CSS_TREE_KEY),
                    convertToIntegerList(tuple.getBinaryByField(Constants.BATCH_CSS_TREE_VALUES)));
        }
    }

    public void rightInsertionTupleSmaller(Tuple tuple) {

        if (tuple.getBooleanByField(Constants.BATCH_CSS_FLAG)) {
            rightStreamLinkedListCSSTree.add(rightStreamCSSTree);
            if (rightStreamMergeSmaller) {
                int i = 0;
                for (Tuple tuples : rightStreamSmallerQueueMerge) {
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
