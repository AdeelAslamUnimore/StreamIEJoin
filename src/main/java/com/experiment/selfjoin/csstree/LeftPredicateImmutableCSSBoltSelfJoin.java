package com.experiment.selfjoin.csstree;

import com.baselinealgorithm.chainbplusandcss.CSSTreeUpdated;
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
import java.net.InetAddress;
import java.util.*;

public class LeftPredicateImmutableCSSBoltSelfJoin extends BaseRichBolt {
    // Two linked that holds the linked list of two relation for example R.Duration and S.Time
    private LinkedList<CSSTreeUpdated> leftStreamLinkedListCSSTree = null;

    // The two stream ids from upstream processing elements
    private String leftStreamGreater;

    // Signature for CSS tree that used as an object
    private CSSTreeUpdated leftStreamCSSTree = null;

    // Two boolean that are used as a flag for merge operation one for R other for S
    private boolean leftStreamMergeGreater = false;

    // Two queues that maintian two stream of tuples during merge and evaluate the tuples of CSS tree that is newly added to the linked list during merge
    private Queue<Tuple> leftStreamSmallerQueueMerge = null;

    // Two boolean for counter that indicate true and flag that tuple removal counter for both is above the threshold
    private boolean leftBooleanTupleRemovalCounter = false;

    // Tuple removal counter
    private static int tupleRemovalCount = 0;
    // Output collector
    private OutputCollector outputCollector;
    //
    private long leftMergeStartTime;
    private int taskID;
    private String hostName;

    public LeftPredicateImmutableCSSBoltSelfJoin() {
        // These are the initialization of upstream streamIDS
        Map<String, Object> map = Configuration.configurationConstantForStreamIDs();
        this.leftStreamGreater = (String) map.get("LeftGreaterPredicateTuple");

    }

    @Override
    public void prepare(Map<String, Object> map, TopologyContext topologyContext, OutputCollector outputCollector) {
        // both linked list and CSS trees are initilized
        leftStreamLinkedListCSSTree = new LinkedList<>();

        leftStreamCSSTree = new CSSTreeUpdated(Constants.ORDER_OF_CSS_TREE);

        // Merge operation
        // Updated  queue during merge
        this.leftStreamSmallerQueueMerge = new LinkedList<>();


        this.outputCollector = outputCollector;
        taskID= topologyContext.getThisTaskId();
        try {
            hostName= InetAddress.getLocalHost().getHostName();
        }catch (Exception e){
            e.printStackTrace();
        }

    }

    @Override
    public void execute(Tuple tuple) {
        // Check tuple from left stream R.Duration
        if (tuple.getSourceStreamId().equals("StreamR")) {
            // IF MERGE FLAG is ON then associated queue must be updated.
            if (leftStreamMergeGreater) {
                this.leftStreamSmallerQueueMerge.offer(tuple);
            }
            // Probing the results
            long leftProbingStart=System.currentTimeMillis();
            HashSet<Integer> leftHashSet = probingResultsGreater(tuple, leftStreamLinkedListCSSTree);
           long probingEnd=System.currentTimeMillis();
            if(leftHashSet!=null) {
               // Only emit whole hashSet if linked list contains the tuples;
               // emit for hashset evaluation/
               outputCollector.emit("LeftPredicate", tuple, new Values(convertHashSetToByteArray(leftHashSet), tuple.getIntegerByField("ID"),leftProbingStart,probingEnd,taskID,hostName));
               outputCollector.ack(tuple);
           }

        }
        // // Check tuple from left stream S.Duration

        // During merge flag that just indicate update for inserting tuples in queue
        if (tuple.getSourceStreamId().equals("LeftCheckForMerge")) {
            //this.leftStreamSmallerQueueMerge.offer(tuple);
            //System.out.println("Left==="+tuple);
            leftStreamMergeGreater = true;
            leftMergeStartTime =System.currentTimeMillis();
        }

        // leftStreamSmaller If the Merging TUples arrives from R.Duration
        if (tuple.getSourceStreamId().equals(leftStreamGreater)) {
            leftInsertionTuplesGreater(tuple);
            outputCollector.emit("LeftMergingTuplesCSSCreation", tuple, new Values(leftMergeStartTime,System.currentTimeMillis(),taskID, hostName));
            outputCollector.ack(tuple);

        }
        //rightStream If merging tuples arrives from S.Time



        /// Change For Both Right Stream and Left Stream depend upon which sliding window you are using
        if (leftBooleanTupleRemovalCounter) {
          //removing tuples only remove last CSS TREE from both R an S such as R.Duration and S.Time

            leftStreamLinkedListCSSTree.remove(leftStreamLinkedListCSSTree.getFirst());
            tupleRemovalCount = tupleRemovalCount - Constants.MUTABLE_WINDOW_SIZE; // Reintilize counter with just substract one length of CSS structure;
            // set removal boolean to false again
            leftBooleanTupleRemovalCounter = false;


        }

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declareStream("LeftPredicate", new Fields(Constants.LEFT_HASH_SET, Constants.TUPLE_ID,"StartProbingTime","EndProbingTime","taskID","hostName"));
        outputFieldsDeclarer.declareStream("LeftMergeHashSet", new Fields(Constants.HASH_SET, Constants.TUPLE_ID, "MergeEvaluationStartTime", "MergeEvaluatingTime","taskID","host"));
        outputFieldsDeclarer.declareStream("LeftMergingTuplesCSSCreation", new Fields("leftMergeStartTime","LeftMergeEnd","taskID", "hostName"));
    }
    // Probing results insert tuple R.Duration search Greater to all for every new tuple.
    public HashSet<Integer> probingResultsSmaller(Tuple tuple, LinkedList<CSSTreeUpdated> linkedListCSSTree) {
        HashSet<Integer> leftHashSet = new HashSet<>();
        // Iterate all linked list
        if (!linkedListCSSTree.isEmpty()) {
            tupleRemovalCount++;
            // Probe to all linked list
            for (CSSTreeUpdated cssTree : linkedListCSSTree) {
                leftHashSet.addAll(cssTree.searchGreater(tuple.getIntegerByField("distance")));

            }
            return leftHashSet;

        }
        return null;

    }
    // Probing results insert tuple S.Time search Greater to all for every new tuple.
    public HashSet<Integer> probingResultsGreater(Tuple tuple, LinkedList<CSSTreeUpdated> linkedListCSSTree) {

        HashSet<Integer> rightHashSet = new HashSet<>();
        if (!linkedListCSSTree.isEmpty()) {
            tupleRemovalCount++;
            for (CSSTreeUpdated cssTree : linkedListCSSTree) {

                rightHashSet.addAll(cssTree.searchSmaller(tuple.getIntegerByField("distance")));

            }
            return rightHashSet;

        }
        return null;
    }
    // Insertion R.Duration insert into the CSS Tree
    public void leftInsertionTuplesGreater(Tuple tuple) {
        if (tuple.getBooleanByField(Constants.BATCH_CSS_FLAG)) { // If tuples arrive and have flag true;
            leftStreamLinkedListCSSTree.add(leftStreamCSSTree);
                // Evaluating tuples during merge
            if (leftStreamMergeGreater) {
                int i = 0;
                long mergeTupleEvaluationStartTime =System.currentTimeMillis();
                for (Tuple tuples : leftStreamSmallerQueueMerge) {
                    i = i + 1;
                    HashSet hashSet = leftStreamCSSTree.searchGreater(tuples.getIntegerByField("distance"));
                    outputCollector.emit("LeftMergeHashSet", tuple,new Values(convertHashSetToByteArray(hashSet), i,mergeTupleEvaluationStartTime, System.currentTimeMillis(),taskID,hostName));
                    outputCollector.ack(tuple);

                }
                // flush the queue that maintain that tuples
                leftStreamSmallerQueueMerge = new LinkedList<>();
                leftStreamMergeGreater = false; // set associated flag to false
            }
            // create new CSS tree object
            leftStreamCSSTree = new CSSTreeUpdated(Constants.ORDER_OF_CSS_TREE);
            // updated the tuple removal counter by newly added tuples from immutable part
            tupleRemovalCount = tupleRemovalCount + Constants.MUTABLE_WINDOW_SIZE;
            // If tuple removal counter approach to limit then its associated boolean is on the structure is removed only when both R.Duration and S.Duration or union of both tuples reaches to the limit
            if (tupleRemovalCount >= Constants.IMMUTABLE_CSS_PART_REMOVAL) {
                this.leftBooleanTupleRemovalCounter = true;
                 leftStreamLinkedListCSSTree.remove(leftStreamLinkedListCSSTree.getFirst());
            }
        } else {
            // leftStreamCSSTree is used for inserting tuples first all upstream tuples are inserted first into css tree
            leftStreamCSSTree.insertBulkUpdate(tuple.getIntegerByField(Constants.BATCH_CSS_TREE_KEY),
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
