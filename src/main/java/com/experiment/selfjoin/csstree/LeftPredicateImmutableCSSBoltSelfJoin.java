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

import java.io.*;
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
    private BufferedWriter bufferedWriter;
    //
   // private BufferedWriter writer;

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
         //   writer= new BufferedWriter(new FileWriter(new File("/home/adeel/Data/Results/Test.csv")));
        }catch (Exception e){
            e.printStackTrace();
        }

    }

    @Override
    public void execute(Tuple tuple) {
        // Check tuple from left stream R.Duration
        if (tuple.getSourceStreamId().equals("LeftStreamR")) {
            if(!leftStreamLinkedListCSSTree.isEmpty()){
                try {
                    probingResultsGreater(tuple);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }

            // IF MERGE FLAG is ON then associated queue must be updated.
            if (leftStreamMergeGreater) {
                this.leftStreamSmallerQueueMerge.offer(tuple);
            }


        }
        // // Check tuple from left stream S.Duration

        // During merge flag that just indicate update for inserting tuples in queue
        if (tuple.getSourceStreamId().equals("LeftCheckForMerge")) {
            leftStreamMergeGreater = true;
            leftMergeStartTime =System.currentTimeMillis();
        }

        // leftStreamSmaller If the Merging TUples arrives from R.Duration
        if (tuple.getSourceStreamId().equals(leftStreamGreater)) {
            try {
                leftInsertionTuplesGreater(tuple);
            } catch (IOException e) {
                e.printStackTrace();
            }

            outputCollector.emit("LeftMergingTuplesCSSCreation", tuple, new Values(leftMergeStartTime,System.currentTimeMillis(),taskID, hostName));
            outputCollector.ack(tuple);

        }



    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
      outputFieldsDeclarer.declareStream("LeftPredicate", new Fields(Constants.LEFT_HASH_SET, Constants.TUPLE_ID,"SplitBoltTime","StartProbingTime","EndProbingTime","taskID","hostName"));
       outputFieldsDeclarer.declareStream("LeftMergeHashSet", new Fields(Constants.HASH_SET, Constants.TUPLE_ID, "MergeEvaluationStartTime", "MergeEvaluatingTime","taskID","host"));
      outputFieldsDeclarer.declareStream("LeftMergingTuplesCSSCreation", new Fields("leftMergeStartTime","LeftMergeEnd","taskID", "hostName"));
    }

    // Probing results insert tuple S.Time search Greater to all for every new tuple.
    public void probingResultsGreater(Tuple tuple) throws IOException {

       // HashSet<Integer> leftHashSet = new HashSet<>();
        BitSet bitSet= new BitSet();
        long leftProbingStart=System.currentTimeMillis();
        if (!leftStreamLinkedListCSSTree.isEmpty()) {

            tupleRemovalCount++;
            for (CSSTreeUpdated cssTree : leftStreamLinkedListCSSTree) {

              cssTree.searchSmallerBitSet(tuple.getIntegerByField(Constants.TUPLE),bitSet);

            }
            outputCollector.emit("LeftPredicate", tuple, new Values(convertToByteArray(bitSet), tuple.getIntegerByField("ID"),tuple.getLongByField("Time"),leftProbingStart,System.currentTimeMillis(),taskID,hostName));
            outputCollector.ack(tuple);

        }
       // return null;
    }
    // Insertion R.Duration insert into the CSS Tree
    public void leftInsertionTuplesGreater(Tuple tuple) throws IOException {
        if (tuple.getBooleanByField(Constants.BATCH_CSS_FLAG)) { // If tuples arrive and have flag true;
            leftStreamLinkedListCSSTree.add(leftStreamCSSTree);
                // Evaluating tuples during merge
            if (leftStreamMergeGreater) {
                int i = 0;
                long mergeTupleEvaluationStartTime =System.currentTimeMillis();
                for (Tuple tuples : leftStreamSmallerQueueMerge) {
                    BitSet bitSet= new BitSet();
                    i = i + 1;
                    leftStreamCSSTree.searchSmallerBitSet(tuples.getIntegerByField(Constants.TUPLE),bitSet);
                   // HashSet hashSet = leftStreamCSSTree.searchSmaller(tuples.getIntegerByField("distance"));
                    outputCollector.emit("LeftMergeHashSet", tuple,new Values(convertToByteArray(bitSet), i,mergeTupleEvaluationStartTime, System.currentTimeMillis(),taskID,hostName));
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

                leftStreamLinkedListCSSTree.remove(leftStreamLinkedListCSSTree.getFirst());
                tupleRemovalCount = tupleRemovalCount - Constants.MUTABLE_WINDOW_SIZE; // Reintilize counter with just substract one length of CSS structure;

               // this.leftBooleanTupleRemovalCounter = true;
               //  leftStreamLinkedListCSSTree.remove(leftStreamLinkedListCSSTree.getFirst());
            }
        } else {

            // leftStreamCSSTree is used for inserting tuples first all upstream tuples are inserted first into css tree
            leftStreamCSSTree.insert(tuple.getIntegerByField(Constants.BATCH_CSS_TREE_KEY),
                   (tuple.getIntegerByField(Constants.BATCH_CSS_TREE_VALUES)));
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


    public synchronized byte[] convertToByteArray(Serializable object) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (ObjectOutputStream objectOutputStream = new ObjectOutputStream(baos)) {
            objectOutputStream.writeObject(object);
            objectOutputStream.flush();
        }
        return baos.toByteArray();
    }
}
