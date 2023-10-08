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

import java.io.*;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.*;

public class LeftPredicateImmutableCSSBolt extends BaseRichBolt {
    // Two linked that holds the linked list of two relation for example R.Duration and S.Time
    private LinkedList<CSSTreeUpdated> leftStreamLinkedListCSSTree = null;
    private LinkedList<CSSTreeUpdated> rightStreamLinkedListCSSTree = null;
    // The two stream ids from upstream processing elements
    private String leftStreamSmaller;
    private String rightStreamSmaller;
    // Signature for CSS tree that used as an object
    private CSSTreeUpdated leftStreamCSSTree = null;
    private CSSTreeUpdated rightStreamCSSTree = null;
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

    // Metrics  for Merging and MutableTupleEvaluation
    private long LeftMergeTimeR;
    private long RightMergeTimeS;
    private int taskID;
    private String hostName;
    private BufferedWriter bufferedWriter;
    //
    private ArrayList<TupleModel> leftArrayList;
    private ArrayList<TupleModel> rightArrayList;

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
        leftStreamCSSTree = new CSSTreeUpdated(Constants.ORDER_OF_CSS_TREE);
        rightStreamCSSTree = new CSSTreeUpdated(Constants.ORDER_OF_CSS_TREE);
        // Merge operation
        // Updated  queue during merge
        this.leftStreamSmallerQueueMerge = new LinkedList<>();
        this.rightStreamSmallerQueueMerge = new LinkedList<>();
        this.taskID=topologyContext.getThisTaskId();
        try {
            this.hostName= InetAddress.getLocalHost().getHostName();
            this.leftArrayList= new ArrayList<>();
            this.rightArrayList= new ArrayList<>();

            this.bufferedWriter= new BufferedWriter(new FileWriter(new File("/home/adeel/Data/Results/LeftPredicateImmutablle.csv")));
        } catch (Exception e) {
            e.printStackTrace();
        }

        this.outputCollector = outputCollector;

    }

    @Override
    public void execute(Tuple tuple) {
        // Check tuple from left stream R.Duration
        if (tuple.getSourceStreamId().equals("StreamR")) {
            long startTime= System.currentTimeMillis();
            // IF MERGE FLAG is ON then associated queue must be updated.
            if (leftStreamMergeSmaller) {
                this.leftStreamSmallerQueueMerge.offer(tuple);
            }
            // Probing the results
           BitSet leftBitSet = probingResultsSmaller(tuple, rightStreamLinkedListCSSTree);

           if(leftBitSet!=null) {
               // Only emit whole hashSet if linked list contains the tuples;
               // emit for hashset evaluation/

               try {
                   outputCollector.emit("LeftPredicate", tuple, new Values(convertToByteArray(leftBitSet), tuple.getIntegerByField("ID")+"L", tuple.getLongByField(Constants.KAFKA_SPOUT_TIME),
                           tuple.getLongByField(Constants.KAFKA_TIME), startTime, System.currentTimeMillis(), taskID, hostName));
               } catch (IOException e) {
                   e.printStackTrace();
               }
               outputCollector.ack(tuple);
           }
           leftArrayList.add(new TupleModel(tuple.getIntegerByField("Duration"), tuple.getIntegerByField("ID")));
           if(leftArrayList.size()==100000){
               CSSTreeUpdated cssTreeUpdated= new CSSTreeUpdated(4);
               for(int i=0;i<leftArrayList.size();i++){
                   cssTreeUpdated.insert(leftArrayList.get(i).getTuple(),leftArrayList.get(i).getId());
               }
               leftStreamLinkedListCSSTree.add(cssTreeUpdated);
               leftArrayList= new ArrayList<>();
           }

        }
        // // Check tuple from left stream S.Duration
        if (tuple.getSourceStreamId().equals("StreamS")) {
            long startTime= System.currentTimeMillis();
            if (rightStreamMergeSmaller) {
                // IF MERGE FLAG is ON then associated queue must be updated.
                this.rightStreamSmallerQueueMerge.offer(tuple);
            }
            BitSet rightBitSet = probingResultsGreater(tuple, leftStreamLinkedListCSSTree);
            if(rightBitSet!=null) {
                try {
                    outputCollector.emit("RightPredicate", tuple, new Values(convertToByteArray(rightBitSet), tuple.getIntegerByField("ID")+"R",  tuple.getLongByField(Constants.KAFKA_SPOUT_TIME),
                            tuple.getLongByField(Constants.KAFKA_TIME), startTime, System.currentTimeMillis(), taskID, hostName));
                } catch (IOException e) {
                    e.printStackTrace();
                }
                outputCollector.ack(tuple);
            }
            rightArrayList.add(new TupleModel(tuple.getIntegerByField("Time"), tuple.getIntegerByField("ID")));
            if(rightArrayList.size()==100000){
                CSSTreeUpdated cssTreeUpdated= new CSSTreeUpdated(4);
                for(int i=0;i<rightArrayList.size();i++){
                    cssTreeUpdated.insert(rightArrayList.get(i).getTuple(),rightArrayList.get(i).getId());
                }
                rightStreamLinkedListCSSTree.add(cssTreeUpdated);
                rightArrayList= new ArrayList<>();
            }
        }
        // During merge flag that just indicate update for inserting tuples in queue
        if (tuple.getSourceStreamId().equals("LeftCheckForMerge")) {
            //this.leftStreamSmallerQueueMerge.offer(tuple);

            leftStreamMergeSmaller = true;
            this.LeftMergeTimeR=tuple.getLongByField("Time");
        }
        if (tuple.getSourceStreamId().equals("RightCheckForMerge")) {
            // this.rightStreamSmallerQueueMerge.offer(tuple);
            rightStreamMergeSmaller = true;
            this.RightMergeTimeS=tuple.getLongByField("Time");
        }
        // leftStreamSmaller If the Merging TUples arrives from R.Duration
        if (tuple.getSourceStreamId().equals(leftStreamSmaller)) {

            try {
                this.bufferedWriter.write("LeftSmaller"+tuple.getValue(0)+","+tuple.getValue(1)+","+System.currentTimeMillis()+"\n");
                this.bufferedWriter.flush();
                leftInsertionTuplesSmaller(tuple);
            } catch (Exception e) {
                try {
                    this.bufferedWriter.write("LeftSmaller=="+e+"\n");
                    this.bufferedWriter.flush();
                } catch (IOException ex) {
                    ex.printStackTrace();
                }

                e.printStackTrace();
            }
        }
        //rightStream If merging tuples arrives from S.Time
        if (tuple.getSourceStreamId().equals(rightStreamSmaller)) {
            try {
                this.bufferedWriter.write("RightSmaller"+tuple.getValue(0)+","+tuple.getValue(1)+","+System.currentTimeMillis()+"\n");
                this.bufferedWriter.flush();
                rightInsertionTupleSmaller(tuple);
            } catch (Exception e) {
                e.printStackTrace();
                try {
                    this.bufferedWriter.write("RightSmaller"+e+"\n");
                    this.bufferedWriter.flush();
                } catch (IOException ex) {
                    ex.printStackTrace();
                }

            }
        }


        /// Change For Both Right Stream and Left Stream depend upon which sliding window you are using
        if (leftBooleanTupleRemovalCounter && rightBooleanTupleRemovalCounter) {
          //removing tuples only remove last CSS TREE from both R an S such as R.Duration and S.Time
            rightStreamLinkedListCSSTree.remove(rightStreamLinkedListCSSTree.getFirst());
            leftStreamLinkedListCSSTree.remove(leftStreamLinkedListCSSTree.getFirst());
            tupleRemovalCount = tupleRemovalCount - Constants.MUTABLE_WINDOW_SIZE; // Reintilize counter with just substract one length of CSS structure;
            // set removal boolean to false again
            leftBooleanTupleRemovalCounter = false;
            rightBooleanTupleRemovalCounter = false;

        }

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declareStream("LeftPredicate", new Fields(Constants.LEFT_HASH_SET, Constants.TUPLE_ID ,Constants.KAFKA_SPOUT_TIME, Constants.KAFKA_TIME, "StartEvaluationTime","EndEvaluationTime",
                "TaskId","HostName"));
        outputFieldsDeclarer.declareStream("RightPredicate", new Fields(Constants.RIGHT_HASH_SET, Constants.TUPLE_ID ,Constants.KAFKA_SPOUT_TIME, Constants.KAFKA_TIME, "StartEvaluationTime","EndEvaluationTime",
                "TaskId","HostName"));
        outputFieldsDeclarer.declareStream("LeftMergeBitSet", new Fields(Constants.LEFT_HASH_SET, Constants.TUPLE_ID, "MergeTimeR","MergecompleteTime", "MergeTupleEvaluationTime", "taskID", "hostName"));

    }
    // Probing results insert tuple R.Duration search Greater to all for every new tuple.
    public BitSet probingResultsSmaller(Tuple tuple, LinkedList<CSSTreeUpdated> linkedListCSSTree) {
       // HashSet<Integer> leftHashSet = new HashSet<>();
        // Iterate all linked list
        BitSet bitSet= new BitSet();
        if (!linkedListCSSTree.isEmpty()) {
            tupleRemovalCount++;
            // Probe to all linked list
            for (CSSTreeUpdated cssTree : linkedListCSSTree) {
               // leftHashSet.addAll(cssTree.searchGreater(tuple.getIntegerByField("Duration")));
                cssTree.searchGreaterBitSet(tuple.getIntegerByField("Duration"), bitSet);

            }
            return bitSet;

        }
        return null;

    }
    // Probing results insert tuple S.Time search Greater to all for every new tuple.
    public BitSet probingResultsGreater(Tuple tuple, LinkedList<CSSTreeUpdated> linkedListCSSTree) {

        //HashSet<Integer> rightHashSet = new HashSet<>();
        BitSet bitSet= new BitSet();
        if (!linkedListCSSTree.isEmpty()) {
            tupleRemovalCount++;
            for (CSSTreeUpdated cssTree : linkedListCSSTree) {

                //rightHashSet.addAll(cssTree.searchSmaller(tuple.getIntegerByField("Time")));
                cssTree.searchSmallerBitSet(tuple.getIntegerByField("Time"),bitSet);

            }
            return bitSet;

        }
        return null;
    }
    // Insertion R.Duration insert into the CSS Tree
    public void leftInsertionTuplesSmaller(Tuple tuple) throws IOException {
        if (tuple.getBooleanByField(Constants.BATCH_CSS_FLAG)) { // If tuples arrive and have flag true;
            leftStreamLinkedListCSSTree.add(leftStreamCSSTree);
                // Evaluating tuples during merge

//            if (leftStreamMergeSmaller) {
//                long completeMergeTime=System.currentTimeMillis();
//                int i = 0;
//                for (Tuple tuples : leftStreamSmallerQueueMerge) {
//                    i = i + 1;
//                    BitSet bitSet= new BitSet();
//                    leftStreamCSSTree.searchGreaterBitSet(tuples.getIntegerByField("Duration"),bitSet);
//
//                    outputCollector.emit("LeftMergeBitSet", new Values(convertToByteArray(bitSet), i,LeftMergeTimeR,completeMergeTime,System.currentTimeMillis(), taskID, hostName));
//
//                }
//                // flush the queue that maintain that tuples
//                leftStreamSmallerQueueMerge = new LinkedList<>();
//                leftStreamMergeSmaller = false; // set associated flag to false
//            }
            // create new CSS tree object
            leftStreamCSSTree = new CSSTreeUpdated(Constants.ORDER_OF_CSS_TREE);
            // updated the tuple removal counter by newly added tuples from immutable part
            tupleRemovalCount = tupleRemovalCount + Constants.MUTABLE_WINDOW_SIZE;
            // If tuple removal counter approach to limit then its associated boolean is on the structure is removed only when both R.Duration and S.Duration or union of both tuples reaches to the limit
            if (tupleRemovalCount >= Constants.IMMUTABLE_CSS_PART_REMOVAL) {
                this.leftBooleanTupleRemovalCounter = true;
                //  leftStreamLinkedListCSSTree.remove(leftStreamLinkedListCSSTree.getLast());
            }
        } else {
            // leftStreamCSSTree is used for inserting tuples first all upstream tuples are inserted first into css tree
            leftStreamCSSTree.insert(tuple.getIntegerByField(Constants.BATCH_CSS_TREE_KEY),
                    tuple.getIntegerByField(Constants.BATCH_CSS_TREE_VALUES));
        }
    }

    public void rightInsertionTupleSmaller(Tuple tuple) throws IOException {

        if (tuple.getBooleanByField(Constants.BATCH_CSS_FLAG)) {
            rightStreamLinkedListCSSTree.add(rightStreamCSSTree);
//            if (rightStreamMergeSmaller) {
//                long completeMergeTime=System.currentTimeMillis();
//                int i = 0;
//                for (Tuple tuples : rightStreamSmallerQueueMerge) {
//                    BitSet bitSet= new BitSet();
//                    i = i + 1;
//                     rightStreamCSSTree.searchGreaterBitSet(tuples.getIntegerByField("Time"),bitSet);
//                    outputCollector.emit("LeftMergeBitSet", new Values(convertToByteArray(bitSet), i,RightMergeTimeS,completeMergeTime, System.currentTimeMillis(),taskID, hostName));
//
//                }
//                rightStreamSmallerQueueMerge = new LinkedList<>();
//                rightStreamMergeSmaller = false;
//            }
            rightStreamCSSTree = new CSSTreeUpdated(Constants.ORDER_OF_CSS_TREE);
            tupleRemovalCount = tupleRemovalCount + Constants.MUTABLE_WINDOW_SIZE;
            if (tupleRemovalCount >= Constants.IMMUTABLE_CSS_PART_REMOVAL) {
                this.rightBooleanTupleRemovalCounter = true;
            }
        } else {

            rightStreamCSSTree.insert(tuple.getIntegerByField(Constants.BATCH_CSS_TREE_KEY),
                    tuple.getIntegerByField(Constants.BATCH_CSS_TREE_VALUES));

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

//    public static byte[] convertHashSetToByteArray(HashSet<Integer> hashSet) {
//        ByteArrayOutputStream bos = new ByteArrayOutputStream();
//        try {
//            ObjectOutputStream oos = new ObjectOutputStream(bos);
//            oos.writeObject(hashSet);
//            oos.flush();
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
//        return bos.toByteArray();
//    }
public synchronized byte[] convertToByteArray(Serializable object) throws IOException {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    try (ObjectOutputStream objectOutputStream = new ObjectOutputStream(baos)) {
        objectOutputStream.writeObject(object);
        objectOutputStream.flush();
    }
    return baos.toByteArray();
}
}
