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

public class RightPredicateImmutableCSSBolt extends BaseRichBolt {
    private LinkedList<CSSTreeUpdated> leftStreamLinkedListCSSTree = null;
    private LinkedList<CSSTreeUpdated> rightStreamLinkedListCSSTree = null;
    private String leftStreamGreater;
    private String rightStreamGreater;
    private HashSet<Integer> leftStreamTuplesHashSet = null;
    private HashSet<Integer> rightStreamTuplesHashSet = null;
    private CSSTreeUpdated leftStreamCSSTree = null;
    private CSSTreeUpdated rightStreamCSSTree = null;
    private boolean leftStreamMergeGreater = false;
    private boolean rightStreamMergeGreater = false;
    private Queue<Tuple> leftStreamGreaterQueueMerge = null;
    private Queue<Tuple> rightStreamGreaterQueueMerge = null;
    private static int tupleRemovalCount = 0;

    private boolean leftBooleanTupleRemovalCounter = false;
    private boolean rightBooleanTupleRemovalCounter = false;
    private OutputCollector outputCollector;
///Left merge
    private long LeftMergeTimeR;
    private long RightMergeTimeS;
    private int taskID;
    private String hostName;
    private BufferedWriter bufferedWriter;
    private ArrayList<TupleModel> leftArrayList;
    private ArrayList<TupleModel> rightArrayList;


    public RightPredicateImmutableCSSBolt() {
        Map<String, Object> map = Configuration.configurationConstantForStreamIDs();
        this.leftStreamGreater = (String) map.get("LeftGreaterPredicateTuple");
        this.rightStreamGreater = (String) map.get("RightGreaterPredicateTuple");
    }

    @Override
    public void prepare(Map<String, Object> map, TopologyContext topologyContext, OutputCollector outputCollector) {
        leftStreamLinkedListCSSTree = new LinkedList<>();
        rightStreamLinkedListCSSTree = new LinkedList<>();
        leftStreamCSSTree = new CSSTreeUpdated(Constants.ORDER_OF_CSS_TREE);
        rightStreamCSSTree = new CSSTreeUpdated(Constants.ORDER_OF_CSS_TREE);
        this.leftStreamGreaterQueueMerge = new LinkedList<>();
        this.rightStreamGreaterQueueMerge = new LinkedList<>();
        this.taskID= topologyContext.getThisTaskId();
        try {
            this.bufferedWriter= new BufferedWriter(new FileWriter(new File("/home/adeel/Data/Results/RightPredicate.csv")));
            this.hostName= InetAddress.getLocalHost().getHostName();
            this.leftArrayList= new ArrayList<>();
            this.rightArrayList= new ArrayList<>();
        } catch (Exception e) {
            e.printStackTrace();
        }
        this.outputCollector = outputCollector;

    }

    @Override
    public void execute(Tuple tuple) {
        if (tuple.getSourceStreamId().equals("StreamR")) {
            long startTime=System.currentTimeMillis();
            if (leftStreamMergeGreater) {
                leftStreamGreaterQueueMerge.offer(tuple);
            }
            BitSet leftBitSet = probingResultsGreater(tuple, rightStreamLinkedListCSSTree);
            if (leftBitSet != null) {
                try {
                    outputCollector.emit("LeftPredicate", tuple, new Values(convertToByteArray(leftBitSet), tuple.getIntegerByField("ID")+"L", tuple.getLongByField(Constants.KAFKA_TIME), tuple.getLongByField(Constants.KAFKA_SPOUT_TIME),
                            startTime,System.currentTimeMillis(), taskID, hostName));
                } catch (IOException e) {
                    e.printStackTrace();
                }
                outputCollector.ack(tuple);

            }
            leftArrayList.add(new TupleModel(tuple.getIntegerByField("Revenue"), tuple.getIntegerByField("ID")));
            if(leftArrayList.size()==100000){
                CSSTreeUpdated cssTreeUpdated= new CSSTreeUpdated(4);
                for(int i=0;i<leftArrayList.size();i++){
                    cssTreeUpdated.insert(leftArrayList.get(i).getTuple(),leftArrayList.get(i).getId());
                }
                leftStreamLinkedListCSSTree.add(cssTreeUpdated);
                leftArrayList= new ArrayList<>();
            }
        }
        if (tuple.getSourceStreamId().equals("StreamS")) {
            long startTime=System.currentTimeMillis();
            if (rightStreamMergeGreater) {
                rightStreamGreaterQueueMerge.offer(tuple);
            }
            BitSet rightBitSet = probingResultsSmaller(tuple, leftStreamLinkedListCSSTree);
            if (rightBitSet != null) {

                try {
                    outputCollector.emit("RightPredicate", tuple, new Values(convertToByteArray(rightBitSet), tuple.getIntegerByField("ID")+"R", tuple.getValue(2), tuple.getValue(3),
                            startTime, System.currentTimeMillis(),taskID, hostName));
                } catch (IOException e) {
                    e.printStackTrace();
                }
                outputCollector.ack(tuple);
            }
            rightArrayList.add(new TupleModel(tuple.getIntegerByField("Cost"), tuple.getIntegerByField("ID")));
            if(rightArrayList.size()==100000){
                CSSTreeUpdated cssTreeUpdated= new CSSTreeUpdated(4);
                for(int i=0;i<rightArrayList.size();i++){
                    cssTreeUpdated.insert(rightArrayList.get(i).getTuple(),rightArrayList.get(i).getId());
                }
                rightStreamLinkedListCSSTree.add(cssTreeUpdated);
                rightArrayList= new ArrayList<>();
            }
        }

        if (tuple.getSourceStreamId().equals(leftStreamGreater)) {
            try {
                bufferedWriter.write("LEft"+ tuple.getValue(0)+","+tuple.getValue(1)+","+System.currentTimeMillis()+"\n");
                this.bufferedWriter.flush();
                leftInsertionTupleGreater(tuple);
            } catch (Exception e) {
                try {
                    bufferedWriter.write("LEft==="+ e+"\n");
                    this.bufferedWriter.flush();
                } catch (IOException ex) {
                    ex.printStackTrace();
                }

                e.printStackTrace();
            }
        }
        if (tuple.getSourceStreamId().equals(rightStreamGreater)) {
            try {
                bufferedWriter.write("Right"+ tuple.getValue(0)+","+tuple.getValue(1)+","+System.currentTimeMillis()+"\n");
                this.bufferedWriter.flush();
                rightInsertionTupleGreater(tuple);
            } catch (IOException e) {
                try {
                    bufferedWriter.write("Right=="+ e+"\n");
                    this.bufferedWriter.flush();
                } catch (IOException ex) {
                    ex.printStackTrace();
                }

                e.printStackTrace();
            }
        }
        if (tuple.getSourceStreamId().equals("LeftCheckForMerge")) {
            LeftMergeTimeR=tuple.getLongByField("Time");
            leftStreamMergeGreater = true;

        }
        if (tuple.getSourceStreamId().equals("RightCheckForMerge")) {
            RightMergeTimeS=tuple.getLongByField("Time");
            rightStreamMergeGreater = true;
        }
        if (leftBooleanTupleRemovalCounter && rightBooleanTupleRemovalCounter) {
            rightStreamLinkedListCSSTree.remove(rightStreamLinkedListCSSTree.getFirst());
            leftStreamLinkedListCSSTree.remove(leftStreamLinkedListCSSTree.getFirst());
            tupleRemovalCount = tupleRemovalCount - Constants.MUTABLE_WINDOW_SIZE;
            leftBooleanTupleRemovalCounter = false;
            rightBooleanTupleRemovalCounter = false;

        }


    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declareStream("LeftPredicate", new Fields(Constants.LEFT_HASH_SET, Constants.TUPLE_ID, Constants.KAFKA_SPOUT_TIME, Constants.KAFKA_TIME, "StartEvaluationTime","EndEvaluationTime",
                "TaskId","HostName"));
        outputFieldsDeclarer.declareStream("RightPredicate", new Fields(Constants.RIGHT_HASH_SET, Constants.TUPLE_ID, Constants.KAFKA_SPOUT_TIME, Constants.KAFKA_TIME, "StartEvaluationTime","EndEvaluationTime",
                "TaskId","HostName"));
        outputFieldsDeclarer.declareStream("RightMergeBitSet", new Fields(Constants.RIGHT_HASH_SET, Constants.TUPLE_ID, "MergeTimeR","MergecompleteTime", "MergeTupleEvaluationTime", "taskID", "hostName"));

    }

    public BitSet probingResultsGreater(Tuple tuple, LinkedList<CSSTreeUpdated> linkedListCSSTree) {
      //  HashSet<Integer> hashSet = new HashSet<>();
        BitSet bitSet= new BitSet();
        if (!linkedListCSSTree.isEmpty()) {
            tupleRemovalCount++;
            for (CSSTreeUpdated cssTree : linkedListCSSTree) {
               cssTree.searchSmallerBitSet(tuple.getIntegerByField("Revenue"), bitSet);

            }
            return bitSet;

        }
        return null;
    }

    public BitSet probingResultsSmaller(Tuple tuple, LinkedList<CSSTreeUpdated> linkedListCSSTree) {
        //HashSet<Integer> hashSet = new HashSet<>();
        BitSet bitSet= new BitSet();
        if (!linkedListCSSTree.isEmpty()) {
            tupleRemovalCount++;
            for (CSSTreeUpdated cssTree : linkedListCSSTree) {
               cssTree.searchGreaterBitSet(tuple.getIntegerByField("Cost"), bitSet);

            }
            return bitSet;

        }
        return null;
    }

    public void leftInsertionTupleGreater(Tuple tuple) throws IOException {

        if (tuple.getBooleanByField(Constants.BATCH_CSS_FLAG)) {
            leftStreamLinkedListCSSTree.add(leftStreamCSSTree);
//            if (leftStreamMergeGreater) {
//                long completeTime=System.currentTimeMillis();
//                int i = 0;
//                for (Tuple tuples : leftStreamGreaterQueueMerge) {
//                    BitSet bitSet= new BitSet();
//                    leftStreamCSSTree.searchSmallerBitSet(tuples.getIntegerByField("Revenue"),bitSet);
//                    i = i + 1;
//                    outputCollector.emit("RightMergeBitSet", new Values(convertToByteArray(bitSet), i, LeftMergeTimeR,completeTime, System.currentTimeMillis(), taskID, hostName));
//                }
//                leftStreamGreaterQueueMerge = new LinkedList<>();
//                leftStreamMergeGreater = false;
//            }
            leftStreamCSSTree = new CSSTreeUpdated(Constants.ORDER_OF_CSS_TREE);
            tupleRemovalCount = tupleRemovalCount + Constants.MUTABLE_WINDOW_SIZE;
            if (tupleRemovalCount >= Constants.IMMUTABLE_CSS_PART_REMOVAL) {
                this.leftBooleanTupleRemovalCounter = true;
            }
        } else {

            leftStreamCSSTree.insert(tuple.getIntegerByField(Constants.BATCH_CSS_TREE_KEY),
                    tuple.getIntegerByField(Constants.BATCH_CSS_TREE_VALUES));
        }
    }

    public void rightInsertionTupleGreater(Tuple tuple) throws IOException {

        if (tuple.getBooleanByField(Constants.BATCH_CSS_FLAG)) {
            rightStreamLinkedListCSSTree.add(rightStreamCSSTree);
//            if (rightStreamMergeGreater) {
//                long completeTime=System.currentTimeMillis();
//                int i = 0;
//                for (Tuple tuples : rightStreamGreaterQueueMerge) {
//                    BitSet bitSet= new BitSet();
//                    i = i + 1;
//                   rightStreamCSSTree.searchSmallerBitSet(tuples.getIntegerByField("Cost"),bitSet);
//                    outputCollector.emit("RightMergeBitSet", new Values(convertToByteArray(bitSet), i, RightMergeTimeS, completeTime,System.currentTimeMillis(), taskID, hostName));
//                }
//                rightStreamGreaterQueueMerge = new LinkedList<>();
//                rightStreamMergeGreater = false;
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
    public synchronized byte[] convertToByteArray(Serializable object) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (ObjectOutputStream objectOutputStream = new ObjectOutputStream(baos)) {
            objectOutputStream.writeObject(object);
            objectOutputStream.flush();
        }
        return baos.toByteArray();
    }
}
