package com.baselinealgorithm.chainbplusandcss;

import com.configurationsandconstants.iejoinandbaseworks.Configuration;
import com.configurationsandconstants.iejoinandbaseworks.Constants;
import com.stormiequality.BTree.BPlusTreeUpdated;
import com.stormiequality.BTree.Key;
import com.stormiequality.BTree.Node;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.io.*;
import java.net.InetAddress;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

public class LeftPredicateCSSTreeBolt extends BaseRichBolt {
    private BPlusTreeUpdated leftStreamBPlusTree = null;
    private BPlusTreeUpdated rightStreamBPlusTree = null;
    private int archiveCount;
    private int counter;
    private int orderOfTreeBothBPlusTreeAndCSSTree;

    private OutputCollector outputCollector;
    private String leftStreamSmaller;
    private String rightStreamSmaller;
    private String leftPredicateSourceStreamIDHashSet;
    private String rightStreamHashSetEmitterStreamID;
    // Number of Tasks
    private List<Integer> listOfTasks;
    private int downStreamTaskLeft;
    private int downStreamTaskRight;
    // Metrics
    private int taskID;
    //HostName
    private String hostName;

    //
    private BufferedWriter bufferedWriter;


    public LeftPredicateCSSTreeBolt() {
        // Map that defines the constants StreamID For Every bolt
        Map<String, Object> map = Configuration.configurationConstantForStreamIDs();
        // Left Stream ID from UP Stream Component Bolt
        this.leftStreamSmaller = (String) map.get("LeftSmallerPredicateTuple");
        // Right Stream ID from UP Stream Component Bolt
        this.rightStreamSmaller = (String) map.get("RightSmallerPredicateTuple");
        // Stream ID for emitting
        this.leftPredicateSourceStreamIDHashSet = (String) map.get("LeftPredicateSourceStreamIDHashSet");
        this.rightStreamHashSetEmitterStreamID = (String) map.get("RightPredicateSourceStreamIDHashSet");

    }

    @Override
    public void prepare(Map<String, Object> map, TopologyContext topologyContext, OutputCollector outputCollector) {
        // For Mutable part initialization

        this.leftStreamBPlusTree = new BPlusTreeUpdated(orderOfTreeBothBPlusTreeAndCSSTree);
        this.rightStreamBPlusTree = new BPlusTreeUpdated(orderOfTreeBothBPlusTreeAndCSSTree);
        // Output collector
        this.outputCollector = outputCollector;
        this.downStreamTaskLeft =0;
        this.downStreamTaskRight=0;
        this.listOfTasks= topologyContext.getComponentTasks(Constants.LEFT_PREDICATE_IMMUTABLE_CSS);
        try{
            this.taskID= topologyContext.getThisTaskId();
            this.hostName= InetAddress.getLocalHost().getHostName();
            this.bufferedWriter= new BufferedWriter(new FileWriter(new File("/home/adeel/Data/Results/LeftEmitting.csv")));

        }catch (Exception e){
            e.printStackTrace();
        }
    }

    @Override
    public void execute(Tuple tuple) {
        long tupleArrivalTime=System.currentTimeMillis();

        counter++; // Counter for mutable to immutable merge
        if (tuple.getSourceStreamId().equals(leftStreamSmaller)) {
            // Stream from left StreamID; per
            leftStreamSmallerPredicateEvaluation(tuple, outputCollector, tupleArrivalTime);
        }
        if (tuple.getSourceStreamId().equals(rightStreamSmaller)) {
            // Stream From right StreamID
            rightStreamSmallerPredicateEvaluation(tuple, outputCollector, tupleArrivalTime);
        }
//        if (counter == Constants.MUTABLE_WINDOW_SIZE) {
//            // When merging counter reached:
//            // Just 2 flags to downstream to holds data for compeletion of tuples;
//            // One flag for left and other for right
////            this.outputCollector.emit("LeftCheckForMerge", new Values(true,System.currentTimeMillis()));
////            this.outputCollector.emit("RightCheckForMerge", new Values(true, System.currentTimeMillis()));
//            // start merging: Node from left
//
//            // data merging to CSS
//
//            // start merging node from right
//            try {
//                Node nodeForLeft = leftStreamBPlusTree.leftMostNode();
//                dataMergingToCSS(nodeForLeft, tuple, this.leftStreamSmaller);
//                Node nodeForRight = rightStreamBPlusTree.leftMostNode();
//                // data for merging
//                dataMergingToCSS(nodeForRight, tuple, this.rightStreamSmaller);
//                this.bufferedWriter.write("LeftEmitted \n");
//                this.bufferedWriter.flush();
//            } catch (Exception e) {
//                e.printStackTrace();
//            }
//
//            counter = 0; // reinitialize counter
//            // Re initialization of BTrees:
//            leftStreamBPlusTree = new BPlusTreeUpdated(Constants.ORDER_OF_B_PLUS_TREE);
//            rightStreamBPlusTree = new BPlusTreeUpdated(Constants.ORDER_OF_B_PLUS_TREE);
//        }


    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        // Declaring for hashset evaluation
        outputFieldsDeclarer.declareStream(this.leftPredicateSourceStreamIDHashSet, new Fields(Constants.LEFT_HASH_SET, Constants.TUPLE_ID,Constants.KAFKA_TIME,Constants.KAFKA_SPOUT_TIME, Constants.SPLIT_BOLT_TIME, Constants.TASK_ID_FOR_SPLIT_BOLT, Constants.HOST_NAME_FOR_SPLIT_BOLT,
                "TupleArrivalTime","TupleEvaluationTime","HostName", "TaskID"));
        // Two stream same as upstream as downstream for merging
        outputFieldsDeclarer.declareStream(this.rightStreamHashSetEmitterStreamID, new Fields(Constants.RIGHT_HASH_SET, Constants.TUPLE_ID,Constants.KAFKA_TIME,Constants.KAFKA_SPOUT_TIME, Constants.SPLIT_BOLT_TIME, Constants.TASK_ID_FOR_SPLIT_BOLT, Constants.HOST_NAME_FOR_SPLIT_BOLT,
                "TupleArrivalTime","TupleEvaluationTime","HostName", "TaskID"));


        outputFieldsDeclarer.declareStream(this.leftStreamSmaller, new Fields(Constants.BATCH_CSS_TREE_KEY, Constants.BATCH_CSS_TREE_VALUES, Constants.BATCH_CSS_FLAG));
        outputFieldsDeclarer.declareStream(this.rightStreamSmaller, new Fields(Constants.BATCH_CSS_TREE_KEY, Constants.BATCH_CSS_TREE_VALUES, Constants.BATCH_CSS_FLAG));
        // Flags holding tmp data during merge
//        outputFieldsDeclarer.declareStream("LeftCheckForMerge", new Fields("mergeFlag","Time"));
//        outputFieldsDeclarer.declareStream("RightCheckForMerge", new Fields("mergeFlag", "Time"));

    }

    private void rightStreamSmallerPredicateEvaluation(Tuple tuple, OutputCollector outputCollector,long tupleArrivalTime) {
// Insert data into BPlus tree:
        rightStreamBPlusTree.insert(tuple.getIntegerByField(Constants.TUPLE), tuple.getIntegerByField(Constants.TUPLE_ID));
// Search tuple from other field If Tuple from R it evaluates against S and Same as for R
        HashSet<Integer> hashSetIds = leftStreamBPlusTree.smallerThenSpecificValueHashSet(tuple.getIntegerByField(Constants.TUPLE));
        if (hashSetIds != null) {
            // emitting Logic
            // Emit for hash set evaluation.
            outputCollector.emit(this.rightStreamHashSetEmitterStreamID, tuple, new Values(convertHashSetToByteArray(hashSetIds), tuple.getIntegerByField(Constants.TUPLE_ID)+"R",tuple.getValue(2),
                    tuple.getValue(3),tuple.getValue(4),tuple.getValue(5),tuple.getValue(6),tupleArrivalTime, System.currentTimeMillis(), taskID, hostName));
            outputCollector.ack(tuple);
        }


    }

    // Right Stream evaluation R<S
    // Here in R<S means from all greater and vice versa for other
    private void leftStreamSmallerPredicateEvaluation(Tuple tuple, OutputCollector outputCollector, long tupleArrivalTime) {
        // Tuple from S stream evaluates
        leftStreamBPlusTree.insert(tuple.getIntegerByField(Constants.TUPLE), tuple.getIntegerByField(Constants.TUPLE_ID));
        HashSet<Integer> hashSetIds = rightStreamBPlusTree.greaterThenSpecificValueHashSet(tuple.getIntegerByField(Constants.TUPLE));
        //Results form BPlus Tree
        if (hashSetIds != null) {
            outputCollector.emit(this.leftPredicateSourceStreamIDHashSet, tuple, new Values(convertHashSetToByteArray(hashSetIds), tuple.getIntegerByField(Constants.TUPLE_ID)+"L", tuple.getValue(2),
                    tuple.getValue(3),tuple.getValue(4),tuple.getValue(5),tuple.getValue(6),tupleArrivalTime, System.currentTimeMillis(), taskID, hostName));
            outputCollector.ack(tuple);
        }
    }

    // Simply iterating from from left most node to the end for iteration
    public void dataMergingToCSS(Node node, Tuple tuple, String streamID) throws InterruptedException {
// node to end of all leaf nodes log n for finding the left node from root and linear scna to end n
        while (node != null) {
            for (Key key : node.getKeys()) {
                for(int i=0;i<key.getValues().size();i++){
                outputCollector.emit(streamID, tuple, new Values(key.getKey(), key.getValues().get(i), false));
                outputCollector.ack(tuple);
            }}
            node = node.getNext();
        }
        // last tuple that indicate the completeness of all tuples with true flag
        outputCollector.emit(streamID, tuple, new Values(0, 0, true));
        outputCollector.ack(tuple);
        downStreamTaskRight++;
        if(downStreamTaskRight ==listOfTasks.size()){
            downStreamTaskRight =0;
        }


    }
    // Byte array for Hash Set
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

    // Byte array conversion because While emitting we are emitting values as bytes array to downstream
    // A key has multple values these values are byte array
    private static byte[] convertToByteArray(List<Integer> integerList) {
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        for (Integer num : integerList) {
            outputStream.write(num.byteValue());
        }
        return outputStream.toByteArray();
    }


}
