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

public class RightPredicateCSSTreeBolt extends BaseRichBolt {
    private BPlusTreeUpdated leftStreamBPlusTree = null;
    private BPlusTreeUpdated rightStreamBPlusTree = null;
    private int counter;
    private int orderOfTreeBothBPlusTreeAndCSSTree;
    private OutputCollector outputCollector;
    private String leftStreamGreater;
    private String rightStreamGreater;
    private String rightStreamHashSetEmitterStreamID;
    private String leftPredicateSourceStreamIDHashSet;
    private List<Integer> listOfDownStreamTasks;
    private int downStreamTask;
    // This data for metrics:
    // Task ID
    private int taskID;
    // Host Address
    private String hostName;
    // private Buffered writter
    private BufferedWriter bufferedWriter;

    public RightPredicateCSSTreeBolt() {
        this.orderOfTreeBothBPlusTreeAndCSSTree = Constants.ORDER_OF_CSS_TREE;
        Map<String, Object> map = Configuration.configurationConstantForStreamIDs();
        this.leftStreamGreater = (String) map.get("LeftGreaterPredicateTuple");
        this.rightStreamGreater = (String) map.get("RightGreaterPredicateTuple");
        this.rightStreamHashSetEmitterStreamID = (String) map.get("RightPredicateSourceStreamIDHashSet");
        this.leftPredicateSourceStreamIDHashSet = (String) map.get("LeftPredicateSourceStreamIDHashSet");

    }

    @Override
    public void prepare(Map<String, Object> map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.leftStreamBPlusTree = new BPlusTreeUpdated(orderOfTreeBothBPlusTreeAndCSSTree);
        this.rightStreamBPlusTree = new BPlusTreeUpdated(orderOfTreeBothBPlusTreeAndCSSTree);
        this.listOfDownStreamTasks= topologyContext.getComponentTasks(Constants.RIGHT_PREDICATE_CSS_TREE_BOLT);
        this.downStreamTask=0;
        this.outputCollector = outputCollector;
        // HostName  and Task ID
        taskID= topologyContext.getThisTaskId();
        try{
            hostName= InetAddress.getLocalHost().getHostName();
            this.bufferedWriter= new BufferedWriter(new FileWriter(new File("/home/adeel/Data/Results/RightEmitting.csv")));

        }catch (Exception e){
            e.printStackTrace();
        }
    }

    @Override
    public void execute(Tuple tuple) {
        long tupleArrivalTime=System.currentTimeMillis();
        counter++;
        if (tuple.getSourceStreamId().equals(leftStreamGreater)) {
            leftPredicateEvaluation(tuple, outputCollector, tupleArrivalTime);
        }
        if (tuple.getSourceStreamId().equals(rightStreamGreater)) {
            rightPredicateEvaluation(tuple, outputCollector, tupleArrivalTime);
        }
//        if (counter == Constants.MUTABLE_WINDOW_SIZE) {
//
////            this.outputCollector.emit("LeftCheckForMerge",new Values(true, System.currentTimeMillis()));
////            this.outputCollector.emit("RightCheckForMerge",new Values(true, System.currentTimeMillis()));
//
//            try {
//                Node nodeForLeft = leftStreamBPlusTree.leftMostNode();
//                dataMergingToCSS(nodeForLeft,tuple,this.leftStreamGreater);
//                Node nodeForRight=rightStreamBPlusTree.leftMostNode();
//                dataMergingToCSS(nodeForRight,tuple,this.rightStreamGreater);
//                this.bufferedWriter.write("Emittted \n");
//                this.bufferedWriter.flush();
//            } catch (Exception e) {
//                e.printStackTrace();
//            }
//
//            counter=0;
//            leftStreamBPlusTree= new BPlusTreeUpdated(Constants.ORDER_OF_B_PLUS_TREE);
//            rightStreamBPlusTree= new BPlusTreeUpdated(Constants.ORDER_OF_B_PLUS_TREE);
//
//        }


    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

        outputFieldsDeclarer.declareStream(this.rightStreamHashSetEmitterStreamID, new Fields(Constants.RIGHT_HASH_SET, Constants.TUPLE_ID, Constants.KAFKA_TIME,Constants.KAFKA_SPOUT_TIME, Constants.SPLIT_BOLT_TIME, Constants.TASK_ID_FOR_SPLIT_BOLT, Constants.HOST_NAME_FOR_SPLIT_BOLT,
       "TupleArrivalTime","TupleEvaluationTime","HostName", "TaskID") );
        outputFieldsDeclarer.declareStream(this.leftPredicateSourceStreamIDHashSet, new Fields(Constants.LEFT_HASH_SET, Constants.TUPLE_ID, Constants.KAFKA_TIME,Constants.KAFKA_SPOUT_TIME, Constants.SPLIT_BOLT_TIME, Constants.TASK_ID_FOR_SPLIT_BOLT, Constants.HOST_NAME_FOR_SPLIT_BOLT,
                "TupleArrivalTime","TupleEvaluationTime","HostName", "TaskID") );
        outputFieldsDeclarer.declareStream(this.leftStreamGreater, new Fields(Constants.BATCH_CSS_TREE_KEY, Constants.BATCH_CSS_TREE_VALUES, Constants.BATCH_CSS_FLAG));
        outputFieldsDeclarer.declareStream(this.rightStreamGreater, new Fields(Constants.BATCH_CSS_TREE_KEY, Constants.BATCH_CSS_TREE_VALUES, Constants.BATCH_CSS_FLAG));
//        outputFieldsDeclarer.declareStream("LeftCheckForMerge",new Fields("mergeFlag","Time"));
//        outputFieldsDeclarer.declareStream("RightCheckForMerge", new Fields("mergeFlag","Time"));

    }

    private void rightPredicateEvaluation(Tuple tuple, OutputCollector outputCollector, long tupleArrivalTime) {
        // Insertion
        rightStreamBPlusTree.insert(tuple.getIntegerByField(Constants.TUPLE), tuple.getIntegerByField(Constants.TUPLE_ID));
        HashSet<Integer> hashSetIds = leftStreamBPlusTree.greaterThenSpecificValueHashSet(tuple.getIntegerByField(Constants.TUPLE));
        //Results form BPlus Tree searching
        if (hashSetIds != null) {
            // emitting Logic
            outputCollector.emit(this.rightStreamHashSetEmitterStreamID, tuple, new Values(convertHashSetToByteArray(hashSetIds), tuple.getIntegerByField(Constants.TUPLE_ID)+"R" ,tuple.getValue(2),
                    tuple.getValue(3),tuple.getValue(4),tuple.getValue(5),tuple.getValue(6),tupleArrivalTime, System.currentTimeMillis(), taskID, hostName));
            outputCollector.ack(tuple);
        }



    }

    private void leftPredicateEvaluation(Tuple tuple, OutputCollector outputCollector, long tupleArrivalTime) {
        leftStreamBPlusTree.insert(tuple.getIntegerByField(Constants.TUPLE), tuple.getIntegerByField(Constants.TUPLE_ID));
        HashSet<Integer> hashSetIds = rightStreamBPlusTree.smallerThenSpecificValueHashSet(tuple.getIntegerByField(Constants.TUPLE));
        if (hashSetIds != null) {
            // emitting Logic
            outputCollector.emit(this.leftPredicateSourceStreamIDHashSet, tuple, new Values(convertHashSetToByteArray(hashSetIds), tuple.getIntegerByField(Constants.TUPLE_ID)+"L",tuple.getValue(2),
                    tuple.getValue(3),tuple.getValue(4),tuple.getValue(5),tuple.getValue(6),tupleArrivalTime, System.currentTimeMillis(), taskID, hostName));
            outputCollector.ack(tuple);
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

    private static byte[] convertToByteArray(List<Integer> integerList) {
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        for (Integer num : integerList) {
            outputStream.write(num.byteValue());
        }
        return outputStream.toByteArray();
    }
    public void dataMergingToCSS( Node node, Tuple tuple, String streamID) throws InterruptedException {

        while (node != null) {
            for (Key key : node.getKeys()) {
                for(int i=0;i<key.getValues().size();i++){
                outputCollector.emit(streamID, tuple, new Values(key.getKey(), key.getValues().get(i), false));
                outputCollector.ack(tuple);

            }}
            node=node.getNext();
        }

        outputCollector.emit(streamID, tuple, new Values(0, 0, true));
        outputCollector.ack(tuple);
        downStreamTask++;
        if(downStreamTask==listOfDownStreamTasks.size()){
            downStreamTask=0;
        }


    }


}
