package com.experiment.selfjoin.csstree;

import com.configurationsandconstants.iejoinandbaseworks.Configuration;
import com.configurationsandconstants.iejoinandbaseworks.Constants;
import com.stormiequality.BTree.BPlusTree;
import com.stormiequality.BTree.Key;
import com.stormiequality.BTree.Node;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.net.InetAddress;
import java.util.BitSet;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

public class LeftPredicateCSSTreeBoltSelfJoin extends BaseRichBolt {
    private BPlusTree leftStreamBPlusTree = null;
    private int archiveCount;
    private int counter;
    private int orderOfTreeBothBPlusTreeAndCSSTree;

    private OutputCollector outputCollector;
    private String leftStreamGreater;

    private String leftPredicateSourceStreamIDHashSet;

    private int taskID;
    private String hostName;
    private long tupleArrivalTime;

    public LeftPredicateCSSTreeBoltSelfJoin() {
        // Map that defines the constants StreamID For Every bolt
        Map<String, Object> map = Configuration.configurationConstantForStreamIDs();
        // Left Stream ID from UP Stream Component Bolt
        this.leftStreamGreater = (String) map.get("LeftGreaterPredicateTuple");
        // Right Stream ID from UP Stream Component Bolt

        // Stream ID for emitting
        this.leftPredicateSourceStreamIDHashSet = (String) map.get("LeftPredicateSourceStreamIDHashSet");
    }

    @Override
    public void prepare(Map<String, Object> map, TopologyContext topologyContext, OutputCollector outputCollector) {
        // For Mutable part initialization

        this.leftStreamBPlusTree = new BPlusTree(orderOfTreeBothBPlusTreeAndCSSTree);
        this.outputCollector = outputCollector;
        try{
            this.taskID=topologyContext.getThisTaskId();
            this.hostName= InetAddress.getLocalHost().getHostName();
        }catch (Exception e){
            e.printStackTrace();
        }
        // Output collector

    }

    @Override
    public void execute(Tuple tuple) {
        this.tupleArrivalTime=System.currentTimeMillis();
        // Counter for mutable to immutable merge
        if (tuple.getSourceStreamId().equals(leftStreamGreater)) {
            // Stream from left StreamID; per
            counter++;
            try {
                leftStreamPredicateEvaluation(tuple, outputCollector);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        if (counter == Constants.MUTABLE_WINDOW_SIZE) {
            // When merging counter reached:
            // Just 2 flags to downstream to holds data for compeletion of tuples;
            // One flag for left and other for right
            this.outputCollector.emit("LeftCheckForMerge", new Values(true, System.currentTimeMillis()));

            // start merging: Node from left
            Node nodeForLeft = leftStreamBPlusTree.leftMostNode();
            // data merging to CSS
            dataMergingToCSS(nodeForLeft, tuple, this.leftStreamGreater);
            // start merging node from right

            // data for merging

            counter = 0; // reinitialize counter
            // Re initialization of BTrees:
            leftStreamBPlusTree = new BPlusTree(Constants.ORDER_OF_B_PLUS_TREE);

        }


    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        // Declaring for hashset evaluation
        outputFieldsDeclarer.declareStream(this.leftPredicateSourceStreamIDHashSet, new Fields(Constants.LEFT_HASH_SET, Constants.TUPLE_ID, Constants.KAFKA_TIME, Constants.KAFKA_SPOUT_TIME,Constants.SPLIT_BOLT_TIME,Constants.TASK_ID_FOR_SPLIT_BOLT,Constants.HOST_NAME_FOR_SPLIT_BOLT, "TupleArrivalTime",
                Constants.GREATER_PREDICATE_EVALUATION_TIME_BOLT, Constants.MUTABLE_BOLT_TASK_ID, Constants.MUTABLE_BOLT_MACHINE));
        // Two stream same as upstream as downstream for merging
        outputFieldsDeclarer.declareStream(this.leftStreamGreater, new Fields(Constants.BATCH_CSS_TREE_KEY, Constants.BATCH_CSS_TREE_VALUES, Constants.BATCH_CSS_FLAG));
          // Flags holding tmp data during merge
        outputFieldsDeclarer.declareStream("LeftCheckForMerge", new Fields("mergeFlag","Time"));


    }


    // Right Stream evaluation R<S
    // Here in R<S means from all greater and vice versa for other
    private void leftStreamPredicateEvaluation(Tuple tuple, OutputCollector outputCollector) throws IOException {
        // Tuple from S stream evaluates
        leftStreamBPlusTree.insert(tuple.getIntegerByField(Constants.TUPLE), tuple.getIntegerByField(Constants.TUPLE_ID));
        BitSet bitSet=leftStreamBPlusTree.lessThenSpecificValue(tuple.getIntegerByField(Constants.TUPLE));

      //  HashSet<Integer> hashSetIds = leftStreamBPlusTree.smallerThenSpecificValueHashSet(tuple.getIntegerByField(Constants.TUPLE));
        //Results form BPlus Tree
        if (bitSet != null) {
            outputCollector.emit(this.leftPredicateSourceStreamIDHashSet, tuple, new Values(convertToByteArray(bitSet), tuple.getIntegerByField(Constants.TUPLE_ID),
                    tuple.getLongByField(Constants.KAFKA_TIME),tuple.getLongByField(Constants.KAFKA_SPOUT_TIME),tuple.getLongByField(Constants.SPLIT_BOLT_TIME),
                    tuple.getIntegerByField(Constants.TASK_ID_FOR_SPLIT_BOLT),tuple.getStringByField(Constants.HOST_NAME_FOR_SPLIT_BOLT), tupleArrivalTime,System.currentTimeMillis(),taskID, hostName));
            outputCollector.ack(tuple);
        }
    }

    // Simply iterating from from left most node to the end for iteration
    public void dataMergingToCSS(Node node, Tuple tuple, String streamID) {
// node to end of all leaf nodes log n for finding the left node from root and linear scna to end n
        while (node != null) {
            for (Key key : node.getKeys()) {
                for(int value: key.getValues()) {
                    outputCollector.emit(streamID, tuple, new Values(key.getKey(), value, false));
                    outputCollector.ack(tuple);
                }
            }
            node = node.getNext();
        }
        // last tuple that indicate the completeness of all tuples with true flag
        outputCollector.emit(streamID, tuple, new Values(0, 0, true));
        outputCollector.ack(tuple);


    }
    public synchronized byte[] convertToByteArray(Serializable object) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (ObjectOutputStream objectOutputStream = new ObjectOutputStream(baos)) {
            objectOutputStream.writeObject(object);
            objectOutputStream.flush();
        }
        return baos.toByteArray();
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
