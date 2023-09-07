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

public class RightPredicateCSSTreeBoltSelfJoin extends BaseRichBolt {
    private BPlusTree rightStreamBPlusTree = null;
    private int counter;
    private int orderOfTreeBothBPlusTreeAndCSSTree;
    private OutputCollector outputCollector;
    private String rightStreamSmaller;
    private String rightStreamHashSetEmitterStreamID;

    private int taskID;
    private String hostName;
    private long tupleArrivalTime;

    public RightPredicateCSSTreeBoltSelfJoin() {
        this.orderOfTreeBothBPlusTreeAndCSSTree = Constants.ORDER_OF_CSS_TREE;
        Map<String, Object> map = Configuration.configurationConstantForStreamIDs();
        this.rightStreamSmaller = (String) map.get("RightSmallerPredicateTuple");
        this.rightStreamHashSetEmitterStreamID = (String) map.get("RightPredicateSourceStreamIDHashSet");
    }

    @Override
    public void prepare(Map<String, Object> map, TopologyContext topologyContext, OutputCollector outputCollector) {

        this.rightStreamBPlusTree = new BPlusTree(orderOfTreeBothBPlusTreeAndCSSTree);
        this.outputCollector = outputCollector;
        try{
            this.taskID=topologyContext.getThisTaskId();
            this.hostName= InetAddress.getLocalHost().getHostName();
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    @Override
    public void execute(Tuple tuple) {
        this.tupleArrivalTime=System.currentTimeMillis();
        if (tuple.getSourceStreamId().equals(rightStreamSmaller)) {
            counter++;
            try {
                rightPredicateEvaluation(tuple, outputCollector);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        if (counter == Constants.MUTABLE_WINDOW_SIZE) {
            this.outputCollector.emit("RightCheckForMerge", new Values(true,System.currentTimeMillis()));

            Node nodeForRight = rightStreamBPlusTree.leftMostNode();
            dataMergingToCSS(nodeForRight, tuple, this.rightStreamSmaller);
            counter = 0;
            rightStreamBPlusTree = new BPlusTree(Constants.ORDER_OF_B_PLUS_TREE);

        }


    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declareStream(this.rightStreamHashSetEmitterStreamID, new Fields(Constants.RIGHT_HASH_SET, Constants.TUPLE_ID, Constants.KAFKA_TIME, Constants.KAFKA_SPOUT_TIME,Constants.SPLIT_BOLT_TIME,Constants.TASK_ID_FOR_SPLIT_BOLT,Constants.HOST_NAME_FOR_SPLIT_BOLT, "TupleArrivalTime",
                Constants.LESSER_PREDICATE_EVALUATION_TIME_BOLT, Constants.MUTABLE_BOLT_TASK_ID, Constants.MUTABLE_BOLT_MACHINE));
        outputFieldsDeclarer.declareStream(this.rightStreamSmaller, new Fields(Constants.BATCH_CSS_TREE_KEY, Constants.BATCH_CSS_TREE_VALUES, Constants.BATCH_CSS_FLAG));

        outputFieldsDeclarer.declareStream("RightCheckForMerge", new Fields("mergeFlag","Time"));

    }

    private void rightPredicateEvaluation(Tuple tuple, OutputCollector outputCollector) throws IOException {
        // Insertion
        rightStreamBPlusTree.insert(tuple.getIntegerByField(Constants.TUPLE), tuple.getIntegerByField(Constants.TUPLE_ID));
        //HashSet<Integer> hashSetIds = rightStreamBPlusTree.greaterThenSpecificValueHashSet(tuple.getIntegerByField(Constants.TUPLE));
        //Results form BPlus Tree searching
        BitSet bitSet= rightStreamBPlusTree.greaterThenSpecificValue(tuple.getIntegerByField(Constants.TUPLE));
        if (bitSet != null) {
            // emitting Logic
            outputCollector.emit(this.rightStreamHashSetEmitterStreamID, tuple, new Values(convertToByteArray(bitSet), tuple.getIntegerByField(Constants.TUPLE_ID),
                    tuple.getLongByField(Constants.KAFKA_TIME),tuple.getLongByField(Constants.KAFKA_SPOUT_TIME),tuple.getLongByField(Constants.SPLIT_BOLT_TIME),
                    tuple.getIntegerByField(Constants.TASK_ID_FOR_SPLIT_BOLT),tuple.getStringByField(Constants.HOST_NAME_FOR_SPLIT_BOLT), tupleArrivalTime,System.currentTimeMillis(),taskID, hostName));
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

//    private static byte[] convertToByteArray(List<Integer> integerList) {
//        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
//        for (Integer num : integerList) {
//            outputStream.write(num.byteValue());
//        }
//        return outputStream.toByteArray();
//    }
    public synchronized byte[] convertToByteArray(Serializable object) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (ObjectOutputStream objectOutputStream = new ObjectOutputStream(baos)) {
            objectOutputStream.writeObject(object);
            objectOutputStream.flush();
        }
        return baos.toByteArray();
    }
    public void dataMergingToCSS(Node node, Tuple tuple, String streamID) {

        while (node != null) {
            for (Key key : node.getKeys()) {
                for(int value: key.getValues()) {
                    outputCollector.emit(streamID, tuple, new Values(key.getKey(), value, false));
                    outputCollector.ack(tuple);
                }
            }
            node = node.getNext();
        }

        outputCollector.emit(streamID, tuple, new Values(0, 0, true));
        outputCollector.ack(tuple);


    }


}
