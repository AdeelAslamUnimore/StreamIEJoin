package com.experiment.selfjoin.csstree;

import com.baselinealgorithm.chainbplusandcss.CSSTree;
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

public class RightPredicateImmutableCSSBoltSelfJoin extends BaseRichBolt {

    private LinkedList<CSSTree> rightStreamLinkedListCSSTree = null;

    private String rightStreamGreater;
    private HashSet<Integer> leftStreamTuplesHashSet = null;
    private HashSet<Integer> rightStreamTuplesHashSet = null;

    private CSSTree rightStreamCSSTree = null;

    private boolean rightStreamMergeGreater = false;

    private Queue<Tuple> rightStreamGreaterQueueMerge = null;
    private static int tupleRemovalCount = 0;

    private boolean rightBooleanTupleRemovalCounter = false;
    private OutputCollector outputCollector;
    private long rightMergeStartTime;
    private int taskID;
    private String hostName;

    public RightPredicateImmutableCSSBoltSelfJoin() {
        Map<String, Object> map = Configuration.configurationConstantForStreamIDs();

        this.rightStreamGreater = (String) map.get("RightSmallerPredicateTuple");
    }

    @Override
    public void prepare(Map<String, Object> map, TopologyContext topologyContext, OutputCollector outputCollector) {

        rightStreamLinkedListCSSTree = new LinkedList<>();

        rightStreamCSSTree = new CSSTree(Constants.ORDER_OF_CSS_TREE);

        this.rightStreamGreaterQueueMerge = new LinkedList<>();
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
        if (tuple.getSourceStreamId().equals("StreamR")) {
            if (rightStreamMergeGreater) {

                rightStreamGreaterQueueMerge.offer(tuple);
            }
            long rightProbingStart=System.currentTimeMillis();
            HashSet<Integer> leftHashSet = probingResultsSmaller(tuple, rightStreamLinkedListCSSTree);
            long probingEnd=System.currentTimeMillis();
            if (leftHashSet != null) {
                outputCollector.emit("RightPredicate", tuple, new Values(convertHashSetToByteArray(leftHashSet), tuple.getIntegerByField("ID"),rightProbingStart,probingEnd,taskID,hostName));
                outputCollector.ack(tuple);

            }
        }


        if (tuple.getSourceStreamId().equals(rightStreamGreater)) {
            rightInsertionTupleGreater(tuple);
            outputCollector.emit("RightMergingTuplesCSSCreation", tuple, new Values(rightMergeStartTime,System.currentTimeMillis(),taskID, hostName));
            outputCollector.ack(tuple);
        }

        if (tuple.getSourceStreamId().equals("RightCheckForMerge")) {
           // System.out.println("Right==="+tuple);
            rightStreamMergeGreater = true;
            rightMergeStartTime =System.currentTimeMillis();
        }



    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

        outputFieldsDeclarer.declareStream("RightPredicate", new Fields(Constants.RIGHT_HASH_SET, Constants.TUPLE_ID,"StartProbing","EndProbing","taskID","hostName"));
        outputFieldsDeclarer.declareStream("RightMergeHashSet", new Fields(Constants.HASH_SET, Constants.TUPLE_ID, "MergeEvaluationStartTime", "MergeEvaluatingTime","taskID","host"));
        outputFieldsDeclarer.declareStream("RightMergingTuplesCSSCreation", new Fields("leftMergeStartTime","LeftMergeEnd","taskID", "hostName"));

    }

    public HashSet<Integer> probingResultsSmaller(Tuple tuple, LinkedList<CSSTree> linkedListCSSTree) {
        HashSet<Integer> hashSet = new HashSet<>();
        if (!linkedListCSSTree.isEmpty()) {
            tupleRemovalCount++;
            for (CSSTree cssTree : linkedListCSSTree) {
                hashSet.addAll(cssTree.searchSmaller(tuple.getIntegerByField("amount")));

            }
            return hashSet;

        }
        return null;
    }

//    public HashSet<Integer> probingResultsSmaller(Tuple tuple, LinkedList<CSSTree> linkedListCSSTree) {
//        HashSet<Integer> hashSet = new HashSet<>();
//        if (!linkedListCSSTree.isEmpty()) {
//            tupleRemovalCount++;
//            for (CSSTree cssTree : linkedListCSSTree) {
//                hashSet.addAll(cssTree.searchSmaller(tuple.getIntegerByField("amount")));
//
//            }
//            return hashSet;
//
//        }
//        return null;
//    }



    public void rightInsertionTupleGreater(Tuple tuple) {

        if (tuple.getBooleanByField(Constants.BATCH_CSS_FLAG)) {
            rightStreamLinkedListCSSTree.add(rightStreamCSSTree);
            if (rightStreamMergeGreater) {
                int i = 0;
                long mergeTupleEvaluationStartTime= System.currentTimeMillis();
                for (Tuple tuples : rightStreamGreaterQueueMerge) {
                    i = i + 1;
                    HashSet hashSet = rightStreamCSSTree.searchGreater(tuples.getIntegerByField("amount"));
                    outputCollector.emit("RightMergeHashSet", new Values(convertHashSetToByteArray(hashSet), i,mergeTupleEvaluationStartTime, System.currentTimeMillis(),taskID,hostName));
                }
                rightStreamGreaterQueueMerge = new LinkedList<>();
                rightStreamMergeGreater = false;
            }
            rightStreamCSSTree = new CSSTree(Constants.ORDER_OF_CSS_TREE);
            tupleRemovalCount = tupleRemovalCount + Constants.MUTABLE_WINDOW_SIZE;
            if (tupleRemovalCount >= Constants.IMMUTABLE_CSS_PART_REMOVAL) {
                this.rightBooleanTupleRemovalCounter = true;
                rightStreamLinkedListCSSTree.remove(rightStreamLinkedListCSSTree.getFirst());
            }
        } else {

            rightStreamCSSTree.insertBulkUpdate(tuple.getIntegerByField(Constants.BATCH_CSS_TREE_KEY),
                    convertToIntegerList(tuple.getBinaryByField(Constants.BATCH_CSS_TREE_VALUES)));
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

    private static List<Integer> convertToIntegerList(byte[] byteArray) {
        List<Integer> integerList = new ArrayList<>();
        ByteArrayInputStream inputStream = new ByteArrayInputStream(byteArray);
        int nextByte;
        while ((nextByte = inputStream.read()) != -1) {
            integerList.add(nextByte);
        }
        return integerList;
    }
}
