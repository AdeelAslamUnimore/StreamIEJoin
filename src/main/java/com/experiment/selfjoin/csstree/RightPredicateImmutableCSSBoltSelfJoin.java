package com.experiment.selfjoin.csstree;

import com.baselinealgorithm.chainbplusandcss.CSSTree;
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

public class RightPredicateImmutableCSSBoltSelfJoin extends BaseRichBolt {

    private LinkedList<CSSTreeUpdated> rightStreamLinkedListCSSTree = null;

    private String rightStreamGreater;
    private HashSet<Integer> leftStreamTuplesHashSet = null;
    private HashSet<Integer> rightStreamTuplesHashSet = null;

    private CSSTreeUpdated rightStreamCSSTree = null;

    private boolean rightStreamMergeGreater = false;

    private Queue<Tuple> rightStreamGreaterQueueMerge = null;
    private static int tupleRemovalCount = 0;

    private boolean rightBooleanTupleRemovalCounter = false;
    private OutputCollector outputCollector;
    private long rightMergeStartTime;
    private int taskID;
    private String hostName;
    private BufferedWriter bufferedWriter;

    public RightPredicateImmutableCSSBoltSelfJoin() {
        Map<String, Object> map = Configuration.configurationConstantForStreamIDs();

        this.rightStreamGreater = (String) map.get("RightSmallerPredicateTuple");
    }

    @Override
    public void prepare(Map<String, Object> map, TopologyContext topologyContext, OutputCollector outputCollector) {

        rightStreamLinkedListCSSTree = new LinkedList<>();

        rightStreamCSSTree = new CSSTreeUpdated(Constants.ORDER_OF_CSS_TREE);

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
        if (tuple.getSourceStreamId().equals("RightStreamR")) {
            if(!rightStreamLinkedListCSSTree.isEmpty()){
                try {
                    probingResultsSmaller(tuple);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
            if (rightStreamMergeGreater) {
                rightStreamGreaterQueueMerge.offer(tuple);
            }
        }


        if (tuple.getSourceStreamId().equals(rightStreamGreater)) {
            try {
            rightInsertionTupleGreater(tuple);
            } catch (Exception e) {
                e.printStackTrace();
            }

            outputCollector.emit("RightMergingTuplesCSSCreation", tuple, new Values(rightMergeStartTime,System.currentTimeMillis(),taskID, hostName));
            outputCollector.ack(tuple);
        }

        if (tuple.getSourceStreamId().equals("RightCheckForMerge")) {
           // System.out.println("Right==="+tuple);
            rightStreamMergeGreater = true;
            rightMergeStartTime =System.currentTimeMillis();
        }



//        if (rightBooleanTupleRemovalCounter) {
//            //removing tuples only remove last CSS TREE from both R an S such as R.Duration and S.Time
//       //     System.out.println("I am here===="+tupleRemovalCount+"====="+rightStreamLinkedListCSSTree.size());
//            rightStreamLinkedListCSSTree.remove(rightStreamLinkedListCSSTree.getFirst());
//            tupleRemovalCount = tupleRemovalCount - Constants.MUTABLE_WINDOW_SIZE; // Reintilize counter with just substract one length of CSS structure;
//            // set removal boolean to false again
//            rightBooleanTupleRemovalCounter = false;
//
//
//        }

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

       outputFieldsDeclarer.declareStream("RightPredicate", new Fields(Constants.RIGHT_HASH_SET, Constants.TUPLE_ID,"SplitBoltTime","StartProbing","EndProbing","taskID","hostName"));
        outputFieldsDeclarer.declareStream("RightMergeHashSet", new Fields(Constants.HASH_SET, Constants.TUPLE_ID, "MergeEvaluationStartTime", "MergeEvaluatingTime","taskID","host"));
        outputFieldsDeclarer.declareStream("RightMergingTuplesCSSCreation", new Fields("leftMergeStartTime","LeftMergeEnd","taskID", "hostName"));

    }

    public void probingResultsSmaller(Tuple tuple) throws IOException {
     //   HashSet<Integer> rightHashSet = new HashSet<>();
        BitSet bitSet= new BitSet();
        long rightProbingStart=System.currentTimeMillis();
        if (!rightStreamLinkedListCSSTree.isEmpty()) {
            tupleRemovalCount++;
            for (CSSTreeUpdated cssTree : rightStreamLinkedListCSSTree) {
                cssTree.searchGreaterBitSet(tuple.getIntegerByField(Constants.TUPLE), bitSet);

            }


            outputCollector.emit("RightPredicate", tuple, new Values(convertToByteArray(bitSet), tuple.getIntegerByField("ID"),tuple.getLongByField("Time"),rightProbingStart,System.currentTimeMillis(),taskID,hostName));
            outputCollector.ack(tuple);
           // return rightHashSet;

        }
       // return null;
    }




    public void rightInsertionTupleGreater(Tuple tuple) throws IOException {

        if (tuple.getBooleanByField(Constants.BATCH_CSS_FLAG)) {
            rightStreamLinkedListCSSTree.add(rightStreamCSSTree);
            if (rightStreamMergeGreater) {
                int i = 0;
                long mergeTupleEvaluationStartTime= System.currentTimeMillis();
                for (Tuple tuples : rightStreamGreaterQueueMerge) {
                    BitSet bitSet= new BitSet();
                    i = i + 1;
                    rightStreamCSSTree.searchGreaterBitSet(tuples.getIntegerByField(Constants.TUPLE),bitSet);
                    outputCollector.emit("RightMergeHashSet", new Values(convertToByteArray(bitSet), i,mergeTupleEvaluationStartTime, System.currentTimeMillis(),taskID,hostName));
                    outputCollector.ack(tuple);
                }
                rightStreamGreaterQueueMerge = new LinkedList<>();
                rightStreamMergeGreater = false;
            }
            rightStreamCSSTree = new CSSTreeUpdated(Constants.ORDER_OF_CSS_TREE);
            tupleRemovalCount = tupleRemovalCount + Constants.MUTABLE_WINDOW_SIZE;
            if (tupleRemovalCount >= Constants.IMMUTABLE_CSS_PART_REMOVAL) {
                rightStreamLinkedListCSSTree.remove(rightStreamLinkedListCSSTree.getFirst());
                tupleRemovalCount = tupleRemovalCount - Constants.MUTABLE_WINDOW_SIZE; // Reintilize counter with just substract one length of CSS structure;
             //   this.rightBooleanTupleRemovalCounter = true;
              //  rightStreamLinkedListCSSTree.remove(rightStreamLinkedListCSSTree.getFirst());
            }
        } else {

            rightStreamCSSTree.insert(tuple.getIntegerByField(Constants.BATCH_CSS_TREE_KEY),tuple.getIntegerByField(Constants.BATCH_CSS_TREE_VALUES));
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

    public synchronized byte[] convertToByteArray(Serializable object) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (ObjectOutputStream objectOutputStream = new ObjectOutputStream(baos)) {
            objectOutputStream.writeObject(object);
            objectOutputStream.flush();
        }
        return baos.toByteArray();
    }
}
