package com.experiment.selfjoin.bplustree;

import com.configurationsandconstants.iejoinandbaseworks.Configuration;
import com.configurationsandconstants.iejoinandbaseworks.Constants;
import com.stormiequality.BTree.BPlusTreeUpdated;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.io.*;
import java.net.InetAddress;
import java.util.BitSet;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Map;

public class LeftStreamPredicateBplusTreeSelfJoin extends BaseRichBolt {
    private LinkedList<BPlusTreeUpdated> leftPredicateLinkedList = null;
    private int treeRemovalThresholdUserDefined;
    private int treeArchiveThresholdDuration;
    private int treeArchiveThresholdTime;
    private int treeArchiveUserDefined;
    private int tupleRemovalCountForLocal;
    private OutputCollector outputCollector;
    private Map<String, Object> map;
    private String leftStreamGreater;
    private int resultCounter;
    private BufferedWriter bufferedWriter;
    private StringBuilder stringBuilder;
    private int taskID;
    private String hostName;


    // Constructor parameter for tuples
    public LeftStreamPredicateBplusTreeSelfJoin() {
        map = Configuration.configurationConstantForStreamIDs();
        treeRemovalThresholdUserDefined = Constants.TUPLE_REMOVAL_THRESHOLD;
        treeArchiveUserDefined = Constants.TUPLE_ARCHIVE_THRESHOLD;
        this.leftStreamGreater = (String) map.get("LeftGreaterPredicateTuple");

    }

    @Override
    public void prepare(Map<String, Object> map, TopologyContext topologyContext, OutputCollector outputCollector) {
        leftPredicateLinkedList = new LinkedList<>();
        this.outputCollector = outputCollector;

        try{
            this.resultCounter=0;
            bufferedWriter=new BufferedWriter(new FileWriter(new File("/home/adeel/Data/Results/LeftStream.csv")));
            this.stringBuilder= new StringBuilder();
            taskID= topologyContext.getThisTaskId();
            hostName= InetAddress.getLocalHost().getHostName();
            bufferedWriter.write( Constants.TUPLE_ID+","+Constants.KAFKA_TIME+","+
                    Constants.KAFKA_SPOUT_TIME+","+Constants.SPLIT_BOLT_TIME+","+Constants.TASK_ID_FOR_SPLIT_BOLT+","+Constants.HOST_NAME_FOR_SPLIT_BOLT+", BeforeTupleEvaluationTime, AfterTupleEvaluationTime,taskID, hostName\n");
            bufferedWriter.flush();
        }catch (Exception e){
            e.printStackTrace();
                }
    }

    @Override
    public void execute(Tuple tuple) {
        long timeBeforeEvaluation=System.currentTimeMillis();
        tupleRemovalCountForLocal++;

        //Left Stream Tuple means Insert in Duration and Search in Time
        if (tuple.getSourceStreamId().equals(leftStreamGreater)) {

            if (!leftPredicateLinkedList.isEmpty()) {
                //New insertion only active sub index structure that always exist on the right that is last index of linkedList
                BPlusTreeUpdated currentBPlusTreeDuration = leftPredicateLinkedList.getLast();
                currentBPlusTreeDuration.insert(tuple.getIntegerByField(Constants.TUPLE), tuple.getIntegerByField(Constants.TUPLE_ID));
                treeArchiveThresholdDuration++;
                //Archive period achieve
                if (treeArchiveThresholdDuration == treeArchiveUserDefined) {
                    treeArchiveThresholdDuration = 0;
                    // New Object of BPlus
                    BPlusTreeUpdated bPlusTree = new BPlusTreeUpdated(Constants.ORDER_OF_B_PLUS_TREE);
                    //Added to the linked list
                    leftPredicateLinkedList.add(bPlusTree);
                }
            } else {
                // When the linkedlist is empty:
                BPlusTreeUpdated bPlusTree = new BPlusTreeUpdated(Constants.ORDER_OF_B_PLUS_TREE);
                treeArchiveThresholdDuration++;
                bPlusTree.insert(tuple.getIntegerByField(Constants.TUPLE), tuple.getIntegerByField(Constants.TUPLE_ID));
                leftPredicateLinkedList.add(bPlusTree);
            }
            //Search of inequality condition and insertion into the hashset

            for (BPlusTreeUpdated bPlusTree : leftPredicateLinkedList) {
                BitSet integerHashSet = bPlusTree.lessThenSpecificValue(tuple.getIntegerByField(Constants.TUPLE));
                if (integerHashSet != null) {

                    try {
                        outputCollector.emit(Constants.LEFT_PREDICATE_BOLT, new Values(convertToByteArray(integerHashSet), tuple.getIntegerByField(Constants.TUPLE_ID)));
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                    outputCollector.ack(tuple);
                }

            }



//            try {
//                leftPredicateEvaluation(tuple);
//            } catch (IOException e) {
//                e.printStackTrace();
//            }
            long timeAfterEvaluation=System.currentTimeMillis();
            this.resultCounter++;
            stringBuilder.append(tuple.getValueByField(Constants.TUPLE_ID) + "," + tuple.getValueByField(Constants.KAFKA_TIME) + "," +
                    tuple.getValueByField(Constants.KAFKA_SPOUT_TIME) + "," + tuple.getValueByField(Constants.SPLIT_BOLT_TIME) + "," + tuple.getValueByField(Constants.TASK_ID_FOR_SPLIT_BOLT) + "," + tuple.getValueByField(Constants.HOST_NAME_FOR_SPLIT_BOLT) + "," + timeBeforeEvaluation + "," + timeAfterEvaluation+"," +taskID+","+hostName+ "\n");
            if(resultCounter==1000) {
                try {
                    bufferedWriter.write(stringBuilder.toString());
                    bufferedWriter.flush();
                    resultCounter=0;
                    this.stringBuilder= new StringBuilder();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }


        }
        if (tupleRemovalCountForLocal == treeRemovalThresholdUserDefined) {
            leftPredicateLinkedList.remove(leftPredicateLinkedList.getFirst());
            tupleRemovalCountForLocal=0;
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declareStream(Constants.LEFT_PREDICATE_BOLT, new Fields(Constants.LEFT_HASH_SET, Constants.TUPLE_ID));
    }

    public void leftPredicateEvaluation(Tuple tuple) throws IOException {

        if (!leftPredicateLinkedList.isEmpty()) {
            //New insertion only active sub index structure that always exist on the right that is last index of linkedList
            BPlusTreeUpdated currentBPlusTreeDuration = leftPredicateLinkedList.getLast();
            currentBPlusTreeDuration.insert(tuple.getIntegerByField(Constants.TUPLE), tuple.getIntegerByField(Constants.TUPLE_ID));
            treeArchiveThresholdDuration++;
            //Archive period achieve
            if (treeArchiveThresholdDuration == treeArchiveUserDefined) {
                treeArchiveThresholdDuration = 0;
                // New Object of BPlus
                BPlusTreeUpdated bPlusTree = new BPlusTreeUpdated(Constants.ORDER_OF_B_PLUS_TREE);
                //Added to the linked list
                leftPredicateLinkedList.add(bPlusTree);
            }
        } else {
            // When the linkedlist is empty:
            BPlusTreeUpdated bPlusTree = new BPlusTreeUpdated(Constants.ORDER_OF_B_PLUS_TREE);
            treeArchiveThresholdDuration++;
            bPlusTree.insert(tuple.getIntegerByField(Constants.TUPLE), tuple.getIntegerByField(Constants.TUPLE_ID));
            leftPredicateLinkedList.add(bPlusTree);
        }
        //Search of inequality condition and insertion into the hashset

        for (BPlusTreeUpdated bPlusTree : leftPredicateLinkedList) {
            BitSet integerHashSet = bPlusTree.lessThenSpecificValue(tuple.getIntegerByField(Constants.TUPLE));
            if (integerHashSet != null) {

                outputCollector.emit(Constants.LEFT_PREDICATE_BOLT, new Values(convertToByteArray(integerHashSet), tuple.getIntegerByField(Constants.TUPLE_ID)));
                outputCollector.ack(tuple);
            }

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
    public synchronized byte[] convertToByteArray(Serializable object) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (ObjectOutputStream objectOutputStream = new ObjectOutputStream(baos)) {
            objectOutputStream.writeObject(object);
            objectOutputStream.flush();
        }
        return baos.toByteArray();
    }
}
