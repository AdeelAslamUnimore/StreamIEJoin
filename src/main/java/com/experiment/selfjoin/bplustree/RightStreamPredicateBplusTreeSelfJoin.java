package com.experiment.selfjoin.bplustree;

import com.configurationsandconstants.iejoinandbaseworks.Configuration;
import com.configurationsandconstants.iejoinandbaseworks.Constants;
import com.stormiequality.BTree.BPlusTree;
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

public class RightStreamPredicateBplusTreeSelfJoin extends BaseRichBolt {
    private LinkedList<BPlusTree> rightBPlusTreeLinkedList = null;
    private int treeRemovalThresholdUserDefined;
    private int treeArchiveThresholdRevenue;
    private int treeArchiveThresholdCost;
    private int treeArchiveUserDefined;
    private int tupleRemovalCountForLocal;
    private String rightStreamSmaller;
    private OutputCollector outputCollector;
    private Map<String, Object> map;

    private int resultCounter;
    private BufferedWriter bufferedWriter;
    private StringBuilder stringBuilder;
    private int taskID;
    private String hostName;

    public RightStreamPredicateBplusTreeSelfJoin() {
        map = Configuration.configurationConstantForStreamIDs();
        treeRemovalThresholdUserDefined = Constants.TUPLE_REMOVAL_THRESHOLD;
        treeArchiveUserDefined = Constants.TUPLE_ARCHIVE_THRESHOLD;
        this.rightStreamSmaller = (String) map.get("RightSmallerPredicateTuple");
    }

    @Override
    public void prepare(Map<String, Object> map, TopologyContext topologyContext, OutputCollector outputCollector) {

        rightBPlusTreeLinkedList = new LinkedList<>();
        this.outputCollector = outputCollector;
        try{
            this.resultCounter=0;
            bufferedWriter=new BufferedWriter(new FileWriter(new File("/home/adeel/Data/Results//RightStream.csv")));
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
        if (tuple.getSourceStreamId().equals(rightStreamSmaller)) {
            if (!rightBPlusTreeLinkedList.isEmpty()) {
                BPlusTree currentBplusTreeDuration = rightBPlusTreeLinkedList.getLast();
                currentBplusTreeDuration.insert(tuple.getIntegerByField(Constants.TUPLE), tuple.getIntegerByField(Constants.TUPLE_ID));
                treeArchiveThresholdRevenue++;
                if (treeArchiveThresholdRevenue == treeArchiveUserDefined) {
                    treeArchiveThresholdRevenue = 0;
                    BPlusTree bPlusTree = new BPlusTree(Constants.ORDER_OF_B_PLUS_TREE);
                    rightBPlusTreeLinkedList.add(bPlusTree);
                }
            } else {
                BPlusTree bPlusTree = new BPlusTree(Constants.ORDER_OF_B_PLUS_TREE);
                treeArchiveThresholdRevenue++;
                bPlusTree.insert(tuple.getIntegerByField(Constants.TUPLE), tuple.getIntegerByField(Constants.TUPLE_ID));
                rightBPlusTreeLinkedList.add(bPlusTree);
            }
            for (BPlusTree bPlusTree : rightBPlusTreeLinkedList) {
                BitSet hashSetGreater = bPlusTree.greaterThenSpecificValue(tuple.getIntegerByField(Constants.TUPLE));

                if(hashSetGreater!=null) {
                    try {
                        outputCollector.emit(Constants.RIGHT_PREDICATE_BOLT, new Values(convertToByteArray(hashSetGreater), tuple.getIntegerByField(Constants.TUPLE_ID)));
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                    outputCollector.ack(tuple);
                }
            }

            long timeAfterEvaluation=System.currentTimeMillis();
            this.resultCounter++;
            stringBuilder.append(tuple.getValueByField(Constants.TUPLE_ID) + "," + tuple.getValueByField(Constants.KAFKA_TIME) + "," +
                    tuple.getValueByField(Constants.KAFKA_SPOUT_TIME) + "," + tuple.getValueByField(Constants.SPLIT_BOLT_TIME) + "," + tuple.getValueByField(Constants.TASK_ID_FOR_SPLIT_BOLT) + "," + tuple.getValueByField(Constants.HOST_NAME_FOR_SPLIT_BOLT) + "," + timeBeforeEvaluation + "," + timeAfterEvaluation +","+taskID+","+hostName+ "\n");
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
            rightBPlusTreeLinkedList.remove(rightBPlusTreeLinkedList.getFirst());

            tupleRemovalCountForLocal= 0;

        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declareStream(Constants.RIGHT_PREDICATE_BOLT, new Fields(Constants.RIGHT_HASH_SET,Constants.TUPLE_ID));

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
