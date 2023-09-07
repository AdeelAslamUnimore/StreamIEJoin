package com.experiment.selfjoin.csstree;

import com.configurationsandconstants.iejoinandbaseworks.Configuration;
import com.configurationsandconstants.iejoinandbaseworks.Constants;
import com.sun.org.omg.CORBA.ExcDescriptionSeqHelper;
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
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

public class JoinerCSSTreeImmutableTupleQueueDuringMerging extends BaseRichBolt {
    private BitSet leftHashSet;
    private BitSet rightHashSet;
    private HashSet<Integer> hashSet;
    private String leftStreamID;
    private String rightStreamID;
    private OutputCollector outputCollector;
    private int taskID;
    private String hostName;
    private String leftStreamRecord;
    private String rightStreamRecord;
    private HashMap<Integer, HashSet> hashMapHashSet;

    private BufferedWriter bufferedWriterRecordMergingLeftEvaluationTuple;
    private BufferedWriter bufferedWriterRecordMergingRightEvaluationTuple;

    public JoinerCSSTreeImmutableTupleQueueDuringMerging(String leftStreamID, String rightStreamID){
        Map<String, Object> map = Configuration.configurationConstantForStreamIDs();
        this.leftStreamID=leftStreamID;
        this.rightStreamID=rightStreamID;

    }

    @Override
    public void prepare(Map<String, Object> map, TopologyContext topologyContext, OutputCollector outputCollector) {
        taskID= topologyContext.getThisTaskId();
        this.outputCollector=outputCollector;
        this.hashMapHashSet= new HashMap<>();
        try{
            bufferedWriterRecordMergingLeftEvaluationTuple = new BufferedWriter(new FileWriter(new File("/home//adeel/Data/Results/bufferedWriterEvaluationLeftMergingTuple.csv")));
            bufferedWriterRecordMergingRightEvaluationTuple = new BufferedWriter(new FileWriter(new File("/home//adeel/Data/Results/bufferedWriterEvaluationRightMergingTuple.csv")));
            bufferedWriterRecordMergingLeftEvaluationTuple.write("ID,LeftMergeStartTime, LeftMergeEndTime, LeftTaskID, LeftHostName,ID,TimeArrivalLeft, RightMergeStartTime, RightTaskID, RightMergeEndTime, TimeArrivalRight, ID,timeAfterCalculation, streamID,TaskID,HostName \n");
            bufferedWriterRecordMergingLeftEvaluationTuple.flush();

            bufferedWriterRecordMergingRightEvaluationTuple.write("ID,LeftMergeStartTime, LeftMergeEndTime, LeftTaskID, LeftHostName,ID,TimeArrivalLeft, RightMergeStartTime, RightTaskID, RightMergeEndTime, TimeArrivalRight, ID,timeAfterCalculation, streamID,TaskID,HostName \n");
            bufferedWriterRecordMergingRightEvaluationTuple.flush();
            hostName= InetAddress.getLocalHost().getHostName();
        }catch (Exception e){

        }
    }

    @Override
    public void execute(Tuple tuple) {

        if(tuple.getSourceStreamId().equals(leftStreamID)) {

            long time=System.currentTimeMillis();
            leftHashSet = convertToObject(tuple.getBinaryByField(Constants.HASH_SET));
            leftStreamRecord=tuple.getValue(1)+","+tuple.getValue(2)+","+tuple.getValue(3)+","+tuple.getValue(4)+","+time;


            try {

                    bufferedWriterRecordMergingLeftEvaluationTuple.write(leftStreamRecord+","+taskID+","+hostName+",Left" +"\n");
                    bufferedWriterRecordMergingLeftEvaluationTuple.flush();


            }
            catch (Exception e){

            }



        }
        if(tuple.getSourceStreamId().equals(rightStreamID)){

            long time=System.currentTimeMillis();
            rightHashSet=convertToObject(tuple.getBinaryByField(Constants.HASH_SET));
            rightStreamRecord=tuple.getValue(1)+","+tuple.getValue(2)+","+tuple.getValue(3)+","+tuple.getValue(4)+","+time;

            try {

                bufferedWriterRecordMergingLeftEvaluationTuple.write(rightStreamRecord+","+taskID+","+hostName+",Right" +"\n");
                bufferedWriterRecordMergingLeftEvaluationTuple.flush();


            }
            catch (Exception e){

            }
        }



    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    //    outputFieldsDeclarer.declareStream("MergingQueueTuple", new Fields("RightStreamRecord","timeAfterCalculation", "streamID","TaskID","HostName","stream"));


    }
    private BitSet convertToObject(byte[] byteData) {
        try (ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(byteData);
             ObjectInputStream objectInputStream = new ObjectInputStream(byteArrayInputStream)) {
            Object object = objectInputStream.readObject();
            if (object instanceof BitSet) {
                return (BitSet) object;
            } else {
                throw new IllegalArgumentException("Invalid object type after deserialization");
            }
        } catch (ClassNotFoundException | IOException e) {
            e.printStackTrace();
            // Handle the exception appropriately
        }
        return null; // Return null or handle the failure case accordingly
    }

}
