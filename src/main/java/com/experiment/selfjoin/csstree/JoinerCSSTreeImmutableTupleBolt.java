package com.experiment.selfjoin.csstree;

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
import java.util.BitSet;
import java.util.HashSet;
import java.util.Map;

public class JoinerCSSTreeImmutableTupleBolt extends BaseRichBolt {
    private BitSet leftHashSet;
    private BitSet rightHashSet;
    private String leftStreamID;
    private String rightStreamID;
    private OutputCollector outputCollector;
    private int taskID;
    private String hostName;
    private String leftStreamRecord;
    private String rightStreamRecord;

    private BufferedWriter bufferedWriterRecordCSSJoin;
    public JoinerCSSTreeImmutableTupleBolt(String leftStreamID, String rightStreamID){
        Map<String, Object> map = Configuration.configurationConstantForStreamIDs();
        this.leftStreamID=leftStreamID;
        this.rightStreamID=rightStreamID;

    }

    @Override
    public void prepare(Map<String, Object> map, TopologyContext topologyContext, OutputCollector outputCollector) {
        taskID= topologyContext.getThisTaskId();
        this.outputCollector=outputCollector;
        try{
            hostName= InetAddress.getLocalHost().getHostName();
            bufferedWriterRecordCSSJoin = new BufferedWriter(new FileWriter(new File("/home/adeel/Data/Results/bufferedWriterRecordCSSJoin.csv")));
            bufferedWriterRecordCSSJoin.write("LeftID,TupleInsertTime,LeftProbingStartTime,LeftProbingEndTime,LeftTaskID, LeftHostName, TimeArrivalLeft, RightID,RightInsertTimee,RightProbingStartTime,RightProbingEndTime,RightTaskID,RightHostName,TimeArrivalRight, timeAfterCalculation,streamID,TaskID,HostName \n");
            bufferedWriterRecordCSSJoin.flush();
        }
        catch (Exception e){

        }
    }

    @Override
    public void execute(Tuple tuple) {
        int streamID=(Integer) tuple.getValue(1);
        long time=System.currentTimeMillis();
        if(tuple.getSourceStreamId().equals(leftStreamID)) {

            leftHashSet = convertToObject(tuple.getBinaryByField(Constants.LEFT_HASH_SET));

            leftStreamRecord=tuple.getValue(1)+","+tuple.getValue(2)+","+tuple.getValue(3)+","+tuple.getValue(4)+","+tuple.getValue(5)+","+tuple.getValue(6)+","+time;
        }
        if(tuple.getSourceStreamId().equals(rightStreamID)){
            rightHashSet=convertToObject(tuple.getBinaryByField(Constants.RIGHT_HASH_SET));;

            rightStreamRecord=tuple.getValue(1)+","+tuple.getValue(2)+","+tuple.getValue(3)+","+tuple.getValue(4)+","+tuple.getValue(5)+","+tuple.getValue(6)+","+time;

        }
        if(leftHashSet!=null&&rightHashSet!=null){
            leftHashSet.and(rightHashSet);

            long timeAfterCalculation= System.currentTimeMillis();
            leftHashSet=null;
            rightHashSet=null;
            try{

                bufferedWriterRecordCSSJoin.write(leftStreamRecord+","+rightStreamRecord+","+timeAfterCalculation+","+streamID+","+taskID+","+hostName+"\n");
                bufferedWriterRecordCSSJoin.flush();
            }catch (Exception e){
                e.printStackTrace();
            }
            this.outputCollector.emit("ResultTuple", tuple, new Values(leftStreamRecord, rightStreamRecord,timeAfterCalculation,streamID,taskID,hostName));
            this.outputCollector.ack(tuple);
        }


    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    outputFieldsDeclarer.declareStream("ResultTuple", new Fields("LeftStreamRecord","RightStreamRecord","timeAfterCalculation", "streamID","TaskID","HostName"));


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

    public static HashSet<Integer> convertByteArrayToHashSet(byte[] byteArray) {
        HashSet<Integer> hashSet = null;
        try {
            ByteArrayInputStream bis = new ByteArrayInputStream(byteArray);
            ObjectInputStream ois = new ObjectInputStream(bis);
            hashSet = (HashSet<Integer>) ois.readObject();
        } catch (IOException | ClassNotFoundException e) {
            e.printStackTrace();
        }
        return hashSet;
    }
}
