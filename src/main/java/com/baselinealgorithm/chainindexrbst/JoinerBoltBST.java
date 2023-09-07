package com.baselinealgorithm.chainindexrbst;

import com.configurationsandconstants.iejoinandbaseworks.Constants;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;

import java.io.*;
import java.net.InetAddress;
import java.util.HashSet;
import java.util.Map;

public class JoinerBoltBST extends BaseRichBolt {
    private HashSet<Integer> leftHashSet;
    private HashSet<Integer> rightHashSet;
    private String leftStreamID;
    private String rightStreamID;
    private int counterRecord;
    private BufferedWriter bufferedWriter;
    private StringBuilder stringBuilder;
    private int taskID;
    private String hostName;
    public JoinerBoltBST(String leftStreamID, String rightStreamID){
        this.leftStreamID=leftStreamID;
        this.rightStreamID=rightStreamID;
    }

    @Override
    public void prepare(Map<String, Object> map, TopologyContext topologyContext, OutputCollector outputCollector) {
        try{
            this.counterRecord=0;
            this.bufferedWriter= new BufferedWriter(new FileWriter(new File("/home/adeel/Data/Results/Joiner.csv")));
            this.bufferedWriter.write("LeftStreamID, RightStreamID, beforeTime,AfterTime,taskID,HostName,\n");
            this.bufferedWriter.flush();
            this.stringBuilder= new StringBuilder();
            taskID=topologyContext.getThisTaskId();
            hostName= InetAddress.getLocalHost().getHostName();
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    @Override
    public void execute(Tuple tuple) {
        long beforeTime=System.currentTimeMillis();
        if(tuple.getSourceStreamId().equals(leftStreamID)) {
            leftHashSet = convertByteArrayToHashSet(tuple.getBinaryByField(Constants.LEFT_HASH_SET));
        }
        if(tuple.getSourceStreamId().equals(rightStreamID)){
            rightHashSet=convertByteArrayToHashSet(tuple.getBinaryByField(Constants.RIGHT_HASH_SET));
        }
        if(leftHashSet!=null&&rightHashSet!=null){
            leftHashSet.retainAll(rightHashSet);
            long afterTime=System.currentTimeMillis();
            counterRecord++;
            this.stringBuilder.append(leftStreamID+","+rightStreamID+","+beforeTime+","+ afterTime+","+taskID+","+hostName+"\n");
            if(counterRecord==100){
                try {
                    bufferedWriter.write(stringBuilder.toString());
                    bufferedWriter.flush();
                    counterRecord=0;
                    this.stringBuilder= new StringBuilder();
                } catch (IOException e) {
                    e.printStackTrace();
                }

            }
            leftHashSet=null;
            rightHashSet=null;
        }

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

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
    }}