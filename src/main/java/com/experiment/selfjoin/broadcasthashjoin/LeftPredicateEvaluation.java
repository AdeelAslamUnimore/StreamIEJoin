package com.experiment.selfjoin.broadcasthashjoin;

import com.configurationsandconstants.iejoinandbaseworks.Constants;
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
import java.util.*;

public class LeftPredicateEvaluation extends BaseRichBolt {
    private ArrayList<BroadCastDataModel> leftWindow;
    private OutputCollector collector;
    private BitSet bitSet;
    private int taskID;
    private String hostName;
    @Override
    public void prepare(Map<String, Object> map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.leftWindow= new ArrayList<>();
        this.collector= outputCollector;
         bitSet= new BitSet();
         this.taskID=topologyContext.getThisTaskId();
         try{
             this.hostName= InetAddress.getLocalHost().getHostName();
         }catch (Exception e){

         }

    }

    @Override
    public void execute(Tuple tuple) {

        long startTime=System.currentTimeMillis();
        if(tuple.getSourceStreamId().equals("LeftInsertion")) {
            this.leftWindow.add(new BroadCastDataModel(tuple.getIntegerByField(Constants.TUPLE_ID), tuple.getInteger(0)));
        }
        if(tuple.getSourceStreamId().equals("LeftSearch")) {
            for (int i = 0; i < leftWindow.size(); i++) {
                if (tuple.getInteger(0)<  leftWindow.get(i).getKey()) {
                    bitSet.set(leftWindow.get(i).getId());
                }
            }
            long endTime=System.currentTimeMillis();
            try {
                if(!bitSet.isEmpty()) {
                    byte[] bytesArray = convertToByteArray(bitSet);
                    collector.emit("Left", new Values(tuple.getIntegerByField(Constants.TUPLE_ID), bytesArray));
                    Values recordValues = new Values(tuple.getIntegerByField(Constants.TUPLE_ID), tuple.getValueByField(Constants.KAFKA_TIME),
                            tuple.getValueByField(Constants.KAFKA_SPOUT_TIME), tuple.getValueByField(Constants.SPLIT_BOLT_TIME), tuple.getValueByField(Constants.TASK_ID_FOR_SPLIT_BOLT),
                            tuple.getValueByField(Constants.HOST_NAME_FOR_SPLIT_BOLT), startTime, endTime, taskID, hostName);
                    collector.emit("LeftRecord", recordValues);
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        if(leftWindow.size()==Constants.TUPLE_WINDOW_SIZE){
            long tupleRemovalTime=System.currentTimeMillis();
            for(int i=0;i<Constants.TUPLE_REMOVAL_COUNT_BCHJ;i++){
                leftWindow.remove(i);
            }
            long tupleRemovalTimeAfter=System.currentTimeMillis();
            Values recordValues = new Values(tuple.getIntegerByField(Constants.TUPLE_ID), 0,
                    0, 0,0,
                    0, tupleRemovalTime, tupleRemovalTimeAfter, taskID, hostName);
            collector.emit("LeftRecord", recordValues);
        }
    }

    public synchronized byte[] convertToByteArray(Serializable object) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (ObjectOutputStream objectOutputStream = new ObjectOutputStream(baos)) {
            objectOutputStream.writeObject(object);
            objectOutputStream.flush();
        }
        return baos.toByteArray();
    }
    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declareStream("Left",new Fields(Constants.TUPLE_ID,Constants.BYTE_ARRAY));
        outputFieldsDeclarer.declareStream("LeftRecord", new Fields(Constants.TUPLE_ID,Constants.KAFKA_TIME,
                Constants.KAFKA_SPOUT_TIME,Constants.SPLIT_BOLT_TIME,Constants.TASK_ID_FOR_SPLIT_BOLT,
                Constants.HOST_NAME_FOR_SPLIT_BOLT,"EvaluationStartTime", "EvaluationEndTime", "taskID", "hostName"));
    }
}
