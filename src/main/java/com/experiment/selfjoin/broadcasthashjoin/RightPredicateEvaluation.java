package com.experiment.selfjoin.broadcasthashjoin;

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
import java.util.ArrayList;
import java.util.BitSet;
import java.util.HashSet;
import java.util.Map;

public class RightPredicateEvaluation extends BaseRichBolt {
    private ArrayList<BroadCastDataModel> rightWindow;
    private OutputCollector collector;

    private BitSet bitSet;
    private int taskID;
    private String hostName;
    @Override
    public void prepare(Map<String, Object> map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector=outputCollector;
        this.rightWindow= new ArrayList<>();
        this.bitSet= new BitSet();
        try {
            this.taskID=topologyContext.getThisTaskId();
            this.hostName= InetAddress.getLocalHost().getHostName();
                    } catch (Exception e) {
            e.printStackTrace();
        }

    }

    @Override
    public void execute(Tuple tuple) {
        long startTime=System.currentTimeMillis();
        if(tuple.getSourceStreamId().equals("RightInsertion")) {
            this.rightWindow.add(new BroadCastDataModel(tuple.getIntegerByField(Constants.TUPLE_ID), tuple.getInteger(0)));
        }
        if(tuple.getSourceStreamId().equals("RightSearch")) {
            for (int i = 0; i < rightWindow.size(); i++) {
                if (tuple.getInteger(0) > rightWindow.get(i).getKey()) {
                    bitSet.set(rightWindow.get(i).getId());
                }
            }
            long endTime=System.currentTimeMillis();
            try {
                if(!bitSet.isEmpty()) {
                    byte[] bytesArray = convertToByteArray(bitSet);
                    collector.emit("Right", new Values(tuple.getIntegerByField(Constants.TUPLE_ID), bytesArray));
                    Values recordValues = new Values(tuple.getIntegerByField(Constants.TUPLE_ID), tuple.getValueByField(Constants.KAFKA_TIME),
                            tuple.getValueByField(Constants.KAFKA_SPOUT_TIME), tuple.getValueByField(Constants.SPLIT_BOLT_TIME), tuple.getValueByField(Constants.TASK_ID_FOR_SPLIT_BOLT),
                            tuple.getValueByField(Constants.HOST_NAME_FOR_SPLIT_BOLT), startTime, endTime, taskID, hostName);
                    collector.emit("RightRecord", recordValues);
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        if(rightWindow.size()==Constants.TUPLE_WINDOW_SIZE){
            long tupleRemovalTime=System.currentTimeMillis();
            for(int i=0;i<Constants.TUPLE_REMOVAL_COUNT_BCHJ;i++){
                rightWindow.remove(i);
            }
            long tupleRemovalTimeAfter=System.currentTimeMillis();
            Values recordValues = new Values(tuple.getIntegerByField(Constants.TUPLE_ID), 0,
                    0, 0,0,
                    0, tupleRemovalTime, tupleRemovalTimeAfter, taskID, hostName);
            collector.emit("RightRecord", recordValues);
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
        outputFieldsDeclarer.declareStream("Right",new Fields(Constants.TUPLE_ID,Constants.BYTE_ARRAY));
        outputFieldsDeclarer.declareStream("RightRecord", new Fields(Constants.TUPLE_ID,Constants.KAFKA_TIME,
                Constants.KAFKA_SPOUT_TIME,Constants.SPLIT_BOLT_TIME,Constants.TASK_ID_FOR_SPLIT_BOLT,
                Constants.HOST_NAME_FOR_SPLIT_BOLT,"EvaluationStartTime", "EvaluationEndTime", "taskID", "hostName"));
    }
}
