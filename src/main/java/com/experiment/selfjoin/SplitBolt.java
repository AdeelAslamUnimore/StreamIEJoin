package com.experiment.selfjoin;

import com.configurationsandconstants.iejoinandbaseworks.Configuration;
import com.configurationsandconstants.iejoinandbaseworks.Constants;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.net.InetAddress;
import java.util.Map;

public class SplitBolt extends BaseRichBolt {
    private OutputCollector outputCollector;
    private String rightStreamSmaller;
    private String leftStreamGreater;
    private int taskID;
    private String hostName;


    public SplitBolt() {
        Map<String, Object> map = Configuration.configurationConstantForStreamIDs();
        this.rightStreamSmaller = (String) map.get("RightSmallerPredicateTuple");
        this.leftStreamGreater = (String) map.get("LeftGreaterPredicateTuple");


    }

    @Override
    public void prepare(Map<String, Object> map, TopologyContext topologyContext, OutputCollector outputCollector) {

        this.outputCollector = outputCollector;
        this.taskID=topologyContext.getThisTaskId();
        try{
          this.hostName= InetAddress.getLocalHost().getHostName();
              }catch (Exception e){
            e.printStackTrace();
        }
    }

    @Override
    public void execute(Tuple tuple) {
        if (tuple.getSourceStreamId().equals("StreamR")) {
            long time=System.currentTimeMillis();
            Values valuesLeft = new Values(tuple.getIntegerByField("distance"), tuple.getIntegerByField("ID"), "LeftStream",
                    tuple.getLongByField("kafkaTime"),tuple.getLongByField("Time"),System.currentTimeMillis(),taskID,hostName);
            Values valuesRight = new Values(tuple.getIntegerByField("amount"), tuple.getIntegerByField("ID"), "LeftStream",
                    tuple.getLongByField("kafkaTime"),tuple.getLongByField("Time"),System.currentTimeMillis(),taskID,hostName);
            this.outputCollector.emit(leftStreamGreater, tuple, valuesRight);
            this.outputCollector.emit(rightStreamSmaller, tuple, valuesLeft);
       //   this.outputCollector.emit("StreamRBolt", tuple, new Values(tuple.getIntegerByField("distance"), tuple.getIntegerByField("amount"),tuple.getIntegerByField("ID"), tuple.getLongByField("KafkaTime"),System.currentTimeMillis()));
//            this.outputCollector.emit("RightStreamR", tuple, new Values(tuple.getIntegerByField("amount"), tuple.getIntegerByField("ID"), System.currentTimeMillis()));
            this.outputCollector.ack(tuple);

        }


    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declareStream(leftStreamGreater, new Fields(Constants.TUPLE, Constants.TUPLE_ID, "StreamID",Constants.KAFKA_TIME,
                Constants.KAFKA_SPOUT_TIME,Constants.SPLIT_BOLT_TIME,Constants.TASK_ID_FOR_SPLIT_BOLT,Constants.HOST_NAME_FOR_SPLIT_BOLT));
        outputFieldsDeclarer.declareStream(rightStreamSmaller, new Fields(Constants.TUPLE, Constants.TUPLE_ID, "StreamID",
               Constants.KAFKA_TIME, Constants.KAFKA_SPOUT_TIME,Constants.SPLIT_BOLT_TIME,Constants.TASK_ID_FOR_SPLIT_BOLT,Constants.HOST_NAME_FOR_SPLIT_BOLT));
       //outputFieldsDeclarer.declareStream("StreamRBolt", new Fields("distance", "amount", "ID", "kafkaTime", "Time"));
//        outputFieldsDeclarer.declareStream("RightStreamR", new Fields(Constants.TUPLE, Constants.TUPLE_ID, "Time"));



    }
}
