package com.stormiequality.inputdata;

import com.configurationsandconstants.iejoinandbaseworks.Configuration;
import com.configurationsandconstants.iejoinandbaseworks.Constants;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.net.InetAddress;
import java.util.Map;

public class SplitBolt extends BaseRichBolt {
    private OutputCollector outputCollector;
    private String leftStreamSmaller;
    private String rightStreamSmaller;
    private String leftStreamGreater;
    private String rightStreamGreater;
    private int taskID;
    private String hostName;


    public SplitBolt() {
        Map<String, Object> map = Configuration.configurationConstantForStreamIDs();
        this.leftStreamSmaller = (String) map.get("LeftSmallerPredicateTuple");
        this.rightStreamSmaller = (String) map.get("RightSmallerPredicateTuple");
        this.leftStreamGreater = (String) map.get("LeftGreaterPredicateTuple");
        this.rightStreamGreater = (String) map.get("RightGreaterPredicateTuple");
    }

    @Override
    public void prepare(Map<String, Object> map, TopologyContext topologyContext, OutputCollector outputCollector) {
      try {
          this.taskID = topologyContext.getThisTaskId();
          this.hostName=InetAddress.getLocalHost().getHostName();
          this.outputCollector = outputCollector;
      }
      catch (Exception e){
          e.printStackTrace();
      }

    }

    @Override
    public void execute(Tuple tuple) {

        if (tuple.getSourceStreamId().equals("StreamR")) {
            long splitTimeForStreamR=System.currentTimeMillis();
            Values valuesRight = new Values(tuple.getIntegerByField("Revenue"), tuple.getIntegerByField("ID") ,tuple.getValueByField(Constants.KAFKA_TIME),
                    tuple.getValueByField(Constants.KAFKA_SPOUT_TIME),splitTimeForStreamR,taskID,hostName);
            Values valuesRightForSearch = new Values(tuple.getIntegerByField("Revenue"), tuple.getIntegerByField("ID") ,tuple.getValueByField(Constants.KAFKA_TIME),
                    tuple.getValueByField(Constants.KAFKA_SPOUT_TIME),splitTimeForStreamR,taskID,hostName);
            Values valuesLeft = new Values(tuple.getIntegerByField("Duration"), tuple.getIntegerByField("ID"),tuple.getValueByField(Constants.KAFKA_TIME),
                    tuple.getValueByField(Constants.KAFKA_SPOUT_TIME),splitTimeForStreamR,taskID,hostName);
            Values valuesLeftForSearch = new Values(tuple.getIntegerByField("Duration"), tuple.getIntegerByField("ID"),tuple.getValueByField(Constants.KAFKA_TIME),
                    tuple.getValueByField(Constants.KAFKA_SPOUT_TIME),splitTimeForStreamR,taskID,hostName);
            this.outputCollector.emit(leftStreamGreater, tuple, valuesRight);
            this.outputCollector.emit("LeftForSearchGreater", tuple, valuesRightForSearch);
            this.outputCollector.emit(leftStreamSmaller, tuple, valuesLeft);
            this.outputCollector.emit("LeftForSearchSmaller", tuple, valuesLeftForSearch);
            this.outputCollector.ack(tuple);

        }
        if (tuple.getSourceStreamId().equals("StreamS")) {
            long splitTimeForStreamS=System.currentTimeMillis();
            Values valuesLeft = new Values(tuple.getIntegerByField("Time"), tuple.getIntegerByField("ID"),tuple.getValueByField(Constants.KAFKA_TIME),
                    tuple.getValueByField(Constants.KAFKA_SPOUT_TIME),splitTimeForStreamS,taskID,hostName);
            Values valuesLeftForSearch = new Values(tuple.getIntegerByField("Time"), tuple.getIntegerByField("ID"),tuple.getValueByField(Constants.KAFKA_TIME),
                    tuple.getValueByField(Constants.KAFKA_SPOUT_TIME),splitTimeForStreamS,taskID,hostName);
            Values valuesRight = new Values(tuple.getIntegerByField("Cost"), tuple.getIntegerByField("ID"),tuple.getValueByField(Constants.KAFKA_TIME),
                    tuple.getValueByField(Constants.KAFKA_SPOUT_TIME), splitTimeForStreamS,taskID,hostName);
            Values valuesRightForSearch = new Values(tuple.getIntegerByField("Cost"), tuple.getIntegerByField("ID"),tuple.getValueByField(Constants.KAFKA_TIME),
                    tuple.getValueByField(Constants.KAFKA_SPOUT_TIME), splitTimeForStreamS,taskID,hostName);

            this.outputCollector.emit(rightStreamGreater, tuple, valuesRight);
            this.outputCollector.emit("rightForSearchGreater", tuple, valuesRightForSearch);
            this.outputCollector.emit(rightStreamSmaller, tuple, valuesLeft);
            this.outputCollector.emit("rightForSearchSmaller", tuple, valuesLeftForSearch);
            this.outputCollector.ack(tuple);
        }

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declareStream(leftStreamSmaller, new Fields(Constants.TUPLE, Constants.TUPLE_ID,Constants.KAFKA_TIME,Constants.KAFKA_SPOUT_TIME, Constants.SPLIT_BOLT_TIME, Constants.TASK_ID_FOR_SPLIT_BOLT, Constants.HOST_NAME_FOR_SPLIT_BOLT));
        outputFieldsDeclarer.declareStream(leftStreamGreater, new Fields(Constants.TUPLE, Constants.TUPLE_ID, Constants.KAFKA_TIME,Constants.KAFKA_SPOUT_TIME, Constants.SPLIT_BOLT_TIME, Constants.TASK_ID_FOR_SPLIT_BOLT, Constants.HOST_NAME_FOR_SPLIT_BOLT));
        outputFieldsDeclarer.declareStream(rightStreamGreater, new Fields(Constants.TUPLE, Constants.TUPLE_ID, Constants.KAFKA_TIME,Constants.KAFKA_SPOUT_TIME, Constants.SPLIT_BOLT_TIME, Constants.TASK_ID_FOR_SPLIT_BOLT, Constants.HOST_NAME_FOR_SPLIT_BOLT));
        outputFieldsDeclarer.declareStream(rightStreamSmaller, new Fields(Constants.TUPLE, Constants.TUPLE_ID, Constants.KAFKA_TIME,Constants.KAFKA_SPOUT_TIME, Constants.SPLIT_BOLT_TIME, Constants.TASK_ID_FOR_SPLIT_BOLT, Constants.HOST_NAME_FOR_SPLIT_BOLT));

        outputFieldsDeclarer.declareStream("LeftForSearchGreater", new Fields(Constants.TUPLE, Constants.TUPLE_ID,Constants.KAFKA_TIME,Constants.KAFKA_SPOUT_TIME, Constants.SPLIT_BOLT_TIME, Constants.TASK_ID_FOR_SPLIT_BOLT, Constants.HOST_NAME_FOR_SPLIT_BOLT));
        outputFieldsDeclarer.declareStream("LeftForSearchSmaller", new Fields(Constants.TUPLE, Constants.TUPLE_ID, Constants.KAFKA_TIME,Constants.KAFKA_SPOUT_TIME, Constants.SPLIT_BOLT_TIME, Constants.TASK_ID_FOR_SPLIT_BOLT, Constants.HOST_NAME_FOR_SPLIT_BOLT));
        outputFieldsDeclarer.declareStream("rightForSearchGreater", new Fields(Constants.TUPLE, Constants.TUPLE_ID, Constants.KAFKA_TIME,Constants.KAFKA_SPOUT_TIME, Constants.SPLIT_BOLT_TIME, Constants.TASK_ID_FOR_SPLIT_BOLT, Constants.HOST_NAME_FOR_SPLIT_BOLT));
        outputFieldsDeclarer.declareStream("rightForSearchSmaller", new Fields(Constants.TUPLE, Constants.TUPLE_ID, Constants.KAFKA_TIME,Constants.KAFKA_SPOUT_TIME, Constants.SPLIT_BOLT_TIME, Constants.TASK_ID_FOR_SPLIT_BOLT, Constants.HOST_NAME_FOR_SPLIT_BOLT));


    }
}
