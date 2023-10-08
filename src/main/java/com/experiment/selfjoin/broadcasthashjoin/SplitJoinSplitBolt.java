package com.experiment.selfjoin.broadcasthashjoin;

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
import java.util.List;
import java.util.Map;

public class SplitJoinSplitBolt extends BaseRichBolt {
    private OutputCollector outputCollector;
    private String rightStreamSmaller;
    private String leftStreamGreater;
    private int taskID;
    private String hostName;
    private List<Integer> leftPredicateEvaluationBoltsTasks;
    private List<Integer> rightPredicateEvaluationBoltsTasks;
    private String leftPredicateEvaluationComponentName;
    private String rightPredicateEvaluationComponentName;
    private int taskCounterForRoundRobin;

    public SplitJoinSplitBolt(String leftPredicateEvaluationComponentName, String rightPredicateEvaluationComponentName){
        Map<String, Object> map = Configuration.configurationConstantForStreamIDs();
        this.rightStreamSmaller = (String) map.get("RightSmallerPredicateTuple");
        this.leftStreamGreater = (String) map.get("LeftGreaterPredicateTuple");
        this.leftPredicateEvaluationComponentName=leftPredicateEvaluationComponentName;
        this.rightPredicateEvaluationComponentName=rightPredicateEvaluationComponentName;


    }
    @Override
    public void prepare(Map<String, Object> map, TopologyContext topologyContext, OutputCollector outputCollector) {

        this.outputCollector = outputCollector;
        this.taskID=topologyContext.getThisTaskId();
        this.leftPredicateEvaluationBoltsTasks= topologyContext.getComponentTasks(leftPredicateEvaluationComponentName);
        this.rightPredicateEvaluationBoltsTasks= topologyContext.getComponentTasks(rightPredicateEvaluationComponentName);
        try{
            this.hostName= InetAddress.getLocalHost().getHostName();
            this.taskCounterForRoundRobin=0;
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
            this.outputCollector.emitDirect(leftPredicateEvaluationBoltsTasks.get(taskCounterForRoundRobin),"LeftInsertion", tuple, valuesRight);
            this.outputCollector.emit("LeftSearch", tuple,valuesRight);
            this.outputCollector.emitDirect(rightPredicateEvaluationBoltsTasks.get(taskCounterForRoundRobin),"RightInsertion", tuple, valuesLeft);
            this.outputCollector.emit("RightSearch",tuple,valuesLeft);
            taskCounterForRoundRobin=taskCounterForRoundRobin+1;
            if(taskCounterForRoundRobin==leftPredicateEvaluationBoltsTasks.size()){
                taskCounterForRoundRobin=0;
            }
            //   this.outputCollector.emit("StreamRBolt", tuple, new Values(tuple.getIntegerByField("distance"), tuple.getIntegerByField("amount"),tuple.getIntegerByField("ID"), tuple.getLongByField("KafkaTime"),System.currentTimeMillis()));
//            this.outputCollector.emit("RightStreamR", tuple, new Values(tuple.getIntegerByField("amount"), tuple.getIntegerByField("ID"), System.currentTimeMillis()));
            this.outputCollector.ack(tuple);

        }


    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declareStream("LeftInsertion", new Fields(Constants.TUPLE, Constants.TUPLE_ID, "StreamID",Constants.KAFKA_TIME,
                Constants.KAFKA_SPOUT_TIME,Constants.SPLIT_BOLT_TIME,Constants.TASK_ID_FOR_SPLIT_BOLT,Constants.HOST_NAME_FOR_SPLIT_BOLT));
        outputFieldsDeclarer.declareStream("LeftSearch", new Fields(Constants.TUPLE, Constants.TUPLE_ID, "StreamID",Constants.KAFKA_TIME,
                Constants.KAFKA_SPOUT_TIME,Constants.SPLIT_BOLT_TIME,Constants.TASK_ID_FOR_SPLIT_BOLT,Constants.HOST_NAME_FOR_SPLIT_BOLT));

        outputFieldsDeclarer.declareStream("RightInsertion", new Fields(Constants.TUPLE, Constants.TUPLE_ID, "StreamID",
                Constants.KAFKA_TIME, Constants.KAFKA_SPOUT_TIME,Constants.SPLIT_BOLT_TIME,Constants.TASK_ID_FOR_SPLIT_BOLT,Constants.HOST_NAME_FOR_SPLIT_BOLT));
        outputFieldsDeclarer.declareStream("RightSearch", new Fields(Constants.TUPLE, Constants.TUPLE_ID, "StreamID",
                Constants.KAFKA_TIME, Constants.KAFKA_SPOUT_TIME,Constants.SPLIT_BOLT_TIME,Constants.TASK_ID_FOR_SPLIT_BOLT,Constants.HOST_NAME_FOR_SPLIT_BOLT));

    }
}

