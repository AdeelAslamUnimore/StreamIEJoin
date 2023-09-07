package com.experiment.selfjoin.broadcasthashjoin;

import com.configurationsandconstants.iejoinandbaseworks.Constants;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.util.Map;

public class RightPredicateEvaluationRecord extends BaseRichBolt {
    private BufferedWriter bufferedWriter;

    @Override
    public void prepare(Map<String, Object> map, TopologyContext topologyContext, OutputCollector outputCollector) {
        try{
            this.bufferedWriter= new BufferedWriter(new FileWriter(new File("/home/adeel/Data/Results///RightPredicateEvaluationRecord.csv")));
            this.bufferedWriter.write(Constants.TUPLE_ID+","+Constants.KAFKA_TIME+","+
                    Constants.KAFKA_SPOUT_TIME+","+Constants.SPLIT_BOLT_TIME+","+Constants.TASK_ID_FOR_SPLIT_BOLT+","+
                    Constants.HOST_NAME_FOR_SPLIT_BOLT+","+"EvaluationStartTime, EvaluationEndTime, taskID, hostName \n");
            this.bufferedWriter.flush();
        }catch (Exception e){

        }
    }

    @Override
    public void execute(Tuple tuple) {
        try{
            this.bufferedWriter.write(tuple.getValue(0)+","+tuple.getValue(1)+","+tuple.getValue(2)+","+tuple.getValue(3)+","+tuple.getValue(4)+","+tuple.getValue(5)+","+tuple.getValue(6)+","+tuple.getValue(7)+","+tuple.getValue(8)+","+tuple.getValue(9)+"\n");
            this.bufferedWriter.flush();
        }catch (Exception e){

        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }
}
