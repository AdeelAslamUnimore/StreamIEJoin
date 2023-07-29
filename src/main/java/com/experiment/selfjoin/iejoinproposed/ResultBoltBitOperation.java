package com.experiment.selfjoin.iejoinproposed;

import com.configurationsandconstants.iejoinandbaseworks.Configuration;
import com.configurationsandconstants.iejoinandbaseworks.Constants;
import org.apache.storm.generated.Bolt;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.util.Map;

public class ResultBoltBitOperation extends BaseRichBolt {
    private String leftPredicateSourceStreamID = null;
    private String rightPredicateSourceStreamID = null;
    private String result = null;
    private StringBuilder stringBuilder = null;
    private int counter = 0;
    private BufferedWriter bufferedWriter;

    public ResultBoltBitOperation() {
        Map<String, Object> map = Configuration.configurationConstantForStreamIDs();
        this.leftPredicateSourceStreamID = (String) map.get("LeftPredicateSourceStreamIDBitSet");
        this.rightPredicateSourceStreamID = (String) map.get("RightPredicateSourceStreamIDBitSet");
        this.result = (String) map.get("Results");
    }

    @Override
    public void prepare(Map<String, Object> map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.stringBuilder = new StringBuilder();
        this.counter = 0;
        try{
            bufferedWriter= new BufferedWriter(new FileWriter(new File("/home/adeel/Data/Results/BitSetEvaluation.csv")));
           String greaterEvaluation=Constants.TUPLE_ID + "," +
                   Constants.KAFKA_TIME + "," + Constants.SPLIT_BOLT_TIME + "," +Constants.TASK_ID_FOR_SPLIT_BOLT + "," +
                   Constants.HOST_NAME_FOR_SPLIT_BOLT + "," +
                   Constants.GREATER_PREDICATE_EVALUATION_TIME_BOLT + "," + Constants.MUTABLE_BOLT_TASK_ID+","+Constants.MUTABLE_BOLT_MACHINE;
           String lesserEvaluation=Constants.TUPLE_ID + "," +
                   Constants.KAFKA_TIME + "," + Constants.SPLIT_BOLT_TIME + "," +Constants.TASK_ID_FOR_SPLIT_BOLT + "," +
                   Constants.HOST_NAME_FOR_SPLIT_BOLT + "," +
                   Constants.LESSER_PREDICATE_EVALUATION_TIME_BOLT + "," + Constants.MUTABLE_BOLT_TASK_ID+","+Constants.MUTABLE_BOLT_MACHINE;
           String conjuctionOperation="Time, taskID, hostName";

            bufferedWriter.write(greaterEvaluation+",,"+lesserEvaluation+",,"+conjuctionOperation+"\n");
        }catch (Exception e){

        }
    }

    @Override
    public void execute(Tuple tuple) {
        if (tuple.getSourceStreamId().equals(leftPredicateSourceStreamID)) {
            String tupleString = tuple.getValueByField(Constants.TUPLE_ID) + "," +
                    tuple.getValueByField(Constants.KAFKA_TIME) + "," + tuple.getValueByField(Constants.SPLIT_BOLT_TIME) + "," + tuple.getValueByField(Constants.TASK_ID_FOR_SPLIT_BOLT) + "," +
                    tuple.getValueByField(Constants.HOST_NAME_FOR_SPLIT_BOLT) + "," +
                    tuple.getValueByField(Constants.GREATER_PREDICATE_EVALUATION_TIME_BOLT) + "," + tuple.getValueByField(Constants.MUTABLE_BOLT_TASK_ID) + "," + tuple.getValueByField(Constants.MUTABLE_BOLT_MACHINE);


            this.stringBuilder.append(tupleString);
            this.stringBuilder.append(",,");
        }
        if (tuple.getSourceStreamId().equals(rightPredicateSourceStreamID)) {
            String tupleString = tuple.getValueByField(Constants.TUPLE_ID) + "," +
                    tuple.getValueByField(Constants.KAFKA_TIME) + "," + tuple.getValueByField(Constants.SPLIT_BOLT_TIME) + "," + tuple.getValueByField(Constants.TASK_ID_FOR_SPLIT_BOLT) + "," +
                    tuple.getValueByField(Constants.HOST_NAME_FOR_SPLIT_BOLT) + "," +
                    tuple.getValueByField(Constants.LESSER_PREDICATE_EVALUATION_TIME_BOLT) + "," + tuple.getValueByField(Constants.MUTABLE_BOLT_TASK_ID) + "," + tuple.getValueByField(Constants.MUTABLE_BOLT_MACHINE);
            this.stringBuilder.append(tupleString);
            this.stringBuilder.append(",,");
        }
        if (tuple.getSourceStreamId().equals(result)) {
            this.counter++;
            this.stringBuilder.append(tuple.getValueByField("time") + "," + tuple.getValueByField("TaskID") + tuple.getValueByField("HostName"));

            if (this.counter == 1000) {
                try{
                    this.bufferedWriter.write(stringBuilder.toString());
                }catch (Exception e){
                    e.printStackTrace();
                }
                this.stringBuilder = new StringBuilder();
            } else {
                this.stringBuilder.append("\n");
            }
        }

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }
}
