package com.experiment.selfjoin.iejoinproposed;

import com.configurationsandconstants.iejoinandbaseworks.Configuration;
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

public class ResultBoltBitOperation extends BaseRichBolt {
    private String leftPredicateSourceStreamID = null;
    private String rightPredicateSourceStreamID = null;
    private String result = null;
    private StringBuilder stringBuilderLeftStream = null;
    private StringBuilder stringBuilderRightStream = null;
    private StringBuilder stringBuilderResultJoiner=null;
    private int counter = 0;
    private BufferedWriter bufferedWriterLeftStream;
    private BufferedWriter bufferedWriterRightStream;
    private BufferedWriter bufferedWriterResultsJoiner;

    public ResultBoltBitOperation() {
        Map<String, Object> map = Configuration.configurationConstantForStreamIDs();
        this.leftPredicateSourceStreamID = (String) map.get("LeftPredicateSourceStreamIDBitSet");
        this.rightPredicateSourceStreamID = (String) map.get("RightPredicateSourceStreamIDBitSet");
        this.result = (String) map.get("Results");
    }

    @Override
    public void prepare(Map<String, Object> map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.stringBuilderLeftStream = new StringBuilder();
        this.stringBuilderRightStream= new StringBuilder();
        this.stringBuilderResultJoiner= new StringBuilder();
        this.counter = 0;
        try {
            bufferedWriterLeftStream = new BufferedWriter(new FileWriter(new File("D://VLDB Format//TestingRecords//IEJoinBitSetEvaluationLeftStream.csv")));
            bufferedWriterRightStream = new BufferedWriter(new FileWriter(new File("D://VLDB Format//TestingRecords//IEJoinBitSetEvaluationRightStream.csv")));
            bufferedWriterResultsJoiner = new BufferedWriter(new FileWriter(new File("D://VLDB Format//TestingRecords//IEJoinBitSetEvaluationStreamJoiner.csv")));

            //    bufferedWriter= new BufferedWriter(new FileWriter(new File("/home/adeel/Data/Results/BitSetEvaluation.csv")));
            String greaterEvaluation = Constants.TUPLE_ID + "," +
                    Constants.KAFKA_TIME + "," + Constants.KAFKA_SPOUT_TIME + "," + Constants.SPLIT_BOLT_TIME + "," + Constants.TASK_ID_FOR_SPLIT_BOLT + "," +
                    Constants.HOST_NAME_FOR_SPLIT_BOLT + "," + " TupleArrivalTime," +
                    Constants.GREATER_PREDICATE_EVALUATION_TIME_BOLT + "," + Constants.MUTABLE_BOLT_TASK_ID + "," + Constants.MUTABLE_BOLT_MACHINE;
            String lesserEvaluation = Constants.TUPLE_ID + "," +
                    Constants.KAFKA_TIME + "," + Constants.KAFKA_SPOUT_TIME + "," + Constants.SPLIT_BOLT_TIME + "," + Constants.TASK_ID_FOR_SPLIT_BOLT + "," +
                    Constants.HOST_NAME_FOR_SPLIT_BOLT + "," + " TupleArrivalTime," +
                    Constants.LESSER_PREDICATE_EVALUATION_TIME_BOLT + "," + Constants.MUTABLE_BOLT_TASK_ID + "," + Constants.MUTABLE_BOLT_MACHINE;
            String conjuctionOperation = "time, timeAfterCalculation,streamID, taskID, hostName";
            bufferedWriterLeftStream.write(greaterEvaluation + "\n");
            bufferedWriterRightStream.write(lesserEvaluation+"\n");
            bufferedWriterResultsJoiner.write(conjuctionOperation+"\n");
            //bufferedWriter.write(greaterEvaluation+",,"+lesserEvaluation+",,"+conjuctionOperation+"\n");
            bufferedWriterLeftStream.flush();
            bufferedWriterRightStream.flush();
            bufferedWriterResultsJoiner.flush();

        } catch (Exception e) {

        }
    }

    @Override
    public void execute(Tuple tuple) {
        if (tuple.getSourceStreamId().equals(leftPredicateSourceStreamID)) {
            String tupleString = tuple.getValueByField(Constants.TUPLE_ID) + "," +
                    tuple.getValueByField(Constants.KAFKA_TIME) + "," + tuple.getValueByField(Constants.KAFKA_SPOUT_TIME) + "," + tuple.getValueByField(Constants.SPLIT_BOLT_TIME) + "," + tuple.getValueByField(Constants.TASK_ID_FOR_SPLIT_BOLT) + "," +
                    tuple.getValueByField(Constants.HOST_NAME_FOR_SPLIT_BOLT) + "," +
                    tuple.getValueByField("TupleArrivalTime") + "," +
                    tuple.getValueByField(Constants.GREATER_PREDICATE_EVALUATION_TIME_BOLT) + "," + tuple.getValueByField(Constants.MUTABLE_BOLT_TASK_ID) + "," + tuple.getValueByField(Constants.MUTABLE_BOLT_MACHINE)+"\n";


            this.stringBuilderLeftStream.append(tupleString);

        }
        if (tuple.getSourceStreamId().equals(rightPredicateSourceStreamID)) {
            String tupleString = tuple.getValueByField(Constants.TUPLE_ID) + "," +
                    tuple.getValueByField(Constants.KAFKA_TIME) + "," + tuple.getValueByField(Constants.KAFKA_SPOUT_TIME) + "," + tuple.getValueByField(Constants.SPLIT_BOLT_TIME) + "," + tuple.getValueByField(Constants.TASK_ID_FOR_SPLIT_BOLT) + "," +
                    tuple.getValueByField(Constants.HOST_NAME_FOR_SPLIT_BOLT) + "," +
                    tuple.getValueByField("TupleArrivalTime") + "," +
                    tuple.getValueByField(Constants.LESSER_PREDICATE_EVALUATION_TIME_BOLT) + "," + tuple.getValueByField(Constants.MUTABLE_BOLT_TASK_ID) + "," + tuple.getValueByField(Constants.MUTABLE_BOLT_MACHINE)+"\n";
            this.stringBuilderRightStream.append(tupleString);


        }
        if (tuple.getSourceStreamId().equals(result)) {
            this.counter++;
            this.stringBuilderResultJoiner.append(tuple.getValue(0) + "," +tuple.getValue(1) + "," + tuple.getValue(2) + "," + tuple.getValue(3) +"," + tuple.getValue(4)+"\n");

            if (this.counter == 1000) {
                try {
                    this.bufferedWriterLeftStream.write(stringBuilderLeftStream.toString());
                    bufferedWriterLeftStream.flush();
                    this.bufferedWriterRightStream.write(stringBuilderRightStream.toString());
                    bufferedWriterRightStream.flush();
                    this.bufferedWriterResultsJoiner.write(stringBuilderResultJoiner.toString());
                    bufferedWriterResultsJoiner.flush();
                } catch (Exception e) {
                    e.printStackTrace();
                }
                this.stringBuilderLeftStream = new StringBuilder();
                this.stringBuilderRightStream= new StringBuilder();
                this.stringBuilderResultJoiner= new StringBuilder();
                this.counter=0;
            }
        }

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }
}
