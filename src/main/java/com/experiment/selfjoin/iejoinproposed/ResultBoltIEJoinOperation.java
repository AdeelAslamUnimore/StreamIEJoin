package com.experiment.selfjoin.iejoinproposed;

import com.configurationsandconstants.iejoinandbaseworks.Configuration;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.util.Map;

public class ResultBoltIEJoinOperation extends BaseRichBolt {
    private String mergingTuplesRecord;
    private String recordIEJoinStreamID;
    private String mergeRecordEvaluationStreamID;
    private StringBuilder recordIEJoinStringBuilder;
    private StringBuilder recordMergingTuplesRecordStringBuilder;
    private StringBuilder mergingTuplesRecordEvaluationStringBuilder;
    private int recordIEJoinCounter;
    private int recordMergingTuplesCounter;
    private int mergingTuplesRecordEvaluationCounter;
    private BufferedWriter bufferedWriterRecordIEJoin;
    private BufferedWriter bufferedWriterRecordMergingTuple;
    private BufferedWriter bufferedWriterRecordEvaluationTuple;

    public ResultBoltIEJoinOperation() {
        Map<String, Object> map = Configuration.configurationConstantForStreamIDs();
        this.recordIEJoinStreamID = (String) map.get("IEJoinResult");
        this.mergingTuplesRecord = (String) map.get("MergingTuplesRecord");
        this.mergeRecordEvaluationStreamID = (String) map.get("MergingTupleEvaluation");
    }

    @Override
    public void prepare(Map<String, Object> map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.recordIEJoinStringBuilder = new StringBuilder();
        this.recordMergingTuplesRecordStringBuilder = new StringBuilder();
        this.mergingTuplesRecordEvaluationStringBuilder = new StringBuilder();
        this.recordIEJoinCounter = 0;
        this.recordMergingTuplesCounter = 0;
        this.mergingTuplesRecordEvaluationCounter = 0;
        try {
            bufferedWriterRecordIEJoin = new BufferedWriter(new FileWriter(new File("/home/adeel/Data/Results/bufferedWriterRecordIEJoin.csv")));
            bufferedWriterRecordMergingTuple = new BufferedWriter(new FileWriter(new File("/home/adeel/Data/Results/bufferedWriterRecordMergingTuple.csv")));
            bufferedWriterRecordEvaluationTuple = new BufferedWriter(new FileWriter(new File("/home/adeel/Data/Results/bufferedWriterRecordEvaluationTuple.csv")));
            bufferedWriterRecordIEJoin.write("KafkaTime,Kafka_SPOUT_TIME, TupleArrivalTime, TupleEvaluationTime, Task, Host \n");
            bufferedWriterRecordIEJoin.flush();
            bufferedWriterRecordMergingTuple.write("MergeStartTime, MergeEndTime, Task, Host \n");
            bufferedWriterRecordMergingTuple.flush();
            bufferedWriterRecordEvaluationTuple.write("EvaluatedTime,Task, Host\n");
            bufferedWriterRecordEvaluationTuple.flush();


        } catch (Exception e) {

        }
    }

    @Override
    public void execute(Tuple tuple) {

        if (tuple.getSourceStreamId().equals(recordIEJoinStreamID)) {
            this.recordIEJoinCounter++;
            recordIEJoinStringBuilder.append(tuple.getValue(0) + "," + tuple.getValue(1) + "," + tuple.getValue(2) +
                    "," + tuple.getValue(3) + "," + tuple.getValue(4) + "," + tuple.getValue(5) +"\n");
        }
        if (tuple.getSourceStreamId().equals(mergingTuplesRecord)) {
            this.recordMergingTuplesCounter++;
            recordMergingTuplesRecordStringBuilder.append(tuple.getValue(0) + "," + tuple.getValue(1) + "," + tuple.getValue(2) + tuple.getValue(4) + "\n");

        }
        if (tuple.getSourceStreamId().equals(mergeRecordEvaluationStreamID)) {
            this.mergingTuplesRecordEvaluationCounter++;
            mergingTuplesRecordEvaluationStringBuilder.append(tuple.getValue(0) + "," + tuple.getValue(1) + "," + tuple.getValue(2) + "\n");
        }
        if (recordIEJoinCounter == 10000) {
            try {
                bufferedWriterRecordIEJoin.write(recordIEJoinStringBuilder.toString());
                bufferedWriterRecordIEJoin.flush();
                recordIEJoinStringBuilder = new StringBuilder();
            } catch (Exception e) {

            }
        }
        if (this.recordMergingTuplesCounter == 100) {
            try {
                bufferedWriterRecordMergingTuple.write(recordMergingTuplesRecordStringBuilder.toString());
                bufferedWriterRecordMergingTuple.flush();
                recordMergingTuplesRecordStringBuilder = new StringBuilder();
            } catch (Exception e) {

            }
        }
        if (mergingTuplesRecordEvaluationCounter == 100) {
            try {
                bufferedWriterRecordEvaluationTuple.write(mergingTuplesRecordEvaluationStringBuilder.toString());
                bufferedWriterRecordEvaluationTuple.flush();
                mergingTuplesRecordEvaluationStringBuilder = new StringBuilder();
            } catch (Exception e) {

            }
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }
}
