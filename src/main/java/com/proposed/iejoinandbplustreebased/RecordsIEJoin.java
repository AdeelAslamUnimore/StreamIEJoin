package com.proposed.iejoinandbplustreebased;

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

public class RecordsIEJoin extends BaseRichBolt {
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
 //   private BufferedWriter bufferedWriterRecordMergingTuple;
    private BufferedWriter bufferedWriterRecordEvaluationTuple;
    //
  //  private int recordCounter;


    public RecordsIEJoin() {
        Map<String, Object> map = Configuration.configurationConstantForStreamIDs();
        this.recordIEJoinStreamID = (String) map.get("IEJoinResult");
        //this.mergingTuplesRecord = (String) map.get("MergingTuplesRecord");
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
      //  this.recordCounter=0;

        try {

            bufferedWriterRecordIEJoin = new BufferedWriter(new FileWriter(new File("D:/VLDB Format/TestingRecords/"+"bufferedWriterRecordIEJoin.csv")));
             bufferedWriterRecordEvaluationTuple = new BufferedWriter(new FileWriter(new File("/D:/VLDB Format/TestingRecords//bufferedWriterRecordEvaluationTupleMerge.csv")));
            bufferedWriterRecordIEJoin.write("ID,KafkaTime,Kafka_SPOUT_TIME, TupleArrivalTime, TupleEvaluationTime, stream_ID,Task, Host \n");
            bufferedWriterRecordIEJoin.flush();
            bufferedWriterRecordEvaluationTuple.write("MergeStartTime,MergeEndTime,EvaluatedTime,Task, Host\n");
            bufferedWriterRecordEvaluationTuple.flush();


        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public void execute(Tuple tuple) {

        if (tuple.getSourceStreamId().equals(recordIEJoinStreamID)) {
           // recordCounter++;
            this.recordIEJoinCounter++;
            recordIEJoinStringBuilder.append(tuple.getValue(0) + "," + tuple.getValue(1) + "," + tuple.getValue(2) +
                    "," + tuple.getValue(3) + "," + tuple.getValue(4) + "," + tuple.getValue(5) + "," + tuple.getValue(6) + "," + tuple.getValue(7)  +"\n");
        }

        if (tuple.getSourceStreamId().equals(mergeRecordEvaluationStreamID)) {

            this.mergingTuplesRecordEvaluationCounter++;
            try {
                bufferedWriterRecordEvaluationTuple.write(tuple.getValue(0) + "," + tuple.getValue(1) + "," + tuple.getValue(2) + "," + tuple.getValue(3) + "," + tuple.getValue(4)  +"\n");
                bufferedWriterRecordEvaluationTuple.flush();
            }
            catch (Exception e){
                e.printStackTrace();
            }
        }
        if (recordIEJoinCounter == 1000) {
            try {
                bufferedWriterRecordIEJoin.write(recordIEJoinStringBuilder.toString());
                bufferedWriterRecordIEJoin.flush();
                recordIEJoinStringBuilder = new StringBuilder();
                recordIEJoinCounter = 0;
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
//        if(this.recordCounter==100000){
//            try {
//                bufferedWriterRecordIEJoin = new BufferedWriter(new FileWriter(new File("D://VLDB Format//TestingRecords//"+recordCounter+"bufferedWriterRecordIEJoin.csv")));
//                 bufferedWriterRecordIEJoin.write("ID,KafkaTime,Kafka_SPOUT_TIME, TupleArrivalTime, TupleEvaluationTime, Task, Host \n");
//                bufferedWriterRecordIEJoin.flush();
//                recordCounter=0;
//            } catch (Exception e) {
//                e.printStackTrace();
//            }
//        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }
}
