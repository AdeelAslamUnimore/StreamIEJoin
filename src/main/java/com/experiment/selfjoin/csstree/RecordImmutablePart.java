package com.experiment.selfjoin.csstree;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.util.Map;

public class RecordImmutablePart extends BaseRichBolt {
    private StringBuilder resultImmutabalePart;
    private int recordCounter;
    private BufferedWriter bufferedWriterRecordCSSJoin;
    private BufferedWriter bufferedWriterRecordMergingLeftEvaluationTuple;
    private BufferedWriter bufferedWriterRecordMergingRightEvaluationTuple;
    private BufferedWriter bufferedWriterRecordEvaluationTimeTuple;
    @Override
    public void prepare(Map<String, Object> map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.resultImmutabalePart = new StringBuilder();
        this.recordCounter=0;
        try{
            bufferedWriterRecordCSSJoin = new BufferedWriter(new FileWriter(new File("/home/adeel/Data/Results/bufferedWriterRecordCSSJoin.csv")));
            bufferedWriterRecordMergingLeftEvaluationTuple = new BufferedWriter(new FileWriter(new File("/home/adeel/Data/Results/bufferedWriterEvaluationLeftMergingTuple.csv")));
            bufferedWriterRecordMergingRightEvaluationTuple = new BufferedWriter(new FileWriter(new File("/home/adeel/Data/Results/bufferedWriterEvaluationRightMergingTuple.csv")));
            bufferedWriterRecordEvaluationTimeTuple = new BufferedWriter(new FileWriter(new File("/home/adeel/Data/Results/bufferedWriterRecordMergingTimeTuple.csv")));
           // tuple.getIntegerByField("ID"),probingEnd,taskID,hostName
            bufferedWriterRecordCSSJoin.write("LeftID,LeftProbingStartTime,LeftProbingEndTime,LeftTaskID, LeftHostName, TimeArrivalLeft, RightID,RightProbingStartTime,RightProbingEndTime,RightTaskID,RightHostName,TimeArrivalRight, timeAfterCalculation,streamID,TaskID,HostName \n");
            bufferedWriterRecordCSSJoin.flush();

            bufferedWriterRecordMergingLeftEvaluationTuple.write("ID,LeftMergeStartTime, LeftMergeEndTime, LeftTaskID, LeftHostName,ID,TimeArrivalLeft, RightMergeStartTime, RightTaskID, RightMergeEndTime, TimeArrivalRight, ID,timeAfterCalculation, streamID,TaskID,HostName \n");
            bufferedWriterRecordMergingLeftEvaluationTuple.flush();

            bufferedWriterRecordMergingRightEvaluationTuple.write("ID,LeftMergeStartTime, LeftMergeEndTime, LeftTaskID, LeftHostName,ID,TimeArrivalLeft, RightMergeStartTime, RightTaskID, RightMergeEndTime, TimeArrivalRight, ID,timeAfterCalculation, streamID,TaskID,HostName \n");
            bufferedWriterRecordMergingRightEvaluationTuple.flush();
            bufferedWriterRecordEvaluationTimeTuple.write("leftMergeStartTime,LeftMergeEnd,LefttaskID,LefthostName,rightMergeStartTime,rightMergeEnd,rightTaskID,rightHostName\n");
            bufferedWriterRecordEvaluationTimeTuple.flush();
        } catch (Exception e){
            e.printStackTrace();
        }
    }

    @Override
    public void execute(Tuple tuple) {
        if(tuple.getSourceStreamId().equals("ResultTuple")){
            recordCounter++;
            this.resultImmutabalePart.append(tuple.getValue(0)+","+tuple.getValue(1)+","+tuple.getValue(2)+","+tuple.getValue(3)+","+tuple.getValue(4)+","+tuple.getValue(5)+"\n");
            if(recordCounter==1000){
                try{
                    bufferedWriterRecordCSSJoin.write(resultImmutabalePart.toString());
                    bufferedWriterRecordCSSJoin.flush();
                }catch (Exception e){
                    e.printStackTrace();
                }
                recordCounter=0;
            }
        }
        if (tuple.getSourceStreamId().equals("MergingQueueTuple")){
            try{
                if(tuple.getValueByField("stream").equals("Left")){
                    bufferedWriterRecordMergingLeftEvaluationTuple.write(tuple.getValue(0)+","+tuple.getValue(1)+","+tuple.getValue(2)+","+tuple.getValue(3)+","+tuple.getValue(4)+"\n");
                    bufferedWriterRecordMergingLeftEvaluationTuple.flush();
                }
                if(tuple.getValueByField("stream").equals("Right")){

                }
                bufferedWriterRecordMergingRightEvaluationTuple.write(tuple.getValue(0)+","+tuple.getValue(1)+","+tuple.getValue(2)+","+tuple.getValue(3)+","+tuple.getValue(4)+"\n");
                bufferedWriterRecordMergingRightEvaluationTuple.flush();
            }catch (Exception e){
                e.printStackTrace();
            }
        }
        if(tuple.getSourceStreamId().equals("MergingTime")){
            try{
                bufferedWriterRecordEvaluationTimeTuple.write(tuple.getValue(0)+","+tuple.getValue(1)+"\n");
                bufferedWriterRecordEvaluationTimeTuple.flush();
            }catch (Exception e){
                e.printStackTrace();
            }
        }

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }
}
