package com.proposed.iejoinandbplustreebased;

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

public class RecordBitSetJoin extends BaseRichBolt {
    BufferedWriter leftStreamWriter;
    // The buffered writer right stream
    BufferedWriter rightStreamWriter;
    private StringBuilder stringBuilderLeft;
    private StringBuilder stringBuilderRight;
    private int counterLeft;
    private int counterRight;
    //private int counterForRecord;
    private String leftPredicateSourceStreamID = null;
    private String rightPredicateSourceStreamID = null;
    public RecordBitSetJoin(){
        Map<String, Object> map = Configuration.configurationConstantForStreamIDs();
        this.leftPredicateSourceStreamID = (String) map.get("LeftPredicateSourceStreamIDBitSet");
        this.rightPredicateSourceStreamID = (String) map.get("RightPredicateSourceStreamIDBitSet");
    }
    @Override
    public void prepare(Map<String, Object> map, TopologyContext topologyContext, OutputCollector outputCollector) {
        try {
          //  this.counterForRecord=0;
            this.stringBuilderLeft = new StringBuilder();
            this.stringBuilderRight = new StringBuilder();
            leftStreamWriter = new BufferedWriter(new FileWriter(new File("/home/adeel/Data/Results/leftStream"+".csv")));
            rightStreamWriter = new BufferedWriter(new FileWriter(new File("/home/adeel/Data/Results/rightStream"+".csv")));
            leftStreamWriter.write(Constants.TUPLE_ID + "," +
                    Constants.KAFKA_TIME + "," + Constants.KAFKA_SPOUT_TIME + "," + Constants.SPLIT_BOLT_TIME + "," + Constants.TASK_ID_FOR_SPLIT_BOLT + "," + Constants.HOST_NAME_FOR_SPLIT_BOLT + "," + "TupleArrivalTime" + "," +
                    Constants.GREATER_PREDICATE_EVALUATION_TIME_BOLT + "," + Constants.MUTABLE_BOLT_TASK_ID + "," + Constants.MUTABLE_BOLT_MACHINE + ",EvaluationStartTime , EvaluationEndTime,TasksID, HostName \n");
            leftStreamWriter.flush();
            rightStreamWriter.write(Constants.TUPLE_ID + "," +
                    Constants.KAFKA_TIME + "," + Constants.KAFKA_SPOUT_TIME + "," + Constants.SPLIT_BOLT_TIME + "," + Constants.TASK_ID_FOR_SPLIT_BOLT + "," + Constants.HOST_NAME_FOR_SPLIT_BOLT + "," + "TupleArrivalTime" + "," +
                    Constants.GREATER_PREDICATE_EVALUATION_TIME_BOLT + "," + Constants.MUTABLE_BOLT_TASK_ID + "," + Constants.MUTABLE_BOLT_MACHINE + ",EvaluationStartTime,EvaluationEndTime,TasksID, HostName \n");

            rightStreamWriter.flush();
        }
        catch (Exception e){
            e.printStackTrace();
        }
    }

    @Override
    public void execute(Tuple tuple) {
      //  counterForRecord++;
        if(tuple.getSourceStreamId().equals(leftPredicateSourceStreamID)){
            counterLeft++;
            this.stringBuilderLeft.append(tuple.getValue(0)+","+tuple
                    .getValue(1)+","+tuple.getValue(2)+","+ tuple.getValue(3)+","+ tuple.getValue(4)+","+ tuple.getValue(5)+","+
                    tuple.getValue(6)+","+tuple.getValue(7)+","+tuple.getValue(8)+","+ tuple.getValue(9)+","+ tuple.getValue(10)+","+tuple.getValue(11)+","+tuple.getValue(12)+","+tuple.getValue(13)+"\n");
            if(counterLeft==1000){

                try {
                    this.leftStreamWriter.write(stringBuilderLeft.toString());
                    this.leftStreamWriter.flush();
                    this.stringBuilderLeft= new StringBuilder();
                    counterLeft=0;
                }
                catch (Exception e){
                    e.printStackTrace();
                }
            }

        }
        if(tuple.getSourceStreamId().equals(rightPredicateSourceStreamID)){
            counterRight++;
            this.stringBuilderRight.append(tuple.getValue(0)+","+tuple
                    .getValue(1)+","+tuple.getValue(2)+","+ tuple.getValue(3)+","+ tuple.getValue(4)+","+ tuple.getValue(5)+","+
                    tuple.getValue(6)+","+tuple.getValue(7)+","+tuple.getValue(8)+","+ tuple.getValue(9)+","+ tuple.getValue(10)+","+tuple.getValue(11)+","+tuple.getValue(12)+","+tuple.getValue(13)+"\n");
            if(counterRight==1000){
                try {
                    this.rightStreamWriter.write(stringBuilderRight.toString());
                    this.rightStreamWriter.flush();
                    this.stringBuilderRight=new StringBuilder();
                    counterRight=0;
                }
                catch (Exception e){
                    e.printStackTrace();
                }

            }


        }
//        if(counterForRecord==200000){
//            try{
//                leftStreamWriter = new BufferedWriter(new FileWriter(new File("D://VLDB Format//TestingRecords/leftStream"+counterForRecord+".csv")));
//                rightStreamWriter = new BufferedWriter(new FileWriter(new File("D://VLDB Format//TestingRecords//rightStream"+counterForRecord+".csv")));
//                leftStreamWriter.write(Constants.TUPLE_ID + "," +
//                        Constants.KAFKA_TIME + "," + Constants.KAFKA_SPOUT_TIME + "," + Constants.SPLIT_BOLT_TIME + "," + Constants.TASK_ID_FOR_SPLIT_BOLT + "," + Constants.HOST_NAME_FOR_SPLIT_BOLT + "," + "TupleArrivalTime" + "," +
//                        Constants.GREATER_PREDICATE_EVALUATION_TIME_BOLT + "," + Constants.MUTABLE_BOLT_TASK_ID + "," + Constants.MUTABLE_BOLT_MACHINE + ",EvaluationStartTime , EvaluationEndTime,TasksID, HostName \n");
//                leftStreamWriter.flush();
//                rightStreamWriter.write(Constants.TUPLE_ID + "," +
//                        Constants.KAFKA_TIME + "," + Constants.KAFKA_SPOUT_TIME + "," + Constants.SPLIT_BOLT_TIME + "," + Constants.TASK_ID_FOR_SPLIT_BOLT + "," + Constants.HOST_NAME_FOR_SPLIT_BOLT + "," + "TupleArrivalTime" + "," +
//                        Constants.GREATER_PREDICATE_EVALUATION_TIME_BOLT + "," + Constants.MUTABLE_BOLT_TASK_ID + "," + Constants.MUTABLE_BOLT_MACHINE + ",EvaluationStartTime,EvaluationEndTime,TasksID, HostName \n");
//
//                rightStreamWriter.flush();
//                counterForRecord=0;
//            }catch (Exception e){
//
//            }

       // }


    }


    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }
}
