package com.baselinealgorithm.chainindexbplustree;

import com.configurationsandconstants.iejoinandbaseworks.Constants;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Map;

public class RecordChainIndex extends BaseRichBolt {
    private BufferedWriter bufferedWriter;
    private int counter=0;
    private StringBuilder stringBuilder;
    @Override
    public void prepare(Map<String, Object> map, TopologyContext topologyContext, OutputCollector outputCollector) {
        try {
            this.bufferedWriter= new BufferedWriter(new FileWriter(new File("/home/adeel/Data/Results//ChainedIndexResult.csv")));
            this.bufferedWriter.write("ID ,KAFKA_TIME,KAFKA_SPOUT_TIME, Constants.SPLIT_BOLT_TIME, TASK_ID_FOR_SPLIT_BOLT ,HOST_NAME_FOR_SPLIT_BOLT,TupleArrivalTimeChainIndex,TupleComputationTime,RightPredicateTaskID,RightPredicateHostID,BeforeTime,AfterTime,TaskID,Machine \n");
            this.bufferedWriter.flush();
            this.stringBuilder= new StringBuilder();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void execute(Tuple tuple) {
        try{
            stringBuilder.append(tuple
            .getValue(0)+","+tuple.getValue(1)+","+tuple.getValue(2)+","+tuple.getValue(3)+","+tuple.getValue(4)
            +","+tuple.getValue(5)+","+tuple.getValue(6)+","+tuple.getValue(7)+","+tuple.getValue(8)+","+tuple.getValue(9)+
                    ","+tuple.getValue(10)+","+tuple.getValue(11)+","+tuple.getValue(12)+","+tuple.getValue(13)+"\n");
            counter++;
            if(counter==1000){
                bufferedWriter.write(stringBuilder.toString());
                counter=0;
                stringBuilder= new StringBuilder();
            }

        }catch (Exception e){

        }


    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }
}
