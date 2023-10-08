package com.experiment.selfjoin.iejoinproposed;

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

public class BitSetJoinerResult extends BaseRichBolt {
    private BufferedWriter writer;

    @Override
    public void prepare(Map<String, Object> map, TopologyContext topologyContext, OutputCollector outputCollector) {
        try{
            writer= new BufferedWriter(new FileWriter(new File("")));
            writer.write(Constants.TUPLE_ID+","+Constants.KAFKA_TIME+","+
                    Constants.KAFKA_SPOUT_TIME+","+Constants.SPLIT_BOLT_TIME+","+Constants.TASK_ID_FOR_SPLIT_BOLT+","+ Constants.HOST_NAME_FOR_SPLIT_BOLT+","+ "TupleArrivalTime"+","+
                    Constants.GREATER_PREDICATE_EVALUATION_TIME_BOLT+","+Constants.MUTABLE_BOLT_TASK_ID+","+Constants.MUTABLE_BOLT_MACHINE+",startEvalutionTime,EndEvaluationTime,TaskID,MachineID,Stream \n");
            writer.flush();
        }catch (Exception e){

        }
    }

    @Override
    public void execute(Tuple tuple) {
        try{
            writer.write(tuple.getValue(0)+","+tuple.getValue(1)+","+
                    tuple.getValue(2)+","+tuple.getValue(3)+","+tuple.getValue(4)+","+ tuple.getValue(5)+","+ tuple.getValue(6)+","+
                    tuple.getValue(7)+","+tuple.getValue(8)+","+tuple.getValue(9)+","+tuple.getValue(10)+","+tuple.getValue(11)+","+tuple.getValue(12)+","+tuple.getValue(13)+","+tuple.getValue(14)+"\n");//getstartEvalutionTime,EndEvaluationTime,TaskID,MachineID,Stream \n");
            writer.flush();
        }catch (Exception e){

        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }
}
