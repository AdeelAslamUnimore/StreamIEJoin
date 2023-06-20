package com.stormiequality.test;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.Map;

public class SplitBolt extends BaseRichBolt {
    private OutputCollector outputCollector;
    @Override
    public void prepare(Map<String, Object> map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.outputCollector= outputCollector;
    }

    @Override
    public void execute(Tuple tuple) {

        if(tuple.getSourceStreamId().equals("LeftStreamTuples")){

            Values valuesLeft= new Values(tuple.getIntegerByField("Duration"),tuple.getIntegerByField("ID"), "LeftStream",tuple.getValueByField("TupleID"));
            this.outputCollector.emit("LeftPredicate", tuple,valuesLeft);
            Values valuesRight= new Values(tuple.getIntegerByField("Revenue"),tuple.getIntegerByField("ID"),"LeftStream",tuple.getValueByField("TupleID"));
            this.outputCollector.emit("RightPredicate",tuple,valuesRight);
            this.outputCollector.ack(tuple);

        }
        if(tuple.getSourceStreamId().equals("RightStream")){

            Values valuesLeft= new Values(tuple.getIntegerByField("Time"),tuple.getIntegerByField("ID"), "RightStream",tuple.getValueByField("TupleID"));
            this.outputCollector.emit("LeftPredicate",tuple, valuesLeft);
            Values valuesRight= new Values(tuple.getIntegerByField("Cost"),tuple.getIntegerByField("ID"),"RightStream",tuple.getValueByField("TupleID"));
            this.outputCollector.emit("RightPredicate",tuple,valuesRight);
            this.outputCollector.ack(tuple);
        }

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declareStream("LeftPredicate", new Fields("Tuple","ID","StreamID","TupleID"));
        outputFieldsDeclarer.declareStream("RightPredicate", new Fields("Tuple","ID","StreamID","TupleID"));
    }
}
