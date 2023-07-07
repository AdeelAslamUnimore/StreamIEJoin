package com.stormiequality.test;

import com.configurationsandconstants.iejoinandbaseworks.Configuration;
import com.configurationsandconstants.iejoinandbaseworks.Constants;
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
    private String leftStreamSmaller;
    private String rightStreamSmaller;
    private String leftStreamGreater;
    private String rightStreamGreater;
    public SplitBolt(){
        Map<String, Object> map= Configuration.configurationConstantForStreamIDs();
        this.leftStreamSmaller = (String) map.get("LeftSmallerPredicateTuple");
        this.rightStreamSmaller = (String) map.get("RightSmallerPredicateTuple");
        this.leftStreamGreater=(String) map.get("LeftGreaterPredicateTuple");
        this.rightStreamGreater= (String) map.get("RightGreaterPredicateTuple");
    }
    @Override
    public void prepare(Map<String, Object> map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.outputCollector= outputCollector;

    }

    @Override
    public void execute(Tuple tuple) {
        if(tuple.getSourceStreamId().equals("LeftStreamTuples")){

            Values valuesLeft= new Values(tuple.getIntegerByField("Duration"),tuple.getIntegerByField("ID"), "LeftStream",tuple.getValueByField("TupleID"));
            this.outputCollector.emit(leftStreamSmaller, tuple,valuesLeft);
            Values valuesRight= new Values(tuple.getIntegerByField("Revenue"),tuple.getIntegerByField("ID"),"LeftStream",tuple.getValueByField("TupleID"));
            this.outputCollector.emit(leftStreamGreater,tuple,valuesRight);
            this.outputCollector.ack(tuple);

        }
        if(tuple.getSourceStreamId().equals("RightStream")){

            Values valuesLeft= new Values(tuple.getIntegerByField("Time"),tuple.getIntegerByField("ID"), "RightStream",tuple.getValueByField("TupleID"));
            this.outputCollector.emit(rightStreamSmaller,tuple, valuesLeft);
            Values valuesRight= new Values(tuple.getIntegerByField("Cost"),tuple.getIntegerByField("ID"),"RightStream",tuple.getValueByField("TupleID"));
            this.outputCollector.emit(rightStreamGreater,tuple,valuesRight);
            this.outputCollector.ack(tuple);
        }

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declareStream(leftStreamSmaller, new Fields(Constants.TUPLE,Constants.TUPLE_ID,"StreamID","TupleID"));
        outputFieldsDeclarer.declareStream(leftStreamGreater, new Fields(Constants.TUPLE,Constants.TUPLE_ID,"StreamID","TupleID"));
        outputFieldsDeclarer.declareStream(rightStreamGreater, new Fields(Constants.TUPLE,Constants.TUPLE_ID,"StreamID","TupleID"));
        outputFieldsDeclarer.declareStream(rightStreamSmaller, new Fields(Constants.TUPLE,Constants.TUPLE_ID,"StreamID","TupleID"));


    }
}
