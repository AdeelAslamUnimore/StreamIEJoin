package com.stormiequality.join;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class TestSpout extends BaseRichSpout {
    private  SpoutOutputCollector outputCollector;
    public void open(Map<String, Object> map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        this.outputCollector=spoutOutputCollector;
    }

    public void nextTuple() {
        Model model1=new Model("value1");
        Model model2= new Model("Value2");
        Model model3= new Model("value3");
        List<Model> values = Arrays.asList(model1, model2, model3);
        outputCollector.emit(new Values(values));

    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("Fields"));
    }
}
