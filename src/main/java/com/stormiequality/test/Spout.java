package com.stormiequality.test;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;

import java.util.Map;
import java.util.Random;

public class Spout extends BaseRichSpout {
    private SpoutOutputCollector spoutOutputCollector;
    private Random random=null;
    private int id=1;
    private long counter=0l;
    private int count;
    public Spout(int count){
        this.count=count;
    }

    @Override
    public void open(Map<String, Object> map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        this.spoutOutputCollector=spoutOutputCollector;
        this.random= new Random();
    }

    @Override
    public void nextTuple() {
        counter++;
        int revenue = random.nextInt(1000 -1)+1;
        int cost= random.nextInt(1000-1)+1;
        int duration=random.nextInt(1500-9)+9;
        int time=random.nextInt(1500-9)+9;

        Values left = new Values(duration,revenue,id,System.currentTimeMillis(), System.currentTimeMillis());
      //  Values right= new Values(time, cost, id,"Right"+counter);
        id++;
//        if(count==100) {
//           //id = 1;
//            count=1;
          Utils.sleep(10);
//        }
 // Utils.sleep(2);
        this.spoutOutputCollector.emit("StreamR",left);
        // ordinary comment
         // this.spoutOutputCollector.emit("RightStream",right);


    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declareStream("StreamR", new Fields("distance","amount","ID", "kafkaTime","Time"));
        outputFieldsDeclarer.declareStream("RightStream", new Fields("Time", "Cost","ID","TupleID"));

    }

}
