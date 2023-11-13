package com.stormiequality.inputdata;

import com.configurationsandconstants.iejoinandbaseworks.Constants;
import com.google.common.util.concurrent.RateLimiter;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;

import java.util.List;
import java.util.Map;
import java.util.Random;

public class Spout extends BaseRichSpout {
    private SpoutOutputCollector spoutOutputCollector;
    private Random random=null;
    private int id=1;
    private long counter=0l;
    private int count;
    private List<Integer> listOfAllTasks;
    private List<Integer> listOfSplitDownStream;
    private int downStreamCounter;
    private int counterForInsertionRate;
    //private RateLimiter rateLimiter;

    public Spout(int count){
        this.count=count;
    }

    @Override
    public void open(Map<String, Object> map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        this.spoutOutputCollector=spoutOutputCollector;
        this.random= new Random();
        downStreamCounter=0;
    //  this.rateLimiter=RateLimiter.create(5000.0);
        listOfAllTasks= topologyContext.getComponentTasks( Constants.OFFSET_AND_IE_JOIN_BOLT_ID);
        listOfSplitDownStream=topologyContext.getComponentTasks("SplitBolt");
    }

    @Override
    public void nextTuple() {
        counter++;
        int revenue = random.nextInt(75000 -1)+1;
      // int cost= random.nextInt(50000-1)+1;
        int duration=random.nextInt(75000-1)+1;
        //    int time=random.nextInt(100000-1)+1;
        //  Values left = new Values(duration,revenue,id, System.currentTimeMillis());
        Values left = new Values(duration,revenue,id, System.currentTimeMillis(),System.currentTimeMillis());
        // Values right= new Values(time, cost, id,System.currentTimeMillis(),System.currentTimeMillis());
        id++;
        counterForInsertionRate++;
        if(counterForInsertionRate==1000) {
          // count++;
          Utils.sleep(10);

       }
        //if (rateLimiter.tryAcquire()) {
            this.spoutOutputCollector.emit("StreamR", left);
     //   }
        // ordinary comment
        // Need Adjustion
  //Utils.sleep(5);
       // this.spoutOutputCollector.emit("StreamS",right);

       downStreamCounter++;
       if(downStreamCounter==listOfSplitDownStream.size()){
           downStreamCounter=0;
       }



    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
     outputFieldsDeclarer.declareStream("StreamR", new Fields("distance","amount","ID","Time",Constants.KAFKA_TIME));
       // outputFieldsDeclarer.declareStream("StreamR", new Fields("Duration","Revenue","ID", Constants.KAFKA_SPOUT_TIME, Constants.KAFKA_TIME));
      //  outputFieldsDeclarer.declareStream("StreamS", new Fields("Time", "Cost","ID",Constants.KAFKA_SPOUT_TIME, Constants.KAFKA_TIME));

    }

}
