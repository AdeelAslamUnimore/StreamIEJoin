package com.correctness.iejoin;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;

import java.util.ArrayList;
import java.util.Map;

public class SpoutCorrectness extends BaseRichSpout {
    private SpoutOutputCollector collector;
    private ArrayList<East> eastArrayList;
    private ArrayList<West> westArrayList;
    private int id;
    private boolean flag;
    public SpoutCorrectness(){

    }
    @Override
    public void open(Map<String, Object> map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        this.collector=spoutOutputCollector;
        this.eastArrayList= new ArrayList<>();
        this.westArrayList= new ArrayList<>();
        this.id=0;
        this.flag= false;
    }

    @Override
    public void nextTuple() {
        if(!(flag)) {
            East east1 = new East(100, 140, 9);
            East east2 = new East(101, 100, 12);
            East east3 = new East(102, 90, 5);
            East east4 = new East(103, 80, 4);
            this.eastArrayList.add(east1);
            this.eastArrayList.add(east2);
            this.eastArrayList.add(east3);
            this.eastArrayList.add(east4);
            West west1 = new West(404, 100, 6);
            West west2 = new West(498, 140, 11);
            West west3 = new West(676, 80, 10);
            West west4 = new West(742, 90, 5);
            this.westArrayList.add(west1);
            this.westArrayList.add(west2);
            this.westArrayList.add(west3);
            this.westArrayList.add(west4);

            //Utils.sleep(10);
            for (int i = 0; i < westArrayList.size(); i++) {
                if(westArrayList.get(i)!=null)
                    this.collector.emit("LeftStreamTuples", new Values(eastArrayList.get(i).getId_East(), eastArrayList.get(i).getDuration(), eastArrayList.get(i).getRevenue(), i));

                   if(eastArrayList.get(i)!=null)
                       this.collector.emit("RightStream", new Values(westArrayList.get(i).getId_West(), westArrayList.get(i).getTime(), westArrayList.get(i).getCost(), i));

            }
//            for (int i = 0; i < westArrayList.size(); i++) {
//                this.collector.emit("RightStream", new Values(westArrayList.get(i).getId_West(), westArrayList.get(i).getTime(), westArrayList.get(i).getCost(), i));
//            }

            Utils.sleep(100000);
//            this.flag=false;
        }
        id++;
}

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declareStream("LeftStreamTuples", new Fields("TupleID","Duration","Revenue","ID"));
        outputFieldsDeclarer.declareStream("RightStream", new Fields("TupleID","Time", "Cost","ID"));
    }
}
