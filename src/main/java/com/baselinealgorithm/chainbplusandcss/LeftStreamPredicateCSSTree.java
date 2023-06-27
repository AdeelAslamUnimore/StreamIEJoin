package com.baselinealgorithm.chainbplusandcss;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;

import java.util.HashSet;
import java.util.LinkedList;
import java.util.Map;

public class LeftStreamPredicateCSSTree extends BaseRichBolt {
    private LinkedList<CSSTree> duration =null;
    private LinkedList<CSSTree> time =null;
    private int treeRemovalThreshold;
    private int treeArchiveThresholdRevenue;
    private int treeArchiveThresholdCost;
    private int treeArchiveUserDefined;
    private OutputCollector outputCollector;
    private int cssTreeInitializations;
    private HashSet<Integer> hashSet;
    public LeftStreamPredicateCSSTree(int treeRemovalThreshold, int treeArchiveUserDefined, int cssTreeInitializations){
        this.treeRemovalThreshold=treeRemovalThreshold;
        this.treeArchiveUserDefined=treeArchiveUserDefined;
        this.cssTreeInitializations = cssTreeInitializations;
    }
    @Override
    public void prepare(Map<String, Object> map, TopologyContext topologyContext, OutputCollector outputCollector) {
        duration = new LinkedList<>();
        time = new LinkedList<>();
        this.outputCollector= outputCollector;
    }

    @Override
    public void execute(Tuple tuple) {
        if(tuple.getSourceStreamId().equals("Left")){
            if(!duration.isEmpty()){
                CSSTree currentCSSTreeDuration= duration.getLast();
                currentCSSTreeDuration.insert(tuple.getIntegerByField("Tuple"), tuple.getIntegerByField("ID"));
                treeArchiveThresholdRevenue++;
                if(treeArchiveThresholdRevenue==treeArchiveUserDefined){
                    treeArchiveThresholdRevenue=0;
                    CSSTree cssTree= new CSSTree(cssTreeInitializations);
                    duration.add(cssTree);
                }
            }else
            {
                CSSTree cssTree= new CSSTree(cssTreeInitializations);
                cssTree.insert(tuple.getIntegerByField("Tuple"), tuple.getIntegerByField("ID"));
                duration.add(cssTree);
            }
            for (CSSTree cssTree : time) {
                HashSet<Integer> hashSetGreater=cssTree.searchSmaller(tuple.getIntegerByField("Tuple"));
                //EmitLogic tomorrow
            }


        }
        if(tuple.getSourceStreamId().equals("Right")){
            if(!time.isEmpty()){
                CSSTree currentCSSTreeDuration= time.getLast();
                currentCSSTreeDuration.insert(tuple.getIntegerByField("Tuple"), tuple.getIntegerByField("ID"));
                treeArchiveThresholdCost++;
                if(treeArchiveThresholdCost==treeArchiveUserDefined){
                    treeArchiveThresholdCost=0;
                    CSSTree cssTree= new CSSTree(cssTreeInitializations);
                    time.add(cssTree);
                }
            }else
            {
                CSSTree cssTree= new CSSTree(cssTreeInitializations);
                cssTree.insert(tuple.getIntegerByField("Tuple"), tuple.getIntegerByField("ID"));
                time.add(cssTree);
            }
            for (CSSTree cssTree : duration) {
                HashSet<Integer> lessThanValues= cssTree.searchGreater(tuple.getIntegerByField("Tuple"));
                //EmitLogic tomorrow
            }

        }
        if(time.size()==treeRemovalThreshold|| duration.size()==treeRemovalThreshold){
            time.remove(time.getFirst());
            duration.remove(duration.getFirst());
        }

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }}
