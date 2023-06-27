package com.baselinealgorithm.chainbplusandcss;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Map;

public class RightStreamPredicateCSSTree extends BaseRichBolt {
    private LinkedList<CSSTree> revenue=null;
    private LinkedList<CSSTree> cost=null;
    private int treeRemovalThreshold;
    private int treeArchiveThresholdRevenue;
    private int treeArchiveThresholdCost;
    private int treeArchiveUserDefined;
    private OutputCollector outputCollector;
    private int cssTreeInitilization;
    private HashSet<Integer> hashSet;
    public RightStreamPredicateCSSTree(int treeRemovalThreshold, int treeArchiveUserDefined, int cssTreeInitilization){
        this.treeRemovalThreshold=treeRemovalThreshold;
        this.treeArchiveUserDefined=treeArchiveUserDefined;
        this.cssTreeInitilization = cssTreeInitilization;
    }
    @Override
    public void prepare(Map<String, Object> map, TopologyContext topologyContext, OutputCollector outputCollector) {
        revenue= new LinkedList<>();
        cost= new LinkedList<>();
        this.outputCollector= outputCollector;
    }

    @Override
    public void execute(Tuple tuple) {
        if(tuple.getSourceStreamId().equals("Left")){
            if(!revenue.isEmpty()){
                CSSTree currentCSSTreeDuration= revenue.getLast();
                currentCSSTreeDuration.insert(tuple.getIntegerByField("Tuple"), tuple.getIntegerByField("ID"));
                treeArchiveThresholdRevenue++;
                if(treeArchiveThresholdRevenue==treeArchiveUserDefined){
                    treeArchiveThresholdRevenue=0;
                    CSSTree cssTree= new CSSTree(cssTreeInitilization);
                    revenue.add(cssTree);
                }
            }else
            {
                CSSTree cssTree= new CSSTree(cssTreeInitilization);
                cssTree.insert(tuple.getIntegerByField("Tuple"), tuple.getIntegerByField("ID"));
                revenue.add(cssTree);
            }
            for (CSSTree cssTree : cost) {
                HashSet<Integer> hashSetGreater=cssTree.searchGreater(tuple.getIntegerByField("Tuple"));
                //EmitLogic tomorrow
            }


        }
        if(tuple.getSourceStreamId().equals("Right")){
            if(!cost.isEmpty()){
                CSSTree currentCSSTreeDuration= cost.getLast();
                currentCSSTreeDuration.insert(tuple.getIntegerByField("Tuple"), tuple.getIntegerByField("ID"));
                treeArchiveThresholdCost++;
                if(treeArchiveThresholdCost==treeArchiveUserDefined){
                    treeArchiveThresholdCost=0;
                    CSSTree cssTree= new CSSTree(cssTreeInitilization);
                    cost.add(cssTree);
                }
            }else
            {
                CSSTree cssTree= new CSSTree(cssTreeInitilization);
                cssTree.insert(tuple.getIntegerByField("Tuple"), tuple.getIntegerByField("ID"));
                cost.add(cssTree);
            }
            for (CSSTree cssTree : revenue) {
               HashSet<Integer> lessThanValues= cssTree.searchSmaller(tuple.getIntegerByField("Tuple"));
                //EmitLogic tomorrow
            }

        }
        if(cost.size()==treeRemovalThreshold||revenue.size()==treeRemovalThreshold){
            cost.remove(cost.getFirst());
            revenue.remove(revenue.getFirst());
        }

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }
}
