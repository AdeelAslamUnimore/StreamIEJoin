package com.baselinealgorithm.chainbplusandcss;

import com.configurationsandconstants.iejoinandbaseworks.Configuration;
import com.configurationsandconstants.iejoinandbaseworks.Constants;
import com.stormiequality.BTree.BPlusTree;
import com.stormiequality.BTree.Key;
import com.stormiequality.BTree.Node;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;

import java.util.HashSet;
import java.util.LinkedList;
import java.util.Map;

public class RightPredicateCSSTreeBolt extends BaseRichBolt {
    private LinkedList<CSSTree> leftStreamLinkedListCSSTree =null;
    private LinkedList<CSSTree> rightStreamLinkedListCSSTree =null;
    private BPlusTree leftStreamBPlusTree =null;
    private BPlusTree rightStreamBPlusTree =null;
    private int archiveCount;
    private int revenueArchiveCount;
    private int costArchiveCount;
    private int tupleRemovalCount;
    private int tupleRemovalCountForLocal;
    private int orderOfTreeBothBPlusTreeAndCSSTree;
    private OutputCollector outputCollector;
    private String leftStreamGreater;
    private String rightStreamGreater;
    public RightPredicateCSSTreeBolt(int archiveCount, int tupleRemovalCount, int orderOfTreeBothBPlusTreeAndCSSTree){
        this.archiveCount= Constants.MUTABLE_WINDOW_SIZE;
        this.tupleRemovalCount= Constants.IMMUTABLE_WINDOW_SIZE;
        this.orderOfTreeBothBPlusTreeAndCSSTree=Constants.ORDER_OF_CSS_TREE;
        Map<String, Object> map= Configuration.configurationConstantForStreamIDs();
        this.leftStreamGreater= (String) map.get("LeftSmallerPredicateTuple");
        this.rightStreamGreater= (String) map.get("RightSmallerPredicateTuple");
    }
    @Override
    public void prepare(Map<String, Object> map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.leftStreamLinkedListCSSTree = new LinkedList<>();
        this.rightStreamLinkedListCSSTree = new LinkedList<>();
        this.leftStreamBPlusTree = new BPlusTree(orderOfTreeBothBPlusTreeAndCSSTree);
        this.rightStreamBPlusTree = new BPlusTree(orderOfTreeBothBPlusTreeAndCSSTree);
        this.revenueArchiveCount=0;
        this.costArchiveCount=0;
        this.outputCollector=outputCollector;
    }

    @Override
    public void execute(Tuple tuple) {
        tupleRemovalCountForLocal++;
        if(tuple.getSourceStreamId().equals(leftStreamGreater)){
           leftPredicateEvaluation (tuple,outputCollector);
        }
        if(tuple.getSourceStreamId().equals(rightStreamGreater)){
            rightPredicateEvaluation(tuple,outputCollector);
        }
        if(tupleRemovalCountForLocal>=tupleRemovalCount){
            this.rightStreamLinkedListCSSTree.remove(rightStreamLinkedListCSSTree.getFirst());
            this.leftStreamLinkedListCSSTree.remove(leftStreamLinkedListCSSTree.getFirst());
        }

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }
    private void rightPredicateEvaluation(Tuple tuple, OutputCollector outputCollector){
        // Insertion
        rightStreamBPlusTree.insert(tuple.getIntegerByField("Tuple"), tuple.getIntegerByField("ID"));
        costArchiveCount++;
        HashSet<Integer> hashSetIds= new HashSet<>();
        //Results form BPlus Tree searching
        hashSetIds.addAll(leftStreamBPlusTree.lessThenSpecificValueHash(tuple.getIntegerByField("Tuple")));
        // Results from immutable B+ CSS Tree
        if(!leftStreamLinkedListCSSTree.isEmpty())
            for(int i = 0; i< leftStreamLinkedListCSSTree.size(); i++){
                hashSetIds.addAll(leftStreamLinkedListCSSTree.get(i).searchSmaller(tuple.getIntegerByField("Tuple")));
        /// Writer Emittier of tuples

        //Archive Interval achieve Then merge the active BPlus Tree into CSS Tree
        if(costArchiveCount>=archiveCount){
            costArchiveCount=0;
            com.stormiequality.BTree.Node node= rightStreamBPlusTree.leftMostNode();
            CSSTree cssTree= new CSSTree(orderOfTreeBothBPlusTreeAndCSSTree);
            while(node!=null){
                for(com.stormiequality.BTree.Key key: node.getKeys()){
                    cssTree.insertBulkUpdate(key.getKey(),key.getValues());
                }

                node= node.getNext();
            }
            //Insert into the linkedList
            rightStreamLinkedListCSSTree.add(cssTree);
        }
    }}
    private void leftPredicateEvaluation(Tuple tuple, OutputCollector outputCollector){
        leftStreamBPlusTree.insert(tuple.getIntegerByField("Tuple"), tuple.getIntegerByField("ID"));
        revenueArchiveCount++;
        HashSet<Integer> hashSetIds= new HashSet<>();
        //Results form BPlus Tree
        hashSetIds.addAll(rightStreamBPlusTree.greaterThenSpecificValueHashSet(tuple.getIntegerByField("Tuple")));
        // Results from immutable B+ CSS Tree
        if(!rightStreamLinkedListCSSTree.isEmpty())
            for(int i = 0; i< rightStreamLinkedListCSSTree.size(); i++){
                hashSetIds.addAll(rightStreamLinkedListCSSTree.get(i).searchGreater(tuple.getIntegerByField("Tuple")));
            }
        /// Writer Emittier of tuples

        //Archive Interval achieve Then merge the active BPlus Tree into CSS Tree
        if(revenueArchiveCount>=archiveCount){
            revenueArchiveCount=0;
            Node node= leftStreamBPlusTree.leftMostNode();
            CSSTree cssTree= new CSSTree(orderOfTreeBothBPlusTreeAndCSSTree);
            while(node!=null){
                for(Key key: node.getKeys()){
                    cssTree.insertBulkUpdate(key.getKey(),key.getValues());
                }

                node= node.getNext();
            }
            //Insert into the linkedList
            leftStreamLinkedListCSSTree.add(cssTree);
        }

    }

}
