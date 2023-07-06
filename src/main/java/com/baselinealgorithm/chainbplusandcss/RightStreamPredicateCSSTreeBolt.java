package com.baselinealgorithm.chainbplusandcss;

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

public class RightStreamPredicateCSSTreeBolt extends BaseRichBolt {
    private LinkedList<CSSTree> revenueLinkedListCSSTree=null;
    private LinkedList<CSSTree> costLinkedListCSSTree=null;
    private BPlusTree revenueBPlusTree=null;
    private BPlusTree costBPlusTree=null;
    private int archiveCount;
    private int revenueArchiveCount;
    private int costArchiveCount;
    private int tupleRemovalCount;
    private int tupleRemovalCountForLocal;
    private int orderOfTreeBothBPlusTreeAndCSSTree;
    private OutputCollector outputCollector;
    public RightStreamPredicateCSSTreeBolt(int archiveCount, int tupleRemovalCount, int orderOfTreeBothBPlusTreeAndCSSTree){
        this.archiveCount=archiveCount;
        this.tupleRemovalCount=tupleRemovalCount;
        this.orderOfTreeBothBPlusTreeAndCSSTree=orderOfTreeBothBPlusTreeAndCSSTree;
    }
    @Override
    public void prepare(Map<String, Object> map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.revenueLinkedListCSSTree= new LinkedList<>();
        this.costLinkedListCSSTree= new LinkedList<>();
        this.revenueBPlusTree= new BPlusTree(orderOfTreeBothBPlusTreeAndCSSTree);
        this.costBPlusTree= new BPlusTree(orderOfTreeBothBPlusTreeAndCSSTree);
        this.revenueArchiveCount=0;
        this.costArchiveCount=0;
        this.outputCollector=outputCollector;
    }

    @Override
    public void execute(Tuple tuple) {
        tupleRemovalCountForLocal++;
        if(tuple.getSourceStreamId().equals("Left")){
           leftPredicateEvaluation (tuple,outputCollector);
        }
        if(tuple.getSourceStreamId().equals("Right")){
            rightPredicateEvaluation(tuple,outputCollector);
        }
        if(tupleRemovalCountForLocal>=tupleRemovalCount){
            this.costLinkedListCSSTree.remove(costLinkedListCSSTree.getFirst());
            this.revenueLinkedListCSSTree.remove(revenueLinkedListCSSTree.getFirst());
        }

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }
    private void rightPredicateEvaluation(Tuple tuple, OutputCollector outputCollector){
        // Insertion
        costBPlusTree.insert(tuple.getIntegerByField("Tuple"), tuple.getIntegerByField("ID"));
        costArchiveCount++;
        HashSet<Integer> hashSetIds= new HashSet<>();
        //Results form BPlus Tree searching
        hashSetIds.addAll(revenueBPlusTree.lessThenSpecificValueHash(tuple.getIntegerByField("Tuple")));
        // Results from immutable B+ CSS Tree
        if(!revenueLinkedListCSSTree.isEmpty())
            for(int i=0;i<revenueLinkedListCSSTree.size();i++){
                hashSetIds.addAll(revenueLinkedListCSSTree.get(i).searchSmaller(tuple.getIntegerByField("Tuple")));
        /// Writer Emittier of tuples

        //Archive Interval achieve Then merge the active BPlus Tree into CSS Tree
        if(costArchiveCount>=archiveCount){
            costArchiveCount=0;
            com.stormiequality.BTree.Node node= costBPlusTree.leftMostNode();
            CSSTree cssTree= new CSSTree(orderOfTreeBothBPlusTreeAndCSSTree);
            while(node!=null){
                for(com.stormiequality.BTree.Key key: node.getKeys()){
                    cssTree.insertBulkUpdate(key.getKey(),key.getValues());
                }

                node= node.getNext();
            }
            //Insert into the linkedList
            costLinkedListCSSTree.add(cssTree);
        }
    }}
    private void leftPredicateEvaluation(Tuple tuple, OutputCollector outputCollector){
        revenueBPlusTree.insert(tuple.getIntegerByField("Tuple"), tuple.getIntegerByField("ID"));
        revenueArchiveCount++;
        HashSet<Integer> hashSetIds= new HashSet<>();
        //Results form BPlus Tree
        hashSetIds.addAll(costBPlusTree.greaterThenSpecificValueHashSet(tuple.getIntegerByField("Tuple")));
        // Results from immutable B+ CSS Tree
        if(!costLinkedListCSSTree.isEmpty())
            for(int i=0;i<costLinkedListCSSTree.size();i++){
                hashSetIds.addAll(costLinkedListCSSTree.get(i).searchGreater(tuple.getIntegerByField("Tuple")));
            }
        /// Writer Emittier of tuples

        //Archive Interval achieve Then merge the active BPlus Tree into CSS Tree
        if(revenueArchiveCount>=archiveCount){
            revenueArchiveCount=0;
            Node node= revenueBPlusTree.leftMostNode();
            CSSTree cssTree= new CSSTree(orderOfTreeBothBPlusTreeAndCSSTree);
            while(node!=null){
                for(Key key: node.getKeys()){
                    cssTree.insertBulkUpdate(key.getKey(),key.getValues());
                }

                node= node.getNext();
            }
            //Insert into the linkedList
            revenueLinkedListCSSTree.add(cssTree);
        }

    }

}
