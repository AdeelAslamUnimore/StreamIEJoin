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

public class LeftStreamPredicateCSSTree extends BaseRichBolt {
    private LinkedList<CSSTree> durationLinkedListCSSTree=null;
    private LinkedList<CSSTree> timeLinkedListCSSTree=null;
    private BPlusTree durationBPlusTree=null;
    private BPlusTree timeBPlusTree=null;
    private int archiveCount;
    private int durationArchiveCount;
    private int timeArchiveCount;
    private int tupleRemovalCount;
    private int orderOfTreeBothBPlusTreeAndCSSTree;
    private int tupleRemovalCountForLocal;
    private OutputCollector outputCollector;
    public LeftStreamPredicateCSSTree(int archiveCount, int tupleRemovalCount, int orderOfTreeBothBPlusTreeAndCSSTree){
        this.archiveCount=archiveCount;
        this.tupleRemovalCount=tupleRemovalCount;
        this.orderOfTreeBothBPlusTreeAndCSSTree=orderOfTreeBothBPlusTreeAndCSSTree;
    }
    @Override
    public void prepare(Map<String, Object> map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.durationLinkedListCSSTree= new LinkedList<>();
        this.timeLinkedListCSSTree= new LinkedList<>();
        this.durationBPlusTree= new BPlusTree(orderOfTreeBothBPlusTreeAndCSSTree);
        this.timeBPlusTree= new BPlusTree(orderOfTreeBothBPlusTreeAndCSSTree);
        this.durationArchiveCount=0;
        this.timeArchiveCount=0;
        this.outputCollector=outputCollector;
    }

    @Override
    public void execute(Tuple tuple) {
        tupleRemovalCountForLocal++;
            if(tuple.getSourceStreamId().equals("Left")){
                leftPredicateEvaluation(tuple,outputCollector);
            }
            if(tuple.getSourceStreamId().equals("Right")){
                rightPredicateEvaluation(tuple,outputCollector);
            }
            if(tupleRemovalCountForLocal>=tupleRemovalCount){
                timeLinkedListCSSTree.remove(timeLinkedListCSSTree.getFirst());
                durationLinkedListCSSTree.remove(durationLinkedListCSSTree.getFirst());
            }

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }
    private void rightPredicateEvaluation(Tuple tuple, OutputCollector outputCollector){
        // Insertion
        timeBPlusTree.insert(tuple.getIntegerByField("Tuple"), tuple.getIntegerByField("ID"));
        timeArchiveCount++;
        HashSet<Integer> hashSetIds= new HashSet<>();
        //Results form BPlus Tree searching
        hashSetIds.addAll(durationBPlusTree.lessThenSpecificValueHash(tuple.getIntegerByField("Tuple")));
        // Results from immutable B+ CSS Tree
        if(!durationLinkedListCSSTree.isEmpty())
            for(int i=0;i<durationLinkedListCSSTree.size();i++){
                hashSetIds.addAll(durationLinkedListCSSTree.get(i).searchSmaller(tuple.getIntegerByField("Tuple")));
            }
        /// Writer Emittier of tuples

        //Archive Interval achieve Then merge the active BPlus Tree into CSS Tree
        if(timeArchiveCount>=archiveCount){
            timeArchiveCount=0;
            Node node= timeBPlusTree.leftMostNode();
            CSSTree cssTree= new CSSTree(orderOfTreeBothBPlusTreeAndCSSTree);
            while(node!=null){
                for(Key key: node.getKeys()){
                    cssTree.insertBulkUpdate(key.getKey(),key.getValues());
                }

                node= node.getNext();
            }
            //Insert into the linkedList
            timeLinkedListCSSTree.add(cssTree);
        }
    }
    private void leftPredicateEvaluation(Tuple tuple, OutputCollector outputCollector){
        durationBPlusTree.insert(tuple.getIntegerByField("Tuple"), tuple.getIntegerByField("ID"));
        durationArchiveCount++;
        HashSet<Integer> hashSetIds= new HashSet<>();
        //Results form BPlus Tree
        hashSetIds.addAll(timeBPlusTree.greaterThenSpecificValueHashSet(tuple.getIntegerByField("Tuple")));
       // Results from immutable B+ CSS Tree
        if(!timeLinkedListCSSTree.isEmpty())
        for(int i=0;i<timeLinkedListCSSTree.size();i++){
            hashSetIds.addAll(timeLinkedListCSSTree.get(i).searchGreater(tuple.getIntegerByField("Tuple")));
        }
        /// Writer Emittier of tuples

        //Archive Interval achieve Then merge the active BPlus Tree into CSS Tree
        if(durationArchiveCount>=archiveCount){
            durationArchiveCount=0;
          Node node= durationBPlusTree.leftMostNode();
          CSSTree cssTree= new CSSTree(orderOfTreeBothBPlusTreeAndCSSTree);
          while(node!=null){
              for(Key key: node.getKeys()){
                  cssTree.insertBulkUpdate(key.getKey(),key.getValues());
              }

              node= node.getNext();
          }
          //Insert into the linkedList
          durationLinkedListCSSTree.add(cssTree);
        }

    }

}
