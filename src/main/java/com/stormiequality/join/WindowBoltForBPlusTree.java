package com.stormiequality.join;

import org.apache.storm.topology.base.BaseRichBolt;

import java.io.Serializable;
// Create Window Bolt For BPlus Tree
public abstract class WindowBoltForBPlusTree extends BaseRichBolt {
    //Constructor For
    // Constructor that initialize the instance
public WindowBoltForBPlusTree(){

}
//Reflexive method to define the count based window
public WindowBoltForBPlusTree withCountBasedWindow(WindowBoltForBPlusTree.Count count){
    if(count==null){
        throw new IllegalArgumentException("Window length count or BPlus Tree never be set null");
    }
    if(count.mergeSize<=0){
        throw new IllegalArgumentException("Window length count or BPlus Tree must be positive");
    }
    return  this;
}
//Reflexive way to define the duration based window
public WindowBoltForBPlusTree withDurationBasedWindow(WindowBoltForBPlusTree.Duration duration){

    /// Need further modification
    if(duration==null){
        throw new IllegalArgumentException("Window length duration or BPlus Tree never be set null");
    }
    if(duration.mergeInterval<=0){
        throw new IllegalArgumentException("Window length duration or BPlus Tree must be positive");
    }
    return this;
}
//Define the window based on the count
public static class Count implements Serializable{
    public final int mergeSize;
   // public final int slideInterval;

        public Count(int mergeSize) {
            this.mergeSize = mergeSize;
           // this.slideInterval=slideInterval;
        }
    }
    //Define the Duration based on the duration.
public static class Duration implements Serializable {
    public final Long mergeInterval;
    public final Long slideInterval;
    public Duration( Long mergeInterval, Long slideInterval){
        this.mergeInterval = mergeInterval;
        this.slideInterval = slideInterval;
    }
    }




}
