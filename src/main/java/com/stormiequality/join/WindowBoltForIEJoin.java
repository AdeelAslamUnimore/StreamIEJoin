package com.stormiequality.join;

import org.apache.storm.topology.base.BaseRichBolt;

import java.io.Serializable;

public abstract class WindowBoltForIEJoin extends BaseRichBolt {


    public WindowBoltForIEJoin withCountBasedWindow(Count count){
        if(count==null){
            throw new IllegalArgumentException("IEJoin Count cannot be null");
        }
        if(count.removalCount<=0){
            throw new IllegalArgumentException("Must be greater than 0");
        }
        return this;
    }

    public WindowBoltForIEJoin withDurationBasedWindow(Duration duration){

        /// Need to be check later
        if(duration==null){
            throw new IllegalArgumentException("IEJoin Count cannot be null");
        }
        return  this;
    }

    public static class Count implements Serializable{
        public final int removalCount;
        public Count(int removalCount){
            this.removalCount=removalCount;
        }

    }
    public static class Duration implements Serializable{
        /// Need to be checked
        public final Long removalDuration;
        public Duration(long removalDuration){
            this.removalDuration=removalDuration;
        }
    }
}
