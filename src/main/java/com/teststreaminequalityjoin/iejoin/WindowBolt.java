package com.teststreaminequalityjoin.iejoin;
import org.apache.storm.Config;
import org.apache.storm.Constants;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

public abstract class WindowBolt extends BaseRichBolt {
    public static final int SECONDS_PER_TIME_UNIT = 1;
    // mapping system time to time in tuples
    public static final int TIME_UNIT_IN_SECONDS = 60;
    private List<Tuple> window = new ArrayList();
    private int windowSize;
    private long lastTime;
    private OutputCollector collector;
    private int windowID;
    public abstract Date getTs(Tuple t);
    public abstract void onWindow(int windowID, List<Tuple> window);

    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector  = collector;
        this.windowID = 0;
    }
    public WindowBolt(int windowSize) {
        this.windowSize = windowSize;
        this.lastTime = 0L;
    }

    public void execute(Tuple tuple) {

        if (isTickTuple(tuple)) {
          //  System.out.println(tuple);
           // System.exit(0);
            advanceWindow();
            if (!window.isEmpty()) {
                onWindow(windowID, window);
            }
            windowID++;
        } else {

            if (lastTime == 0) {
                // this is the first tuple received,
                // set lastTime to something
                lastTime = getTs(tuple).getTime();
            }

            if (!tupleInWindow(tuple)) {
                window.add(tuple);
            }
        }

        collector.ack(tuple);
    }
    private boolean tupleInWindow(Tuple tuple) {
        int id = tuple.getInteger(0);

        for (Tuple t : window) {
            int oid = t.getInteger(0);
            if (id == oid) {
                return true;
            }
        }

        return false;
    }
    private void advanceWindow() {
        if (lastTime == 0) {
            // Tick tuple received before first tuple.
            // Do nothing. We start advancing the window
            // from the first processed tuple on.
            return;
        }

        lastTime += TIME_UNIT_IN_SECONDS * 1000;
        long windowSize = this.windowSize * 1000; // ms
        int removeStop = 0;

        for (Tuple t : window) {
            long delta = lastTime - getTs(t).getTime();
            if (delta > windowSize) {
                removeStop++;
            } else {
                break;
            }
        }

        for (int i = 0; i < removeStop; i++) {
            window.remove(0);
        }
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {

    }
    protected boolean isTickTuple(Tuple tuple) {
        return tuple.getSourceComponent().equals(Constants.SYSTEM_COMPONENT_ID)
                && tuple.getSourceStreamId().equals(Constants.SYSTEM_TICK_STREAM_ID);
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        Config conf = new Config();
        conf.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, SECONDS_PER_TIME_UNIT);
        return conf;
    }
}
