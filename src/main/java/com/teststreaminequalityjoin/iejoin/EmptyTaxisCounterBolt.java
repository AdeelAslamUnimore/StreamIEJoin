package com.teststreaminequalityjoin.iejoin;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;


import java.util.HashMap;
import java.util.Map;
import java.util.logging.FileHandler;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;

public class EmptyTaxisCounterBolt extends BaseRichBolt {
  //  private TaskMonitor taskMonitor;
    private OutputCollector collector;
    private static Logger logger = Logger.getLogger("Log");
    private static FileHandler fh;
    private Map<String, Integer[]> counter;

    public static final String OUT_STREAM_ID = "no_empty_taxis_stream";
    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
//                try{
//            //  writer = new BufferedWriter(new FileWriter(new File("/public/home/abrar/output/result.csv")));
//            fh = new FileHandler("/home/adeelaslam/Input/log");
//            logger.addHandler(fh);
//            SimpleFormatter formatter = new SimpleFormatter();
//            fh.setFormatter(formatter);
//        }catch (Exception e){
//
//        }
//        WorkerMonitor.getInstance().setContextInfo(context);
//        taskMonitor = new TaskMonitor(context.getThisTaskId());
        this.counter = new HashMap();
    }

    @Override
    public void execute(Tuple input) {
      //  logger.info("tuples"+input);
       // taskMonitor.notifyTupleReceived(input);
        int windowID = input.getInteger(0);
        String cell = input.getString(1);

        Integer[] count = counter.get(cell);

        if (count == null) {
            count = new Integer[2];
            count[0] = windowID;
            count[1] = 0;
        }

        if (windowID > count[0]) {
            this.collector.emit(OUT_STREAM_ID,
                    new Values(count[0], cell, count[1],input.getLongByField("SpoutTime"))
            );
            counter.remove(cell);
        } else if (windowID == count[0]) {
            count[1]++;
            counter.put(cell, count);
        } else {
            // should never happen
            this.collector.emit(
                    OUT_STREAM_ID,
                    new Values(count[0], cell, count[1],input.getLongByField("SpoutTime"))
            );
            //System.out.println(windowID + " < " + count[0]);
            //throw new RuntimeException("Out of sequence window ID: " + windowID + " < " + count[0]);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declareStream(
                OUT_STREAM_ID,
                new Fields("windowID", "cell", "empty_taxis_count","SpoutTime")
        );
    }
}
