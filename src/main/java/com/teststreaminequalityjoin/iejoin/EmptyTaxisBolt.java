package com.teststreaminequalityjoin.iejoin;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;


import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.FileHandler;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;

public class EmptyTaxisBolt extends WindowBolt {
    private OutputCollector collector;
   // private TaskMonitor taskMonitor;
    private static Logger logger = Logger.getLogger("Log");
    private static FileHandler fh;
    public EmptyTaxisBolt(int windowSize) {
        super(windowSize);
    }
    @Override
    public Date getTs(Tuple t) {
        return ((TaxiRide) t.getValue(1)).dropoffTS;
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        super.prepare(stormConf, context, collector);
               // try{
            //  writer = new BufferedWriter(new FileWriter(new File("/public/home/abrar/output/result.csv")));
//            fh = new FileHandler("/home/adeelaslam/Input/log");
//            logger.addHandler(fh);
//            SimpleFormatter formatter = new SimpleFormatter();
//            fh.setFormatter(formatter);
//        }catch (Exception e){
//
//        }
//        WorkerMonitor.getInstance().setContextInfo(context);
//        taskMonitor = new TaskMonitor(context.getThisTaskId());
        this.collector = collector;
    }

    @Override
    public void onWindow(int windowID, List<Tuple> window) {
        //logger.info("add"+window.get(0));
//        for(int i=0;i<window.size();i++) {
//            taskMonitor.notifyTupleReceived(window.get(i));
//        }
        Map<String, Tuple> emptyTaxis = getEmptyTaxis(window);
        for (Map.Entry<String, Tuple> e : emptyTaxis.entrySet()) {
            TaxiRide tr = ((TaxiRide) e.getValue().getValue(1));
            String taxiID = e.getKey();
            this.collector.emit(new Values(windowID, tr.dropoffCell, taxiID, window.get(0).getLongByField("SpoutTime") ));
        }
    }
    private Map<String, Tuple> getEmptyTaxis(List<Tuple> window) {
        Map<String, Tuple> res = new HashMap();

        for (Tuple t : window) {
            String taxiID = ((TaxiRide) t.getValue(1)).taxiID;
            res.put(taxiID, t);
        }

        return res;
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        super.declareOutputFields(declarer);
        declarer.declare(
                new Fields("windowID", "cell", "taxiID","SpoutTime")
        );
    }
}
