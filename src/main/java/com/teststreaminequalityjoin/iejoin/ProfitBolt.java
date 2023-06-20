package com.teststreaminequalityjoin.iejoin;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.*;
import java.util.logging.FileHandler;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;

public class ProfitBolt extends WindowBolt {
    private OutputCollector collector;
    long count;
  //  private TaskMonitor taskMonitor;
    private static Logger logger = Logger.getLogger("Log");
    private static FileHandler fh;
    public static final String OUT_STREAM_ID = "profit_stream";

    public ProfitBolt(int windowSize) {
        super(windowSize);
    }
    @Override
    public Date getTs(Tuple t) {
       return ((TaxiRide) t.getValue(1)).dropoffTS;
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        super.prepare(stormConf, context, collector);
//        WorkerMonitor.getInstance().setContextInfo(context);
//        taskMonitor = new TaskMonitor(context.getThisTaskId());
        this.collector=collector;
        count=0L;
//        try{
//            //  writer = new BufferedWriter(new FileWriter(new File("/public/home/abrar/output/result.csv")));
//            fh = new FileHandler("/home/adeelaslam/Input/log");
//            logger.addHandler(fh);
//            SimpleFormatter formatter = new SimpleFormatter();
//            fh.setFormatter(formatter);
//        }catch (Exception e){
//
//        }
    }

    @Override
    public void onWindow(int windowID, List<Tuple> window) {
//        logger.info("tuples"+window.get(0));


        Map<String, List<Double>> profitPerCell = getProfitPerCell(window);
        Map<String, Double> medianPerCell = getMedianPerCell(profitPerCell);
        for (Map.Entry<String, Double> e : medianPerCell.entrySet()) {
            this.collector.emit(
                    OUT_STREAM_ID,
                    new Values(
                            windowID,
                            e.getKey(), // cell
                            e.getValue(), window.get(0).getLongByField("SpoutTime")// median profit
                    )
            );
        }
    }


    private Map<String, List<Double>> getProfitPerCell(List<Tuple> window) {
        Map<String, List<Double>> profitPerCell = new HashMap();
        for (Tuple t : window) {
            TaxiRide tr = ((TaxiRide) t.getValue(1));
            String pickupCell = tr.pickupCell;
            double fare = tr.fare;
            double tip = tr.tip;
            //System.out.println(fare+"--"+count++);
            List<Double> profit = profitPerCell.get(pickupCell);
            if (profit == null) {
                profit = new ArrayList();
            }

            int i = 0;
            for ( ; i < profit.size(); i++) {
                if(profit.get(i) > fare + tip) {
                    break;
                }
            }

            profit.add(i, fare + tip);


            profitPerCell.put(pickupCell, profit);
        }
        return profitPerCell;
    }

    private Map<String, Double> getMedianPerCell(Map<String, List<Double>> profitPerCell) {
        Map<String, Double> medianPerCell = new HashMap();
        for (Map.Entry<String, List<Double>> e : profitPerCell.entrySet()) {
            medianPerCell.put(e.getKey(), median(e.getValue()));
        }
        return medianPerCell;
    }

    private Double median(List<Double> profits) {
        int size = profits.size();
        if (size % 2 == 0) {
            double first = profits.get((size / 2) - 1);
            double second = profits.get(size / 2);
            return (first + second) / 2;
        }

        return profits.get(size / 2);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        super.declareOutputFields(declarer);
        declarer.declareStream(
                OUT_STREAM_ID,
                new Fields("windowID", "cell", "profit","SpoutTime")
        );
    }
}
