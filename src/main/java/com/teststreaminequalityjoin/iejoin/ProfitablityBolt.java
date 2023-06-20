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

public class ProfitablityBolt extends BaseRichBolt {
    private OutputCollector collector;
   // private TaskMonitor taskMonitor;
    private static Logger logger = Logger.getLogger("Log");
    private static FileHandler fh;
    private Map<Integer, Map<String, Double>> nums;
    private Map<Integer, Map<String, Integer>> dens;

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
//                       try{
//            //  writer = new BufferedWriter(new FileWriter(new File("/public/home/abrar/output/result.csv")));
//            fh = new FileHandler("/home/adeelaslam/Input/log");
//            logger.addHandler(fh);
//            SimpleFormatter formatter = new SimpleFormatter();
//            fh.setFormatter(formatter);
//        }catch (Exception e){
//
//        }
//        WorkerMonitor.getInstance().setContextInfo(context);
//        taskMonitor= new TaskMonitor(context.getThisTaskId());
        this.nums = new HashMap();
        this.dens = new HashMap();
    }

    @Override
    public void execute(Tuple input) {
       // logger.info("tuples"+input);
      //  taskMonitor.notifyTupleReceived(input);
        int windowID = input.getInteger(0);
        String cell = input.getString(1);
        Map<String, Double> n = nums.get(windowID);
        Map<String, Integer> d = dens.get(windowID);

        if (n == null) {
            n = new HashMap();
        }
        if (d == null) {
            d = new HashMap();
        }

        Double profit = n.get(cell);
        Integer count = d.get(cell);

        if (input.getSourceStreamId().equals(ProfitBolt.OUT_STREAM_ID)) {
            profit = input.getDouble(2);
            n.put(cell, profit);
        } else {
            count = input.getInteger(2);
            d.put(cell, count);
        }

        nums.put(windowID, n);
        dens.put(windowID, d);

        if (profit != null && count != null && count != 0) {
            collector.emit(new Values(windowID, cell, profit / count, input.getLongByField("SpoutTime")));
        }

        collector.ack(input);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("windowID", "cell", "profitability","SpoutTime"));
    }
}
