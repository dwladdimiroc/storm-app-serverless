package com.github.dwladdimiroc.serverlessApp.bolt;

import com.github.dwladdimiroc.serverlessApp.util.Process;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.utils.Time;
import org.apache.storm.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.Map;

public class Metrics implements IRichBolt, Serializable {
    private static final Logger logger = LoggerFactory.getLogger(BoltD.class);
    private OutputCollector outputCollector;
    private Map mapConf;
    private String id;
    private int[] array;

    private long events;
    private float latencyTotal;

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        logger.info("Prepare Latency");

        this.mapConf = stormConf;
        this.outputCollector = collector;
        this.id = context.getThisComponentId();
        this.array = Process.createArray(50000);

        Thread latencyMsg = new Thread(new LatencyMsg());
        latencyMsg.start();
    }

    class LatencyMsg implements Runnable {
        @Override
        public void run() {
            while (true) {
                logger.info("[metric] {latency,{}}", latencyTotal);
                events=0;
                latencyTotal=0;
                Utils.sleep(5000);
            }
        }
    }

    @Override
    public void execute(Tuple input) {
        long timeInit = input.getLong(0);
        long timeFinal = Time.currentTimeMillis();
        long latencyEvent = timeFinal - timeInit;
        this.latencyTotal = (latencyEvent + (this.events * this.latencyTotal)) / (this.events + 1);
        this.events++;
        this.outputCollector.ack(input);
    }

    @Override
    public void cleanup() {
        System.runFinalization();
        System.gc();
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("timestamp"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return mapConf;
    }
}
