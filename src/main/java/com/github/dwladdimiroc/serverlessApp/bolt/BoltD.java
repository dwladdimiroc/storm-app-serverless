package com.github.dwladdimiroc.serverlessApp.bolt;

import com.github.dwladdimiroc.serverlessApp.spout.Spout;
import com.github.dwladdimiroc.serverlessApp.util.Process;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Time;
import org.apache.storm.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.Map;

public class BoltD implements IRichBolt, Serializable {
    private static final Logger logger = LoggerFactory.getLogger(BoltD.class);
    private OutputCollector outputCollector;
    private Map mapConf;
    private String id;
    private int[] array;

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        logger.info("Prepare BoltD");

        this.mapConf = stormConf;
        this.outputCollector = collector;
        this.id = context.getThisComponentId();
        this.array = Process.createArray(50000);
    }

    @Override
    public void execute(Tuple input) {
        Process.processing(this.array);
        Values v = new Values(input.getValue(0));
        this.outputCollector.emit("Latency", v);
        this.outputCollector.ack(input);
    }

    @Override
    public void cleanup() {
        System.runFinalization();
        System.gc();
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declareStream("Latency", new Fields("timestamp"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return mapConf;
    }
}
