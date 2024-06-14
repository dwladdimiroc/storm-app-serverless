package com.github.dwladdimiroc.serverlessApp.bolt.complex;

import com.github.dwladdimiroc.serverlessApp.util.Process;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.Map;

public class BoltA implements IRichBolt, Serializable {
    private static final Logger logger = LoggerFactory.getLogger(BoltA.class);

    private OutputCollector outputCollector;
    private Map mapConf;
    private String id;
    private int[] array;

    public BoltA() {
        logger.info("Constructor BoltA");
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        logger.info("Prepare BoltA");

        this.mapConf = stormConf;
        this.outputCollector = collector;
        this.id = context.getThisComponentId();
        this.array = Process.createArray(10000);
    }

    @Override
    public void execute(Tuple input) {
        Process.processing(this.array);

        Values v = new Values(input.getValue(0));
        this.outputCollector.emit("BoltB", v);
        this.outputCollector.ack(input);
    }

    @Override
    public void cleanup() {
        System.runFinalization();
        System.gc();
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declareStream("BoltB", new Fields("timestamp"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return mapConf;
    }
}
