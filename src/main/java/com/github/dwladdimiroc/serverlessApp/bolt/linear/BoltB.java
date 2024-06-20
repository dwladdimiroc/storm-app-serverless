package com.github.dwladdimiroc.serverlessApp.bolt.linear;

import com.github.dwladdimiroc.serverlessApp.util.HTTP;
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

public class BoltB implements IRichBolt, Serializable {
    private static final Logger logger = LoggerFactory.getLogger(BoltB.class);
    private OutputCollector outputCollector;
    private Map mapConf;
    private String id;
    private int[] array;

    private boolean serverless;

    public BoltB() {
        logger.info("Constructor BoltB");
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        logger.info("Prepare BoltB");

        this.mapConf = stormConf;
        this.outputCollector = collector;
        this.id = context.getThisComponentId();
        this.array = Process.createArray(100000);

        this.serverless = true;
    }

    @Override
    public void execute(Tuple input) {
        if (serverless) {
            HTTP.function(this.array.length);
        } else{
            Process.processing(this.array);
        }

        Values v = new Values(input.getValue(0));
        this.outputCollector.emit("BoltC", v);
        this.outputCollector.ack(input);
    }

    @Override
    public void cleanup() {
        System.runFinalization();
        System.gc();
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declareStream("BoltC", new Fields("timestamp"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return mapConf;
    }
}
