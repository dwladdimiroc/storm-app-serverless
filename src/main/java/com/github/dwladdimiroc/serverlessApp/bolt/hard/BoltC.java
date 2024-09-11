package com.github.dwladdimiroc.serverlessApp.bolt.hard;

import com.github.dwladdimiroc.serverlessApp.util.Process;
import com.github.dwladdimiroc.serverlessApp.util.Serverless;
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
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.util.Map;

public class BoltC implements IRichBolt, Serializable {
    private static final Logger logger = LoggerFactory.getLogger(BoltC.class);
    private OutputCollector outputCollector;
    private Map mapConf;
    private String id;
    private int size;

    private int taskIndex;
    private int numParallelism;

    private boolean serverless;
    private HttpClient client;
    private HttpRequest request;

    public BoltC(boolean serverless, int numParallelism) {
        logger.info("Constructor BoltC");
        this.numParallelism = numParallelism;
        this.serverless = serverless;
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        logger.info("Prepare BoltC");

        this.mapConf = stormConf;
        this.outputCollector = collector;
        this.id = context.getThisComponentId();
        this.size = (int) (7500000 * 0.15);

        this.taskIndex = context.getThisTaskIndex() + 1;

        if (serverless && (this.numParallelism * Serverless.USE_SERVERLESS) < this.taskIndex && this.taskIndex <= this.numParallelism) {
            this.client = HttpClient.newHttpClient();
            this.request = Serverless.request(this.getClass().getSimpleName(), this.taskIndex, this.size);
        } else {
            this.serverless = false;
        }
    }

    @Override
    public void execute(Tuple input) {
        if (serverless) {
            Serverless.function(client, request);
        } else {
            Process.processing(this.size);
        }

        Values v = new Values(input.getValue(0));
        this.outputCollector.emit("BoltD", v);
//        this.outputCollector.ack(input);
    }

    @Override
    public void cleanup() {
        System.runFinalization();
        System.gc();
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declareStream("BoltD", new Fields("timestamp"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return mapConf;
    }
}
