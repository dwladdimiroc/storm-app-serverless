package com.github.dwladdimiroc.serverlessApp.spout;

import com.github.dwladdimiroc.serverlessApp.util.Distribution;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichSpout;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Time;
import org.apache.storm.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;

public class Spout implements IRichSpout, Serializable {
    private static final Logger logger = LoggerFactory.getLogger(Spout.class);
    private Map conf;
    private TopologyContext context;
    private SpoutOutputCollector collector;

    private LinkedBlockingQueue<Integer> queue;
    private String distribution;
    private float[] samples;
    private int indexSamples;

    private String id;

    public Spout(String distribution) {
        this.distribution = distribution;
    }

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        this.conf = conf;
        this.context = context;
        this.collector = collector;
        this.id = context.getThisComponentId();
        this.queue = new LinkedBlockingQueue<Integer>(100000);

        Distribution file = new Distribution(this.distribution);
        this.samples = file.Input();

        this.indexSamples = 0;

        Thread createTuples = new Thread(new TuplesCreator());
        createTuples.start();
    }

    class TuplesCreator implements Runnable {
        @Override
        public void run() {
            createTuples();
        }

        public void createTuples() {
            while (true) {
                for (int i = 0; i < samples[indexSamples]; i++) {
                    queue.add(i);
                }
                indexSamples++;
                Utils.sleep(1000);
            }
        }
    }

    @Override
    public void close() {
        logger.info("Close");
    }

    @Override
    public void activate() {
        logger.info("Activate");
    }

    @Override
    public void deactivate() {
        logger.info("Deactivate");
    }


    @Override
    public void nextTuple() {
        Integer nums = queue.poll();
        if (nums == null) {
            Utils.sleep(10);
        } else {
            Values values = new Values(Time.currentTimeMillis());
            this.collector.emit("BoltA", values);
        }
    }

    @Override
    public void ack(Object msgId) {
    }

    @Override
    public void fail(Object msgId) {
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declareStream("BoltA", new Fields("timestamp"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return conf;
    }
}


