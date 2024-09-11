package com.github.dwladdimiroc.serverlessApp.bolt.complex;

import com.github.dwladdimiroc.serverlessApp.util.Process;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.io.Serializable;
import java.util.Map;

public class BoltF implements IRichBolt, Serializable {
    private OutputCollector outputCollector;
    private Map mapConf;
    private String id;
    private int size;
    private int events;

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.mapConf = stormConf;
        this.outputCollector = collector;
        this.id = context.getThisComponentId();
        this.events = 0;
        this.size = 20000 + (int) (Math.random() * 1000);
    }

    @Override
    public void execute(Tuple input) {
        Process.processing(this.size);
        this.events++;
        if (this.events % 10 == 0) {
            long idReplica = 0;
            Values v = new Values(input.getValue(0));
            this.outputCollector.emit("BoltG", v);
        } else {
            long idReplica = 0;
            Values v = new Values(input.getValue(0));
            this.outputCollector.emit("BoltH", v);
        }
        this.outputCollector.ack(input);
    }

    @Override
    public void cleanup() {
        System.runFinalization();
        System.gc();
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declareStream("BoltG", new Fields("timestamp"));
        declarer.declareStream("BoltH", new Fields("timestamp"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return mapConf;
    }
}
