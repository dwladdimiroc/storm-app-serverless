package com.github.dwladdimiroc.serverlessApp.bolt;

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
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.Map;

public class Metrics implements IRichBolt, Serializable {
    private static final Logger logger = LoggerFactory.getLogger(Metrics.class);
        private static String AUTOSCALE_SERVER = "10.200.0.3";
//    private static String AUTOSCALE_SERVER = "localhost";
    private static int AUTOSCALE_PORT = 3000;

    private OutputCollector outputCollector;
    private Map mapConf;

    private long events;
    private float latencyTotal;

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        logger.info("Prepare Latency");

        this.mapConf = stormConf;
        this.outputCollector = collector;

        Thread latencyMsg = new Thread(new LatencyMsg());
        latencyMsg.start();
    }

    class LatencyMsg implements Runnable {
        private HttpClient client;

        public LatencyMsg() {
            this.client = HttpClient.newHttpClient();
        }

        public void sendLatency(String json) {
            String uri = "http://" + AUTOSCALE_SERVER + ":" + AUTOSCALE_PORT + "/sendLatency";
            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(uri))
                    .header("Content-Type", "application/json")
                    .POST(HttpRequest.BodyPublishers.ofString(json))
                    .build();

            try {
                this.client.send(request, HttpResponse.BodyHandlers.ofString());
            } catch (Exception e) {
                e.printStackTrace();
            }
        }


        @Override
        public void run() {
            while (true) {
                latencyTotal /= (float) (events);
                //logger.info("[metric] {latency,{}}", latencyTotal);
                String json = "{\"latency\": " + latencyTotal + "}";
                sendLatency(json);

                events = 0;
                latencyTotal = 0;
                Utils.sleep(5000);
            }
        }
    }

    @Override
    public void execute(Tuple input) {
        long timeInit = input.getLong(0);
        long timeFinal = Time.currentTimeMillis();
        long latencyEvent = timeFinal - timeInit;
        this.latencyTotal = latencyEvent + this.latencyTotal;
        this.events++;
//        this.outputCollector.ack(input);
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
