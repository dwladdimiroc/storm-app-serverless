package com.github.dwladdimiroc.serverlessApp.topology;

import com.github.dwladdimiroc.serverlessApp.bolt.Metrics;
import com.github.dwladdimiroc.serverlessApp.bolt.hard.*;
import com.github.dwladdimiroc.serverlessApp.spout.Spout;
import com.github.dwladdimiroc.serverlessApp.util.PoolGrouping;
import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.topology.TopologyBuilder;

import java.io.Serializable;

public class HardTopology implements Serializable {
    public static final String TOPOLOGY_NAME = "hardApp";

    public static final int NUM_WORKERS = 7;
    public static final int QUEUE_SIZE = 1000000;
    public static final int TIMEOUT = 30;

    public static void main(String[] args) {
        Config config = new Config();
        config.setMessageTimeoutSecs(TIMEOUT);
        config.setNumWorkers(NUM_WORKERS);
        config.setNumAckers(0);
        config.setMaxSpoutPending(QUEUE_SIZE);

        String streamDistribution = args[0];
        int numParallelism = Integer.parseInt(args[1]);
        boolean serverless;
        if (args.length > 2) {
            serverless = args[2].equals("serverless");
        } else {
            serverless = false;
        }

        TopologyBuilder builder = new TopologyBuilder();

        // Set Spout
        builder.setSpout("Spout", new Spout(streamDistribution), 1);
        // Set Bolts
        builder.setBolt("BoltA", new BoltA(serverless, numParallelism), numParallelism).setNumTasks(numParallelism).
                customGrouping("Spout", "BoltA", new PoolGrouping());
        builder.setBolt("BoltB", new BoltB(serverless, numParallelism), numParallelism).setNumTasks(numParallelism).
                customGrouping("BoltA", "BoltB", new PoolGrouping());
        builder.setBolt("BoltC", new BoltC(serverless, numParallelism), numParallelism).setNumTasks(numParallelism).
                customGrouping("BoltB", "BoltC", new PoolGrouping());
        builder.setBolt("BoltD", new BoltD(serverless, numParallelism), numParallelism).setNumTasks(numParallelism).
                customGrouping("BoltC", "BoltD", new PoolGrouping());
        builder.setBolt("Latency", new Metrics(), 1).setNumTasks(1).
                globalGrouping("BoltD", "Latency");

        try {
            StormSubmitter.submitTopology(TOPOLOGY_NAME, config, builder.createTopology());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}