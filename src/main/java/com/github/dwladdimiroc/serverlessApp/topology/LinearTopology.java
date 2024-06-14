package com.github.dwladdimiroc.serverlessApp.topology;

import com.github.dwladdimiroc.serverlessApp.bolt.*;
import com.github.dwladdimiroc.serverlessApp.bolt.linear.BoltA;
import com.github.dwladdimiroc.serverlessApp.bolt.linear.BoltB;
import com.github.dwladdimiroc.serverlessApp.bolt.linear.BoltC;
import com.github.dwladdimiroc.serverlessApp.bolt.linear.BoltD;
import com.github.dwladdimiroc.serverlessApp.spout.Spout;
import com.github.dwladdimiroc.serverlessApp.util.PoolGrouping;
import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.topology.TopologyBuilder;

import java.io.Serializable;

public class LinearTopology implements Serializable {
    private static final String TOPOLOGY_NAME = "serverlessApp";

    public static void main(String[] args) {
        Config config = new Config();
        config.setMessageTimeoutSecs(125);
        config.setNumWorkers(7);

        int numParallelism = Integer.parseInt(args[1]);

        TopologyBuilder builder = new TopologyBuilder();

        // Set Spout
        builder.setSpout("Spout", new Spout(args[0]), 1);
        // Set Bolts
        builder.setBolt("BoltA", new BoltA(), numParallelism).setNumTasks(numParallelism).
                customGrouping("Spout", "BoltA", new PoolGrouping());
        builder.setBolt("BoltB", new BoltB(), numParallelism).setNumTasks(numParallelism).
                customGrouping("BoltA", "BoltB", new PoolGrouping());
        builder.setBolt("BoltC", new BoltC(), numParallelism).setNumTasks(numParallelism).
                customGrouping("BoltB", "BoltC", new PoolGrouping());
        builder.setBolt("BoltD", new BoltD(), numParallelism).setNumTasks(numParallelism).
                customGrouping("BoltC", "BoltD", new PoolGrouping());
        builder.setBolt("Latency", new Metrics(), 1).setNumTasks(1).
                shuffleGrouping("BoltD", "Latency");

        try {
            StormSubmitter.submitTopology(TOPOLOGY_NAME, config, builder.createTopology());
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}