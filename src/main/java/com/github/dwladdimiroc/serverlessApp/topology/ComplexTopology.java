package com.github.dwladdimiroc.serverlessApp.topology;

import com.github.dwladdimiroc.serverlessApp.bolt.*;
import com.github.dwladdimiroc.serverlessApp.bolt.complex.BoltE;
import com.github.dwladdimiroc.serverlessApp.bolt.complex.BoltF;
import com.github.dwladdimiroc.serverlessApp.bolt.complex.BoltG;
import com.github.dwladdimiroc.serverlessApp.bolt.complex.BoltH;
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

public class ComplexTopology implements Serializable {
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
        // Spout -> BoltA -> BoltB
        builder.setBolt("BoltA", new BoltA(), numParallelism).setNumTasks(numParallelism).
                customGrouping("Spout", "BoltA", new PoolGrouping());
        // BoltA -> BoltB -> BoltC || BoltF
        builder.setBolt("BoltB", new BoltB(), numParallelism).setNumTasks(numParallelism).
                customGrouping("BoltA", "BoltB", new PoolGrouping());
        // BoltB -> BoltC -> BoltD
        builder.setBolt("BoltC", new BoltC(), numParallelism).setNumTasks(numParallelism).
                customGrouping("BoltB", "BoltC", new PoolGrouping());
        // BoltC -> BoltD -> BoltE
        builder.setBolt("BoltD", new BoltD(), numParallelism).setNumTasks(numParallelism)
                .customGrouping("BoltC", "BoltD", new PoolGrouping());
        // BoltD || BoltG -> BoltE
        builder.setBolt("BoltE", new BoltE(), numParallelism).setNumTasks(numParallelism)
                .customGrouping("BoltD", "BoltE", new PoolGrouping())
                .customGrouping("BoltG", "BoltE", new PoolGrouping());
        // BoltB -> BoltF -> BoltG || BoltH
        builder.setBolt("BoltF", new BoltF(), numParallelism).setNumTasks(numParallelism)
                .customGrouping("BoltB", "BoltF", new PoolGrouping());
        // BoltF || BoltH -> BoltG -> BoltE
        builder.setBolt("BoltG", new BoltG(), numParallelism).setNumTasks(numParallelism)
                .customGrouping("BoltF", "BoltG", new PoolGrouping())
                .customGrouping("BoltH", "BoltG", new PoolGrouping());
        // BoltF -> BoltH -> BoltG
        builder.setBolt("BoltH", new BoltH(), numParallelism).setNumTasks(numParallelism)
                .customGrouping("BoltF", "BoltH", new PoolGrouping());

        builder.setBolt("Latency", new Metrics(), 1).setNumTasks(1).
                shuffleGrouping("BoltE", "Latency");

        try {
            StormSubmitter.submitTopology(TOPOLOGY_NAME, config, builder.createTopology());
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}