package com.github.dwladdimiroc.serverlessApp.util;

import org.apache.storm.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Replicas implements Runnable {
    private static final Logger logger = LoggerFactory.getLogger(Replicas.class);

    private final String name;
    private int numReplicas;

    public Replicas(String name) {
        logger.info("Thread PoolGrouping {}", name);
        this.name = name;
        this.numReplicas = 0;
    }

    public String getName(){return this.name;}

    public int getNumReplicas() {
        return this.numReplicas;
    }

    @Override
    public void run() {
        Redis redis = new Redis();
        while (true) {
            this.numReplicas = redis.getReplicas(this.name);
            Utils.sleep(1000);
        }
    }
}
