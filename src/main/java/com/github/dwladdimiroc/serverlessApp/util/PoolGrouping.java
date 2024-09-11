package com.github.dwladdimiroc.serverlessApp.util;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.storm.generated.GlobalStreamId;
import org.apache.storm.grouping.CustomStreamGrouping;
import org.apache.storm.task.WorkerTopologyContext;


public class PoolGrouping implements CustomStreamGrouping, Serializable {
    private ArrayList<List<Integer>> choices;
    private AtomicInteger current;

    private Replicas replica;

    @Override
    public void prepare(WorkerTopologyContext context, GlobalStreamId stream, List<Integer> targetTasks) {
        this.replica = new Replicas(stream.get_streamId());
        Thread tReplica = new Thread(replica);
        tReplica.start();

        choices = new ArrayList<List<Integer>>(targetTasks.size());
        for (Integer i : targetTasks) {
            choices.add(Arrays.asList(i));
        }
        current = new AtomicInteger(0);
        Collections.shuffle(choices, new Random());
    }

    @Override
    public List<Integer> chooseTasks(int taskId, List<Object> values) {
        int rightNow;
        int size = this.replica.getNumReplicas();
        while (true) {
            rightNow = current.incrementAndGet();
            if (rightNow < size) {
                return choices.get(rightNow);
            } else if (rightNow == size) {
                current.set(0);
                return choices.get(0);
            } else {
                current.set(0);
                return choices.get(new Random().nextInt(size));
            }
        }
    }
}