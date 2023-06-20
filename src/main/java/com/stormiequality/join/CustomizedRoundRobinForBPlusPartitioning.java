package com.stormiequality.join;

import org.apache.storm.generated.GlobalStreamId;
import org.apache.storm.grouping.CustomStreamGrouping;
import org.apache.storm.task.WorkerTopologyContext;

import java.util.ArrayList;
import java.util.List;

public class CustomizedRoundRobinForBPlusPartitioning implements CustomStreamGrouping {
    private List<Integer> targetTasks;
    private int index;
    private int counter;
    @Override
    public void prepare(WorkerTopologyContext workerTopologyContext, GlobalStreamId globalStreamId, List<Integer> list) {
        this.targetTasks = new ArrayList<>(list);
        this.index = 0;
        this.counter=0;
    }

    @Override
    public List<Integer> chooseTasks(int i, List<Object> list) {
      //  System.out.println("Sending: " + list);
        List<Integer> tasks = new ArrayList<>();
        tasks.add(targetTasks.get(index));
        /////Op
        this.counter++;
        if(counter==100000) {
            index = (index + 1) % targetTasks.size();
            counter=0;
        }
        return tasks;
    }
}
