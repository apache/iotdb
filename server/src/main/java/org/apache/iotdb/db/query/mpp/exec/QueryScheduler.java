package org.apache.iotdb.db.query.mpp.exec;

import org.apache.iotdb.db.query.mpp.plan.FragmentInstance;

import java.util.List;

/**
 * QueryScheduler is used to dispatch the fragment instances of a query to target nodes. And it will continue to
 * collect and monitor the query execution before the query is finished.
 *
 * Later, we can add more control logic for a QueryExecution such as retry, kill and so on by this scheduler.
 */
public class QueryScheduler {
    //The stateMachine of the QueryExecution owned by this QueryScheduler
    private QueryStateMachine stateMachine;

    // The fragment instances which should be sent to corresponding Nodes.
    private List<FragmentInstance> instances;

    public QueryScheduler(QueryStateMachine stateMachine, List<FragmentInstance> instances) {
        this.stateMachine = stateMachine;
        this.instances = instances;
    }

    public void start() {

    }

    // Send the instances to other nodes
    private void sendFragmentInstances() {

    }

    // After sending, start to collect the states of these fragment instances
    private void startMonitorInstances() {

    }
}
