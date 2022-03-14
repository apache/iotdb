package org.apache.iotdb.db.query.mpp.exec;

import org.apache.iotdb.db.query.mpp.common.Analysis;
import org.apache.iotdb.db.query.mpp.common.QueryContext;
import org.apache.iotdb.db.query.mpp.common.QueryId;
import org.apache.iotdb.db.query.mpp.plan.*;
import org.apache.iotdb.db.query.mpp.plan.optimzation.PlanOptimizer;

import java.nio.ByteBuffer;
import java.util.List;

/**
 * QueryExecution stores all the status of a query which is being prepared or running inside the MPP frame.
 * It takes three main responsibilities:
 *      1. Prepare a query. Transform a query from statement to DistributedQueryPlan with fragment instances.
 *      2. Dispatch all the fragment instances to corresponding physical nodes.
 *      3. Collect and monitor the progress/states of this query.
 */
public class QueryExecution {
    private QueryContext context;
    private QueryScheduler scheduler;
    private QueryStateMachine stateMachine;

    private List<PlanOptimizer> planOptimizers;

    private Analysis analysis;
    private LogicalQueryPlan logicalPlan;
    private DistributedQueryPlan distributedPlan;
    private List<PlanFragment> fragments;
    private List<FragmentInstance> fragmentInstances;

    public QueryExecution(QueryContext context) {
        this.context = context;
    }

    public void plan() {
        analyze();
        doLogicalPlan();
        doDistributedPlan();
        planFragmentInstances();
    }

    public void schedule() {
        this.scheduler = new QueryScheduler(this.stateMachine, this.fragmentInstances);
        this.scheduler.start();
    }

    // Analyze the statement in QueryContext. Generate the analysis this query need
    public void analyze() {
        // initialize the variable `analysis`

    }

    // Use LogicalPlanner to do the logical query plan and logical optimization
    public void doLogicalPlan() {
        LogicalPlanner planner = new LogicalPlanner(this.analysis, this.context, this.planOptimizers);
        this.logicalPlan = planner.plan();
    }

    // Generate the distributed plan and split it into fragments
    public void doDistributedPlan() {
        DistributionPlanner planner = new DistributionPlanner(this.analysis, this.logicalPlan);
        this.distributedPlan = planner.planFragments();

    }

    // Convert fragment to detailed instance
    // And for parallel-able fragment, clone it into several instances with different params.
    public void planFragmentInstances() {

    }

    /**
     * This method will be called by the request thread from client connection.
     * This method will block until one of these conditions occurs:
     *   1. There is a batch of result
     *   2. There is no more result
     *   3. The query has been cancelled
     *   4. The query is timeout
     * This method will fetch the result from DataStreamManager use the virtual ResultOperator's ID
     * (This part will be designed and implemented with DataStreamManager)
     */
    public ByteBuffer getBatchResult() {
        return null;
    }
}
