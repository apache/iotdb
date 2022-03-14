package org.apache.iotdb.db.query.mpp.exec;

import org.apache.iotdb.db.query.mpp.common.Analysis;
import org.apache.iotdb.db.query.mpp.common.QueryContext;
import org.apache.iotdb.db.query.mpp.common.QueryId;
import org.apache.iotdb.db.query.mpp.plan.LogicalPlanner;
import org.apache.iotdb.db.query.mpp.plan.optimzation.PlanOptimizer;

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

    public QueryExecution(QueryContext context) {
        this.context = context;
    }

    // Analyze the statement in QueryContext. Generate the analysis this query need
    public void analyze() {
        // initialize the variable `analysis`
    }

    // Use LogicalPlanner to do the logical query plan and logical optimization
    public void doLogicalPlan() {

    }

    // Generate the distributed plan and split it into fragments
    public void doDistributedPlan() {

    }

    // Convert fragment to detailed instance
    // And for parallel-able fragment, clone it into several instances with different params.
    public void planFragmentInstances() {

    }
}
