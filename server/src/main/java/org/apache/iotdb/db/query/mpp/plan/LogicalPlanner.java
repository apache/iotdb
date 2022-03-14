package org.apache.iotdb.db.query.mpp.plan;

import org.apache.iotdb.db.query.mpp.common.Analysis;
import org.apache.iotdb.db.query.mpp.common.QueryContext;
import org.apache.iotdb.db.query.mpp.plan.optimzation.PlanOptimizer;

import java.util.List;

public class LogicalPlanner {
    private Analysis analysis;
    private QueryContext context;
    private List<PlanOptimizer> optimizers;

    public LogicalPlanner(Analysis analysis, QueryContext context, List<PlanOptimizer> optimizers) {
        this.analysis = analysis;
        this.context = context;
        this.optimizers = optimizers;
    }

    public LogicalQueryPlan plan() {
        return null;
    }
}
