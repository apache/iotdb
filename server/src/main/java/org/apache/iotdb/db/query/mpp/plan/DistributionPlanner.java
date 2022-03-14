package org.apache.iotdb.db.query.mpp.plan;

import org.apache.iotdb.db.query.mpp.common.Analysis;

public class DistributionPlanner {
    private Analysis analysis;
    private LogicalQueryPlan logicalPlan;

    public DistributionPlanner(Analysis analysis, LogicalQueryPlan logicalPlan) {
        this.analysis = analysis;
        this.logicalPlan = logicalPlan;
    }

    public DistributedQueryPlan planFragments() {
        return null;
    }
}
