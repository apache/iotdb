package cn.edu.thu.tsfiledb.qp.strategy.optimizer.physical;

import cn.edu.thu.tsfiledb.qp.executor.QueryProcessExecutor;
import cn.edu.thu.tsfiledb.qp.physical.PhysicalPlan;

/**
 * This class do nothing for an input TSPlanContext.
 * 
 * @author kangrong
 *
 */
public class NonePhysicalOptimizer implements IPhysicalOptimizer {

    private QueryProcessExecutor executor;

    public NonePhysicalOptimizer(QueryProcessExecutor executor) {
        this.executor = executor;
    }
    
    @Override
    public PhysicalPlan transform(PhysicalPlan physicalPlan) {
        return physicalPlan;
    }
}
