package cn.edu.thu.tsfiledb.qp.physical.optimizer;

import cn.edu.thu.tsfiledb.qp.executor.QueryProcessExecutor;
import cn.edu.thu.tsfiledb.qp.physical.plan.PhysicalPlan;



/**
 * This class do nothing for an input TSPlanContext.
 * 
 * @author kangrong
 *
 */
public class NonePhysicalOptimizer implements IPhysicalOptimizer {
    
    @Override
    public PhysicalPlan transform(PhysicalPlan physicalPlan, QueryProcessExecutor executor) {
        return physicalPlan;
    }
}
