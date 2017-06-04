package cn.edu.thu.tsfiledb.qp.physical.optimizer;

import cn.edu.thu.tsfiledb.qp.exec.QueryProcessExecutor;
import cn.edu.thu.tsfiledb.qp.physical.plan.PhysicalPlan;



/**
 * This class do nothing for an input TSPlanContext.
 * 
 * @author kangrong
 *
 */
public class NonePhycicalOptimizer implements IPhysicalOptimizer {
    
    @Override
    public PhysicalPlan transform(PhysicalPlan context, QueryProcessExecutor conf) {
        return context;
    }
}
