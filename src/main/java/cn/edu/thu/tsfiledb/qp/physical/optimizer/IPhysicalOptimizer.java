package cn.edu.thu.tsfiledb.qp.physical.optimizer;

import cn.edu.thu.tsfiledb.qp.executor.QueryProcessExecutor;
import cn.edu.thu.tsfiledb.qp.physical.plan.PhysicalPlan;

/**
 * this class is used for optimizing physical plan.
 * 
 * @author kangrong
 *
 */
public interface IPhysicalOptimizer {
    PhysicalPlan transform(PhysicalPlan physicalPlan, QueryProcessExecutor executor);
}
