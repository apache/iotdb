package cn.edu.thu.tsfiledb.qp.strategy.optimizer.physical;

import cn.edu.thu.tsfiledb.qp.physical.PhysicalPlan;

/**
 * this class is used for optimizing physical plan.
 * 
 * @author kangrong
 *
 */
public interface IPhysicalOptimizer {
    PhysicalPlan transform(PhysicalPlan physicalPlan);
}
