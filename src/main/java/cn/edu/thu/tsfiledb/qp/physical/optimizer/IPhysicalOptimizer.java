package cn.edu.thu.tsfiledb.qp.physical.optimizer;

import cn.edu.thu.tsfiledb.qp.exec.QueryProcessExecutor;
import cn.edu.thu.tsfiledb.qp.physical.plan.PhysicalPlan;


/**
 * this class is used for optimizing operator tree in physical layer.
 * 
 * @author kangrong
 *
 */
public interface IPhysicalOptimizer {
    public PhysicalPlan transform(PhysicalPlan context, QueryProcessExecutor conf);
}
