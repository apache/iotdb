package cn.edu.thu.tsfiledb.qp.logical.operator;

import cn.edu.thu.tsfiledb.qp.exception.QueryProcessorException;
import cn.edu.thu.tsfiledb.qp.exec.QueryProcessExecutor;
import cn.edu.thu.tsfiledb.qp.logical.operator.crud.SFWOperator;
import cn.edu.thu.tsfiledb.qp.physical.plan.PhysicalPlan;

/**
 * RootOperator indicates the operator that could be executed as a entire command. RootOperator
 * consists of SFWOperator, like INSERT/UPDATE/DELETE, and AuthorOperator.
 *
 * @author kangrong
 */
public abstract class RootOperator extends Operator {


    public RootOperator(int tokenIntType) {
        super(tokenIntType);
    }

    /**
     * transform this root operator tree to a physical plan tree.Node that, before this method
     * called, the where filter has been dealt with
     * {@linkplain cn.edu.thu.tsfiledb.qp.logical.optimizer.filter.MergeSingleFilterOptimizer}
     */
    public abstract PhysicalPlan transformToPhysicalPlan(QueryProcessExecutor conf)
            throws QueryProcessorException;
    
}
