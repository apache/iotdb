package cn.edu.thu.tsfiledb.qp.strategy.optimizer.logical;

import cn.edu.thu.tsfiledb.qp.exception.logical.optimize.LogicalOptimizeException;
import cn.edu.thu.tsfiledb.qp.logical.Operator;


/**
 * provide a context, transform it for optimization.
 * 
 * @author kangrong
 *
 */
public interface ILogicalOptimizer {

    Operator transform(Operator operator) throws LogicalOptimizeException;
}
