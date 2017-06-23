package cn.edu.thu.tsfiledb.qp.strategy.optimizer;

import cn.edu.thu.tsfiledb.qp.exception.LogicalOptimizeException;
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
