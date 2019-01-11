package cn.edu.tsinghua.iotdb.qp.strategy.optimizer;

import cn.edu.tsinghua.iotdb.exception.qp.LogicalOptimizeException;
import cn.edu.tsinghua.iotdb.qp.logical.Operator;

/**
 * provide a context, transform it for optimization.
 */
public interface ILogicalOptimizer {

    Operator transform(Operator operator) throws LogicalOptimizeException;
}
