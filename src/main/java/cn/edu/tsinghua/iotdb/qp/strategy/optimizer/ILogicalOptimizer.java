package cn.edu.tsinghua.iotdb.qp.strategy.optimizer;

import cn.edu.tsinghua.iotdb.qp.exception.LogicalOptimizeException;
import cn.edu.tsinghua.iotdb.qp.logical.Operator;

/**
 * provide a context, transform it for optimization.
 * 
 * @author kangrong
 *
 */
public interface ILogicalOptimizer {

    Operator transform(Operator operator) throws LogicalOptimizeException;
}
