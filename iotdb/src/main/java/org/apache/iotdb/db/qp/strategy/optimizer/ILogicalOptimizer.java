package org.apache.iotdb.db.qp.strategy.optimizer;

import org.apache.iotdb.db.exception.qp.LogicalOptimizeException;
import org.apache.iotdb.db.qp.logical.Operator;
import org.apache.iotdb.db.exception.qp.LogicalOptimizeException;
import org.apache.iotdb.db.qp.logical.Operator;

/**
 * provide a context, transform it for optimization.
 */
public interface ILogicalOptimizer {

    Operator transform(Operator operator) throws LogicalOptimizeException;
}
