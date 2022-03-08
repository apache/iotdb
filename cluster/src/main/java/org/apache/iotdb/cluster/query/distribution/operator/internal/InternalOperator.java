package org.apache.iotdb.cluster.query.distribution.operator;

import org.apache.iotdb.cluster.query.distribution.common.TreeNode;

/**
 * @author xingtanzjr The base class of query executable operators, which is used to compose logical
 *     query plan. TODO: consider how to restrict the children type for each type of ExecOperator
 */
public abstract class InternalOperator<T> extends TreeNode<InternalOperator<?>> {

  // Judge whether current operator has more result
  public abstract boolean hasNext();

  // Get next result batch of this operator
  // Return null if there is no more result to return
  public abstract T getNextBatch();
}
