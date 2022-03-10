package org.apache.iotdb.cluster.query.distribution.plan;

import org.apache.iotdb.cluster.query.distribution.common.TreeNode;

/**
 * @author xingtanzjr
 * The base class of query executable operators, which is used to compose logical query plan.
 * TODO: consider how to restrict the children type for each type of ExecOperator
 */
public abstract class PlanNode<T> extends TreeNode<PlanNode<?>> {

}
