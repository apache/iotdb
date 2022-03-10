package org.apache.iotdb.cluster.query.distribution.plan.process;

import org.apache.iotdb.cluster.query.distribution.common.TsBlock;
import org.apache.iotdb.cluster.query.distribution.common.WithoutPolicy;
import org.apache.iotdb.cluster.query.distribution.plan.PlanNode;

/**
 * WithoutOperator is used to discard specific result from upstream operators.
 *
 * Children type: [All the operators whose result set is Tablet]
 */
public class WithoutNode extends ProcessNode<TsBlock> {

    // The policy to discard the result from upstream operator
    private WithoutPolicy discardPolicy;
}
