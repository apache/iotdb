package org.apache.iotdb.cluster.query.distribution.plan.process;

import org.apache.iotdb.cluster.query.distribution.common.FillPolicy;
import org.apache.iotdb.cluster.query.distribution.common.TsBlock;
import org.apache.iotdb.cluster.query.distribution.plan.PlanNode;

/**
 * FillOperator is used to fill the empty field in one row.
 *
 * Children type: [All the operators whose result set is Tablet]
 */
public class FillNode extends ProcessNode<TsBlock> {

    // The policy to discard the result from upstream operator
    private FillPolicy fillPolicy;
}
