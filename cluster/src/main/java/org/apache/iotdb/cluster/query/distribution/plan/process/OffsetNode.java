package org.apache.iotdb.cluster.query.distribution.plan.process;

import org.apache.iotdb.cluster.query.distribution.common.TsBlock;
import org.apache.iotdb.cluster.query.distribution.plan.PlanNode;

/**
 * OffsetOperator is used to skip top n result from upstream operators. It uses the default order of upstream operators
 *
 * Children type: [All the operators whose result set is Tablet]
 */
public class OffsetNode extends ProcessNode<TsBlock> {

    // The limit count
    private int offset;
}
