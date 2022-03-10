package org.apache.iotdb.cluster.query.distribution.plan.process;

import org.apache.iotdb.cluster.query.distribution.common.TsBlock;
import org.apache.iotdb.cluster.query.distribution.common.TraversalOrder;
import org.apache.iotdb.cluster.query.distribution.common.WithoutPolicy;
import org.apache.iotdb.cluster.query.distribution.plan.PlanNode;

/**
 * TimeJoinOperator is responsible for join two or more series.
 * The join algorithm is like outer join by timestamp column.
 * The output result of TimeJoinOperator is sorted by timestamp
 *
 * Children type: [SeriesScanOperator]
 */
public class TimeJoinNode extends ProcessNode<TsBlock> {

    // This parameter indicates the order when executing multiway merge sort.
    private TraversalOrder mergeOrder;

    // The policy to decide whether a row should be discarded
    // The without policy is able to be push down to the TimeJoinOperator because we can know whether a row contains
    // null or not in this operator the situation won't be changed by the downstream operators.
    private WithoutPolicy withoutPolicy;
}
