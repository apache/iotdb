package org.apache.iotdb.cluster.query.distribution.plan.process;

import org.apache.iotdb.cluster.query.distribution.common.GroupByTimeParameter;
import org.apache.iotdb.cluster.query.distribution.plan.PlanNodeId;
import org.apache.iotdb.db.query.expression.unary.FunctionExpression;

import java.util.List;

/**
 * This node is used to aggregate required series by raw data.
 * The raw data will be input as a TsBlock. This node will output the series aggregated result represented by TsBlock
 * Thus, the columns in output TsBlock will be different from input TsBlock.
 */
public class RowBasedSeriesAggregateNode extends ProcessNode {
    // The parameter of `group by time`
    // Its value will be null if there is no `group by time` clause,
    private GroupByTimeParameter groupByTimeParameter;

    // The list of aggregation functions, each FunctionExpression will be output as one column of result TsBlock
    // (Currently we only support one series in the aggregation function)
    // TODO: need consider whether it is suitable the aggregation function using FunctionExpression
    private List<FunctionExpression> aggregateFuncList;

    public RowBasedSeriesAggregateNode(PlanNodeId id) {
        super(id);
    }

    public RowBasedSeriesAggregateNode(PlanNodeId id, List<FunctionExpression> aggregateFuncList) {
        this(id);
        this.aggregateFuncList = aggregateFuncList;
    }

    public RowBasedSeriesAggregateNode(PlanNodeId id, List<FunctionExpression> aggregateFuncList, GroupByTimeParameter groupByTimeParameter) {
        this(id, aggregateFuncList);
        this.groupByTimeParameter = groupByTimeParameter;
    }
}
