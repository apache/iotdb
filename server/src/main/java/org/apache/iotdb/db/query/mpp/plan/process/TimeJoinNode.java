package org.apache.iotdb.db.query.mpp.plan.process;

import org.apache.iotdb.db.query.mpp.common.OrderBy;
import org.apache.iotdb.db.query.mpp.common.TsBlock;
import org.apache.iotdb.db.query.mpp.common.WithoutPolicy;
import org.apache.iotdb.db.query.mpp.plan.PlanNode;
import org.apache.iotdb.db.query.mpp.plan.PlanNodeId;

import java.util.Arrays;

/**
 * TimeJoinOperator is responsible for join two or more TsBlock.
 * The join algorithm is like outer join by timestamp column. It will join two or more TsBlock by Timestamp column.
 * The output result of TimeJoinOperator is sorted by timestamp
 */
//TODO: define the TimeJoinMergeNode for distributed plan
public class TimeJoinNode extends ProcessNode {

    // This parameter indicates the order when executing multiway merge sort.
    private OrderBy mergeOrder;

    // The policy to decide whether a row should be discarded
    // The without policy is able to be push down to the TimeJoinOperator because we can know whether a row contains
    // null or not.
    private WithoutPolicy withoutPolicy;

    public TimeJoinNode(PlanNodeId id) {
        super(id);
        this.mergeOrder = OrderBy.TIMESTAMP_ASC;
    }

    public TimeJoinNode(PlanNodeId id, PlanNode<TsBlock>... children) {
        super(id);
        this.children.addAll(Arrays.asList(children));
    }

    public void addChild(PlanNode<TsBlock> child) {
        this.children.add(child);
    }

    public void setMergeOrder(OrderBy mergeOrder) {
        this.mergeOrder = mergeOrder;
    }

    public void setWithoutPolicy(WithoutPolicy withoutPolicy) {
        this.withoutPolicy = withoutPolicy;
    }
}
