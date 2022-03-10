package org.apache.iotdb.cluster.query.distribution.plan.process;

import org.apache.iotdb.cluster.query.distribution.common.FillPolicy;
import org.apache.iotdb.cluster.query.distribution.common.TsBlock;
import org.apache.iotdb.cluster.query.distribution.plan.PlanNode;
import org.apache.iotdb.cluster.query.distribution.plan.PlanNodeId;

/**
 * FillNode is used to fill the empty field in one row.
 *
 */
public class FillNode extends ProcessNode {

    // The policy to discard the result from upstream node
    private FillPolicy fillPolicy;

    public FillNode(PlanNodeId id) {
        super(id);
    }

    public FillNode(PlanNodeId id, FillPolicy fillPolicy) {
        this(id);
        this.fillPolicy = fillPolicy;
    }
}
