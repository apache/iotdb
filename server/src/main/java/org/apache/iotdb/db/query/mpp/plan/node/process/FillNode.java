package org.apache.iotdb.db.query.mpp.plan.node.process;

import org.apache.iotdb.db.query.mpp.common.FillPolicy;
import org.apache.iotdb.db.query.mpp.plan.node.PlanNodeId;

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
