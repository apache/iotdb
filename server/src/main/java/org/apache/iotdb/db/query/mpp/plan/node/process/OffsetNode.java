package org.apache.iotdb.db.query.mpp.plan.node.process;

import org.apache.iotdb.db.query.mpp.plan.node.PlanNodeId;

/**
 * OffsetNode is used to skip top n result from upstream nodes. It uses the default order of upstream nodes
 *
 */
public class OffsetNode extends ProcessNode {

    // The limit count
    private int offset;

    public OffsetNode(PlanNodeId id) {
        super(id);
    }

    public OffsetNode(PlanNodeId id, int offset) {
        this(id);
        this.offset = offset;
    }
}
