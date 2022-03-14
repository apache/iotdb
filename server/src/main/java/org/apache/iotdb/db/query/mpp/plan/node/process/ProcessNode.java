package org.apache.iotdb.db.query.mpp.plan.node.process;

import org.apache.iotdb.db.query.mpp.common.TsBlock;
import org.apache.iotdb.db.query.mpp.plan.node.PlanNode;
import org.apache.iotdb.db.query.mpp.plan.node.PlanNodeId;

public class ProcessNode extends PlanNode<TsBlock> {
    public ProcessNode(PlanNodeId id) {
        super(id);
    }
}
