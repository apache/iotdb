package org.apache.iotdb.db.query.mpp.plan.process;

import org.apache.iotdb.db.query.mpp.common.TsBlock;
import org.apache.iotdb.db.query.mpp.plan.PlanNode;
import org.apache.iotdb.db.query.mpp.plan.PlanNodeId;

public class ProcessNode extends PlanNode<TsBlock> {
    public ProcessNode(PlanNodeId id) {
        super(id);
    }
}
