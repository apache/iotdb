package org.apache.iotdb.cluster.query.distribution.plan.process;

import org.apache.iotdb.cluster.query.distribution.common.TsBlock;
import org.apache.iotdb.cluster.query.distribution.plan.PlanNode;
import org.apache.iotdb.cluster.query.distribution.plan.PlanNodeId;

public class ProcessNode extends PlanNode<TsBlock> {
    public ProcessNode(PlanNodeId id) {
        super(id);
    }
}
