package org.apache.iotdb.db.query.mpp.plan.node.sink;

import org.apache.iotdb.db.query.mpp.plan.node.PlanNodeId;

public class FragmentSinkNode extends SinkNode {
    public FragmentSinkNode(PlanNodeId id) {
        super(id);
    }

    @Override
    public void send() {

    }

    @Override
    public void close() throws Exception {

    }
}
