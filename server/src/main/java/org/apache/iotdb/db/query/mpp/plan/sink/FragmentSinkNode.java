package org.apache.iotdb.db.query.mpp.plan.sink;

import org.apache.iotdb.db.query.mpp.plan.PlanNodeId;

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
