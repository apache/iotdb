package org.apache.iotdb.cluster.query.distribution.plan.sink;


import org.apache.iotdb.cluster.query.distribution.plan.PlanNodeId;

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
