package org.apache.iotdb.db.queryengine.plan.relational.planner.node;

import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.SingleChildProcessNode;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

public class OffsetNode extends SingleChildProcessNode {
  private final long count;

  public OffsetNode(PlanNodeId id, PlanNode child, long count) {
    super(id, child);
    this.count = count;
  }

  @Override
  public PlanNode clone() {
    return null;
  }

  @Override
  public List<String> getOutputColumnNames() {
    return null;
  }

  @Override
  protected void serializeAttributes(ByteBuffer byteBuffer) {}

  @Override
  protected void serializeAttributes(DataOutputStream stream) throws IOException {}
}
