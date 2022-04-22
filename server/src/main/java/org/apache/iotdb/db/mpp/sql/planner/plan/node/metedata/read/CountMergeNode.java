package org.apache.iotdb.db.mpp.sql.planner.plan.node.metedata.read;

import org.apache.iotdb.db.mpp.sql.planner.plan.node.PlanNode;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.PlanNodeType;

import java.nio.ByteBuffer;

public class CountMergeNode extends AbstractSchemaMergeNode {

  public CountMergeNode(PlanNodeId id) {
    super(id);
  }

  @Override
  public PlanNode clone() {
    return new CountMergeNode(getPlanNodeId());
  }

  @Override
  protected void serializeAttributes(ByteBuffer byteBuffer) {
    PlanNodeType.COUNT_MERGE.serialize(byteBuffer);
  }

  public static PlanNode deserialize(ByteBuffer byteBuffer) {
    PlanNodeId planNodeId = PlanNodeId.deserialize(byteBuffer);
    return new CountMergeNode(planNodeId);
  }
}
