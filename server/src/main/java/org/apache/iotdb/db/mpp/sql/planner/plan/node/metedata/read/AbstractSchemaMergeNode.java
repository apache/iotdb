package org.apache.iotdb.db.mpp.sql.planner.plan.node.metedata.read;

import org.apache.iotdb.db.mpp.sql.planner.plan.node.PlanNode;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.process.ProcessNode;

import java.util.ArrayList;
import java.util.List;

public abstract class AbstractSchemaMergeNode extends ProcessNode {

  private List<PlanNode> children;

  public AbstractSchemaMergeNode(PlanNodeId id) {
    super(id);
    children = new ArrayList<>();
  }

  @Override
  public List<PlanNode> getChildren() {
    return children;
  }

  @Override
  public void addChild(PlanNode child) {
    children.add(child);
  }

  @Override
  public int allowedChildCount() {
    return CHILD_COUNT_NO_LIMIT;
  }
}
