package org.apache.iotdb.db.mpp.sql.planner.plan.node.metedata.read;

import org.apache.iotdb.db.mpp.sql.planner.plan.node.PlanNode;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.PlanNodeIdAllocator;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.PlanVisitor;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.process.ProcessNode;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class MetaMergeNode extends ProcessNode {

  private boolean orderByHeat;

  private List<PlanNode> children;

  public MetaMergeNode(PlanNodeId id) {
    super(id);
    children = new ArrayList<>();
  }

  public MetaMergeNode(PlanNodeId id, boolean orderByHeat) {
    this(id);
    this.orderByHeat = orderByHeat;
  }

  @Override
  public List<PlanNode> getChildren() {
    return children;
  }

  @Override
  public void addChild(PlanNode child) {
    this.children.add(child);
  }

  @Override
  public PlanNode clone() {
    return new MetaMergeNode(PlanNodeIdAllocator.generateId(), this.orderByHeat);
  }

  @Override
  public int allowedChildCount() {
    return CHILD_COUNT_NO_LIMIT;
  }

  @Override
  public List<String> getOutputColumnNames() {
    return null;
  }

  @Override
  public void serialize(ByteBuffer byteBuffer) {}

  @Override
  public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
    return visitor.visitMetaMerge(this, context);
  }
}
