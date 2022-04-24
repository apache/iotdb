package org.apache.iotdb.db.mpp.sql.planner.plan.node.metedata.read;

import org.apache.iotdb.db.mpp.common.header.ColumnHeader;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.PlanNode;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.process.ProcessNode;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

import java.util.ArrayList;
import java.util.Collections;
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

  @Override
  public List<ColumnHeader> getOutputColumnHeaders() {
    if (children.size() > 0) {
      return children.get(0).getOutputColumnHeaders();
    }
    return Collections.emptyList();
  }

  @Override
  public List<String> getOutputColumnNames() {
    if (children.size() > 0) {
      return children.get(0).getOutputColumnNames();
    }
    return Collections.emptyList();
  }

  @Override
  public List<TSDataType> getOutputColumnTypes() {
    if (children.size() > 0) {
      return children.get(0).getOutputColumnTypes();
    }
    return Collections.emptyList();
  }
}
