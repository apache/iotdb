package org.apache.iotdb.db.mpp.sql.planner.plan.node.metedata.read;

import org.apache.iotdb.db.metadata.path.PartialPath;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.PlanNode;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.PlanNodeId;

import java.util.List;

public class ShowTimeSeriesNode extends ShowNode {

  private String key;
  private String value;
  private boolean isContains;

  // if is true, the result will be sorted according to the inserting frequency of the timeseries
  private boolean orderByHeat;

  public ShowTimeSeriesNode(
      PlanNodeId id,
      PartialPath partialPath,
      String key,
      String value,
      int limit,
      int offset,
      boolean orderByHeat,
      boolean isContains) {
    super(id);
    super.setLimit(limit);
    this.path = partialPath;
    this.key = key;
    this.value = value;
    this.offset = offset;
    this.orderByHeat = orderByHeat;
    this.isContains = isContains;
  }

  public String getKey() {
    return key;
  }

  public void setKey(String key) {
    this.key = key;
  }

  public String getValue() {
    return value;
  }

  public void setValue(String value) {
    this.value = value;
  }

  public boolean isContains() {
    return isContains;
  }

  public void setContains(boolean contains) {
    isContains = contains;
  }

  public boolean isOrderByHeat() {
    return orderByHeat;
  }

  public void setOrderByHeat(boolean orderByHeat) {
    this.orderByHeat = orderByHeat;
  }

  @Override
  public List<PlanNode> getChildren() {
    return null;
  }

  @Override
  public void addChildren(PlanNode child) {}

  @Override
  public PlanNode clone() {
    return null;
  }

  @Override
  public PlanNode cloneWithChildren(List<PlanNode> children) {
    return null;
  }

  @Override
  public List<String> getOutputColumnNames() {
    return null;
  }
}
