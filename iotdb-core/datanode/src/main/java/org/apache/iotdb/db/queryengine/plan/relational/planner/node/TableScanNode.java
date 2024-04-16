package org.apache.iotdb.db.queryengine.plan.relational.planner.node;

import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanVisitor;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.ColumnSchema;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.DeviceEntry;
import org.apache.iotdb.db.queryengine.plan.relational.planner.Symbol;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;

public class TableScanNode extends PlanNode {

  // db.tablename
  private final String qualifiedTableName;
  private final List<Symbol> outputSymbols;
  private final Map<Symbol, ColumnSchema> assignments;

  private List<DeviceEntry> deviceEntries;
  private Map<Symbol, Integer> attributesMap;

  public TableScanNode(
      PlanNodeId id,
      String qualifiedTableName,
      List<Symbol> outputSymbols,
      Map<Symbol, ColumnSchema> assignments) {
    super(id);
    this.qualifiedTableName = qualifiedTableName;
    this.outputSymbols = outputSymbols;
    this.assignments = assignments;
  }

  @Override
  public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
    return visitor.visitTableScan(this, context);
  }

  @Override
  public List<PlanNode> getChildren() {
    return null;
  }

  @Override
  public void addChild(PlanNode child) {}

  @Override
  public PlanNode clone() {
    return null;
  }

  @Override
  public int allowedChildCount() {
    return 0;
  }

  @Override
  public List<String> getOutputColumnNames() {
    return null;
  }

  @Override
  protected void serializeAttributes(ByteBuffer byteBuffer) {}

  @Override
  protected void serializeAttributes(DataOutputStream stream) throws IOException {}

  @Override
  public List<Symbol> getOutputSymbols() {
    return outputSymbols;
  }
}
