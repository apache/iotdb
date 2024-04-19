package org.apache.iotdb.db.queryengine.plan.relational.planner.node;

import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanVisitor;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.SingleChildProcessNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.Symbol;

import com.google.common.collect.ImmutableList;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

public class OutputNode extends SingleChildProcessNode {

  private final List<String> columnNames;

  // column name = symbol
  private final List<Symbol> outputs;

  public OutputNode(PlanNodeId id, PlanNode child, List<String> columnNames, List<Symbol> outputs) {
    super(id, child);
    this.id = id;
    this.child = child;
    this.columnNames = ImmutableList.copyOf(columnNames);
    this.outputs = ImmutableList.copyOf(outputs);
  }

  @Override
  public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
    return visitor.visitOutput(this, context);
  }

  @Override
  public PlanNode clone() {
    return new OutputNode(id, child, columnNames, outputs);
  }

  @Override
  public List<String> getOutputColumnNames() {
    return this.columnNames;
  }

  @Override
  protected void serializeAttributes(ByteBuffer byteBuffer) {}

  @Override
  protected void serializeAttributes(DataOutputStream stream) throws IOException {}

  public List<String> getColumnNames() {
    return this.columnNames;
  }

  public List<Symbol> getOutputSymbols() {
    return outputs;
  }
}
