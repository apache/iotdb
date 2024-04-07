package org.apache.iotdb.db.queryengine.plan.relational.planner.node;

import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.SingleChildProcessNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.Symbol;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

public class OutputNode extends SingleChildProcessNode {

  private final List<String> outputColumnNames;

  private final List<Symbol> symbols;

  public OutputNode(
      PlanNodeId id, PlanNode child, List<String> outputColumnNames, List<Symbol> symbols) {
    super(id, child);
    this.id = id;
    this.child = child;
    this.outputColumnNames = outputColumnNames;
    this.symbols = symbols;
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
