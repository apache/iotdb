package org.apache.iotdb.db.queryengine.plan.relational.planner.node;

import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.SingleChildProcessNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.OrderingScheme;
import org.apache.iotdb.db.queryengine.plan.relational.planner.Symbol;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

public class SortNode extends SingleChildProcessNode {
  private final OrderingScheme orderingScheme;
  private final boolean partial;

  public SortNode(PlanNodeId id, PlanNode child, OrderingScheme scheme, boolean partial) {
    super(id, child);
    this.orderingScheme = scheme;
    this.partial = partial;
  }

  @Override
  public PlanNode clone() {
    return new SortNode(id, child, orderingScheme, partial);
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
    return child.getOutputSymbols();
  }
}
