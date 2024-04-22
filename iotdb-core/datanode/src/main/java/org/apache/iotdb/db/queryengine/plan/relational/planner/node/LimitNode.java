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
import java.util.Optional;

public class LimitNode extends SingleChildProcessNode {
  private final long count;
  // TODO what's the meaning?
  private final Optional<OrderingScheme> tiesResolvingScheme;
  // private final boolean partial;
  // private final List<Symbol> preSortedInputs;

  public LimitNode(
      PlanNodeId id, PlanNode child, long count, Optional<OrderingScheme> tiesResolvingScheme) {
    super(id, child);
    this.count = count;
    this.tiesResolvingScheme = tiesResolvingScheme;
  }

  @Override
  public PlanNode clone() {
    return new LimitNode(id, child, count, tiesResolvingScheme);
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
