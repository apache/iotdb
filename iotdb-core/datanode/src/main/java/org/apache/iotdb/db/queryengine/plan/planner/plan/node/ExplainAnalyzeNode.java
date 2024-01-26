package org.apache.iotdb.db.queryengine.plan.planner.plan.node;

import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.SingleChildProcessNode;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;

public class ExplainAnalyzeNode extends SingleChildProcessNode {
  private final boolean verbose;

  public ExplainAnalyzeNode(PlanNodeId id, PlanNode child, boolean verbose) {
    super(id, child);
    this.verbose = verbose;
  }

  @Override
  public PlanNode clone() {
    return new ExplainAnalyzeNode(getPlanNodeId(), child, verbose);
  }

  @Override
  public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
    return visitor.visitExplainAnalyze(this, context);
  }

  @Override
  public List<String> getOutputColumnNames() {
    return Collections.singletonList("Explain Analyze");
  }

  @Override
  protected void serializeAttributes(ByteBuffer byteBuffer) {
    PlanNodeType.EXPLAIN_ANALYZE.serialize(byteBuffer);
    ReadWriteIOUtils.write(verbose, byteBuffer);
  }

  @Override
  protected void serializeAttributes(DataOutputStream stream) throws IOException {
    PlanNodeType.EXPLAIN_ANALYZE.serialize(stream);
    ReadWriteIOUtils.write(verbose, stream);
  }

  public static ExplainAnalyzeNode deserialize(ByteBuffer byteBuffer) {
    boolean verbose = ReadWriteIOUtils.readBool(byteBuffer);
    PlanNodeId planNodeId = PlanNodeId.deserialize(byteBuffer);
    return new ExplainAnalyzeNode(planNodeId, null, verbose);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof ExplainAnalyzeNode)) return false;
    ExplainAnalyzeNode that = (ExplainAnalyzeNode) o;
    return verbose == that.verbose;
  }

  @Override
  public int hashCode() {
    return super.hashCode() + Boolean.hashCode(verbose);
  }
}
