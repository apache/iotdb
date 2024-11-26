/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.iotdb.db.queryengine.plan.relational.planner.node;

import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanVisitor;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.SingleChildProcessNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.Symbol;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;

public class ExplainAnalyzeNode extends SingleChildProcessNode {
  private final boolean verbose;

  private final long queryId;
  private final long timeout;
  private final Symbol outputSymbol;

  public ExplainAnalyzeNode(
      PlanNodeId id,
      PlanNode child,
      boolean verbose,
      long queryId,
      long timeout,
      Symbol outputSymbol) {
    super(id, child);
    this.verbose = verbose;
    this.timeout = timeout;
    this.queryId = queryId;
    this.outputSymbol = outputSymbol;
  }

  @Override
  public PlanNode clone() {
    return new ExplainAnalyzeNode(getPlanNodeId(), child, verbose, queryId, timeout, outputSymbol);
  }

  @Override
  public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
    return visitor.visitExplainAnalyze(this, context);
  }

  @Override
  public List<String> getOutputColumnNames() {
    return null;
  }

  @Override
  public List<Symbol> getOutputSymbols() {
    return Collections.singletonList(outputSymbol);
  }

  @Override
  public PlanNode replaceChildren(List<PlanNode> newChildren) {
    return new org.apache.iotdb.db.queryengine.plan.relational.planner.node.ExplainAnalyzeNode(
        getPlanNodeId(), newChildren.get(0), verbose, queryId, timeout, outputSymbol);
  }

  // ExplainAnalyze should be at the same region as Coordinator all the time. Therefore, there will
  // be no serialization and deserialization process.
  @Override
  protected void serializeAttributes(ByteBuffer byteBuffer) {
    throw new UnsupportedOperationException("ExplainAnalyzeNode should not be serialized");
  }

  @Override
  protected void serializeAttributes(DataOutputStream stream) throws IOException {
    throw new UnsupportedOperationException("ExplainAnalyzeNode should not be serialized");
  }

  public boolean isVerbose() {
    return verbose;
  }

  public long getQueryId() {
    return queryId;
  }

  public long getTimeout() {
    return timeout;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof ExplainAnalyzeNode)) return false;
    ExplainAnalyzeNode that = (ExplainAnalyzeNode) o;
    return verbose == that.verbose && queryId == that.queryId && timeout == that.timeout;
  }

  @Override
  public int hashCode() {
    return super.hashCode()
        + Boolean.hashCode(verbose)
        + Long.hashCode(queryId)
        + Long.hashCode(timeout);
  }
}
