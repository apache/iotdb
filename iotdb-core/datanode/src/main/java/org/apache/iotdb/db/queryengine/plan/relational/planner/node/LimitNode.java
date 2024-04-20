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

  public long getCount() {
    return count;
  }
}
