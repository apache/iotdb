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
