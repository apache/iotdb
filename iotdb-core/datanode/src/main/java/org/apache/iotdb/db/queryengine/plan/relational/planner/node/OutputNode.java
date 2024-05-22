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
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeType;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanVisitor;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.SingleChildProcessNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.Symbol;

import com.google.common.collect.ImmutableList;
import org.apache.tsfile.utils.ReadWriteIOUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
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

  public OutputNode(PlanNodeId id, List<String> columnNames, List<Symbol> outputs) {
    super(id);
    this.id = id;
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
  protected void serializeAttributes(ByteBuffer byteBuffer) {
    PlanNodeType.TABLE_OUTPUT_NODE.serialize(byteBuffer);
    ReadWriteIOUtils.write(columnNames.size(), byteBuffer);
    columnNames.forEach(columnName -> ReadWriteIOUtils.write(columnName, byteBuffer));
    ReadWriteIOUtils.write(outputs.size(), byteBuffer);
    outputs.forEach(symbol -> Symbol.serialize(symbol, byteBuffer));
  }

  @Override
  protected void serializeAttributes(DataOutputStream stream) throws IOException {
    PlanNodeType.TABLE_OUTPUT_NODE.serialize(stream);
    ReadWriteIOUtils.write(columnNames.size(), stream);
    for (String columnName : columnNames) {
      ReadWriteIOUtils.write(columnName, stream);
    }
    ReadWriteIOUtils.write(outputs.size(), stream);
    for (Symbol symbol : outputs) {
      Symbol.serialize(symbol, stream);
    }
  }

  public static OutputNode deserialize(ByteBuffer byteBuffer) {
    int size = ReadWriteIOUtils.readInt(byteBuffer);
    List<String> columnNames = new ArrayList<>(size);
    while (size-- > 0) {
      columnNames.add(ReadWriteIOUtils.readString(byteBuffer));
    }
    size = ReadWriteIOUtils.readInt(byteBuffer);
    List<Symbol> outputs = new ArrayList<>(size);
    while (size-- > 0) {
      outputs.add(Symbol.deserialize(byteBuffer));
    }
    PlanNodeId planNodeId = PlanNodeId.deserialize(byteBuffer);
    return new OutputNode(planNodeId, columnNames, outputs);
  }

  public List<String> getColumnNames() {
    return this.columnNames;
  }

  public List<Symbol> getOutputSymbols() {
    return outputs;
  }
}
