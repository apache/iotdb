/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.db.queryengine.plan.relational.planner.node;

import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeType;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanVisitor;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.MultiChildProcessNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.Symbol;

import com.google.common.base.Objects;
import org.apache.tsfile.utils.ReadWriteIOUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * CollectNode output the content of children. Normally it will output the child one by one, but in
 * some cases, while some children are blocked, it may output the content of other children.
 */
public class CollectNode extends MultiChildProcessNode {

  private List<Symbol> outputSymbols;

  public CollectNode(PlanNodeId id) {
    super(id);
  }

  public CollectNode(PlanNodeId id, List<PlanNode> children) {
    super(id, children);
  }

  @Override
  public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
    return visitor.visitCollect(this, context);
  }

  @Override
  public PlanNode clone() {
    CollectNode collectNode = new CollectNode(id);
    collectNode.outputSymbols = outputSymbols;
    return collectNode;
  }

  public void setOutputSymbols(List<Symbol> outputSymbols) {
    this.outputSymbols = outputSymbols;
  }

  @Override
  public List<Symbol> getOutputSymbols() {
    return outputSymbols;
  }

  @Override
  public List<String> getOutputColumnNames() {
    throw new UnsupportedOperationException();
  }

  @Override
  public PlanNode replaceChildren(List<PlanNode> newChildren) {
    checkArgument(children.size() == newChildren.size(), "wrong number of new children");
    return new CollectNode(id, newChildren);
  }

  @Override
  protected void serializeAttributes(ByteBuffer byteBuffer) {
    PlanNodeType.TABLE_COLLECT_NODE.serialize(byteBuffer);
    ReadWriteIOUtils.write(outputSymbols.size(), byteBuffer);
    outputSymbols.forEach(symbol -> Symbol.serialize(symbol, byteBuffer));
  }

  @Override
  protected void serializeAttributes(DataOutputStream stream) throws IOException {
    PlanNodeType.TABLE_COLLECT_NODE.serialize(stream);
    ReadWriteIOUtils.write(outputSymbols.size(), stream);
    for (Symbol symbol : outputSymbols) {
      Symbol.serialize(symbol, stream);
    }
  }

  public static CollectNode deserialize(ByteBuffer byteBuffer) {
    int size = ReadWriteIOUtils.readInt(byteBuffer);
    List<Symbol> outputs = new ArrayList<>(size);
    while (size-- > 0) {
      outputs.add(Symbol.deserialize(byteBuffer));
    }
    PlanNodeId planNodeId = PlanNodeId.deserialize(byteBuffer);
    CollectNode collectNode = new CollectNode(planNodeId);
    collectNode.setOutputSymbols(outputs);
    return collectNode;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    if (!super.equals(o)) return false;
    return false;
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(super.hashCode());
  }

  @Override
  public String toString() {
    return "CollectNode-" + this.getPlanNodeId();
  }
}
