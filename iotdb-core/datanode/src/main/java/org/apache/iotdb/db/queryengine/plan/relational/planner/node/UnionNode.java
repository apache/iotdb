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
import org.apache.iotdb.db.queryengine.plan.relational.planner.Symbol;

import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.ListMultimap;
import org.apache.tsfile.utils.ReadWriteIOUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class UnionNode extends SetOperationNode {
  public UnionNode(
      PlanNodeId id,
      List<PlanNode> children,
      ListMultimap<Symbol, Symbol> outputToInputs,
      List<Symbol> outputs) {
    super(id, children, outputToInputs, outputs);
  }

  private UnionNode(
      PlanNodeId id, ListMultimap<Symbol, Symbol> outputToInputs, List<Symbol> outputs) {
    super(id, outputToInputs, outputs);
  }

  @Override
  public PlanNode clone() {
    return new UnionNode(getPlanNodeId(), getSymbolMapping(), getOutputSymbols());
  }

  @Override
  public List<String> getOutputColumnNames() {
    throw new UnsupportedOperationException();
  }

  @Override
  public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
    return visitor.visitUnion(this, context);
  }

  @Override
  protected void serializeAttributes(ByteBuffer byteBuffer) {
    PlanNodeType.TABLE_UNION_NODE.serialize(byteBuffer);
    ReadWriteIOUtils.write(getOutputSymbols().size(), byteBuffer);
    getOutputSymbols().forEach(symbol -> Symbol.serialize(symbol, byteBuffer));

    ListMultimap<Symbol, Symbol> multimap = getSymbolMapping();
    ReadWriteIOUtils.write(multimap.size(), byteBuffer);
    for (Symbol key : multimap.keySet()) {
      Symbol.serialize(key, byteBuffer);
      ReadWriteIOUtils.write(multimap.get(key).size(), byteBuffer);
      for (Symbol value : multimap.get(key)) {
        Symbol.serialize(value, byteBuffer);
      }
    }
  }

  @Override
  protected void serializeAttributes(DataOutputStream stream) throws IOException {
    PlanNodeType.TABLE_UNION_NODE.serialize(stream);
    ReadWriteIOUtils.write(getOutputSymbols().size(), stream);
    for (Symbol symbol : getOutputSymbols()) {
      Symbol.serialize(symbol, stream);
    }

    ListMultimap<Symbol, Symbol> multimap = getSymbolMapping();
    ReadWriteIOUtils.write(multimap.keySet().size(), stream);
    for (Symbol key : multimap.keySet()) {
      Symbol.serialize(key, stream);
      ReadWriteIOUtils.write(multimap.get(key).size(), stream);
      for (Symbol value : multimap.get(key)) {
        Symbol.serialize(value, stream);
      }
    }
  }

  public static UnionNode deserialize(ByteBuffer byteBuffer) {
    int size = ReadWriteIOUtils.readInt(byteBuffer);
    List<Symbol> outputs = new ArrayList<>(size);
    while (size-- > 0) {
      outputs.add(Symbol.deserialize(byteBuffer));
    }
    ImmutableListMultimap.Builder<Symbol, Symbol> builder = ImmutableListMultimap.builder();
    size = ReadWriteIOUtils.readInt(byteBuffer);
    while (size-- > 0) {
      Symbol key = Symbol.deserialize(byteBuffer);
      int valueSize = ReadWriteIOUtils.readInt(byteBuffer);
      for (int i = 0; i < valueSize; i++) {
        builder.put(key, Symbol.deserialize(byteBuffer));
      }
    }
    PlanNodeId planNodeId = PlanNodeId.deserialize(byteBuffer);
    return new UnionNode(planNodeId, builder.build(), outputs);
  }

  @Override
  public PlanNode replaceChildren(List<PlanNode> newChildren) {
    return new UnionNode(getPlanNodeId(), newChildren, getSymbolMapping(), getOutputSymbols());
  }
}
