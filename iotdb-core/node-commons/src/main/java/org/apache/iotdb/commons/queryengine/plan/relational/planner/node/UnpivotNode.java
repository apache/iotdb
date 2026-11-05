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

package org.apache.iotdb.commons.queryengine.plan.relational.planner.node;

import org.apache.iotdb.commons.queryengine.plan.planner.plan.node.ICoreQueryPlanVisitor;
import org.apache.iotdb.commons.queryengine.plan.planner.plan.node.IPlanVisitor;
import org.apache.iotdb.commons.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.commons.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.commons.queryengine.plan.planner.plan.node.PlanNodeType;
import org.apache.iotdb.commons.queryengine.plan.planner.plan.node.process.SingleChildProcessNode;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.Symbol;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import org.apache.tsfile.utils.ReadWriteIOUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Expands each input row once for every source group, without consuming the dedicated time slot.
 */
public final class UnpivotNode extends SingleChildProcessNode {
  private final List<Symbol> retained;
  private final List<List<Symbol>> sources;
  private final List<String> names;
  private final List<Symbol> outputs;
  private final boolean includeNulls;

  public UnpivotNode(
      PlanNodeId id,
      PlanNode child,
      List<Symbol> retained,
      List<List<Symbol>> sources,
      List<String> names,
      List<Symbol> outputs,
      boolean includeNulls) {
    super(id, child);
    this.retained = ImmutableList.copyOf(retained);
    this.sources =
        sources.stream().map(ImmutableList::copyOf).collect(ImmutableList.toImmutableList());
    this.names = ImmutableList.copyOf(names);
    this.outputs = ImmutableList.copyOf(outputs);
    this.includeNulls = includeNulls;
    if (sources.size() != names.size()
        || sources.isEmpty()
        || sources.stream()
            .anyMatch(source -> source.size() != outputs.size() - retained.size() - 1)) {
      throw new IllegalArgumentException();
    }
  }

  public List<Symbol> getRetained() {
    return retained;
  }

  public List<List<Symbol>> getSources() {
    return sources;
  }

  public List<String> getNames() {
    return names;
  }

  public boolean isIncludeNulls() {
    return includeNulls;
  }

  @Override
  public PlanNodeType getType() {
    return PlanNodeType.TABLE_UNPIVOT_NODE;
  }

  @Override
  public List<Symbol> getOutputSymbols() {
    return outputs;
  }

  @Override
  public List<String> getOutputColumnNames() {
    return outputs.stream().map(Symbol::getName).collect(ImmutableList.toImmutableList());
  }

  @Override
  public <R, C> R accept(IPlanVisitor<R, C> visitor, C context) {
    return ((ICoreQueryPlanVisitor<R, C>) visitor).visitUnpivot(this, context);
  }

  @Override
  public PlanNode clone() {
    return new UnpivotNode(id, null, retained, sources, names, outputs, includeNulls);
  }

  @Override
  public PlanNode replaceChildren(List<PlanNode> newChildren) {
    return new UnpivotNode(
        id, Iterables.getOnlyElement(newChildren), retained, sources, names, outputs, includeNulls);
  }

  private void writeAttributes(ByteBuffer buffer) {
    PlanNodeType.TABLE_UNPIVOT_NODE.serialize(buffer);
    ReadWriteIOUtils.write(retained.size(), buffer);
    retained.forEach(symbol -> Symbol.serialize(symbol, buffer));
    ReadWriteIOUtils.write(sources.size(), buffer);
    for (int i = 0; i < sources.size(); i++) {
      ReadWriteIOUtils.write(names.get(i), buffer);
      ReadWriteIOUtils.write(sources.get(i).size(), buffer);
      sources.get(i).forEach(symbol -> Symbol.serialize(symbol, buffer));
    }
    ReadWriteIOUtils.write(outputs.size(), buffer);
    outputs.forEach(symbol -> Symbol.serialize(symbol, buffer));
    ReadWriteIOUtils.write(includeNulls, buffer);
  }

  @Override
  protected void serializeAttributes(ByteBuffer buffer) {
    writeAttributes(buffer);
  }

  @Override
  protected void serializeAttributes(DataOutputStream stream) throws IOException {
    PlanNodeType.TABLE_UNPIVOT_NODE.serialize(stream);
    ReadWriteIOUtils.write(retained.size(), stream);
    for (Symbol symbol : retained) {
      Symbol.serialize(symbol, stream);
    }
    ReadWriteIOUtils.write(sources.size(), stream);
    for (int i = 0; i < sources.size(); i++) {
      ReadWriteIOUtils.write(names.get(i), stream);
      ReadWriteIOUtils.write(sources.get(i).size(), stream);
      for (Symbol symbol : sources.get(i)) {
        Symbol.serialize(symbol, stream);
      }
    }
    ReadWriteIOUtils.write(outputs.size(), stream);
    for (Symbol symbol : outputs) {
      Symbol.serialize(symbol, stream);
    }
    ReadWriteIOUtils.write(includeNulls, stream);
  }

  public static UnpivotNode deserialize(ByteBuffer buffer) {
    int retainedSize = ReadWriteIOUtils.readInt(buffer);
    List<Symbol> retained = new ArrayList<>(retainedSize);
    for (int i = 0; i < retainedSize; i++) {
      retained.add(Symbol.deserialize(buffer));
    }
    int sourceCount = ReadWriteIOUtils.readInt(buffer);
    List<String> names = new ArrayList<>(sourceCount);
    List<List<Symbol>> sources = new ArrayList<>(sourceCount);
    for (int i = 0; i < sourceCount; i++) {
      names.add(ReadWriteIOUtils.readString(buffer));
      int valueCount = ReadWriteIOUtils.readInt(buffer);
      List<Symbol> group = new ArrayList<>(valueCount);
      for (int j = 0; j < valueCount; j++) {
        group.add(Symbol.deserialize(buffer));
      }
      sources.add(group);
    }
    int outputCount = ReadWriteIOUtils.readInt(buffer);
    List<Symbol> outputs = new ArrayList<>(outputCount);
    for (int i = 0; i < outputCount; i++) {
      outputs.add(Symbol.deserialize(buffer));
    }
    boolean includeNulls = ReadWriteIOUtils.readBool(buffer);
    return new UnpivotNode(
        PlanNodeId.deserialize(buffer), null, retained, sources, names, outputs, includeNulls);
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (!(obj instanceof UnpivotNode)) {
      return false;
    }
    UnpivotNode other = (UnpivotNode) obj;
    return super.equals(other)
        && retained.equals(other.retained)
        && sources.equals(other.sources)
        && names.equals(other.names)
        && outputs.equals(other.outputs)
        && includeNulls == other.includeNulls;
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), retained, sources, names, outputs, includeNulls);
  }
}
