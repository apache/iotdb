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
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.SingleChildProcessNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.Symbol;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import org.apache.tsfile.utils.ReadWriteIOUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class MarkDistinctNode extends SingleChildProcessNode {
  private final Symbol markerSymbol;

  private final Optional<Symbol> hashSymbol;
  private final List<Symbol> distinctSymbols;

  public MarkDistinctNode(
      PlanNodeId id,
      PlanNode child,
      Symbol markerSymbol,
      List<Symbol> distinctSymbols,
      Optional<Symbol> hashSymbol) {
    super(id);
    this.child = child;
    this.markerSymbol = requireNonNull(markerSymbol, "markerSymbol is null");
    this.hashSymbol = requireNonNull(hashSymbol, "hashSymbol is null");
    requireNonNull(distinctSymbols, "distinctSymbols is null");
    checkArgument(!distinctSymbols.isEmpty(), "distinctSymbols cannot be empty");
    this.distinctSymbols = ImmutableList.copyOf(distinctSymbols);
  }

  @Override
  public List<Symbol> getOutputSymbols() {
    return ImmutableList.<Symbol>builder()
        .addAll(child.getOutputSymbols())
        .add(markerSymbol)
        .build();
  }

  @Override
  public List<PlanNode> getChildren() {
    return ImmutableList.of(child);
  }

  @Override
  public PlanNode clone() {
    return new MarkDistinctNode(id, child, markerSymbol, distinctSymbols, hashSymbol);
  }

  @Override
  public List<String> getOutputColumnNames() {
    throw new UnsupportedOperationException();
  }

  public Symbol getMarkerSymbol() {
    return markerSymbol;
  }

  public List<Symbol> getDistinctSymbols() {
    return distinctSymbols;
  }

  public Optional<Symbol> getHashSymbol() {
    return hashSymbol;
  }

  @Override
  public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
    return visitor.visitMarkDistinct(this, context);
  }

  @Override
  protected void serializeAttributes(ByteBuffer byteBuffer) {
    PlanNodeType.MARK_DISTINCT_NODE.serialize(byteBuffer);
    Symbol.serialize(markerSymbol, byteBuffer);
    if (hashSymbol.isPresent()) {
      ReadWriteIOUtils.write(true, byteBuffer);
      Symbol.serialize(hashSymbol.get(), byteBuffer);
    } else {
      ReadWriteIOUtils.write(false, byteBuffer);
    }
    ReadWriteIOUtils.write(distinctSymbols.size(), byteBuffer);
    for (Symbol distinctSymbol : distinctSymbols) {
      Symbol.serialize(distinctSymbol, byteBuffer);
      ;
    }
  }

  @Override
  protected void serializeAttributes(DataOutputStream stream) throws IOException {
    PlanNodeType.MARK_DISTINCT_NODE.serialize(stream);
    Symbol.serialize(markerSymbol, stream);
    if (hashSymbol.isPresent()) {
      ReadWriteIOUtils.write(true, stream);
      Symbol.serialize(hashSymbol.get(), stream);
    } else {
      ReadWriteIOUtils.write(false, stream);
    }
    ReadWriteIOUtils.write(distinctSymbols.size(), stream);
    for (Symbol distinctSymbol : distinctSymbols) {
      Symbol.serialize(distinctSymbol, stream);
    }
  }

  public static MarkDistinctNode deserialize(ByteBuffer byteBuffer) {
    Symbol markerSymbol = Symbol.deserialize(byteBuffer);
    Optional<Symbol> hashSymbol = Optional.empty();
    if (ReadWriteIOUtils.readBool(byteBuffer)) {
      hashSymbol = Optional.of(Symbol.deserialize(byteBuffer));
    }
    int size = ReadWriteIOUtils.readInt(byteBuffer);
    List<Symbol> distinctSymbols = new ArrayList<>(size);
    while (size-- > 0) {
      distinctSymbols.add(Symbol.deserialize(byteBuffer));
    }

    PlanNodeId planNodeId = PlanNodeId.deserialize(byteBuffer);
    return new MarkDistinctNode(planNodeId, null, markerSymbol, distinctSymbols, hashSymbol);
  }

  @Override
  public PlanNode replaceChildren(List<PlanNode> newChildren) {
    return new MarkDistinctNode(
        getPlanNodeId(),
        Iterables.getOnlyElement(newChildren),
        markerSymbol,
        distinctSymbols,
        hashSymbol);
  }
}
