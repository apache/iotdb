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
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.TwoChildProcessNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.Symbol;

import com.google.common.collect.ImmutableList;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Objects;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class SemiJoinNode extends TwoChildProcessNode {
  private final Symbol sourceJoinSymbol;
  private final Symbol filteringSourceJoinSymbol;
  private final Symbol semiJoinOutput;

  public SemiJoinNode(
      PlanNodeId id,
      PlanNode source,
      PlanNode filteringSource,
      Symbol sourceJoinSymbol,
      Symbol filteringSourceJoinSymbol,
      Symbol semiJoinOutput) {
    super(id, source, filteringSource);
    this.sourceJoinSymbol = requireNonNull(sourceJoinSymbol, "sourceJoinSymbol is null");
    this.filteringSourceJoinSymbol =
        requireNonNull(filteringSourceJoinSymbol, "filteringSourceJoinSymbol is null");
    this.semiJoinOutput = requireNonNull(semiJoinOutput, "semiJoinOutput is null");

    if (source != null) {
      checkArgument(
          source.getOutputSymbols().contains(sourceJoinSymbol),
          "Source does not contain join symbol");
    }

    if (filteringSource != null) {
      checkArgument(
          filteringSource.getOutputSymbols().contains(filteringSourceJoinSymbol),
          "Filtering source does not contain filtering join symbol");
    }
  }

  public PlanNode getSource() {
    return leftChild;
  }

  public PlanNode getFilteringSource() {
    return rightChild;
  }

  public Symbol getSourceJoinSymbol() {
    return sourceJoinSymbol;
  }

  public Symbol getFilteringSourceJoinSymbol() {
    return filteringSourceJoinSymbol;
  }

  public Symbol getSemiJoinOutput() {
    return semiJoinOutput;
  }

  @Override
  public List<Symbol> getOutputSymbols() {
    return ImmutableList.<Symbol>builder()
        .addAll(leftChild.getOutputSymbols())
        .add(semiJoinOutput)
        .build();
  }

  @Override
  public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
    return visitor.visitSemiJoin(this, context);
  }

  @Override
  public PlanNode replaceChildren(List<PlanNode> newChildren) {
    checkArgument(newChildren.size() == 2, "expected newChildren to contain 2 nodes");
    return new SemiJoinNode(
        getPlanNodeId(),
        newChildren.get(0),
        newChildren.get(1),
        sourceJoinSymbol,
        filteringSourceJoinSymbol,
        semiJoinOutput);
  }

  @Override
  public PlanNode clone() {
    // clone without children
    return new SemiJoinNode(
        getPlanNodeId(), null, null, sourceJoinSymbol, filteringSourceJoinSymbol, semiJoinOutput);
  }

  @Override
  public List<String> getOutputColumnNames() {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }

    if (obj == null || !this.getClass().equals(obj.getClass())) {
      return false;
    }

    if (!super.equals(obj)) {
      return false;
    }

    SemiJoinNode other = (SemiJoinNode) obj;

    return Objects.equals(this.sourceJoinSymbol, other.sourceJoinSymbol)
        && Objects.equals(this.filteringSourceJoinSymbol, other.filteringSourceJoinSymbol)
        && Objects.equals(this.semiJoinOutput, other.semiJoinOutput);
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        super.hashCode(), sourceJoinSymbol, filteringSourceJoinSymbol, semiJoinOutput);
  }

  @Override
  protected void serializeAttributes(ByteBuffer byteBuffer) {
    PlanNodeType.TABLE_SEMI_JOIN_NODE.serialize(byteBuffer);

    Symbol.serialize(sourceJoinSymbol, byteBuffer);
    Symbol.serialize(filteringSourceJoinSymbol, byteBuffer);
    Symbol.serialize(semiJoinOutput, byteBuffer);
  }

  @Override
  protected void serializeAttributes(DataOutputStream stream) throws IOException {
    PlanNodeType.TABLE_SEMI_JOIN_NODE.serialize(stream);

    Symbol.serialize(sourceJoinSymbol, stream);
    Symbol.serialize(filteringSourceJoinSymbol, stream);
    Symbol.serialize(semiJoinOutput, stream);
  }

  public static SemiJoinNode deserialize(ByteBuffer byteBuffer) {
    Symbol sourceJoinSymbol = Symbol.deserialize(byteBuffer);
    Symbol filteringSourceJoinSymbol = Symbol.deserialize(byteBuffer);
    Symbol semiJoinOutput = Symbol.deserialize(byteBuffer);
    PlanNodeId planNodeId = PlanNodeId.deserialize(byteBuffer);
    return new SemiJoinNode(
        planNodeId, null, null, sourceJoinSymbol, filteringSourceJoinSymbol, semiJoinOutput);
  }
}
