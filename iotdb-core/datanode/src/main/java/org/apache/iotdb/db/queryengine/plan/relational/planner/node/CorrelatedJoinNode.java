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
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanVisitor;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.TwoChildProcessNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.Symbol;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Expression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Node;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.NullLiteral;

import com.google.common.collect.ImmutableList;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

/**
 * For every row from {@link #leftChild}(input) a {@link #rightChild}(subquery) relation is
 * calculated. Then input row is cross joined with subquery relation and returned as a result.
 *
 * <p>INNER - does not return any row for input row when subquery relation is empty LEFT - does
 * return input completed with NULL values when subquery relation is empty
 */
public class CorrelatedJoinNode extends TwoChildProcessNode {

  /** Correlation symbols, returned from input (outer plan) used in subquery (inner plan) */
  private final List<Symbol> correlation;

  private final JoinNode.JoinType type;
  private final Expression filter;

  /** HACK! Used for error reporting in case this ApplyNode is not supported */
  private final Node originSubquery;

  public CorrelatedJoinNode(
      PlanNodeId id,
      PlanNode input,
      PlanNode subquery,
      List<Symbol> correlation,
      JoinNode.JoinType type,
      Expression filter,
      Node originSubquery) {
    super(id, input, subquery);
    requireNonNull(correlation, "correlation is null");
    requireNonNull(filter, "filter is null");
    // The condition doesn't guarantee that filter is of type boolean, but was found to be a
    // practical way to identify
    // places where CorrelatedJoinNode could be created without appropriate coercions.
    checkArgument(
        !(filter instanceof NullLiteral),
        "Filter must be an expression of boolean type: %s",
        filter);
    requireNonNull(originSubquery, "originSubquery is null");

    if (input != null) {
      checkArgument(
          input.getOutputSymbols().containsAll(correlation),
          "Input does not contain symbols from correlation");
    }

    this.correlation = ImmutableList.copyOf(correlation);
    this.type = type;
    this.filter = filter;
    this.originSubquery = originSubquery;
  }

  public PlanNode getInput() {
    return leftChild;
  }

  public PlanNode getSubquery() {
    return rightChild;
  }

  public List<Symbol> getCorrelation() {
    return correlation;
  }

  public JoinNode.JoinType getJoinType() {
    return type;
  }

  public Expression getFilter() {
    return filter;
  }

  public Node getOriginSubquery() {
    return originSubquery;
  }

  public List<PlanNode> getSources() {
    return ImmutableList.of(leftChild, rightChild);
  }

  @Override
  public List<Symbol> getOutputSymbols() {
    return ImmutableList.<Symbol>builder()
        .addAll(leftChild.getOutputSymbols())
        .addAll(rightChild.getOutputSymbols())
        .build();
  }

  @Override
  public PlanNode replaceChildren(List<PlanNode> newChildren) {
    checkArgument(newChildren.size() == 2, "expected newChildren to contain 2 nodes");
    return new CorrelatedJoinNode(
        getPlanNodeId(),
        newChildren.get(0),
        newChildren.get(1),
        correlation,
        type,
        filter,
        originSubquery);
  }

  @Override
  public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
    return visitor.visitCorrelatedJoin(this, context);
  }

  @Override
  public PlanNode clone() {
    // clone without children
    return new CorrelatedJoinNode(
        getPlanNodeId(), null, null, correlation, type, filter, originSubquery);
  }

  @Override
  public List<String> getOutputColumnNames() {
    throw new UnsupportedOperationException();
  }

  @Override
  protected void serializeAttributes(ByteBuffer byteBuffer) {
    // CorrelatedJoinNode should be transformed to other nodes after planning, so serialization is
    // not expected.
    throw new UnsupportedOperationException();
  }

  @Override
  protected void serializeAttributes(DataOutputStream stream) throws IOException {
    // CorrelatedJoinNode should be transformed to other nodes after planning, so serialization is
    // not expected.
    throw new UnsupportedOperationException();
  }
}
