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
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Node;

import com.google.common.collect.ImmutableList;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class ApplyNode extends TwoChildProcessNode {

  /** Correlation symbols, returned from input (outer plan) used in subquery (inner plan) */
  private final List<Symbol> correlation;

  /**
   * Expressions that use subquery symbols.
   *
   * <p>Subquery expressions are different than other expressions in a sense that they might use an
   * entire subquery result as an input (e.g: "x IN (subquery)", "x < ALL (subquery)"). Such
   * expressions are invalid in linear operator context (e.g: ProjectNode) in logical plan, but are
   * correct in ApplyNode context.
   *
   * <p>Example 1: - expression: input_symbol_X IN (subquery_symbol_Y) - meaning: if set consisting
   * of all values for subquery_symbol_Y contains value represented by input_symbol_X
   *
   * <p>Example 2: - expression: input_symbol_X < ALL (subquery_symbol_Y) - meaning: if
   * input_symbol_X is smaller than all subquery values represented by subquery_symbol_Y
   *
   * <p>
   */
  private final Map<Symbol, SetExpression> subqueryAssignments;

  /** HACK! Used for error reporting in case this ApplyNode is not supported */
  private final Node originSubquery;

  public ApplyNode(
      PlanNodeId id,
      // will be set as leftChild
      PlanNode input,
      // will be set as rightChild
      PlanNode subquery,
      Map<Symbol, SetExpression> subqueryAssignments,
      List<Symbol> correlation,
      Node originSubquery) {
    super(id, input, subquery);
    requireNonNull(subqueryAssignments, "subqueryAssignments is null");
    requireNonNull(correlation, "correlation is null");
    requireNonNull(originSubquery, "originSubquery is null");

    if (input != null) {
      checkArgument(
          input.getOutputSymbols().containsAll(correlation),
          "Input does not contain symbols from correlation");
    }

    this.subqueryAssignments = subqueryAssignments;
    this.correlation = ImmutableList.copyOf(correlation);
    this.originSubquery = originSubquery;
  }

  public PlanNode getInput() {
    return leftChild;
  }

  public PlanNode getSubquery() {
    return rightChild;
  }

  public Map<Symbol, SetExpression> getSubqueryAssignments() {
    return subqueryAssignments;
  }

  public List<Symbol> getCorrelation() {
    return correlation;
  }

  public Node getOriginSubquery() {
    return originSubquery;
  }

  @Override
  public List<Symbol> getOutputSymbols() {
    return ImmutableList.<Symbol>builder()
        .addAll(leftChild.getOutputSymbols())
        .addAll(subqueryAssignments.keySet())
        .build();
  }

  @Override
  public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
    return visitor.visitApply(this, context);
  }

  @Override
  public PlanNode replaceChildren(List<PlanNode> newChildren) {
    checkArgument(newChildren.size() == 2, "expected newChildren to contain 2 nodes");
    return new ApplyNode(
        getPlanNodeId(),
        newChildren.get(0),
        newChildren.get(1),
        subqueryAssignments,
        correlation,
        originSubquery);
  }

  @Override
  public PlanNode clone() {
    // clone without children
    return new ApplyNode(
        getPlanNodeId(), null, null, subqueryAssignments, correlation, originSubquery);
  }

  @Override
  public List<String> getOutputColumnNames() {
    throw new UnsupportedOperationException();
  }

  @Override
  protected void serializeAttributes(ByteBuffer byteBuffer) {
    // ApplyNode should be transformed to other nodes after planning, so serialization is not
    // expected.
    throw new UnsupportedOperationException();
  }

  @Override
  protected void serializeAttributes(DataOutputStream stream) throws IOException {
    // ApplyNode should be transformed to other nodes after planning, so serialization is not
    // expected.
    throw new UnsupportedOperationException();
  }

  public interface SetExpression {
    List<Symbol> inputs();
  }

  public static class In implements SetExpression {
    private final Symbol value;
    private final Symbol reference;

    public In(Symbol value, Symbol reference) {
      this.value = value;
      this.reference = reference;
    }

    public Symbol getValue() {
      return value;
    }

    public Symbol getReference() {
      return reference;
    }

    @Override
    public List<Symbol> inputs() {
      return ImmutableList.of(value, reference);
    }
  }

  public static class Exists implements SetExpression {
    @Override
    public List<Symbol> inputs() {
      return ImmutableList.of();
    }
  }

  public static class QuantifiedComparison implements SetExpression {
    private final Operator operator;
    private final Quantifier quantifier;
    private final Symbol value;
    private final Symbol reference;

    public QuantifiedComparison(
        Operator operator, Quantifier quantifier, Symbol value, Symbol reference) {
      this.operator = operator;
      this.quantifier = quantifier;
      this.value = value;
      this.reference = reference;
    }

    public Operator getOperator() {
      return operator;
    }

    public Quantifier getQuantifier() {
      return quantifier;
    }

    public Symbol getValue() {
      return value;
    }

    public Symbol getReference() {
      return reference;
    }

    @Override
    public List<Symbol> inputs() {
      return ImmutableList.of(value, reference);
    }
  }

  public enum Operator {
    EQUAL,
    NOT_EQUAL,
    LESS_THAN,
    LESS_THAN_OR_EQUAL,
    GREATER_THAN,
    GREATER_THAN_OR_EQUAL,
  }

  public enum Quantifier {
    ALL,
    ANY,
    SOME,
  }
}
