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
package org.apache.iotdb.db.queryengine.plan.relational.planner.iterative.rule;

import org.apache.iotdb.db.queryengine.common.QueryId;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.queryengine.plan.relational.function.BoundSignature;
import org.apache.iotdb.db.queryengine.plan.relational.planner.Assignments;
import org.apache.iotdb.db.queryengine.plan.relational.planner.OrderingScheme;
import org.apache.iotdb.db.queryengine.plan.relational.planner.Symbol;
import org.apache.iotdb.db.queryengine.plan.relational.planner.SymbolAllocator;
import org.apache.iotdb.db.queryengine.plan.relational.planner.SymbolsExtractor;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.ProjectNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.TopKRankingNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.WindowNode;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Expression;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import org.apache.tsfile.read.common.type.TimestampType;
import org.apache.tsfile.read.common.type.Type;

import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.Iterables.getOnlyElement;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.node.TopKRankingNode.RankingType.RANK;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.node.TopKRankingNode.RankingType.ROW_NUMBER;

final class Util {
  private Util() {}

  /**
   * Prune the set of available inputs to those required by the given expressions.
   *
   * <p>If all inputs are used, return Optional.empty() to indicate that no pruning is necessary.
   */
  public static Optional<Set<Symbol>> pruneInputs(
      Collection<Symbol> availableInputs, Collection<Expression> expressions) {
    Set<Symbol> availableInputsSet = ImmutableSet.copyOf(availableInputs);
    Set<Symbol> prunedInputs =
        Sets.filter(availableInputsSet, SymbolsExtractor.extractUnique(expressions)::contains);

    if (prunedInputs.size() == availableInputsSet.size()) {
      return Optional.empty();
    }

    return Optional.of(prunedInputs);
  }

  /** Transforms a plan like P->C->X to C->P->X */
  public static PlanNode transpose(PlanNode parent, PlanNode child) {
    return child.replaceChildren(ImmutableList.of(parent.replaceChildren(child.getChildren())));
  }

  /**
   * @return If the node has outputs not in permittedOutputs, returns an identity projection
   *     containing only those node outputs also in permittedOutputs.
   */
  public static Optional<PlanNode> restrictOutputs(
      QueryId idAllocator, PlanNode node, Set<Symbol> permittedOutputs) {
    List<Symbol> restrictedOutputs =
        node.getOutputSymbols().stream()
            .filter(permittedOutputs::contains)
            .collect(toImmutableList());

    if (restrictedOutputs.size() == node.getOutputSymbols().size()) {
      return Optional.empty();
    }

    return Optional.of(
        new ProjectNode(
            idAllocator.genPlanNodeId(), node, Assignments.identity(restrictedOutputs)));
  }

  /**
   * @return The original node, with identity projections possibly inserted between node and each
   *     child, limiting the columns to those permitted. Returns a present Optional if at least one
   *     child was rewritten.
   */
  @SafeVarargs
  public static Optional<PlanNode> restrictChildOutputs(
      QueryId idAllocator, PlanNode node, Set<Symbol>... permittedChildOutputsArgs) {
    List<Set<Symbol>> permittedChildOutputs = ImmutableList.copyOf(permittedChildOutputsArgs);

    checkArgument(
        (node.getChildren().size() == permittedChildOutputs.size()),
        "Mismatched child (%s) and permitted outputs (%s) sizes",
        node.getChildren().size(),
        permittedChildOutputs.size());

    ImmutableList.Builder<PlanNode> newChildrenBuilder = ImmutableList.builder();
    boolean rewroteChildren = false;

    for (int i = 0; i < node.getChildren().size(); ++i) {
      PlanNode oldChild = node.getChildren().get(i);
      Optional<PlanNode> newChild =
          restrictOutputs(idAllocator, oldChild, permittedChildOutputs.get(i));
      rewroteChildren |= newChild.isPresent();
      newChildrenBuilder.add(newChild.orElse(oldChild));
    }

    if (!rewroteChildren) {
      return Optional.empty();
    }
    return Optional.of(node.replaceChildren(newChildrenBuilder.build()));
  }

  public static Optional<TopKRankingNode.RankingType> toTopNRankingType(WindowNode node) {
    if (node.getWindowFunctions().size() != 1
        || !node.getSpecification().getOrderingScheme().isPresent()) {
      return Optional.empty();
    }

    BoundSignature signature =
        getOnlyElement(node.getWindowFunctions().values()).getResolvedFunction().getSignature();
    if (!signature.getArgumentTypes().isEmpty()) {
      return Optional.empty();
    }
    if (signature.getName().equals("row_number")) {
      return Optional.of(ROW_NUMBER);
    }
    if (signature.getName().equals("rank")) {
      return Optional.of(RANK);
    }
    return Optional.empty();
  }

  /**
   * Returns true when the window is ROW_NUMBER with ORDER BY on a single TIMESTAMP column (ASC or
   * DESC). In IoTDB, data is naturally sorted by (device, time) and table scans support both
   * forward and reverse iteration, so we can use the streaming LimitKRankingNode instead of the
   * buffered TopKRankingNode for both ASC (earliest K) and DESC (latest K).
   */
  public static boolean canUseLimitKRanking(
      WindowNode windowNode,
      TopKRankingNode.RankingType rankingType,
      SymbolAllocator symbolAllocator) {
    if (rankingType != ROW_NUMBER) {
      return false;
    }

    Optional<OrderingScheme> orderingScheme = windowNode.getSpecification().getOrderingScheme();
    if (!orderingScheme.isPresent()) {
      return false;
    }

    List<Symbol> orderBy = orderingScheme.get().getOrderBy();
    if (orderBy.size() != 1) {
      return false;
    }

    Symbol orderSymbol = orderBy.get(0);

    Type type = symbolAllocator.getTypes().getTableModelType(orderSymbol);
    return type instanceof TimestampType;
  }
}
