/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.db.queryengine.plan.relational.planner.optimizations;

import org.apache.iotdb.db.queryengine.plan.relational.metadata.Metadata;
import org.apache.iotdb.db.queryengine.plan.relational.planner.EqualityInference;
import org.apache.iotdb.db.queryengine.plan.relational.planner.Symbol;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.JoinNode;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.ComparisonExpression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Expression;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import java.util.Collection;
import java.util.Objects;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.SymbolsExtractor.extractUnique;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.ir.DeterminismEvaluator.isDeterministic;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.ir.IrUtils.combineConjuncts;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.ir.IrUtils.extractConjuncts;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.ir.IrUtils.filterDeterministicConjuncts;
import static org.apache.iotdb.db.queryengine.plan.relational.sql.ast.BooleanLiteral.TRUE_LITERAL;

public class JoinUtils {
  public static final String UNSUPPORTED_JOIN_CRITERIA =
      "Unsupported Join creteria [%s] after predicate push down";

  private JoinUtils() {}

  static Expression extractJoinPredicate(JoinNode joinNode) {
    ImmutableList.Builder<Expression> builder = ImmutableList.builder();
    for (JoinNode.EquiJoinClause equiJoinClause : joinNode.getCriteria()) {
      builder.add(equiJoinClause.toExpression());
    }
    joinNode.getFilter().ifPresent(builder::add);
    return combineConjuncts(builder.build());
  }

  /**
   * If the expression is EQUAL ComparisonExpression
   *
   * @return true if the expression is EQUAL ComparisonExpression and leftSymbols contains
   *     expression.getLeft() and rightSymbols contains expression.getRight().
   */
  static boolean joinEqualityExpression(
      Expression expression, Collection<Symbol> leftSymbols, Collection<Symbol> rightSymbols) {
    return joinComparisonExpression(
        expression,
        leftSymbols,
        rightSymbols,
        ImmutableSet.of(ComparisonExpression.Operator.EQUAL));
  }

  private static boolean joinComparisonExpression(
      Expression expression,
      Collection<Symbol> leftSymbols,
      Collection<Symbol> rightSymbols,
      Set<ComparisonExpression.Operator> operators) {
    // At this point in time, our join predicates need to be deterministic
    if (expression instanceof ComparisonExpression && isDeterministic(expression)) {
      ComparisonExpression comparison = (ComparisonExpression) expression;
      if (operators.contains(comparison.getOperator())) {
        Set<Symbol> symbols1 = extractUnique(comparison.getLeft());
        Set<Symbol> symbols2 = extractUnique(comparison.getRight());
        if (symbols1.isEmpty() || symbols2.isEmpty()) {
          return false;
        }
        return (leftSymbols.containsAll(symbols1) && rightSymbols.containsAll(symbols2))
            || (rightSymbols.containsAll(symbols1) && leftSymbols.containsAll(symbols2));
      }
    }
    return false;
  }

  static InnerJoinPushDownResult processInnerJoin(
      Metadata metadata,
      Expression inheritedPredicate,
      Expression leftEffectivePredicate,
      Expression rightEffectivePredicate,
      Expression joinPredicate,
      Collection<Symbol> leftSymbols,
      Collection<Symbol> rightSymbols) {
    checkArgument(
        leftSymbols.containsAll(extractUnique(leftEffectivePredicate)),
        "leftEffectivePredicate must only contain symbols from leftSymbols");
    checkArgument(
        rightSymbols.containsAll(extractUnique(rightEffectivePredicate)),
        "rightEffectivePredicate must only contain symbols from rightSymbols");

    ImmutableList.Builder<Expression> leftPushDownConjuncts = ImmutableList.builder();
    ImmutableList.Builder<Expression> rightPushDownConjuncts = ImmutableList.builder();
    ImmutableList.Builder<Expression> joinConjuncts = ImmutableList.builder();

    // Strip out non-deterministic conjuncts
    extractConjuncts(inheritedPredicate).stream()
        .filter(deterministic -> !isDeterministic(deterministic))
        .forEach(joinConjuncts::add);
    inheritedPredicate = filterDeterministicConjuncts(inheritedPredicate);

    extractConjuncts(joinPredicate).stream()
        .filter(expression -> !isDeterministic(expression))
        .forEach(joinConjuncts::add);
    joinPredicate = filterDeterministicConjuncts(joinPredicate);

    leftEffectivePredicate = filterDeterministicConjuncts(leftEffectivePredicate);
    rightEffectivePredicate = filterDeterministicConjuncts(rightEffectivePredicate);

    ImmutableSet<Symbol> leftScope = ImmutableSet.copyOf(leftSymbols);
    ImmutableSet<Symbol> rightScope = ImmutableSet.copyOf(rightSymbols);

    // Generate equality inferences
    EqualityInference allInference =
        new EqualityInference(
            metadata,
            inheritedPredicate,
            leftEffectivePredicate,
            rightEffectivePredicate,
            joinPredicate);
    EqualityInference allInferenceWithoutLeftInferred =
        new EqualityInference(metadata, inheritedPredicate, rightEffectivePredicate, joinPredicate);
    EqualityInference allInferenceWithoutRightInferred =
        new EqualityInference(metadata, inheritedPredicate, leftEffectivePredicate, joinPredicate);

    // Add equalities from the inference back in
    leftPushDownConjuncts.addAll(
        allInferenceWithoutLeftInferred
            .generateEqualitiesPartitionedBy(leftScope)
            .getScopeEqualities());
    rightPushDownConjuncts.addAll(
        allInferenceWithoutRightInferred
            .generateEqualitiesPartitionedBy(rightScope)
            .getScopeEqualities());
    joinConjuncts.addAll(
        allInference
            .generateEqualitiesPartitionedBy(leftScope)
            .getScopeStraddlingEqualities()); // scope straddling equalities get dropped in as
    // part of the join predicate

    // Sort through conjuncts in inheritedPredicate that were not used for inference
    EqualityInference.nonInferrableConjuncts(metadata, inheritedPredicate)
        .forEach(
            conjunct -> {
              Expression leftRewrittenConjunct = allInference.rewrite(conjunct, leftScope);
              if (leftRewrittenConjunct != null) {
                leftPushDownConjuncts.add(leftRewrittenConjunct);
              }

              Expression rightRewrittenConjunct = allInference.rewrite(conjunct, rightScope);
              if (rightRewrittenConjunct != null) {
                rightPushDownConjuncts.add(rightRewrittenConjunct);
              }

              // Drop predicate after join only if unable to push down to either side
              if (leftRewrittenConjunct == null && rightRewrittenConjunct == null) {
                joinConjuncts.add(conjunct);
              }
            });

    // See if we can push the right effective predicate to the left side
    EqualityInference.nonInferrableConjuncts(metadata, rightEffectivePredicate)
        .map(conjunct -> allInference.rewrite(conjunct, leftScope))
        .filter(Objects::nonNull)
        .forEach(leftPushDownConjuncts::add);

    // See if we can push the left effective predicate to the right side
    EqualityInference.nonInferrableConjuncts(metadata, leftEffectivePredicate)
        .map(conjunct -> allInference.rewrite(conjunct, rightScope))
        .filter(Objects::nonNull)
        .forEach(rightPushDownConjuncts::add);

    // See if we can push any parts of the join predicates to either side
    EqualityInference.nonInferrableConjuncts(metadata, joinPredicate)
        .forEach(
            conjunct -> {
              Expression leftRewritten = allInference.rewrite(conjunct, leftScope);
              if (leftRewritten != null) {
                leftPushDownConjuncts.add(leftRewritten);
              }

              Expression rightRewritten = allInference.rewrite(conjunct, rightScope);
              if (rightRewritten != null) {
                rightPushDownConjuncts.add(rightRewritten);
              }

              if (leftRewritten == null && rightRewritten == null) {
                joinConjuncts.add(conjunct);
              }
            });

    return new InnerJoinPushDownResult(
        combineConjuncts(leftPushDownConjuncts.build()),
        combineConjuncts(rightPushDownConjuncts.build()),
        combineConjuncts(joinConjuncts.build()),
        TRUE_LITERAL);
  }

  static class InnerJoinPushDownResult {
    private final Expression leftPredicate;
    private final Expression rightPredicate;
    private final Expression joinPredicate;
    private final Expression postJoinPredicate;

    public InnerJoinPushDownResult(
        Expression leftPredicate,
        Expression rightPredicate,
        Expression joinPredicate,
        Expression postJoinPredicate) {
      this.leftPredicate = leftPredicate;
      this.rightPredicate = rightPredicate;
      this.joinPredicate = joinPredicate;
      this.postJoinPredicate = postJoinPredicate;
    }

    public Expression getLeftPredicate() {
      return leftPredicate;
    }

    public Expression getRightPredicate() {
      return rightPredicate;
    }

    public Expression getJoinPredicate() {
      return joinPredicate;
    }

    public Expression getPostJoinPredicate() {
      return postJoinPredicate;
    }
  }

  static OuterJoinPushDownResult processLimitedOuterJoin(
      Metadata metadata,
      Expression inheritedPredicate,
      Expression outerEffectivePredicate,
      Expression innerEffectivePredicate,
      Expression joinPredicate,
      Collection<Symbol> outerSymbols,
      Collection<Symbol> innerSymbols) {
    checkArgument(
        outerSymbols.containsAll(extractUnique(outerEffectivePredicate)),
        "outerEffectivePredicate must only contain symbols from outerSymbols");
    checkArgument(
        innerSymbols.containsAll(extractUnique(innerEffectivePredicate)),
        "innerEffectivePredicate must only contain symbols from innerSymbols");

    ImmutableList.Builder<Expression> outerPushdownConjuncts = ImmutableList.builder();
    ImmutableList.Builder<Expression> innerPushdownConjuncts = ImmutableList.builder();
    ImmutableList.Builder<Expression> postJoinConjuncts = ImmutableList.builder();
    ImmutableList.Builder<Expression> joinConjuncts = ImmutableList.builder();

    // Strip out non-deterministic conjuncts
    extractConjuncts(inheritedPredicate).stream()
        .filter(expression -> !isDeterministic(expression))
        .forEach(postJoinConjuncts::add);
    inheritedPredicate = filterDeterministicConjuncts(inheritedPredicate);

    outerEffectivePredicate = filterDeterministicConjuncts(outerEffectivePredicate);
    innerEffectivePredicate = filterDeterministicConjuncts(innerEffectivePredicate);
    extractConjuncts(joinPredicate).stream()
        .filter(expression -> !isDeterministic(expression))
        .forEach(joinConjuncts::add);
    joinPredicate = filterDeterministicConjuncts(joinPredicate);

    // Generate equality inferences
    EqualityInference inheritedInference = new EqualityInference(metadata, inheritedPredicate);
    EqualityInference outerInference =
        new EqualityInference(metadata, inheritedPredicate, outerEffectivePredicate);

    Set<Symbol> innerScope = ImmutableSet.copyOf(innerSymbols);
    Set<Symbol> outerScope = ImmutableSet.copyOf(outerSymbols);

    EqualityInference.EqualityPartition equalityPartition =
        inheritedInference.generateEqualitiesPartitionedBy(outerScope);
    Expression outerOnlyInheritedEqualities =
        combineConjuncts(equalityPartition.getScopeEqualities());
    EqualityInference potentialNullSymbolInference =
        new EqualityInference(
            metadata,
            outerOnlyInheritedEqualities,
            outerEffectivePredicate,
            innerEffectivePredicate,
            joinPredicate);

    // Push outer and join equalities into the inner side. For example:
    // SELECT * FROM nation LEFT OUTER JOIN region ON nation.regionkey = region.regionkey and
    // nation.name = region.name WHERE nation.name = 'blah'

    EqualityInference potentialNullSymbolInferenceWithoutInnerInferred =
        new EqualityInference(
            metadata, outerOnlyInheritedEqualities, outerEffectivePredicate, joinPredicate);
    innerPushdownConjuncts.addAll(
        potentialNullSymbolInferenceWithoutInnerInferred
            .generateEqualitiesPartitionedBy(innerScope)
            .getScopeEqualities());

    EqualityInference.EqualityPartition joinEqualityPartition =
        new EqualityInference(metadata, joinPredicate).generateEqualitiesPartitionedBy(innerScope);
    innerPushdownConjuncts.addAll(joinEqualityPartition.getScopeEqualities());
    joinConjuncts
        .addAll(joinEqualityPartition.getScopeComplementEqualities())
        .addAll(joinEqualityPartition.getScopeStraddlingEqualities());

    // Add the equalities from the inferences back in
    outerPushdownConjuncts.addAll(equalityPartition.getScopeEqualities());
    postJoinConjuncts.addAll(equalityPartition.getScopeComplementEqualities());
    postJoinConjuncts.addAll(equalityPartition.getScopeStraddlingEqualities());

    // See if we can push inherited predicates down
    EqualityInference.nonInferrableConjuncts(metadata, inheritedPredicate)
        .forEach(
            conjunct -> {
              Expression outerRewritten = outerInference.rewrite(conjunct, outerScope);
              if (outerRewritten != null) {
                outerPushdownConjuncts.add(outerRewritten);

                // A conjunct can only be pushed down into an inner side if it can be rewritten in
                // terms of the outer side
                Expression innerRewritten =
                    potentialNullSymbolInference.rewrite(outerRewritten, innerScope);
                if (innerRewritten != null) {
                  innerPushdownConjuncts.add(innerRewritten);
                }
              } else {
                postJoinConjuncts.add(conjunct);
              }
            });

    // See if we can push down any outer effective predicates to the inner side
    EqualityInference.nonInferrableConjuncts(metadata, outerEffectivePredicate)
        .map(conjunct -> potentialNullSymbolInference.rewrite(conjunct, innerScope))
        .filter(Objects::nonNull)
        .forEach(innerPushdownConjuncts::add);

    // See if we can push down join predicates to the inner side
    EqualityInference.nonInferrableConjuncts(metadata, joinPredicate)
        .forEach(
            conjunct -> {
              Expression innerRewritten =
                  potentialNullSymbolInference.rewrite(conjunct, innerScope);
              if (innerRewritten != null) {
                innerPushdownConjuncts.add(innerRewritten);
              } else {
                joinConjuncts.add(conjunct);
              }
            });

    return new OuterJoinPushDownResult(
        combineConjuncts(outerPushdownConjuncts.build()),
        combineConjuncts(innerPushdownConjuncts.build()),
        combineConjuncts(joinConjuncts.build()),
        combineConjuncts(postJoinConjuncts.build()));
  }

  static class OuterJoinPushDownResult {
    private final Expression outerJoinPredicate;
    private final Expression innerJoinPredicate;
    private final Expression joinPredicate;
    private final Expression postJoinPredicate;

    private OuterJoinPushDownResult(
        Expression outerJoinPredicate,
        Expression innerJoinPredicate,
        Expression joinPredicate,
        Expression postJoinPredicate) {
      this.outerJoinPredicate = outerJoinPredicate;
      this.innerJoinPredicate = innerJoinPredicate;
      this.joinPredicate = joinPredicate;
      this.postJoinPredicate = postJoinPredicate;
    }

    public Expression getOuterJoinPredicate() {
      return outerJoinPredicate;
    }

    public Expression getInnerJoinPredicate() {
      return innerJoinPredicate;
    }

    public Expression getJoinPredicate() {
      return joinPredicate;
    }

    public Expression getPostJoinPredicate() {
      return postJoinPredicate;
    }
  }
}
