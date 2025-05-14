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

import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.Metadata;
import org.apache.iotdb.db.queryengine.plan.relational.planner.Assignments;
import org.apache.iotdb.db.queryengine.plan.relational.planner.Symbol;
import org.apache.iotdb.db.queryengine.plan.relational.planner.iterative.Rule;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.AssignUniqueId;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.CorrelatedJoinNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.EnforceSingleRowNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.FilterNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.MarkDistinctNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.ProjectNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.optimizations.Cardinality;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Cast;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.FunctionCall;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.QualifiedName;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.SimpleCaseExpression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.StringLiteral;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.WhenClause;
import org.apache.iotdb.db.queryengine.plan.relational.utils.matching.Captures;
import org.apache.iotdb.db.queryengine.plan.relational.utils.matching.Pattern;

import com.google.common.collect.ImmutableList;

import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.PlanNodeSearcher.searchFrom;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.node.JoinNode.JoinType.INNER;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.node.JoinNode.JoinType.LEFT;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.node.Patterns.CorrelatedJoin.correlation;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.node.Patterns.CorrelatedJoin.filter;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.node.Patterns.correlatedJoin;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.optimizations.QueryCardinalityUtil.extractCardinality;
import static org.apache.iotdb.db.queryengine.plan.relational.sql.ast.BooleanLiteral.TRUE_LITERAL;
import static org.apache.iotdb.db.queryengine.plan.relational.type.TypeSignatureTranslator.toSqlType;
import static org.apache.iotdb.db.queryengine.plan.relational.utils.matching.Pattern.nonEmpty;
import static org.apache.iotdb.db.queryengine.transformation.dag.column.FailFunctionColumnTransformer.FAIL_FUNCTION_NAME;
import static org.apache.tsfile.read.common.type.BooleanType.BOOLEAN;
import static org.apache.tsfile.read.common.type.LongType.INT64;

/**
 * Scalar filter scan query is something like:
 *
 * <pre>
 *     SELECT a,b,c FROM rel WHERE a = correlated1 AND b = correlated2
 * </pre>
 *
 * <p>This optimizer can rewrite to mark distinct and filter over a left outer join:
 *
 * <p>From:
 *
 * <pre>
 * - CorrelatedJoin (with correlation list: [C])
 *   - (input) plan which produces symbols: [A, B, C]
 *   - (scalar subquery) Project F
 *     - Filter(D = C AND E > 5)
 *       - plan which produces symbols: [D, E, F]
 * </pre>
 *
 * to:
 *
 * <pre>
 * - Filter(CASE isDistinct WHEN true THEN true ELSE fail('Scalar sub-query has returned multiple rows'))
 *   - MarkDistinct(isDistinct)
 *     - CorrelatedJoin (with correlation list: [C])
 *       - AssignUniqueId(adds symbol U)
 *         - (input) plan which produces symbols: [A, B, C]
 *       - non scalar subquery
 * </pre>
 *
 * <p>This must be run after aggregation decorrelation rules.
 *
 * <p>This rule is used to support non-aggregation scalar subquery.
 */
public class TransformCorrelatedScalarSubquery implements Rule<CorrelatedJoinNode> {
  private static final Pattern<CorrelatedJoinNode> PATTERN =
      correlatedJoin().with(nonEmpty(correlation())).with(filter().equalTo(TRUE_LITERAL));

  private final Metadata metadata;

  public TransformCorrelatedScalarSubquery(Metadata metadata) {
    this.metadata = requireNonNull(metadata, "metadata is null");
  }

  @Override
  public Pattern<CorrelatedJoinNode> getPattern() {
    return PATTERN;
  }

  @Override
  public Result apply(CorrelatedJoinNode correlatedJoinNode, Captures captures, Context context) {
    // lateral references are only allowed for INNER or LEFT correlated join
    checkArgument(
        correlatedJoinNode.getJoinType() == INNER || correlatedJoinNode.getJoinType() == LEFT,
        "unexpected correlated join type: %s",
        correlatedJoinNode.getJoinType());
    PlanNode subquery = context.getLookup().resolve(correlatedJoinNode.getSubquery());

    if (!searchFrom(subquery, context.getLookup())
        .where(EnforceSingleRowNode.class::isInstance)
        .recurseOnlyWhen(ProjectNode.class::isInstance)
        .matches()) {
      return Result.empty();
    }

    PlanNode rewrittenSubquery =
        searchFrom(subquery, context.getLookup())
            .where(EnforceSingleRowNode.class::isInstance)
            .recurseOnlyWhen(ProjectNode.class::isInstance)
            .removeFirst();

    Cardinality subqueryCardinality = extractCardinality(rewrittenSubquery, context.getLookup());
    boolean producesAtMostOneRow = subqueryCardinality.isAtMostScalar();
    if (producesAtMostOneRow) {
      boolean producesSingleRow = subqueryCardinality.isScalar();
      return Result.ofPlanNode(
          new CorrelatedJoinNode(
              context.getIdAllocator().genPlanNodeId(),
              correlatedJoinNode.getInput(),
              rewrittenSubquery,
              correlatedJoinNode.getCorrelation(),
              // EnforceSingleRowNode guarantees that exactly single matching row is produced
              // for every input row (independently of correlated join type). Decorrelated plan
              // must preserve this semantics.
              producesSingleRow ? INNER : LEFT,
              correlatedJoinNode.getFilter(),
              correlatedJoinNode.getOriginSubquery()));
    }

    Symbol unique = context.getSymbolAllocator().newSymbol("unique", INT64);

    CorrelatedJoinNode rewrittenCorrelatedJoinNode =
        new CorrelatedJoinNode(
            context.getIdAllocator().genPlanNodeId(),
            new AssignUniqueId(
                context.getIdAllocator().genPlanNodeId(), correlatedJoinNode.getInput(), unique),
            rewrittenSubquery,
            correlatedJoinNode.getCorrelation(),
            LEFT,
            correlatedJoinNode.getFilter(),
            correlatedJoinNode.getOriginSubquery());

    Symbol isDistinct = context.getSymbolAllocator().newSymbol("is_distinct", BOOLEAN);
    MarkDistinctNode markDistinctNode =
        new MarkDistinctNode(
            context.getIdAllocator().genPlanNodeId(),
            rewrittenCorrelatedJoinNode,
            isDistinct,
            rewrittenCorrelatedJoinNode.getInput().getOutputSymbols(),
            Optional.empty());

    FilterNode filterNode =
        new FilterNode(
            context.getIdAllocator().genPlanNodeId(),
            markDistinctNode,
            new SimpleCaseExpression(
                isDistinct.toSymbolReference(),
                ImmutableList.of(new WhenClause(TRUE_LITERAL, TRUE_LITERAL)),
                new Cast(
                    new FunctionCall(
                        QualifiedName.of(FAIL_FUNCTION_NAME),
                        ImmutableList.of(
                            new StringLiteral("Scalar sub-query has returned multiple rows."))),
                    toSqlType(BOOLEAN))));

    return Result.ofPlanNode(
        new ProjectNode(
            context.getIdAllocator().genPlanNodeId(),
            filterNode,
            Assignments.identity(correlatedJoinNode.getOutputSymbols())));
  }
}
