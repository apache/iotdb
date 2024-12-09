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

package org.apache.iotdb.db.queryengine.plan.relational.planner.iterative.rule;

import org.apache.iotdb.db.queryengine.plan.relational.planner.Assignments;
import org.apache.iotdb.db.queryengine.plan.relational.planner.Symbol;
import org.apache.iotdb.db.queryengine.plan.relational.planner.iterative.Lookup;
import org.apache.iotdb.db.queryengine.plan.relational.planner.iterative.Rule;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.CorrelatedJoinNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.JoinNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.ProjectNode;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Cast;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Expression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.IfExpression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.NullLiteral;
import org.apache.iotdb.db.queryengine.plan.relational.utils.matching.Captures;
import org.apache.iotdb.db.queryengine.plan.relational.utils.matching.Pattern;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import org.apache.tsfile.read.common.type.Type;

import java.util.Optional;

import static com.google.common.base.Preconditions.checkState;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.node.Patterns.CorrelatedJoin.correlation;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.node.Patterns.correlatedJoin;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.optimizations.QueryCardinalityUtil.extractCardinality;
import static org.apache.iotdb.db.queryengine.plan.relational.sql.ast.BooleanLiteral.TRUE_LITERAL;
import static org.apache.iotdb.db.queryengine.plan.relational.type.TypeSignatureTranslator.toSqlType;
import static org.apache.iotdb.db.queryengine.plan.relational.utils.matching.Pattern.empty;

public class TransformUncorrelatedSubqueryToJoin implements Rule<CorrelatedJoinNode> {
  private static final Pattern<CorrelatedJoinNode> PATTERN =
      correlatedJoin().with(empty(correlation()));

  @Override
  public Pattern<CorrelatedJoinNode> getPattern() {
    return PATTERN;
  }

  @Override
  public Result apply(CorrelatedJoinNode correlatedJoinNode, Captures captures, Context context) {
    // handle INNER and LEFT correlated join
    if (correlatedJoinNode.getJoinType() == JoinNode.JoinType.INNER
        || correlatedJoinNode.getJoinType() == JoinNode.JoinType.LEFT) {
      return Result.ofPlanNode(
          rewriteToJoin(
              correlatedJoinNode,
              correlatedJoinNode.getJoinType(),
              correlatedJoinNode.getFilter(),
              context.getLookup()));
    }

    checkState(
        correlatedJoinNode.getJoinType() == JoinNode.JoinType.RIGHT
            || correlatedJoinNode.getJoinType() == JoinNode.JoinType.FULL,
        "unexpected CorrelatedJoin type: " + correlatedJoinNode.getType());

    // handle RIGHT and FULL correlated join ON TRUE
    JoinNode.JoinType type;
    if (correlatedJoinNode.getJoinType() == JoinNode.JoinType.RIGHT) {
      type = JoinNode.JoinType.INNER;
    } else {
      type = JoinNode.JoinType.LEFT;
    }
    JoinNode joinNode = rewriteToJoin(correlatedJoinNode, type, TRUE_LITERAL, context.getLookup());

    if (correlatedJoinNode.getFilter().equals(TRUE_LITERAL)) {
      return Result.ofPlanNode(joinNode);
    }

    // handle RIGHT correlated join on condition other than TRUE
    if (correlatedJoinNode.getJoinType() == JoinNode.JoinType.RIGHT) {
      Assignments.Builder assignments = Assignments.builder();
      assignments.putIdentities(
          Sets.intersection(
              ImmutableSet.copyOf(correlatedJoinNode.getSubquery().getOutputSymbols()),
              ImmutableSet.copyOf(correlatedJoinNode.getOutputSymbols())));
      for (Symbol inputSymbol :
          Sets.intersection(
              ImmutableSet.copyOf(correlatedJoinNode.getInput().getOutputSymbols()),
              ImmutableSet.copyOf(correlatedJoinNode.getOutputSymbols()))) {
        Type inputType = context.getSymbolAllocator().getTypes().getTableModelType(inputSymbol);
        assignments.put(
            inputSymbol,
            new IfExpression(
                correlatedJoinNode.getFilter(),
                inputSymbol.toSymbolReference(),
                new Cast(new NullLiteral(), toSqlType(inputType))));
      }
      ProjectNode projectNode =
          new ProjectNode(context.getIdAllocator().genPlanNodeId(), joinNode, assignments.build());

      return Result.ofPlanNode(projectNode);
    }

    // no support for FULL correlated join on condition other than TRUE
    return Result.empty();
  }

  private JoinNode rewriteToJoin(
      CorrelatedJoinNode parent, JoinNode.JoinType type, Expression filter, Lookup lookup) {
    if (type == JoinNode.JoinType.LEFT
        && extractCardinality(parent.getSubquery(), lookup).isAtLeastScalar()
        && filter.equals(TRUE_LITERAL)) {
      // input rows will always be matched against subquery rows
      type = JoinNode.JoinType.INNER;
    }
    return new JoinNode(
        parent.getPlanNodeId(),
        type,
        parent.getInput(),
        parent.getSubquery(),
        ImmutableList.of(),
        parent.getInput().getOutputSymbols(),
        parent.getSubquery().getOutputSymbols(),
        filter.equals(TRUE_LITERAL) ? Optional.empty() : Optional.of(filter),
        Optional.empty());
  }
}
