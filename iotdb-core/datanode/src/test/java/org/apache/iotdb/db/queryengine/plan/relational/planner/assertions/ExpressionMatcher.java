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

package org.apache.iotdb.db.queryengine.plan.relational.planner.assertions;

import org.apache.iotdb.db.queryengine.common.SessionInfo;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.Metadata;
import org.apache.iotdb.db.queryengine.plan.relational.planner.Symbol;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.ProjectNode;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Expression;

import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

public class ExpressionMatcher implements RvalueMatcher {
  private final String sql;
  private final Expression expression;

  ExpressionMatcher(Expression expression) {
    this.expression = requireNonNull(expression, "expression is null");
    this.sql = expression.toString();
  }

  @Override
  public Optional<Symbol> getAssignedSymbol(
      PlanNode node, SessionInfo sessionInfo, Metadata metadata, SymbolAliases symbolAliases) {
    Optional<Symbol> result = Optional.empty();
    ImmutableList.Builder<Expression> matchesBuilder = ImmutableList.builder();
    Map<Symbol, Expression> assignments = getAssignments(node);

    if (assignments == null) {
      return result;
    }

    ExpressionVerifier verifier = new ExpressionVerifier(symbolAliases);

    for (Map.Entry<Symbol, Expression> assignment : assignments.entrySet()) {
      if (verifier.process(assignment.getValue(), expression)) {
        result = Optional.of(assignment.getKey());
        matchesBuilder.add(assignment.getValue());
      }
    }

    List<Expression> matches = matchesBuilder.build();
    checkState(
        matches.size() < 2,
        "Ambiguous expression %s matches multiple assignments: %s",
        expression,
        matches);
    return result;
  }

  private static Map<Symbol, Expression> getAssignments(PlanNode node) {
    if (node instanceof ProjectNode) {

      return ((ProjectNode) node).getAssignments().getMap();
    }
    return null;
  }

  @Override
  public String toString() {
    return sql;
  }
}
