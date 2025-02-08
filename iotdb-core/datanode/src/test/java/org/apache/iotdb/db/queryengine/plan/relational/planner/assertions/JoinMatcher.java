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
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.JoinNode;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Expression;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.errorprone.annotations.CanIgnoreReturnValue;

import java.util.List;
import java.util.Optional;
import java.util.Set;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static java.util.Objects.requireNonNull;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.assertions.MatchResult.NO_MATCH;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.assertions.PlanMatchPattern.equiJoinClause;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.assertions.PlanMatchPattern.node;

public final class JoinMatcher implements Matcher {
  private final JoinNode.JoinType joinType;
  private final List<ExpectedValueProvider<JoinNode.EquiJoinClause>> equiCriteria;
  private final boolean ignoreEquiCriteria;
  private final Optional<Expression> filter;

  JoinMatcher(
      JoinNode.JoinType joinType,
      List<ExpectedValueProvider<JoinNode.EquiJoinClause>> equiCriteria,
      boolean ignoreEquiCriteria,
      Optional<Expression> filter) {
    this.joinType = requireNonNull(joinType, "joinType is null");
    this.equiCriteria = requireNonNull(equiCriteria, "equiCriteria is null");
    if (ignoreEquiCriteria && !equiCriteria.isEmpty()) {
      throw new IllegalArgumentException("ignoreEquiCriteria passed with non-empty equiCriteria");
    }
    this.ignoreEquiCriteria = ignoreEquiCriteria;
    this.filter = requireNonNull(filter, "filter cannot be null");
  }

  @Override
  public boolean shapeMatches(PlanNode node) {
    if (!(node instanceof JoinNode)) {
      return false;
    }

    return ((JoinNode) node).getJoinType() == joinType;
  }

  @Override
  public MatchResult detailMatches(
      PlanNode node, SessionInfo sessionInfo, Metadata metadata, SymbolAliases symbolAliases) {
    checkState(
        shapeMatches(node),
        "Plan testing framework error: shapeMatches returned false in detailMatches in %s",
        this.getClass().getName());

    JoinNode joinNode = (JoinNode) node;

    if (!ignoreEquiCriteria && joinNode.getCriteria().size() != equiCriteria.size()) {
      return NO_MATCH;
    }

    if (filter.isPresent()) {
      if (!joinNode.getFilter().isPresent()) {
        return NO_MATCH;
      }
      if (!new ExpressionVerifier(symbolAliases)
          .process(joinNode.getFilter().get(), filter.get())) {
        return NO_MATCH;
      }
    } else {
      if (joinNode.getFilter().isPresent()) {
        return NO_MATCH;
      }
    }

    if (!ignoreEquiCriteria) {
      /*
       * Have to use order-independent comparison; there are no guarantees what order
       * the equi criteria will have after planning and optimizing.
       */
      Set<JoinNode.EquiJoinClause> actual = ImmutableSet.copyOf(joinNode.getCriteria());
      Set<JoinNode.EquiJoinClause> expected =
          equiCriteria.stream()
              .map(maker -> maker.getExpectedValue(symbolAliases))
              .collect(toImmutableSet());

      if (!expected.equals(actual)) {
        return NO_MATCH;
      }
    }

    return MatchResult.match();
  }

  @Override
  public String toString() {
    return toStringHelper(this)
        .omitNullValues()
        .add("type", joinType)
        .add("equiCriteria", equiCriteria)
        .add("filter", filter.orElse(null))
        .toString();
  }

  public static class Builder {
    private final JoinNode.JoinType joinType;
    private Optional<List<ExpectedValueProvider<JoinNode.EquiJoinClause>>> equiCriteria =
        Optional.empty();
    private PlanMatchPattern left;
    private PlanMatchPattern right;
    private Optional<Expression> filter = Optional.empty();
    private boolean ignoreEquiCriteria;

    public Builder(JoinNode.JoinType joinType) {
      this.joinType = joinType;
    }

    @CanIgnoreReturnValue
    public Builder equiCriteria(
        List<ExpectedValueProvider<JoinNode.EquiJoinClause>> expectedEquiCriteria) {
      this.equiCriteria = Optional.of(expectedEquiCriteria);

      return this;
    }

    @CanIgnoreReturnValue
    public Builder equiCriteria(String left, String right) {
      this.equiCriteria = Optional.of(ImmutableList.of(equiJoinClause(left, right)));

      return this;
    }

    @CanIgnoreReturnValue
    public Builder filter(Expression expectedFilter) {
      this.filter = Optional.of(expectedFilter);

      return this;
    }

    @CanIgnoreReturnValue
    public Builder left(PlanMatchPattern left) {
      this.left = left;

      return this;
    }

    @CanIgnoreReturnValue
    public Builder right(PlanMatchPattern right) {
      this.right = right;

      return this;
    }

    public Builder ignoreEquiCriteria() {
      this.ignoreEquiCriteria = true;
      return this;
    }

    public PlanMatchPattern build() {
      return node(JoinNode.class, left, right)
          .with(
              new JoinMatcher(
                  joinType, equiCriteria.orElse(ImmutableList.of()), ignoreEquiCriteria, filter));
    }
  }
}
