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
import org.apache.iotdb.db.queryengine.plan.relational.planner.OrderingScheme;
import org.apache.iotdb.db.queryengine.plan.relational.planner.assertions.PlanMatchPattern.Ordering;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.LimitNode;

import com.google.common.collect.ImmutableList;

import java.util.List;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.assertions.MatchResult.NO_MATCH;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.assertions.MatchResult.match;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.assertions.Util.orderingSchemeMatches;

public class LimitMatcher implements Matcher {
  private final long limit;
  private final List<Ordering> tiesResolvers;
  // private final boolean partial;
  private final List<SymbolAlias> preSortedInputs;

  public LimitMatcher(long limit, List<Ordering> tiesResolvers, List<SymbolAlias> preSortedInputs) {
    this.limit = limit;
    this.tiesResolvers =
        ImmutableList.copyOf(requireNonNull(tiesResolvers, "tiesResolvers is null"));
    this.preSortedInputs =
        ImmutableList.copyOf(requireNonNull(preSortedInputs, "requiresPreSortedInputs is null"));
  }

  @Override
  public boolean shapeMatches(PlanNode node) {
    if (!(node instanceof LimitNode)) {
      return false;
    }
    LimitNode limitNode = (LimitNode) node;

    return limitNode.getCount() == limit
        && limitNode.isWithTies() == !tiesResolvers.isEmpty()
        // && limitNode.isPartial() == partial
        && limitNode.requiresPreSortedInputs() == !preSortedInputs.isEmpty();
  }

  @Override
  public MatchResult detailMatches(
      PlanNode node, SessionInfo sessionInfo, Metadata metadata, SymbolAliases symbolAliases) {
    checkState(shapeMatches(node));
    LimitNode limitNode = (LimitNode) node;

    if (!limitNode.isWithTies()) {
      return match();
    }
    OrderingScheme tiesResolvingScheme = limitNode.getTiesResolvingScheme().get();
    if (orderingSchemeMatches(tiesResolvers, tiesResolvingScheme, symbolAliases)) {
      return match();
    }

    if (!limitNode.requiresPreSortedInputs()) {
      return match();
    }
    /*if (preSortedInputs.stream()
            .map(alias -> alias.toSymbol(symbolAliases))
            .collect(toImmutableSet())
            .equals(ImmutableSet.copyOf(limitNode.getPreSortedInputs()))) {
        return match();
    }*/
    return NO_MATCH;
  }

  @Override
  public String toString() {
    return toStringHelper(this)
        .add("limit", limit)
        .add("tiesResolvers", tiesResolvers)
        // .add("partial", partial)
        .add("requiresPreSortedInputs", preSortedInputs)
        .toString();
  }
}
