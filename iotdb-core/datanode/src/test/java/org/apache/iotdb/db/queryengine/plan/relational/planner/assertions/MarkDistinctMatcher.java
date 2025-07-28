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
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.MarkDistinctNode;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import java.util.List;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static java.util.Objects.requireNonNull;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.assertions.MatchResult.NO_MATCH;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.assertions.MatchResult.match;

public class MarkDistinctMatcher implements Matcher {
  private final PlanTestSymbol markerSymbol;
  private final List<PlanTestSymbol> distinctSymbols;
  private final Optional<PlanTestSymbol> hashSymbol;

  public MarkDistinctMatcher(
      PlanTestSymbol markerSymbol,
      List<PlanTestSymbol> distinctSymbols,
      Optional<PlanTestSymbol> hashSymbol) {
    this.markerSymbol = requireNonNull(markerSymbol, "markerSymbol is null");
    this.distinctSymbols = ImmutableList.copyOf(distinctSymbols);
    this.hashSymbol = requireNonNull(hashSymbol, "hashSymbol is null");
  }

  @Override
  public boolean shapeMatches(PlanNode node) {
    return node instanceof MarkDistinctNode;
  }

  @Override
  public MatchResult detailMatches(
      PlanNode node, SessionInfo session, Metadata metadata, SymbolAliases symbolAliases) {
    checkState(
        shapeMatches(node),
        "Plan testing framework error: shapeMatches returned false in detailMatches in %s",
        this.getClass().getName());
    MarkDistinctNode markDistinctNode = (MarkDistinctNode) node;

    if (!markDistinctNode
        .getHashSymbol()
        .equals(hashSymbol.map(alias -> alias.toSymbol(symbolAliases)))) {
      return NO_MATCH;
    }

    if (!ImmutableSet.copyOf(markDistinctNode.getDistinctSymbols())
        .equals(
            distinctSymbols.stream()
                .map(alias -> alias.toSymbol(symbolAliases))
                .collect(toImmutableSet()))) {
      return NO_MATCH;
    }

    return match(markerSymbol.toString(), markDistinctNode.getMarkerSymbol().toSymbolReference());
  }

  @Override
  public String toString() {
    return toStringHelper(this)
        .add("markerSymbol", markerSymbol)
        .add("distinctSymbols", distinctSymbols)
        .add("hashSymbol", hashSymbol)
        .toString();
  }
}
