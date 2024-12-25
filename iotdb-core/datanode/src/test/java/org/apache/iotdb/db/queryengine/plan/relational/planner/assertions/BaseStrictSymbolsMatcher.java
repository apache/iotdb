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

import java.util.Set;
import java.util.function.Function;

import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

public abstract class BaseStrictSymbolsMatcher implements Matcher {
  private final Function<PlanNode, Set<Symbol>> getActual;

  public BaseStrictSymbolsMatcher(Function<PlanNode, Set<Symbol>> getActual) {
    this.getActual = requireNonNull(getActual, "getActual is null");
  }

  @Override
  public boolean shapeMatches(PlanNode node) {
    try {
      Set<Symbol> ignored = getActual.apply(node);
      return true;
    } catch (ClassCastException e) {
      return false;
    }
  }

  @Override
  public MatchResult detailMatches(
      PlanNode node, SessionInfo sessionInfo, Metadata metadata, SymbolAliases symbolAliases) {
    checkState(
        shapeMatches(node),
        "Plan testing framework error: shapeMatches returned false in detailMatches in %s",
        this.getClass().getName());
    return new MatchResult(
        getActual
            .apply(node)
            .equals(getExpectedSymbols(node, sessionInfo, metadata, symbolAliases)));
  }

  protected abstract Set<Symbol> getExpectedSymbols(
      PlanNode node, SessionInfo sessionInfo, Metadata metadata, SymbolAliases symbolAliases);
}
