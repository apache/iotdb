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
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Expression;

import com.google.common.collect.ImmutableList;

import java.util.List;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.assertions.MatchResult.NO_MATCH;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.assertions.MatchResult.match;

public class OutputMatcher implements Matcher {
  private final List<String> aliases;

  OutputMatcher(List<String> aliases) {
    this.aliases = ImmutableList.copyOf(requireNonNull(aliases, "aliases is null"));
  }

  @Override
  public boolean shapeMatches(PlanNode node) {
    return true;
  }

  @Override
  public MatchResult detailMatches(
      PlanNode node, SessionInfo sessionInfo, Metadata metadata, SymbolAliases symbolAliases) {
    int i = 0;
    for (String alias : aliases) {
      Expression expression = symbolAliases.get(alias);
      boolean found = false;
      while (i < node.getOutputSymbols().size()) {
        Symbol outputSymbol = node.getOutputSymbols().get(i++);
        if (expression.equals(outputSymbol.toSymbolReference())) {
          found = true;
          break;
        }
      }
      if (!found) {
        return NO_MATCH;
      }
    }
    return match();
  }

  @Override
  public String toString() {
    return toStringHelper(this).add("outputs", aliases).toString();
  }
}
