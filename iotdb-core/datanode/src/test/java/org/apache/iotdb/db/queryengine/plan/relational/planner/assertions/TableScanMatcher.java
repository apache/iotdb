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
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.TableScanNode;

import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.assertions.MatchResult.NO_MATCH;

public abstract class TableScanMatcher implements Matcher {
  protected final String expectedTableName;
  protected final Optional<Boolean> hasTableLayout;
  // this field empty means no need to match
  protected final List<String> outputSymbols;
  // this field empty means no need to match
  protected Set<String> assignmentsKeys;

  public TableScanMatcher(
      String expectedTableName,
      Optional<Boolean> hasTableLayout,
      List<String> outputSymbols,
      Set<String> assignmentsKeys) {
    this.expectedTableName = requireNonNull(expectedTableName, "expectedTableName is null");
    this.hasTableLayout = requireNonNull(hasTableLayout, "hasTableLayout is null");
    this.outputSymbols = requireNonNull(outputSymbols, "outputSymbols is null");
    this.assignmentsKeys = requireNonNull(assignmentsKeys, "assignmentsKeys is null");
  }

  @Override
  public MatchResult detailMatches(
      PlanNode node, SessionInfo sessionInfo, Metadata metadata, SymbolAliases symbolAliases) {
    checkState(
        shapeMatches(node),
        "Plan testing framework error: shapeMatches returned false in detailMatches in %s",
        this.getClass().getName());

    TableScanNode tableScanNode = (TableScanNode) node;
    String actualTableName = tableScanNode.getQualifiedObjectName().toString();

    // TODO (https://github.com/trinodb/trino/issues/17) change to equals()
    if (!expectedTableName.equalsIgnoreCase(actualTableName)) {
      return NO_MATCH;
    }

    if (!outputSymbols.isEmpty()
        && !outputSymbols.equals(
            tableScanNode.getOutputSymbols().stream()
                .map(Symbol::getName)
                .collect(Collectors.toList()))) {
      return NO_MATCH;
    }

    if (!assignmentsKeys.isEmpty()
        && !assignmentsKeys.equals(
            tableScanNode.getAssignments().keySet().stream()
                .map(Symbol::getName)
                .collect(Collectors.toSet()))) {
      return NO_MATCH;
    }

    return new MatchResult(true);
  }
}
