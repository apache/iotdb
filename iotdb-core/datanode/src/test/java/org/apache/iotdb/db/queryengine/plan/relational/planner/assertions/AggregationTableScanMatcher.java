/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.iotdb.db.queryengine.plan.relational.planner.assertions;

import org.apache.iotdb.db.queryengine.common.SessionInfo;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.Metadata;
import org.apache.iotdb.db.queryengine.plan.relational.planner.Symbol;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.AggregationNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.AggregationTableScanNode;

import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.assertions.MatchResult.NO_MATCH;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.assertions.MatchResult.match;

public class AggregationTableScanMatcher extends TableScanMatcher {
  private final PlanMatchPattern.GroupingSetDescriptor groupingSets;
  private final List<String> masks;
  private final List<String> preGroupedSymbols;
  private final Optional<Symbol> groupId;
  private final AggregationNode.Step step;

  public AggregationTableScanMatcher(
      PlanMatchPattern.GroupingSetDescriptor groupingSets,
      List<String> preGroupedSymbols,
      List<String> masks,
      Optional<Symbol> groupId,
      AggregationNode.Step step,
      String expectedTableName,
      Optional<Boolean> hasTableLayout,
      List<String> outputSymbols,
      Set<String> assignmentsKeys) {
    super(expectedTableName, hasTableLayout, outputSymbols, assignmentsKeys);
    this.groupingSets = groupingSets;
    this.masks = masks;
    this.preGroupedSymbols = preGroupedSymbols;
    this.groupId = groupId;
    this.step = step;
  }

  @Override
  public boolean shapeMatches(PlanNode node) {
    return node instanceof AggregationTableScanNode;
  }

  @Override
  public MatchResult detailMatches(
      PlanNode node, SessionInfo session, Metadata metadata, SymbolAliases symbolAliases) {
    checkState(
        shapeMatches(node),
        "Plan testing framework error: shapeMatches returned false in detailMatches in %s",
        this.getClass().getName());
    AggregationTableScanNode aggregationTableScanNode = (AggregationTableScanNode) node;

    String actualTableName = aggregationTableScanNode.getQualifiedObjectName().toString();

    // TODO (https://github.com/trinodb/trino/issues/17) change to equals()
    if (!expectedTableName.equalsIgnoreCase(actualTableName)) {
      return NO_MATCH;
    }

    if (!outputSymbols.isEmpty()
        && !outputSymbols.equals(
            aggregationTableScanNode.getOutputSymbols().stream()
                .map(Symbol::getName)
                .collect(Collectors.toList()))) {
      return NO_MATCH;
    }

    if (!assignmentsKeys.isEmpty()
        && !assignmentsKeys.equals(
            aggregationTableScanNode.getAssignments().keySet().stream()
                .map(Symbol::getName)
                .collect(Collectors.toSet()))) {
      return NO_MATCH;
    }

    /*if (groupId.isPresent() != aggregationNode.getGroupIdSymbol().isPresent()) {
      return NO_MATCH;
    }*/

    if (!groupingSets
        .getGroupingKeys()
        .equals(
            aggregationTableScanNode.getGroupingKeys().stream()
                .map(Symbol::getName)
                .collect(Collectors.toList()))) {
      return NO_MATCH;
    }

    if (groupingSets.getGroupingSetCount() != aggregationTableScanNode.getGroupingSetCount()) {
      return NO_MATCH;
    }

    if (!groupingSets
        .getGlobalGroupingSets()
        .equals(aggregationTableScanNode.getGlobalGroupingSets())) {
      return NO_MATCH;
    }

    Set<Symbol> actualMasks =
        aggregationTableScanNode.getAggregations().values().stream()
            .filter(aggregation -> aggregation.getMask().isPresent())
            .map(aggregation -> aggregation.getMask().get())
            .collect(toImmutableSet());

    Set<Symbol> expectedMasks = masks.stream().map(Symbol::new).collect(toImmutableSet());

    if (!actualMasks.equals(expectedMasks)) {
      return NO_MATCH;
    }

    if (step != aggregationTableScanNode.getStep()) {
      return NO_MATCH;
    }

    if (!preGroupedSymbols.equals(
        aggregationTableScanNode.getPreGroupedSymbols().stream()
            .map(Symbol::getName)
            .collect(Collectors.toList()))) {
      return NO_MATCH;
    }

    if (!preGroupedSymbols.isEmpty() && !aggregationTableScanNode.isStreamable()) {
      return NO_MATCH;
    }

    return match();
  }

  static boolean matches(
      Collection<String> expectedAliases,
      Collection<Symbol> actualSymbols,
      SymbolAliases symbolAliases) {
    if (expectedAliases.size() != actualSymbols.size()) {
      return false;
    }

    List<Symbol> expectedSymbols =
        expectedAliases.stream()
            .map(alias -> new Symbol(symbolAliases.get(alias).getName()))
            .collect(toImmutableList());
    for (Symbol symbol : expectedSymbols) {
      if (!actualSymbols.contains(symbol)) {
        return false;
      }
    }
    return true;
  }

  @Override
  public String toString() {
    return toStringHelper(this)
        .add("groupingSets", groupingSets)
        .add("preGroupedSymbols", preGroupedSymbols)
        .add("masks", masks)
        .add("groupId", groupId)
        .add("step", step)
        .omitNullValues()
        .add("expectedTableName", expectedTableName)
        .add("hasTableLayout", hasTableLayout.orElse(null))
        .add("outputSymbols", outputSymbols)
        .add("originalTableAssignmentsKeys", assignmentsKeys)
        .toString();
  }
}
