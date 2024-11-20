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
package org.apache.iotdb.db.queryengine.plan.relational.planner.iterative.rule;

import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.ColumnSchema;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.Metadata;
import org.apache.iotdb.db.queryengine.plan.relational.planner.Symbol;
import org.apache.iotdb.db.queryengine.plan.relational.planner.SymbolsExtractor;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.AggregationTableScanNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.TableScanNode;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static java.util.Objects.requireNonNull;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.node.Patterns.tableScan;

/** This is a special case of PushProjectionIntoTableScan that performs only column pruning. */
public class PruneTableScanColumns extends ProjectOffPushDownRule<TableScanNode> {
  private final Metadata metadata;

  public PruneTableScanColumns(Metadata metadata) {
    super(tableScan());
    this.metadata = requireNonNull(metadata, "metadata is null");
  }

  @Override
  protected Optional<PlanNode> pushDownProjectOff(
      Context context, TableScanNode node, Set<Symbol> referencedOutputs) {
    return pruneColumns(node, referencedOutputs);
  }

  public static Optional<PlanNode> pruneColumns(TableScanNode node, Set<Symbol> referencedOutputs) {
    if (node instanceof AggregationTableScanNode) {
      return Optional.empty();
    }
    List<Symbol> newOutputs = new ArrayList<>();
    Map<Symbol, ColumnSchema> newAssignments = new LinkedHashMap<>();
    for (Symbol symbol : node.getOutputSymbols()) {
      if (referencedOutputs.contains(symbol)) {
        newOutputs.add(symbol);
        newAssignments.put(symbol, node.getAssignments().get(symbol));
      }
    }
    if (newOutputs.size() == node.getOutputSymbols().size()) {
      return Optional.empty();
    }

    // add entry in PushDownPredicate
    if (node.getPushDownPredicate() != null) {
      SymbolsExtractor.extractUnique(node.getPushDownPredicate())
          .forEach(symbol -> newAssignments.put(symbol, node.getAssignments().get(symbol)));
    }

    // add time entry if TimePredicate exists
    node.getTimePredicate()
        .ifPresent(
            timePredicate ->
                SymbolsExtractor.extractUnique(timePredicate)
                    .forEach(
                        symbol -> newAssignments.put(symbol, node.getAssignments().get(symbol))));

    return Optional.of(
        new TableScanNode(
            node.getPlanNodeId(),
            node.getQualifiedObjectName(),
            newOutputs,
            newAssignments,
            node.getDeviceEntries(),
            node.getIdAndAttributeIndexMap(),
            node.getScanOrder(),
            node.getTimePredicate().orElse(null),
            node.getPushDownPredicate(),
            node.getPushDownLimit(),
            node.getPushDownOffset(),
            node.isPushLimitToEachDevice()));
  }
}
