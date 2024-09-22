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
import org.apache.iotdb.db.queryengine.plan.relational.metadata.ColumnSchema;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.Metadata;
import org.apache.iotdb.db.queryengine.plan.relational.planner.Symbol;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.AggregationTableScanNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.TableScanNode;

import java.util.Map;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkState;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class ColumnReference implements RvalueMatcher {
  private final String tableName;
  private final String columnName;

  public ColumnReference(String tableName, String columnName) {
    this.tableName = requireNonNull(tableName, "tableName is null");
    this.columnName = requireNonNull(columnName, "columnName is null");
  }

  @Override
  public Optional<Symbol> getAssignedSymbol(
      PlanNode node, SessionInfo session, Metadata metadata, SymbolAliases symbolAliases) {
    String actualTableName;
    Map<Symbol, ColumnSchema> assignments;

    if (node instanceof TableScanNode) {
      TableScanNode tableScanNode = (TableScanNode) node;
      actualTableName = tableScanNode.getQualifiedObjectName().toString();
      assignments = tableScanNode.getAssignments();
    }
    /*else if (node instanceof IndexSourceNode indexSourceNode) {
        tableHandle = indexSourceNode.getTableHandle();
        assignments = indexSourceNode.getAssignments();
    }*/
    else {
      return Optional.empty();
    }

    // Wrong table -> doesn't match.
    if (!tableName.equalsIgnoreCase(actualTableName)) {
      return Optional.empty();
    }

    return getAssignedSymbol(assignments, node instanceof AggregationTableScanNode);
  }

  private Optional<Symbol> getAssignedSymbol(
      Map<Symbol, ColumnSchema> assignments, boolean isAggregationTableScan) {
    Optional<Symbol> result = Optional.empty();
    for (Map.Entry<Symbol, ColumnSchema> entry : assignments.entrySet()) {
      if (entry.getValue().getName().equals(columnName)) {
        checkState(
            !result.isPresent(),
            "Multiple ColumnHandles found for %s:%s in table scan assignments",
            tableName,
            columnName);
        result = Optional.of(entry.getKey());
      }
    }
    // we don't check the existence of column in AggregationTableScan
    if (isAggregationTableScan && !result.isPresent()) {
      result = Optional.of(Symbol.of(columnName));
    }
    return result;
  }

  @Override
  public String toString() {
    return format("Column %s:%s", tableName, columnName);
  }
}
