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

package org.apache.iotdb.db.queryengine.plan.relational.planner.optimizations;

import org.apache.iotdb.commons.schema.table.column.TsTableColumnCategory;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanVisitor;
import org.apache.iotdb.db.queryengine.plan.relational.analyzer.Analysis;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.ColumnSchema;
import org.apache.iotdb.db.queryengine.plan.relational.planner.OrderingScheme;
import org.apache.iotdb.db.queryengine.plan.relational.planner.Symbol;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.SortNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.StreamSortNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.TableScanNode;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.apache.iotdb.db.utils.constant.TestConstant.TIMESTAMP_STR;

/**
 * <b>Optimization phase:</b> Distributed plan planning.
 *
 * <p>This optimize rule implement the rules below.
 * <li>When order by time and there is only one device entry in TableScanNode below, the SortNode
 *     can be eliminated.
 * <li>When order by all IDColumns and time, the SortNode can be eliminated.
 */
public class SortElimination implements PlanOptimizer {

  @Override
  public PlanNode optimize(PlanNode plan, PlanOptimizer.Context context) {
    if (!context.getAnalysis().hasSortNode()) {
      return plan;
    }

    return plan.accept(new Rewriter(context.getAnalysis()), new Context());
  }

  private static class Rewriter extends PlanVisitor<PlanNode, Context> {
    private final Analysis analysis;

    public Rewriter(Analysis analysis) {
      this.analysis = analysis;
    }

    @Override
    public PlanNode visitPlan(PlanNode node, Context context) {
      PlanNode newNode = node.clone();
      for (PlanNode child : node.getChildren()) {
        newNode.addChild(child.accept(this, context));
      }
      return newNode;
    }

    @Override
    public PlanNode visitSort(SortNode node, Context context) {
      Context newContext = new Context();
      PlanNode child = node.getChild().accept(this, newContext);
      OrderingScheme orderingScheme = node.getOrderingScheme();
      TableScanNode tableScanNode = newContext.getTableScanNode();
      if (newContext.getTotalDeviceEntrySize() == 1
          && TIMESTAMP_STR.equalsIgnoreCase(orderingScheme.getOrderBy().get(0).getName())) {
        return child;
      }
      return tryRemoveSortWhenOrderByAllIDsAndTime(node, child, tableScanNode);
    }

    @Override
    public PlanNode visitStreamSort(StreamSortNode node, Context context) {
      PlanNode child = node.getChild().accept(this, context);
      TableScanNode tableScanNode = context.getTableScanNode();
      return tryRemoveSortWhenOrderByAllIDsAndTime(node, child, tableScanNode);
    }

    @Override
    public PlanNode visitTableScan(TableScanNode node, Context context) {
      context.addDeviceEntrySize(node.getDeviceEntries().size());
      context.setTableScanNode(node);
      return node;
    }

    private PlanNode tryRemoveSortWhenOrderByAllIDsAndTime(
        SortNode sortNode, PlanNode child, TableScanNode tableScanNode) {
      Set<Symbol> sortSymbolsBeforeTime = new HashSet<>();
      Map<Symbol, ColumnSchema> tableColumnSchema =
          analysis.getTableColumnSchema(tableScanNode.getQualifiedObjectName());
      for (Symbol orderBy : sortNode.getOrderingScheme().getOrderBy()) {
        if (!tableColumnSchema.containsKey(orderBy)
            || tableColumnSchema.get(orderBy).getColumnCategory()
                == TsTableColumnCategory.MEASUREMENT) {
          return sortNode;
        } else if (tableColumnSchema.get(orderBy).getColumnCategory()
            == TsTableColumnCategory.TIME) {
          break;
        } else {
          sortSymbolsBeforeTime.add(orderBy);
        }
      }

      for (Map.Entry<Symbol, ColumnSchema> entry : tableColumnSchema.entrySet()) {
        if (entry.getValue().getColumnCategory() == TsTableColumnCategory.ID
            && !sortSymbolsBeforeTime.contains(entry.getKey())) {
          return sortNode;
        }
      }

      return child;
    }
  }

  private static class Context {
    private int totalDeviceEntrySize = 0;
    private TableScanNode tableScanNode;

    Context() {}

    public void addDeviceEntrySize(int deviceEntrySize) {
      this.totalDeviceEntrySize += deviceEntrySize;
    }

    public int getTotalDeviceEntrySize() {
      return totalDeviceEntrySize;
    }

    public TableScanNode getTableScanNode() {
      return tableScanNode;
    }

    public void setTableScanNode(TableScanNode tableScanNode) {
      this.tableScanNode = tableScanNode;
    }
  }
}
