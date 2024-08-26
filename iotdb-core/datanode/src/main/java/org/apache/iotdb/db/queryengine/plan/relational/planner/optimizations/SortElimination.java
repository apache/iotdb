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

import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanVisitor;
import org.apache.iotdb.db.queryengine.plan.relational.planner.OrderingScheme;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.SortNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.StreamSortNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.TableScanNode;

import static org.apache.iotdb.db.utils.constant.TestConstant.TIMESTAMP_STR;

/**
 * <b>Optimization phase:</b> Distributed plan planning.
 *
 * <p>This optimize rule implement the rules below.
 * <li>When order by time and there is only one device entry in TableScanNode below, the SortNode
 *     can be eliminated.
 * <li>When order by all IDColumns and time, the SortNode can be eliminated.
 * <li>When StreamSortIndex==OrderBy size()-1, remove this StreamSortNode
 */
public class SortElimination implements PlanOptimizer {

  @Override
  public PlanNode optimize(PlanNode plan, PlanOptimizer.Context context) {
    if (!context.getAnalysis().hasSortNode()) {
      return plan;
    }

    return plan.accept(new Rewriter(), new Context());
  }

  private static class Rewriter extends PlanVisitor<PlanNode, Context> {
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
      if (newContext.getTotalDeviceEntrySize() == 1
          && TIMESTAMP_STR.equalsIgnoreCase(orderingScheme.getOrderBy().get(0).getName())) {
        return child;
      }

      return node.isOrderByAllIdsAndTime() ? child : node;
    }

    @Override
    public PlanNode visitStreamSort(StreamSortNode node, Context context) {
      PlanNode child = node.getChild().accept(this, context);
      return node.isOrderByAllIdsAndTime()
              || node.getStreamCompareKeyEndIndex()
                  == node.getOrderingScheme().getOrderBy().size() - 1
          ? child
          : node;
    }

    @Override
    public PlanNode visitTableScan(TableScanNode node, Context context) {
      context.addDeviceEntrySize(node.getDeviceEntries().size());
      return node;
    }
  }

  private static class Context {
    private int totalDeviceEntrySize = 0;

    Context() {}

    public void addDeviceEntrySize(int deviceEntrySize) {
      this.totalDeviceEntrySize += deviceEntrySize;
    }

    public int getTotalDeviceEntrySize() {
      return totalDeviceEntrySize;
    }
  }
}
