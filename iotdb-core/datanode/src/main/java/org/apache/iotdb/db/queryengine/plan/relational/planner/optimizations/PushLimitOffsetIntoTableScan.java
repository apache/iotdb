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
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.CollectNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.FilterNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.LimitNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.OffsetNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.OutputNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.ProjectNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.SortNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.StreamSortNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.TableScanNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.TopKNode;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Expression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Query;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.apache.iotdb.db.queryengine.plan.relational.planner.optimizations.PushPredicateIntoTableScan.containsDiffFunction;
import static org.apache.iotdb.db.utils.constant.TestConstant.TIMESTAMP_STR;

/**
 * <b>Optimization phase:</b> Distributed plan planning.
 *
 * <p>The LIMIT OFFSET condition can be pushed down to the TableScanNode, when the following
 * conditions are met:
 * <li>Time series query (not aggregation query).
 * <li>The query expressions are all scalar expression.
 * <li>Order by all IDs, limit can be pushed down, set pushDownToEachDevice==false
 * <li>Order by some IDs or order by time, limit can be pushed down, set pushDownToEachDevice==true
 */
public class PushLimitOffsetIntoTableScan implements PlanOptimizer {

  @Override
  public PlanNode optimize(PlanNode plan, PlanOptimizer.Context context) {
    if (!(context.getAnalysis().getStatement() instanceof Query)) {
      return plan;
    }

    return plan.accept(new Rewriter(), new Context(context.getAnalysis()));
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
    public PlanNode visitOutput(OutputNode node, Context context) {
      return visitPlan(node, context);
    }

    @Override
    public PlanNode visitLimit(LimitNode node, Context context) {
      context.setLimit(node.getCount());
      node.setChild(node.getChild().accept(this, context));
      return node;
    }

    @Override
    public PlanNode visitOffset(OffsetNode node, Context context) {
      context.setOffset(node.getCount());
      // already use rule {@link PushLimitThroughOffset}
      //      if (context.getLimit() > 0) {
      //        context.setLimit(context.getLimit() + context.getOffset());
      //      }
      node.setChild(node.getChild().accept(this, context));
      return node;
    }

    @Override
    public PlanNode visitCollect(CollectNode node, Context context) {
      PlanNode newNode = node.clone();
      for (PlanNode child : node.getChildren()) {
        newNode.addChild(child.accept(this, context));
      }
      return newNode;
    }

    @Override
    public PlanNode visitProject(ProjectNode node, Context context) {
      for (Expression expression : node.getAssignments().getMap().values()) {
        if (containsDiffFunction(expression)) {
          context.setEnablePushDown(false);
          return node;
        }
      }
      return visitPlan(node, context);
    }

    @Override
    public PlanNode visitSort(SortNode node, Context context) {
      Context newContext =
          new Context(
              context.analysis,
              context.getLimit(),
              context.getOffset(),
              context.isEnablePushDown(),
              context.canPushLimitToEachDevice());
      PlanNode child = node.getChild().accept(this, newContext);
      if (!newContext.isEnablePushDown()) {
        return node;
      }

      OrderingScheme orderingScheme = node.getOrderingScheme();
      TableScanNode tableScanNode = newContext.getTableScanNode();
      Map<Symbol, ColumnSchema> tableColumnSchema =
          context.getAnalysis().getTableColumnSchema(tableScanNode.getQualifiedObjectName());
      Set<Symbol> sortSymbols = new HashSet<>();
      for (Symbol orderBy : orderingScheme.getOrderBy()) {
        if (TIMESTAMP_STR.equalsIgnoreCase(orderBy.getName())) {
          break;
        }

        // order by measurement or expression, can not push down limit
        if (!tableColumnSchema.containsKey(orderBy)
            || tableColumnSchema.get(orderBy).getColumnCategory()
                == TsTableColumnCategory.MEASUREMENT) {
          tableScanNode.setPushDownLimit(0);
          tableScanNode.setPushDownOffset(0);
          return node;
        }

        sortSymbols.add(orderBy);
      }

      boolean pushLimitToEachDevice = false;
      for (Map.Entry<Symbol, ColumnSchema> entry : tableColumnSchema.entrySet()) {
        if (entry.getValue().getColumnCategory() == TsTableColumnCategory.ID
            && !sortSymbols.contains(entry.getKey())) {
          pushLimitToEachDevice = true;
          break;
        }
      }
      tableScanNode.setPushLimitToEachDevice(pushLimitToEachDevice);
      node.setChild(child);
      return node;
    }

    @Override
    public PlanNode visitTopK(TopKNode node, Context context) {
      throw new IllegalStateException(
          "TopKNode must be appeared after PushLimitOffsetIntoTableScan");
    }

    @Override
    public PlanNode visitStreamSort(StreamSortNode node, Context context) {
      return visitSort(node, context);
    }

    @Override
    public PlanNode visitFilter(FilterNode node, Context context) {
      // If there is still a FilterNode here, it means that there are read filter conditions that
      // cannot be pushed
      // down to TableScan.
      context.setEnablePushDown(false);
      return node;
    }

    @Override
    public PlanNode visitTableScan(TableScanNode node, Context context) {
      context.setTableScanNode(node);
      if (context.isEnablePushDown()) {
        if (context.getLimit() > 0) {
          node.setPushDownLimit(context.getLimit());
        }
        // TODO only one data region, pushDownOffset can be set
        //      if (context.getOffset() > 0) {
        //        node.setPushDownOffset(context.getOffset());
        //      }
        if (context.canPushLimitToEachDevice()) {
          node.setPushLimitToEachDevice(true);
        }
      }
      return node;
    }
  }

  private static class Context {
    private final Analysis analysis;
    private long limit;
    private long offset;
    private boolean enablePushDown = true;
    private boolean pushLimitToEachDevice = false;
    private TableScanNode tableScanNode;

    public Context(Analysis analysis) {
      this.analysis = analysis;
    }

    public Context(
        Analysis analysis,
        long limit,
        long offset,
        boolean enablePushDown,
        boolean pushLimitToEachDevice) {
      this.analysis = analysis;
      this.limit = limit;
      this.offset = offset;
      this.enablePushDown = enablePushDown;
      this.pushLimitToEachDevice = pushLimitToEachDevice;
    }

    public Analysis getAnalysis() {
      return analysis;
    }

    public long getLimit() {
      return limit;
    }

    public void setLimit(long limit) {
      this.limit = limit;
    }

    public long getOffset() {
      return offset;
    }

    public void setOffset(long offset) {
      this.offset = offset;
    }

    public boolean isEnablePushDown() {
      return enablePushDown;
    }

    public void setEnablePushDown(boolean enablePushDown) {
      this.enablePushDown = enablePushDown;
    }

    public boolean canPushLimitToEachDevice() {
      return pushLimitToEachDevice;
    }

    public void setPushLimitToE5achDevice(boolean pushLimitToEachDevice) {
      this.pushLimitToEachDevice = pushLimitToEachDevice;
    }

    public TableScanNode getTableScanNode() {
      return tableScanNode;
    }

    public void setTableScanNode(TableScanNode tableScanNode) {
      this.tableScanNode = tableScanNode;
    }
  }
}
