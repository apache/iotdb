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
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.AggregationNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.FilterNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.GapFillNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.JoinNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.LimitNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.LinearFillNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.PreviousFillNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.ProjectNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.SortNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.StreamSortNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.TableScanNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.TopKNode;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Expression;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.apache.iotdb.db.queryengine.plan.relational.planner.optimizations.PushPredicateIntoTableScan.containsDiffFunction;

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
    if (!(context.getAnalysis().isQuery())) {
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
    public PlanNode visitJoin(JoinNode node, Context context) {
      PlanNode leftChild = node.getLeftChild().accept(this, new Context());
      PlanNode rightChild = node.getRightChild().accept(this, new Context());
      node.setLeftChild(leftChild);
      node.setRightChild(rightChild);

      // TODO(beyyes) optimize for outer, left, right join
      context.enablePushDown = false;

      return node;
    }

    @Override
    public PlanNode visitFilter(FilterNode node, Context context) {
      // In Filter-TableScan and Filter-Project-TableScan case, limit can not be pushed down.
      // In later, we need consider other case such as Filter-Values.
      // FilterNode in outer query can not be pushed down.
      if (node.getChild() instanceof TableScanNode
          || (node.getChild() instanceof ProjectNode
              && ((ProjectNode) node.getChild()).getChild() instanceof TableScanNode)) {
        context.enablePushDown = false;
        return node;
      }
      node.setChild(node.getChild().accept(this, context));
      return node;
    }

    @Override
    public PlanNode visitProject(ProjectNode node, Context context) {
      for (Expression expression : node.getAssignments().getMap().values()) {
        if (containsDiffFunction(expression)) {
          context.enablePushDown = false;
          return node;
        }
      }
      node.setChild(node.getChild().accept(this, context));
      return node;
    }

    @Override
    public PlanNode visitGapFill(GapFillNode node, Context context) {
      context.enablePushDown = false;
      return node;
    }

    @Override
    public PlanNode visitLinearFill(LinearFillNode node, Context context) {
      context.enablePushDown = false;
      return node;
    }

    @Override
    public PlanNode visitPreviousFill(PreviousFillNode node, Context context) {
      if (node.getGroupingKeys().isPresent()) {
        context.enablePushDown = false;
        return node;
      } else {
        PlanNode newNode = node.clone();
        for (PlanNode child : node.getChildren()) {
          newNode.addChild(child.accept(this, context));
        }
        return newNode;
      }
    }

    @Override
    public PlanNode visitLimit(LimitNode node, Context context) {
      Context subContext = new Context();
      node.setChild(node.getChild().accept(this, subContext));
      context.existLimitNode = true;
      if (!subContext.enablePushDown || subContext.existLimitNode) {
        context.enablePushDown = false;
        return node;
      } else {
        TableScanNode tableScanNode = subContext.tableScanNode;
        context.tableScanNode = tableScanNode;
        if (tableScanNode != null) {
          tableScanNode.setPushDownLimit(node.getCount());
        }
        return node;
      }
    }

    @Override
    public PlanNode visitSort(SortNode node, Context context) {
      Context subContext = new Context();
      node.setChild(node.getChild().accept(this, subContext));
      context.existSortNode = true;
      // Children of SortNode have SortNode or LimitNode, set enablePushDown==false.
      // In later, there will have more Nodes to perfect this judgement.
      if (!subContext.enablePushDown || subContext.existSortNode || subContext.existLimitNode) {
        context.enablePushDown = false;
        return node;
      }

      TableScanNode tableScanNode = subContext.tableScanNode;
      context.tableScanNode = tableScanNode;
      OrderingScheme orderingScheme = node.getOrderingScheme();
      Map<Symbol, ColumnSchema> tableColumnSchema =
          analysis.getTableColumnSchema(tableScanNode.getQualifiedObjectName());
      Set<Symbol> sortSymbols = new HashSet<>();
      for (Symbol orderBy : orderingScheme.getOrderBy()) {
        if (tableScanNode.isTimeColumn(orderBy)) {
          break;
        }

        // order by measurement or expression, can not push down limit
        if (!tableColumnSchema.containsKey(orderBy)
            || tableColumnSchema.get(orderBy).getColumnCategory()
                == TsTableColumnCategory.MEASUREMENT) {
          context.enablePushDown = false;
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
      return node;
    }

    @Override
    public PlanNode visitStreamSort(StreamSortNode node, Context context) {
      return visitSort(node, context);
    }

    @Override
    public PlanNode visitAggregation(AggregationNode node, Context context) {
      context.enablePushDown = false;
      return node;
    }

    @Override
    public PlanNode visitTableScan(TableScanNode node, Context context) {
      context.tableScanNode = node;
      return node;
    }

    @Override
    public PlanNode visitTopK(TopKNode node, Context context) {
      throw new IllegalStateException(
          "TopKNode must be appeared after PushLimitOffsetIntoTableScan");
    }
  }

  private static class Context {
    // means if limit and offset can be pushed down into TableScanNode
    private boolean enablePushDown = true;
    private TableScanNode tableScanNode;
    private boolean existSortNode = false;
    private boolean existLimitNode = false;
  }
}
