/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.db.queryengine.plan.relational.planner.optimizations;

import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanGraphPrinter;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanVisitor;
import org.apache.iotdb.db.queryengine.plan.relational.analyzer.Analysis;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.ColumnSchema;
import org.apache.iotdb.db.queryengine.plan.relational.planner.DataOrganizationSpecification;
import org.apache.iotdb.db.queryengine.plan.relational.planner.OrderingScheme;
import org.apache.iotdb.db.queryengine.plan.relational.planner.Symbol;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.AggregationNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.AggregationTableScanNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.AuxSortNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.CorrelatedJoinNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.DeviceTableScanNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.JoinNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.ProjectNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.SemiJoinNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.SortNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.StreamSortNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.TableFunctionProcessorNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.TopKNode;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.iotdb.commons.schema.table.column.TsTableColumnCategory.TAG;
import static org.apache.iotdb.commons.schema.table.column.TsTableColumnCategory.TIME;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.optimizations.ParallelizeAuxSort.CanPushDown.ENABLE;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.optimizations.ParallelizeAuxSort.CanPushDown.PENDING;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.optimizations.ParallelizeAuxSort.CanPushDown.UNABLE;

public class ParallelizeAuxSort implements PlanOptimizer {
  @Override
  public PlanNode optimize(PlanNode plan, PlanOptimizer.Context context) {
    if (!(context.getAnalysis().isQuery())) {
      return plan;
    }
    System.out.println("before optimize ParallelizeAuxSort ==========================");
    PlanGraphPrinter.print(plan);
    PlanNode res = plan.accept(new Rewriter(context.getAnalysis()), new Context(null, 0));
    System.out.println("after optimize ParallelizeAuxSort ==========================");
    PlanGraphPrinter.print(res);
    return res;
    //        return plan.accept(new Rewriter(context.getAnalysis()), new Context());
  }

  private void print(PlanNode node) {
    PlanGraphPrinter.print(node);
    for (PlanNode child : node.getChildren()) {
      print(child);
    }
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

    /**
     * We need to make sure:
     *
     * <ul>
     *   <li>(1) All keys in context#orderKey are used for partition.
     *   <li>(2) childOrderSchema can match the prefix of context#orderKey, so that partition-based
     *       operation can be pushed down.
     * </ul>
     */
    private void checkPrefixMatch(Context context, List<Symbol> childOrder) {
      if (context.canSkip()) {
        return;
      }
      OrderingScheme prefix = context.orderKey;
      if (prefix.getOrderBy().size() != context.partitionKeyCount) {
        context.canPushDown = UNABLE;
        return;
      }
      if (prefix.getOrderBy().size() > childOrder.size()) {
        context.canPushDown = UNABLE;
        return;
      }
      for (int i = 0; i < prefix.getOrderBy().size(); i++) {
        Symbol lhs = prefix.getOrderBy().get(i);
        Symbol rhs = childOrder.get(i);
        if (!lhs.equals(rhs)) {
          context.canPushDown = UNABLE;
          return;
        }
      }
      context.canPushDown = ENABLE;
    }

    @Override
    public PlanNode visitAuxSort(AuxSortNode node, Context context) {
      checkPrefixMatch(context, node.getOrderingScheme().getOrderBy());
      Context newContext = new Context(node.getOrderingScheme(), node.getPartitionKeyCount());
      AuxSortNode newNode = (AuxSortNode) node.clone();
      newNode.addChild(node.getChild().accept(this, newContext));
      if (newContext.canPushDown.equals(ENABLE)) {
        newNode.setEnableParalleled(true);
      }
      return newNode;
    }

    @Override
    public PlanNode visitSort(SortNode node, Context context) {
      checkPrefixMatch(context, node.getOrderingScheme().getOrderBy());
      return visitPlan(node, context);
    }

    @Override
    public PlanNode visitStreamSort(StreamSortNode node, Context context) {
      checkPrefixMatch(context, node.getOrderingScheme().getOrderBy());
      return visitPlan(node, context);
    }

    @Override
    public PlanNode visitTopK(TopKNode node, Context context) {
      checkPrefixMatch(context, node.getOrderingScheme().getOrderBy());
      return visitPlan(node, context);
    }

    @Override
    public PlanNode visitJoin(JoinNode node, Context context) {
      context.canPushDown = UNABLE;
      return visitPlan(node, context);
    }

    @Override
    public PlanNode visitCorrelatedJoin(CorrelatedJoinNode node, Context context) {
      context.canPushDown = UNABLE;
      return visitPlan(node, context);
    }

    @Override
    public PlanNode visitSemiJoin(SemiJoinNode node, Context context) {
      context.canPushDown = UNABLE;
      return visitPlan(node, context);
    }

    @Override
    public PlanNode visitTableFunctionProcessor(TableFunctionProcessorNode node, Context context) {
      if (!context.canSkip()) {
        if (node.getChildren().isEmpty()) {
          // leaf node
          context.canPushDown = UNABLE;
          return node;
        }
        Optional<DataOrganizationSpecification> dataOrganizationSpecification =
            node.getDataOrganizationSpecification();
        if (!dataOrganizationSpecification.isPresent()) {
          context.canPushDown = UNABLE;
        } else {
          checkPrefixMatch(context, dataOrganizationSpecification.get().getPartitionBy());
        }
      }
      return visitPlan(node, context);
    }

    @Override
    public PlanNode visitProject(ProjectNode node, Context context) {
      if (!context.canSkip()) {
        OrderingScheme orderKey = context.orderKey;
        for (int i = 0; i < orderKey.getOrderBy().size(); i++) {
          if (!node.getAssignments().contains(orderKey.getOrderBy().get(i))) {
            context.canPushDown = UNABLE;
            break;
          }
        }
      }
      return visitPlan(node, context);
    }

    @Override
    public PlanNode visitDeviceTableScan(DeviceTableScanNode node, Context context) {
      if (!context.canSkip()) {
        OrderingScheme orderKey = context.orderKey;
        Map<Symbol, ColumnSchema> tableColumnSchema =
            analysis.getTableColumnSchema(node.getQualifiedObjectName());
        // 1. It is possible for the last sort key to be a time column
        if (orderKey.getOrderBy().size() > context.partitionKeyCount + 1) {
          context.canPushDown = UNABLE;
          return node;
        } else if (orderKey.getOrderBy().size() == context.partitionKeyCount + 1) {
          Symbol lastSymbol = orderKey.getOrderBy().get(context.partitionKeyCount);
          if (!tableColumnSchema.containsKey(lastSymbol)
              || tableColumnSchema.get(lastSymbol).getColumnCategory() != TIME) {
            context.canPushDown = UNABLE;
            return node;
          }
        }
        // 2. check there are no field in orderKey and all tags in orderKey
        Set<Symbol> tagSymbols =
            tableColumnSchema.entrySet().stream()
                .filter(entry -> entry.getValue().getColumnCategory() == TAG)
                .map(Map.Entry::getKey)
                .collect(Collectors.toSet());
        for (int i = 0; i < context.partitionKeyCount; i++) {
          Symbol symbol = orderKey.getOrderBy().get(i);
          if (!tableColumnSchema.containsKey(symbol)) {
            context.canPushDown = UNABLE;
            return node;
          }
          switch (tableColumnSchema.get(symbol).getColumnCategory()) {
            case TAG:
              tagSymbols.remove(symbol);
              break;
            case ATTRIBUTE:
              // If all tags in partition key, attributes must be the same in one partition.
              break;
            default:
              context.canPushDown = UNABLE;
              return node;
          }
        }
        if (!tagSymbols.isEmpty()) {
          context.canPushDown = UNABLE;
          return node;
        }
        context.canPushDown = ENABLE;
      }
      return node;
    }

    @Override
    public PlanNode visitAggregation(AggregationNode node, Context context) {
      return super.visitAggregation(node, context);
    }

    @Override
    public PlanNode visitAggregationTableScan(AggregationTableScanNode node, Context context) {
      return super.visitAggregationTableScan(node, context);
    }
  }

  private static class Context {
    private final OrderingScheme orderKey;
    private final int partitionKeyCount;
    private CanPushDown canPushDown = PENDING;

    private Context(OrderingScheme orderKey, int sortKeyOffset) {
      this.orderKey = orderKey;
      this.partitionKeyCount = sortKeyOffset;
    }

    private boolean canSkip() {
      return orderKey == null || canPushDown != PENDING;
    }
  }

  protected enum CanPushDown {
    ENABLE,
    UNABLE,
    PENDING
  }
}
