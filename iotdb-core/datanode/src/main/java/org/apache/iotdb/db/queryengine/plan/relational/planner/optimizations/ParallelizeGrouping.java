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

import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanVisitor;
import org.apache.iotdb.db.queryengine.plan.relational.analyzer.Analysis;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.ColumnSchema;
import org.apache.iotdb.db.queryengine.plan.relational.planner.DataOrganizationSpecification;
import org.apache.iotdb.db.queryengine.plan.relational.planner.OrderingScheme;
import org.apache.iotdb.db.queryengine.plan.relational.planner.Symbol;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.AggregationNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.AggregationTableScanNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.CorrelatedJoinNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.DeviceTableScanNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.GroupNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.JoinNode;
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
import static org.apache.iotdb.db.queryengine.plan.relational.planner.optimizations.ParallelizeGrouping.CanParalleled.ENABLE;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.optimizations.ParallelizeGrouping.CanParalleled.PENDING;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.optimizations.ParallelizeGrouping.CanParalleled.UNABLE;

/**
 * This rule is used to determine whether the GroupNode can be optimized during logical plan.
 *
 * <p>Optimization phase: Logical plan planning.
 *
 * <ul>
 *   The GroupNode can be eliminated if the lasted offspring that guarantees the data is grouped by
 *   PartitionKey and ordered by OrderKey. For example:
 *   <ul>
 *     <li>GroupNode[PK={device_id}, OK={time}] -> ... -> TableDeviceScanNode
 *     <li>GroupNode[PK={device_id,attr}, OK={time}] -> ... -> TableDeviceScanNode
 *     <li>GroupNode[PK={tag1,tag2}, OK={tag3}] -> SortNode[sort={tag1,tag2,tag3}]
 *     <li>GroupNode[PK={tag1,tag2}, OK={tag3}] -> TopKNode[sort={tag1,tag2,tag3}]
 *     <li>GroupNode[PK={tag1,tag2}, OK={}] -> AggregationNode[group={tag1,tag2}]
 *     <li>GroupNode[PK={tag1}, OK={}] -> TableFunctionNode[partition={tag1,tag2}]
 *   </ul>
 * </ul>
 *
 * <ul>
 *   The GroupNode can be transformed into a StreamSortNode if the lasted offspring that guarantees
 *   the data is grouped by PartitionKey but not ordered by OrderKey. For example:
 *   <ul>
 *     <li>GroupNode[PK={device_id}, OK={s1}] -> ... -> TableDeviceScanNode
 *     <li>GroupNode[PK={device_id,attr}, OK={s1}] -> ... -> TableDeviceScanNode
 *     <li>GroupNode[PK={tag1,tag2}, OK={s1}] -> SortNode[sort={tag1,tag2,tag3}]
 *     <li>GroupNode[PK={tag1,tag2}, OK={s1}] -> TopKNode[sort={tag1,tag2,tag3}]
 *     <li>GroupNode[PK={tag1,tag2}, OK={s1}] -> AggregationNode[group={tag1,tag2,s1}]
 *   </ul>
 * </ul>
 *
 * <p>Otherwise, the GroupNode cannot be optimized. It will be transformed into a SortNode.
 */
public class ParallelizeGrouping implements PlanOptimizer {
  @Override
  public PlanNode optimize(PlanNode plan, PlanOptimizer.Context context) {
    if (!(context.getAnalysis().isQuery())) {
      return plan;
    }
    return plan.accept(new Rewriter(context.getAnalysis()), new Context(null, 0));
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

    /** We need to make sure: context#partitionKey can match the prefix of childOrderSchema */
    private void checkPrefixMatch(Context context, List<Symbol> childOrder) {
      if (context.canSkip()) {
        return;
      }
      if (context.partitionKeyCount > childOrder.size()) {
        context.canParalleled = UNABLE;
        return;
      }
      OrderingScheme prefix = context.sortKey;
      for (int i = 0; i < context.partitionKeyCount; i++) {
        Symbol lhs = prefix.getOrderBy().get(i);
        Symbol rhs = childOrder.get(i);
        if (!lhs.equals(rhs)) {
          context.canParalleled = UNABLE;
          return;
        }
      }
      context.canParalleled = ENABLE;
    }

    @Override
    public PlanNode visitGroup(GroupNode node, Context context) {
      checkPrefixMatch(context, node.getOrderingScheme().getOrderBy());
      Context newContext = new Context(node.getOrderingScheme(), node.getPartitionKeyCount());
      GroupNode newNode = (GroupNode) node.clone();
      newNode.addChild(node.getChild().accept(this, newContext));
      if (newContext.canParalleled.equals(ENABLE)) {
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
      context.canParalleled = UNABLE;
      return visitPlan(node, context);
    }

    @Override
    public PlanNode visitCorrelatedJoin(CorrelatedJoinNode node, Context context) {
      context.canParalleled = UNABLE;
      return visitPlan(node, context);
    }

    @Override
    public PlanNode visitSemiJoin(SemiJoinNode node, Context context) {
      context.canParalleled = UNABLE;
      return visitPlan(node, context);
    }

    @Override
    public PlanNode visitTableFunctionProcessor(TableFunctionProcessorNode node, Context context) {
      if (!context.canSkip()) {
        if (node.getChildren().isEmpty()) {
          // leaf node
          context.canParalleled = UNABLE;
          return node;
        }
        Optional<DataOrganizationSpecification> dataOrganizationSpecification =
            node.getDataOrganizationSpecification();
        if (!dataOrganizationSpecification.isPresent()) {
          context.canParalleled = UNABLE;
        } else {
          checkPrefixMatch(context, dataOrganizationSpecification.get().getPartitionBy());
        }
      }
      return visitPlan(node, context);
    }

    @Override
    public PlanNode visitDeviceTableScan(DeviceTableScanNode node, Context context) {
      if (!context.canSkip()) {
        OrderingScheme sortKey = context.sortKey;
        Map<Symbol, ColumnSchema> tableColumnSchema =
            analysis.getTableColumnSchema(node.getQualifiedObjectName());
        //        // 1. It is possible for the last sort key to be a time column
        //        if (sortKey.getOrderBy().size() > context.partitionKeyCount + 1) {
        //          context.canParalleled = UNABLE;
        //          return node;
        //        } else if (sortKey.getOrderBy().size() == context.partitionKeyCount + 1) {
        //          Symbol lastSymbol = sortKey.getOrderBy().get(context.partitionKeyCount);
        //          if (!tableColumnSchema.containsKey(lastSymbol)
        //              || tableColumnSchema.get(lastSymbol).getColumnCategory() != TIME) {
        //            context.canParalleled = UNABLE;
        //            return node;
        //          }
        //        }
        // 2. check there are no field in sortKey and all tags in sortKey
        Set<Symbol> tagSymbols =
            tableColumnSchema.entrySet().stream()
                .filter(entry -> entry.getValue().getColumnCategory() == TAG)
                .map(Map.Entry::getKey)
                .collect(Collectors.toSet());
        for (int i = 0; i < context.partitionKeyCount; i++) {
          Symbol symbol = sortKey.getOrderBy().get(i);
          if (!tableColumnSchema.containsKey(symbol)) {
            context.canParalleled = UNABLE;
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
              context.canParalleled = UNABLE;
              return node;
          }
        }
        if (!tagSymbols.isEmpty()) {
          context.canParalleled = UNABLE;
          return node;
        }
        context.canParalleled = ENABLE;
      }
      return node;
    }

    @Override
    public PlanNode visitAggregation(AggregationNode node, Context context) {
      checkPrefixMatch(context, node.getGroupingKeys());
      return visitPlan(node, context);
    }

    @Override
    public PlanNode visitAggregationTableScan(AggregationTableScanNode node, Context context) {
      checkPrefixMatch(context, node.getGroupingKeys());
      return node;
    }
  }

  private static class Context {
    private final OrderingScheme sortKey;
    private final int partitionKeyCount;
    private CanParalleled canParalleled = PENDING;

    private Context(OrderingScheme sortKey, int partitionKeyCount) {
      this.sortKey = sortKey;
      this.partitionKeyCount = partitionKeyCount;
    }

    private boolean canSkip() {
      return sortKey == null || canParalleled != PENDING;
    }
  }

  protected enum CanParalleled {
    ENABLE,
    UNABLE,
    PENDING
  }
}
