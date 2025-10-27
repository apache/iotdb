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
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.PatternRecognitionNode;
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
import static org.apache.iotdb.db.queryengine.plan.relational.planner.optimizations.ParallelizeGrouping.CanOptimized.ENABLE_PARALLEL;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.optimizations.ParallelizeGrouping.CanOptimized.PENDING;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.optimizations.ParallelizeGrouping.CanOptimized.TO_SORT;

/**
 * This rule is used to determine whether the GroupNode can be executed parallel.
 *
 * <p>Optimization phase: Logical plan planning.
 *
 * <p>
 *
 * <ul>
 *   The GroupNode can be paralleled if the lasted offspring that guarantees the data is grouped by
 *   PartitionKey or is sorted by PartitionKey. For example:
 *   <ul>
 *     <li>GroupNode[PK={device_id}, OK={time}] -> ... -> TableDeviceScanNode
 *     <li>GroupNode[PK={device_id}, OK={s1}] -> ... -> TableDeviceScanNode
 *     <li>GroupNode[PK={device_id,attr}, OK={time}] -> ... -> TableDeviceScanNode
 *     <li>GroupNode[PK={tag1,tag2}, OK={tag3}] -> SortNode[sort={tag1,tag2,tag3}]
 *     <li>GroupNode[PK={tag1,tag2}, OK={s1}] -> TopKNode[sort={tag1,tag2,tag3}]
 *     <li>GroupNode[PK={tag1,tag2}, OK={}] -> AggregationNode[group={tag1,tag2}]
 *     <li>GroupNode[PK={tag1,tag2}, OK={s1}] -> AggregationNode[group={tag1,tag2,s1}]
 *     <li>GroupNode[PK={tag1}, OK={}] -> TableFunctionNode[partition={tag1,tag2}]
 *   </ul>
 * </ul>
 *
 * <p>Otherwise, the GroupNode should be transformed into a SortNode.
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
        context.canOptimized = TO_SORT;
        return;
      }
      OrderingScheme prefix = context.sortKey;
      for (int i = 0; i < context.partitionKeyCount; i++) {
        Symbol lhs = prefix.getOrderBy().get(i);
        Symbol rhs = childOrder.get(i);
        if (!lhs.equals(rhs)) {
          context.canOptimized = TO_SORT;
          return;
        }
      }
      context.canOptimized = ENABLE_PARALLEL;
    }

    @Override
    public PlanNode visitGroup(GroupNode node, Context context) {
      checkPrefixMatch(
          context, node.getOrderingScheme().getOrderBy().subList(0, node.getPartitionKeyCount()));
      Context newContext = new Context(node.getOrderingScheme(), node.getPartitionKeyCount());
      PlanNode child = node.getChild().accept(this, newContext);
      switch (newContext.canOptimized) {
        case ENABLE_PARALLEL:
          GroupNode newNode = (GroupNode) node.clone();
          newNode.addChild(child);
          return newNode;
        case TO_SORT:
        default:
          return new SortNode(node.getPlanNodeId(), child, node.getOrderingScheme(), false, false);
      }
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
      context.canOptimized = TO_SORT;
      return visitPlan(node, context);
    }

    @Override
    public PlanNode visitCorrelatedJoin(CorrelatedJoinNode node, Context context) {
      context.canOptimized = TO_SORT;
      return visitPlan(node, context);
    }

    @Override
    public PlanNode visitSemiJoin(SemiJoinNode node, Context context) {
      context.canOptimized = TO_SORT;
      return visitPlan(node, context);
    }

    @Override
    public PlanNode visitTableFunctionProcessor(TableFunctionProcessorNode node, Context context) {
      if (!context.canSkip()) {
        if (node.getChildren().isEmpty()) {
          // leaf node
          context.canOptimized = TO_SORT;
          return node;
        }
        Optional<DataOrganizationSpecification> dataOrganizationSpecification =
            node.getDataOrganizationSpecification();
        if (!dataOrganizationSpecification.isPresent()) {
          context.canOptimized = TO_SORT;
        } else {
          checkPrefixMatch(context, dataOrganizationSpecification.get().getPartitionBy());
        }
      }
      return visitPlan(node, context);
    }

    @Override
    public PlanNode visitPatternRecognition(PatternRecognitionNode node, Context context) {
      if (!context.canSkip()) {
        if (node.getChildren().isEmpty()) {
          // leaf node
          context.canOptimized = TO_SORT;
          return node;
        }

        List<Symbol> partitionBy = node.getPartitionBy();
        Optional<OrderingScheme> orderingScheme = node.getOrderingScheme();

        if (!orderingScheme.isPresent()) {
          context.canOptimized = TO_SORT;
        } else {
          checkPrefixMatch(context, partitionBy);
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
        //  check there are no field in sortKey and all tags in sortKey
        Set<Symbol> tagSymbols =
            tableColumnSchema.entrySet().stream()
                .filter(entry -> entry.getValue().getColumnCategory() == TAG)
                .map(Map.Entry::getKey)
                .collect(Collectors.toSet());
        for (int i = 0; i < context.partitionKeyCount; i++) {
          Symbol symbol = sortKey.getOrderBy().get(i);
          if (!tableColumnSchema.containsKey(symbol)) {
            context.canOptimized = TO_SORT;
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
              context.canOptimized = TO_SORT;
              return node;
          }
        }
        if (!tagSymbols.isEmpty()) {
          context.canOptimized = TO_SORT;
        } else {
          context.canOptimized = ENABLE_PARALLEL;
        }
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
    private CanOptimized canOptimized = PENDING;

    private Context(OrderingScheme sortKey, int partitionKeyCount) {
      this.sortKey = sortKey;
      this.partitionKeyCount = partitionKeyCount;
    }

    private boolean canSkip() {
      return sortKey == null || canOptimized != PENDING;
    }
  }

  protected enum CanOptimized {
    ENABLE_PARALLEL,
    TO_SORT,
    PENDING
  }
}
