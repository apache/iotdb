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

import org.apache.iotdb.commons.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.commons.queryengine.plan.planner.plan.node.process.SingleChildProcessNode;
import org.apache.iotdb.commons.queryengine.plan.relational.metadata.ColumnSchema;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.DataOrganizationSpecification;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.OrderingScheme;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.Symbol;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.node.AggregationNode;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.node.CorrelatedJoinNode;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.node.GroupNode;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.node.JoinNode;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.node.PatternRecognitionNode;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.node.RowNumberNode;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.node.SemiJoinNode;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.node.SortNode;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.node.StreamSortNode;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.node.TableFunctionProcessorNode;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.node.TopKNode;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.node.TopKRankingNode;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.node.UnionNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanVisitor;
import org.apache.iotdb.db.queryengine.plan.relational.analyzer.Analysis;
import org.apache.iotdb.db.queryengine.plan.relational.planner.TagColumnUtils;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.AggregationTableScanNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.DeviceTableScanNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.ExternalTsFileAggregationScanNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.ExternalTsFileScanNode;

import org.apache.ratis.thirdparty.com.google.common.collect.ImmutableSet;

import java.util.HashSet;
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
 *     <li>GroupNode[PK={tag1,tag2}, OK={s1}] -> TopKNode[sort={tag1,tag2,s1}]
 *     <li>GroupNode[PK={tag1,tag2}] -> AggregationNode[group={tag1,tag2}]
 *     <li>GroupNode[PK={tag1,tag2}] -> AggregationTableScanNode[group={tag1,tag2,s1},
 *         preGrouped={tag1,tag2}]
 *     <li>GroupNode[PK={tag1,tag2}] -> TableFunctionNode[partition={tag2,tag1}]
 *   </ul>
 * </ul>
 *
 * <p>The child states its guarantee in one of two forms, and they are not interchangeable:
 *
 * <ul>
 *   <li>One globally ordered stream ({@link SortNode}, {@link StreamSortNode}, {@link TopKNode}, or
 *       a {@link GroupNode} that degrades to a sort): clustered by any permutation of the first
 *       {@code partitionKeyCount} keys.
 *   <li>Per-instance partitioning or grouping (a group's partition keys, a table function's,
 *       pattern recognition's, row number's or ranking's PARTITION BY, a pushed-down aggregation
 *       scan's pre-grouped keys, an aggregation's grouping keys): an exact key set, or a smaller
 *       key set that still covers every TAG column, since such a partition is a single device and a
 *       device cannot span instances. A completed aggregation is also clustered by its exact
 *       grouping keys even when it hashed.
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

  private static class Rewriter implements PlanVisitor<PlanNode, Context> {
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
    public PlanNode visitUnion(UnionNode node, Context context) {
      if (node.getChildren().size() <= 1) {
        return visitPlan(node, context);
      }
      if (!context.canSkip()) {
        context.canOptimized = TO_SORT;
      }
      // Neither branch may establish a property for the interleaved Union output.
      return node.replaceChildren(
          node.getChildren().stream()
              .map(child -> child.accept(this, new Context(null, 0)))
              .collect(Collectors.toList()));
    }

    /**
     * Whether a single ordered stream satisfies the required partition keys. The stream is
     * clustered by any permutation of the first {@code partitionKeyCount} sort keys, since
     * partition columns are identified by value. The ORDER BY part needs no check: the group node
     * sorts within every partition run.
     */
    private void checkPrefixMatch(Context context, List<Symbol> childOrder) {
      if (context.canSkip()) {
        return;
      }
      int partitionKeyCount = context.partitionKeyCount;
      if (partitionKeyCount > childOrder.size()) {
        context.canOptimized = TO_SORT;
        return;
      }
      List<Symbol> requiredPrefix = context.sortKey.getOrderBy().subList(0, partitionKeyCount);
      List<Symbol> actualPrefix = childOrder.subList(0, partitionKeyCount);
      context.canOptimized =
          ImmutableSet.copyOf(requiredPrefix).equals(ImmutableSet.copyOf(actualPrefix))
              ? ENABLE_PARALLEL
              : TO_SORT;
    }

    /**
     * Whether per-instance partitioning satisfies the required partition keys. An exact key set is
     * always accepted, as is a smaller key set that still covers every TAG column when the instance
     * stream is ordered by a key set containing it: such a partition is a single device, and a
     * device cannot span instances (cross-region devices are merged by device order).
     */
    private void checkPartitionKeysMatch(
        Context context, List<Symbol> childPartitionKeys, Set<Symbol> tagSymbols) {
      if (context.canSkip()) {
        return;
      }
      List<Symbol> requiredKeys =
          context.sortKey.getOrderBy().subList(0, context.partitionKeyCount);
      context.canOptimized =
          sameKeys(requiredKeys, childPartitionKeys)
                  || splitsByDevice(requiredKeys, childPartitionKeys, tagSymbols)
              ? ENABLE_PARALLEL
              : TO_SORT;
    }

    /** Whether every partition of the required keys is a single device. */
    private static boolean splitsByDevice(
        List<Symbol> requiredKeys, List<Symbol> orderedKeys, Set<Symbol> tagSymbols) {
      return !tagSymbols.isEmpty()
          && requiredKeys.containsAll(tagSymbols)
          && ImmutableSet.copyOf(orderedKeys).containsAll(requiredKeys);
    }

    private static boolean sameKeys(List<Symbol> requiredKeys, List<Symbol> childKeys) {
      return requiredKeys.size() == childKeys.size()
          && ImmutableSet.copyOf(requiredKeys).equals(ImmutableSet.copyOf(childKeys));
    }

    /** Symbols of the TAG columns of the table scanned below the node, empty when unknown. */
    private Set<Symbol> tagSymbolsBelow(PlanNode node) {
      PlanNode cursor = node;
      while (!(cursor instanceof DeviceTableScanNode)) {
        if (cursor.getChildren().size() != 1) {
          return ImmutableSet.of();
        }
        cursor = cursor.getChildren().get(0);
      }
      if (cursor instanceof ExternalTsFileScanNode
          || cursor instanceof ExternalTsFileAggregationScanNode) {
        return ImmutableSet.of();
      }
      Set<Symbol> tagSymbols = new HashSet<>();
      for (Map.Entry<Symbol, ColumnSchema> entry :
          analysis
              .getTableColumnSchema(((DeviceTableScanNode) cursor).getQualifiedObjectName())
              .entrySet()) {
        if (entry.getValue().getColumnCategory() == TAG) {
          tagSymbols.add(entry.getKey());
        }
      }
      return tagSymbols;
    }

    @Override
    public PlanNode visitGroup(GroupNode node, Context context) {
      // A GroupNode without partition keys is a pure global ordering requirement.
      if (node.getPartitionKeyCount() == 0) {
        return new SortNode(
            node.getPlanNodeId(),
            node.getChild().accept(this, new Context(null, 0)),
            node.getOrderingScheme(),
            false,
            false);
      }

      // Decide the group's own fate first: the claim it makes to its ancestors depends on
      // whether it survives as a partitioned group or degrades to one globally ordered sort.
      Context newContext = new Context(node.getOrderingScheme(), node.getPartitionKeyCount());
      PlanNode child = node.getChild().accept(this, newContext);
      if (newContext.canOptimized == ENABLE_PARALLEL) {
        // The group survives and its output is only per-instance partitioned. When the ancestor's
        // claim cannot reuse it, TO_SORT is recorded on its context and the ancestor sorts this
        // group's output itself.
        checkPartitionKeysMatch(
            context,
            node.getOrderingScheme().getOrderBy().subList(0, node.getPartitionKeyCount()),
            tagSymbolsBelow(node));
        GroupNode newNode = (GroupNode) node.clone();
        newNode.addChild(child);
        return newNode;
      }
      // The group degrades to a sort, so its output is one stream ordered by the full scheme.
      checkPrefixMatch(context, node.getOrderingScheme().getOrderBy());
      return new SortNode(node.getPlanNodeId(), child, node.getOrderingScheme(), false, false);
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
    public PlanNode visitRowNumber(RowNumberNode node, Context context) {
      return visitRanking(node, context, node.getPartitionBy());
    }

    @Override
    public PlanNode visitTopKRanking(TopKRankingNode node, Context context) {
      return visitRanking(node, context, node.getSpecification().getPartitionBy());
    }

    /**
     * A row number or ranking forwards rows in input order and the distributed planner drops a sort
     * below it, so only a surviving group can satisfy an ancestor's claim. Nothing below may decide
     * the claim, so the child is processed first with it already rejected.
     */
    private PlanNode visitRanking(
        SingleChildProcessNode node, Context context, List<Symbol> partitionBy) {
      if (context.canSkip()) {
        return visitPlan(node, context);
      }
      context.canOptimized = TO_SORT;
      PlanNode child = node.getChild().accept(this, context);
      PlanNode result = node.clone();
      result.addChild(child);
      if (child instanceof GroupNode) {
        List<Symbol> requiredKeys =
            context.sortKey.getOrderBy().subList(0, context.partitionKeyCount);
        if (sameKeys(requiredKeys, partitionBy)
            || splitsByDevice(requiredKeys, partitionBy, tagSymbolsBelow(node))) {
          context.canOptimized = ENABLE_PARALLEL;
        }
      }
      return result;
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
          checkPartitionKeysMatch(
              context, dataOrganizationSpecification.get().getPartitionBy(), tagSymbolsBelow(node));
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
          checkPartitionKeysMatch(context, partitionBy, tagSymbolsBelow(node));
        }
      }
      return visitPlan(node, context);
    }

    @Override
    public PlanNode visitDeviceTableScan(DeviceTableScanNode node, Context context) {
      if (!context.canSkip()) {
        OrderingScheme sortKey = context.sortKey;
        Map<Symbol, ColumnSchema> tableColumnSchema;
        if (node instanceof ExternalTsFileScanNode) {
          tableColumnSchema =
              ((ExternalTsFileScanNode) node)
                  .getExternalTsFileQueryResource()
                  .getTableColumnSchema();
        } else if (node instanceof ExternalTsFileAggregationScanNode) {
          tableColumnSchema =
              ((ExternalTsFileAggregationScanNode) node)
                  .getExternalTsFileQueryResource()
                  .getTableColumnSchema();
        } else {
          tableColumnSchema = analysis.getTableColumnSchema(node.getQualifiedObjectName());
        }
        //  check there are no field in sortKey and all tags in sortKey
        for (int i = 0; i < context.partitionKeyCount; i++) {
          Symbol symbol = sortKey.getOrderBy().get(i);
          if (!tableColumnSchema.containsKey(symbol)) {
            context.canOptimized = TO_SORT;
            return node;
          }
          switch (tableColumnSchema.get(symbol).getColumnCategory()) {
            case TAG:
              break;
            case ATTRIBUTE:
              // If all tags in partition key, attributes must be the same in one partition.
              break;
            default:
              context.canOptimized = TO_SORT;
              return node;
          }
        }
        Set<String> tagColumnNames =
            TagColumnUtils.tagColumnNames(
                sortKey.getOrderBy().subList(0, context.partitionKeyCount), tableColumnSchema);
        context.canOptimized =
            TagColumnUtils.coversAllTagColumns(tableColumnSchema, tagColumnNames)
                ? ENABLE_PARALLEL
                : TO_SORT;
      }
      return node;
    }

    @Override
    public PlanNode visitAggregation(AggregationNode node, Context context) {
      // Exact set only: a hashed aggregation, or one whose pre-grouping is dropped in distributed
      // planning, does not keep device runs contiguous.
      checkPartitionKeysMatch(context, node.getGroupingKeys(), ImmutableSet.of());
      return visitPlan(node, context);
    }

    @Override
    public PlanNode visitAggregationTableScan(AggregationTableScanNode node, Context context) {
      if (!context.canSkip()) {
        // The scan emits rows ordered by the pre-grouped keys, so their exact set is also valid.
        List<Symbol> requiredKeys =
            context.sortKey.getOrderBy().subList(0, context.partitionKeyCount);
        context.canOptimized =
            sameKeys(requiredKeys, node.getGroupingKeys())
                    || sameKeys(requiredKeys, node.getPreGroupedSymbols())
                    || splitsByDevice(
                        requiredKeys, node.getPreGroupedSymbols(), tagSymbolsBelow(node))
                ? ENABLE_PARALLEL
                : TO_SORT;
      }
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
