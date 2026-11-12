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
import org.apache.iotdb.commons.queryengine.plan.relational.metadata.ColumnSchema;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.DataOrganizationSpecification;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.OrderingScheme;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.Symbol;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.node.AggregationNode;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.node.CorrelatedJoinNode;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.node.GroupNode;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.node.JoinNode;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.node.PatternRecognitionNode;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.node.SemiJoinNode;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.node.SortNode;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.node.StreamSortNode;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.node.TableFunctionProcessorNode;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.node.TopKNode;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.node.UnionNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanVisitor;
import org.apache.iotdb.db.queryengine.plan.relational.analyzer.Analysis;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.AggregationTableScanNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.DeviceTableScanNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.ExternalTsFileAggregationScanNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.ExternalTsFileScanNode;

import com.google.common.collect.ImmutableSet;

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
 *     <li>GroupNode[PK={tag1,tag2}] -> TableFunctionNode[partition={tag2,tag1}]
 *   </ul>
 * </ul>
 *
 * <p>The child states its guarantee in one of two forms, and they are not interchangeable:
 *
 * <ul>
 *   <li>A single globally ordered stream ({@link SortNode}, {@link StreamSortNode}, {@link
 *       TopKNode}). Such a stream is clustered by every prefix of its sort keys, so a positional
 *       prefix match is enough ({@link Rewriter#checkPrefixMatch}).
 *   <li>Per-instance partitioning or grouping (a {@link GroupNode}'s own partition keys, a table
 *       function's or pattern recognition's PARTITION BY, an aggregation's grouping keys). This
 *       only says that each instance's stream is clustered by those keys: a required prefix may
 *       span several instances (several DataRegions, an unordered fan-in) and be interleaved, and a
 *       hash aggregation does not cluster its output at all. Only an exact key set makes the
 *       consumer's partitions the child's partitions ({@link Rewriter#checkPartitionKeysMatch}).
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
     * Whether the child's single ordered stream satisfies the required partition prefix.
     *
     * <p>Used when the child reports a globally sorted stream ({@link SortNode}, {@link
     * StreamSortNode}, {@link TopKNode}). Such a stream is clustered by every prefix of its sort
     * keys, so the prefix matches positionally. A permutation of the same key set also matches:
     * partition columns are identified by their values, and the group node re-establishes the
     * requested order inside every partition run.
     *
     * <p>The ORDER BY part of the requirement needs no check here. A group node sorts within every
     * partition run by its full ordering scheme, so the input only has to be clustered; whatever
     * order rows have inside a partition is re-established locally by the group's own sort.
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
      boolean sameKeySet =
          partitionKeyCount == childOrder.size()
              && ImmutableSet.copyOf(requiredPrefix).equals(ImmutableSet.copyOf(actualPrefix));
      context.canOptimized =
          sameKeySet || requiredPrefix.equals(actualPrefix) ? ENABLE_PARALLEL : TO_SORT;
    }

    /**
     * Whether the child's per-instance partitioning satisfies the required partition keys.
     *
     * <p>Used for partition and grouping claims: a {@link GroupNode}'s own partition keys, a table
     * function's or pattern recognition's PARTITION BY, and an aggregation's grouping keys.
     *
     * <p>Only an exact key set, in any order, is accepted. Each instance only guarantees that its
     * own stream is clustered by the full key set; a required prefix of those keys can be
     * interleaved across instances (several DataRegions, an unordered fan-in) or not clustered at
     * all (hash aggregation). Only when the consumer's partitions are the child's partitions can
     * the consumer be split per stream.
     */
    private void checkPartitionKeysMatch(Context context, List<Symbol> childPartitionKeys) {
      if (context.canSkip()) {
        return;
      }
      int partitionKeyCount = context.partitionKeyCount;
      List<Symbol> requiredKeys = context.sortKey.getOrderBy().subList(0, partitionKeyCount);
      context.canOptimized =
          partitionKeyCount == childPartitionKeys.size()
                  && ImmutableSet.copyOf(requiredKeys)
                      .equals(ImmutableSet.copyOf(childPartitionKeys))
              ? ENABLE_PARALLEL
              : TO_SORT;
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
        // The group survives and its output is only per-instance partitioned.
        checkPartitionKeysMatch(
            context, node.getOrderingScheme().getOrderBy().subList(0, node.getPartitionKeyCount()));
        if (!context.canSkip() && context.canOptimized != ENABLE_PARALLEL) {
          return new SortNode(node.getPlanNodeId(), child, node.getOrderingScheme(), false, false);
        }
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
          checkPartitionKeysMatch(context, dataOrganizationSpecification.get().getPartitionBy());
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
          checkPartitionKeysMatch(context, partitionBy);
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
      if (!context.canSkip()) {
        if (node.getPreGroupedSymbols().isEmpty()) {
          // A hash aggregation's output is not clustered by its grouping keys.
          context.canOptimized = TO_SORT;
        } else {
          checkPartitionKeysMatch(context, node.getGroupingKeys());
        }
      }
      return visitPlan(node, context);
    }

    @Override
    public PlanNode visitAggregationTableScan(AggregationTableScanNode node, Context context) {
      if (!context.canSkip()) {
        if (node.getPreGroupedSymbols().isEmpty()) {
          context.canOptimized = TO_SORT;
        } else {
          checkPartitionKeysMatch(context, node.getGroupingKeys());
        }
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
