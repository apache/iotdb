/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.db.queryengine.plan.relational.planner.distribute;

import org.apache.iotdb.common.rpc.thrift.TDataNodeLocation;
import org.apache.iotdb.common.rpc.thrift.TRegionReplicaSet;
import org.apache.iotdb.common.rpc.thrift.TSeriesPartitionSlot;
import org.apache.iotdb.common.rpc.thrift.TTimePartitionSlot;
import org.apache.iotdb.commons.exception.IoTDBRuntimeException;
import org.apache.iotdb.commons.partition.DataPartition;
import org.apache.iotdb.commons.partition.SchemaPartition;
import org.apache.iotdb.commons.schema.table.TsTable;
import org.apache.iotdb.commons.schema.table.column.TsTableColumnCategory;
import org.apache.iotdb.commons.utils.TimePartitionUtils;
import org.apache.iotdb.db.exception.sql.SemanticException;
import org.apache.iotdb.db.queryengine.common.MPPQueryContext;
import org.apache.iotdb.db.queryengine.common.QueryId;
import org.apache.iotdb.db.queryengine.plan.ClusterTopology;
import org.apache.iotdb.db.queryengine.plan.planner.TableOperatorGenerator;
import org.apache.iotdb.db.queryengine.plan.planner.distribution.NodeDistribution;
import org.apache.iotdb.db.queryengine.plan.planner.exceptions.RootFIPlacementException;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanVisitor;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.WritePlanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.SingleChildProcessNode;
import org.apache.iotdb.db.queryengine.plan.relational.analyzer.Analysis;
import org.apache.iotdb.db.queryengine.plan.relational.function.BoundSignature;
import org.apache.iotdb.db.queryengine.plan.relational.function.FunctionId;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.AlignedDeviceEntry;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.ColumnSchema;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.DeviceEntry;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.QualifiedObjectName;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.ResolvedFunction;
import org.apache.iotdb.db.queryengine.plan.relational.planner.Assignments;
import org.apache.iotdb.db.queryengine.plan.relational.planner.OrderingScheme;
import org.apache.iotdb.db.queryengine.plan.relational.planner.SortOrder;
import org.apache.iotdb.db.queryengine.plan.relational.planner.Symbol;
import org.apache.iotdb.db.queryengine.plan.relational.planner.SymbolAllocator;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.AggregationNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.AggregationTableScanNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.AggregationTreeDeviceViewScanNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.AssignUniqueId;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.CollectNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.DeviceTableScanNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.EnforceSingleRowNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.ExplainAnalyzeNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.FillNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.FilterNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.GapFillNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.GroupNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.InformationSchemaTableScanNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.IntoNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.JoinNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.LimitNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.MarkDistinctNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.MergeSortNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.OffsetNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.OutputNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.PatternRecognitionNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.ProjectNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.SemiJoinNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.SortNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.StreamSortNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.TableFunctionProcessorNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.TopKNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.TreeAlignedDeviceViewScanNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.TreeDeviceViewScanNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.TreeNonAlignedDeviceViewScanNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.UnionNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.ValueFillNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.WindowNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.schema.AbstractTableDeviceQueryNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.schema.TableDeviceFetchNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.schema.TableDeviceQueryCountNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.schema.TableDeviceQueryScanNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.optimizations.DataNodeLocationSupplierFactory;
import org.apache.iotdb.db.queryengine.plan.relational.planner.optimizations.PushPredicateIntoTableScan;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.CoalesceExpression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Expression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.FunctionCall;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Insert;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.SymbolReference;
import org.apache.iotdb.db.queryengine.plan.statement.component.Ordering;
import org.apache.iotdb.db.schemaengine.table.DataNodeTableCache;
import org.apache.iotdb.db.schemaengine.table.DataNodeTreeViewSchemaUtils;
import org.apache.iotdb.rpc.TSStatusCode;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ListMultimap;
import org.apache.tsfile.common.conf.TSFileConfig;
import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.read.common.type.LongType;
import org.apache.tsfile.read.filter.basic.Filter;
import org.apache.tsfile.utils.Pair;

import javax.annotation.Nonnull;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static org.apache.iotdb.commons.partition.DataPartition.NOT_ASSIGNED;
import static org.apache.iotdb.db.queryengine.plan.relational.function.FunctionKind.AGGREGATE;
import static org.apache.iotdb.db.queryengine.plan.relational.metadata.FunctionNullability.getAggregationFunctionNullability;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.node.AggregationNode.Step.FINAL;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.node.AggregationNode.Step.SINGLE;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.optimizations.PushPredicateIntoTableScan.containsDiffFunction;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.optimizations.TransformSortToStreamSort.isOrderByAllIdsAndTime;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.optimizations.Util.split;
import static org.apache.iotdb.db.utils.constant.SqlConstant.COUNT;
import static org.apache.tsfile.utils.Preconditions.checkArgument;

/** This class is used to generate distributed plan for table model. */
public class TableDistributedPlanGenerator
    extends PlanVisitor<List<PlanNode>, TableDistributedPlanGenerator.PlanContext> {
  private final MPPQueryContext queryContext;
  private final QueryId queryId;
  private final Analysis analysis;
  private final SymbolAllocator symbolAllocator;
  private final Map<PlanNodeId, OrderingScheme> nodeOrderingMap = new HashMap<>();
  private final DataNodeLocationSupplierFactory.DataNodeLocationSupplier dataNodeLocationSupplier;
  private final ClusterTopology topology = ClusterTopology.getInstance();

  public TableDistributedPlanGenerator(
      final MPPQueryContext queryContext,
      final Analysis analysis,
      final SymbolAllocator symbolAllocator,
      final DataNodeLocationSupplierFactory.DataNodeLocationSupplier dataNodeLocationSupplier) {
    this.queryContext = queryContext;
    this.queryId = queryContext.getQueryId();
    this.analysis = analysis;
    this.symbolAllocator = symbolAllocator;
    this.dataNodeLocationSupplier = dataNodeLocationSupplier;
  }

  public List<PlanNode> genResult(final PlanNode node, final PlanContext context) {
    final List<PlanNode> res = node.accept(this, context);
    if (res.size() == 1) {
      return res;
    } else if (res.size() > 1) {
      final CollectNode collectNode =
          new CollectNode(queryId.genPlanNodeId(), res.get(0).getOutputSymbols());
      res.forEach(collectNode::addChild);
      return Collections.singletonList(collectNode);
    } else {
      throw new IllegalStateException("List<PlanNode>.size should >= 1, but now is 0");
    }
  }

  @Override
  public List<PlanNode> visitPlan(
      final PlanNode node, final TableDistributedPlanGenerator.PlanContext context) {
    if (node instanceof WritePlanNode) {
      return Collections.singletonList(node);
    }

    final List<List<PlanNode>> children =
        node.getChildren().stream()
            .map(child -> child.accept(this, context))
            .collect(toImmutableList());

    final PlanNode newNode = node.clone();
    for (final List<PlanNode> planNodes : children) {
      planNodes.forEach(newNode::addChild);
    }
    return Collections.singletonList(newNode);
  }

  @Override
  public List<PlanNode> visitExplainAnalyze(
      final ExplainAnalyzeNode node, final PlanContext context) {
    final List<PlanNode> children = genResult(node.getChild(), context);
    node.setChild(children.get(0));
    return Collections.singletonList(node);
  }

  @Override
  public List<PlanNode> visitOutput(final OutputNode node, final PlanContext context) {
    final List<PlanNode> childrenNodes = node.getChild().accept(this, context);
    final OrderingScheme childOrdering = nodeOrderingMap.get(childrenNodes.get(0).getPlanNodeId());
    if (childOrdering != null) {
      nodeOrderingMap.put(node.getPlanNodeId(), childOrdering);
    }

    node.setChild(mergeChildrenViaCollectOrMergeSort(childOrdering, childrenNodes));
    return Collections.singletonList(node);
  }

  @Override
  public List<PlanNode> visitInto(final IntoNode node, final PlanContext context) {
    final List<PlanNode> childrenNodes = node.getChild().accept(this, context);
    if (childrenNodes.size() == 1) {
      node.setChild(childrenNodes.get(0));
      return Collections.singletonList(node);
    }

    List<PlanNode> resultNodeList = new ArrayList<>();
    for (PlanNode child : childrenNodes) {
      IntoNode subIntoNode =
          new IntoNode(
              queryId.genPlanNodeId(),
              child,
              node.getDatabase(),
              node.getTable(),
              node.getColumns(),
              node.getNeededInputColumnNames(),
              node.getRowCountSymbol());
      resultNodeList.add(subIntoNode);
    }
    PlanNode collectNode = mergeChildrenViaCollectOrMergeSort(null, resultNodeList);

    // prepare aggregation map
    Map<Symbol, AggregationNode.Aggregation> aggregations = new LinkedHashMap<>();
    ResolvedFunction countFunction =
        new ResolvedFunction(
            new BoundSignature(COUNT, LongType.INT64, Collections.singletonList(LongType.INT64)),
            FunctionId.NOOP_FUNCTION_ID,
            AGGREGATE,
            true,
            getAggregationFunctionNullability(1));
    AggregationNode.Aggregation countAggregation =
        new AggregationNode.Aggregation(
            countFunction,
            ImmutableList.of(new SymbolReference(Insert.ROWS)),
            false,
            Optional.empty(),
            Optional.empty(),
            Optional.empty());
    aggregations.put(node.getRowCountSymbol(), countAggregation);

    // final count aggregation actually does sum calculation
    PlanNode aggregationNode =
        new AggregationNode(
            queryId.genPlanNodeId(),
            collectNode,
            aggregations,
            AggregationNode.singleGroupingSet(ImmutableList.of()),
            ImmutableList.of(),
            FINAL,
            Optional.empty(),
            Optional.empty());

    return Collections.singletonList(aggregationNode);
  }

  @Override
  public List<PlanNode> visitFill(FillNode node, PlanContext context) {
    if (!(node instanceof ValueFillNode)) {
      context.clearExpectedOrderingScheme();
    }
    return dealWithPlainSingleChildNode(node, context);
  }

  @Override
  public List<PlanNode> visitGapFill(GapFillNode node, PlanContext context) {
    context.clearExpectedOrderingScheme();
    return dealWithPlainSingleChildNode(node, context);
  }

  @Override
  public List<PlanNode> visitLimit(LimitNode node, PlanContext context) {
    // push down LimitNode in distributed plan optimize rule
    return dealWithPlainSingleChildNode(node, context);
  }

  @Override
  public List<PlanNode> visitOffset(OffsetNode node, PlanContext context) {
    return dealWithPlainSingleChildNode(node, context);
  }

  @Override
  public List<PlanNode> visitProject(ProjectNode node, PlanContext context) {
    List<PlanNode> childrenNodes = node.getChild().accept(this, context);
    OrderingScheme childOrdering = nodeOrderingMap.get(childrenNodes.get(0).getPlanNodeId());
    boolean containAllSortItem = false;
    if (childOrdering != null) {
      // the column used for order by has been pruned, we can't copy this node to sub nodeTrees.
      containAllSortItem =
          ImmutableSet.copyOf(node.getOutputSymbols()).containsAll(childOrdering.getOrderBy());
    }
    if (childrenNodes.size() == 1) {
      PlanNode child = childrenNodes.get(0);
      if (containAllSortItem) {
        nodeOrderingMap.put(node.getPlanNodeId(), childOrdering);
      }

      // Now the join implement but CROSS is MergeSortJoin, so it can keep order
      if (child instanceof JoinNode) {
        JoinNode joinNode = (JoinNode) child;

        // We only process FULL Join here, other type will be processed in visitJoinNode()
        if (joinNode.getJoinType() == JoinNode.JoinType.FULL
            && !joinNode.getAsofCriteria().isPresent()) {
          Map<Symbol, Expression> assignmentsMap = node.getAssignments().getMap();
          // If these Coalesces are all appear in ProjectNode, the ProjectNode is ordered
          int coalescesSize = joinNode.getCriteria().size();

          // We use map to memorize Symbol of according Coalesce, use linked to avoid twice query of
          // this Map when constructOrderingSchema
          Map<Expression, Symbol> orderedCoalesces = new LinkedHashMap<>(coalescesSize);
          for (JoinNode.EquiJoinClause clause : joinNode.getCriteria()) {
            orderedCoalesces.put(
                new CoalesceExpression(
                    ImmutableList.of(
                        clause.getLeft().toSymbolReference(),
                        clause.getRight().toSymbolReference())),
                null);
          }

          for (Map.Entry<Symbol, Expression> assignment : assignmentsMap.entrySet()) {
            if (orderedCoalesces.containsKey(assignment.getValue())) {
              coalescesSize--;
              orderedCoalesces.put(assignment.getValue(), assignment.getKey());
            }
          }

          // All Coalesces appear in ProjectNode
          if (coalescesSize == 0) {
            nodeOrderingMap.put(
                node.getPlanNodeId(),
                constructOrderingSchema(new ArrayList<>(orderedCoalesces.values())));
          }
        }
      }

      node.setChild(child);
      return Collections.singletonList(node);
    }

    boolean containsDiff =
        node.getAssignments().getMap().values().stream()
            .anyMatch(PushPredicateIntoTableScan::containsDiffFunction);
    if (containsDiff) {
      if (containAllSortItem) {
        nodeOrderingMap.put(node.getPlanNodeId(), childOrdering);
      }
      node.setChild(mergeChildrenViaCollectOrMergeSort(childOrdering, childrenNodes));
      return Collections.singletonList(node);
    }

    List<PlanNode> resultNodeList = new ArrayList<>(childrenNodes.size());
    for (PlanNode child : childrenNodes) {
      ProjectNode subProjectNode =
          new ProjectNode(queryId.genPlanNodeId(), child, node.getAssignments());
      resultNodeList.add(subProjectNode);
      if (containAllSortItem) {
        nodeOrderingMap.put(subProjectNode.getPlanNodeId(), childOrdering);
      }
    }
    return resultNodeList;
  }

  @Override
  public List<PlanNode> visitTopK(TopKNode node, PlanContext context) {
    context.setExpectedOrderingScheme(node.getOrderingScheme());
    nodeOrderingMap.put(node.getPlanNodeId(), node.getOrderingScheme());

    checkArgument(
        node.getChildren().size() == 1, "Size of TopKNode can only be 1 in logical plan.");
    List<PlanNode> childrenNodes = node.getChildren().get(0).accept(this, context);
    if (childrenNodes.size() == 1) {
      if (canTopKEliminated(node.getOrderingScheme(), node.getCount(), childrenNodes.get(0))) {
        return childrenNodes;
      }
      node.setChildren(Collections.singletonList(childrenNodes.get(0)));
      return Collections.singletonList(node);
    }

    TopKNode newTopKNode = (TopKNode) node.clone();
    for (PlanNode child : childrenNodes) {
      PlanNode newChild;
      if (canTopKEliminated(node.getOrderingScheme(), node.getCount(), child)) {
        newChild = child;
      } else {
        newChild =
            new TopKNode(
                queryId.genPlanNodeId(),
                Collections.singletonList(child),
                node.getOrderingScheme(),
                node.getCount(),
                node.getOutputSymbols(),
                node.isChildrenDataInOrder());
      }
      newTopKNode.addChild(newChild);
    }
    nodeOrderingMap.put(newTopKNode.getPlanNodeId(), newTopKNode.getOrderingScheme());

    return Collections.singletonList(newTopKNode);
  }

  // if DeviceTableScanNode has limit <= K and with same order, we can eliminate TopK
  private boolean canTopKEliminated(OrderingScheme orderingScheme, long k, PlanNode child) {
    // if DeviceTableScanNode has limit <= K and with same order, we can directly return
    // DeviceTableScanNode
    if (child instanceof DeviceTableScanNode && !(child instanceof AggregationTableScanNode)) {
      DeviceTableScanNode tableScanNode = (DeviceTableScanNode) child;
      if (canSortEliminated(orderingScheme, nodeOrderingMap.get(child.getPlanNodeId()))) {
        if (tableScanNode.getPushDownLimit() <= 0) {
          tableScanNode.setPushDownLimit(k);
        } else {
          tableScanNode.setPushDownLimit(Math.min(k, tableScanNode.getPushDownLimit()));
        }
        return true;
      }
    }
    return false;
  }

  @Override
  public List<PlanNode> visitGroup(GroupNode node, PlanContext context) {
    context.setExpectedOrderingScheme(node.getOrderingScheme());
    nodeOrderingMap.put(node.getPlanNodeId(), node.getOrderingScheme());
    context.setPushDownGrouping(true);
    List<PlanNode> result = new ArrayList<>();
    context.setExpectedOrderingScheme(node.getOrderingScheme());
    List<PlanNode> childrenNodes = node.getChild().accept(this, context);
    for (PlanNode child : childrenNodes) {
      if (canSortEliminated(node.getOrderingScheme(), nodeOrderingMap.get(child.getPlanNodeId()))) {
        result.add(child);
      } else {
        GroupNode subGroupNode =
            new GroupNode(
                queryId.genPlanNodeId(),
                child,
                node.getOrderingScheme(),
                node.getPartitionKeyCount());
        result.add(subGroupNode);
        // should not set nodeOrderingMap here
      }
    }
    return result;
  }

  @Override
  public List<PlanNode> visitSort(SortNode node, PlanContext context) {
    context.setExpectedOrderingScheme(node.getOrderingScheme());
    nodeOrderingMap.put(node.getPlanNodeId(), node.getOrderingScheme());

    List<PlanNode> childrenNodes = node.getChild().accept(this, context);
    if (childrenNodes.size() == 1) {
      if (canSortEliminated(
          node.getOrderingScheme(), nodeOrderingMap.get(childrenNodes.get(0).getPlanNodeId()))) {
        return childrenNodes;
      } else {
        node.setChild(childrenNodes.get(0));
        return Collections.singletonList(node);
      }
    }

    // may have ProjectNode above SortNode later, so use MergeSortNode but not return SortNode list
    MergeSortNode mergeSortNode =
        new MergeSortNode(
            queryId.genPlanNodeId(), node.getOrderingScheme(), node.getOutputSymbols());
    for (PlanNode child : childrenNodes) {
      if (canSortEliminated(node.getOrderingScheme(), nodeOrderingMap.get(child.getPlanNodeId()))) {
        mergeSortNode.addChild(child);
      } else {
        SortNode subSortNode =
            new SortNode(queryId.genPlanNodeId(), child, node.getOrderingScheme(), false, false);
        mergeSortNode.addChild(subSortNode);
      }
    }
    nodeOrderingMap.put(mergeSortNode.getPlanNodeId(), mergeSortNode.getOrderingScheme());

    return Collections.singletonList(mergeSortNode);
  }

  // if current SortNode is prefix of child, this SortNode doesn't need to exist, return true
  private boolean canSortEliminated(
      @Nonnull OrderingScheme sortOrderingSchema, OrderingScheme childOrderingSchema) {
    if (childOrderingSchema == null) {
      return false;
    } else {
      List<Symbol> symbolsOfSort = sortOrderingSchema.getOrderBy();
      List<Symbol> symbolsOfChild = childOrderingSchema.getOrderBy();
      if (symbolsOfSort.size() > symbolsOfChild.size()) {
        return false;
      } else {
        for (int i = 0, size = symbolsOfSort.size(); i < size; i++) {
          Symbol symbolOfSort = symbolsOfSort.get(i);
          SortOrder sortOrderOfSortNode = sortOrderingSchema.getOrdering(symbolOfSort);
          Symbol symbolOfChild = symbolsOfChild.get(i);
          SortOrder sortOrderOfChild = childOrderingSchema.getOrdering(symbolOfChild);
          if (!symbolOfSort.equals(symbolOfChild) || sortOrderOfSortNode != sortOrderOfChild) {
            return false;
          }
        }
        return true;
      }
    }
  }

  @Override
  public List<PlanNode> visitStreamSort(StreamSortNode node, PlanContext context) {
    context.setExpectedOrderingScheme(node.getOrderingScheme());
    nodeOrderingMap.put(node.getPlanNodeId(), node.getOrderingScheme());

    List<PlanNode> childrenNodes = node.getChild().accept(this, context);
    if (childrenNodes.size() == 1) {
      if (canSortEliminated(
          node.getOrderingScheme(), nodeOrderingMap.get(childrenNodes.get(0).getPlanNodeId()))) {
        return childrenNodes;
      } else {
        node.setChild(childrenNodes.get(0));
        return Collections.singletonList(node);
      }
    }

    // may have ProjectNode above SortNode later, so use MergeSortNode but not return SortNode list
    MergeSortNode mergeSortNode =
        new MergeSortNode(
            queryId.genPlanNodeId(), node.getOrderingScheme(), node.getOutputSymbols());
    for (PlanNode child : childrenNodes) {
      if (canSortEliminated(node.getOrderingScheme(), nodeOrderingMap.get(child.getPlanNodeId()))) {
        mergeSortNode.addChild(child);
      } else {
        StreamSortNode subSortNode =
            new StreamSortNode(
                queryId.genPlanNodeId(),
                child,
                node.getOrderingScheme(),
                false,
                node.isOrderByAllIdsAndTime(),
                node.getStreamCompareKeyEndIndex());
        mergeSortNode.addChild(subSortNode);
      }
    }
    nodeOrderingMap.put(mergeSortNode.getPlanNodeId(), mergeSortNode.getOrderingScheme());

    return Collections.singletonList(mergeSortNode);
  }

  @Override
  public List<PlanNode> visitFilter(FilterNode node, PlanContext context) {
    List<PlanNode> childrenNodes = node.getChild().accept(this, context);
    OrderingScheme childOrdering = nodeOrderingMap.get(childrenNodes.get(0).getPlanNodeId());
    if (childOrdering != null) {
      nodeOrderingMap.put(node.getPlanNodeId(), childOrdering);
    }

    if (childrenNodes.size() == 1) {
      node.setChild(childrenNodes.get(0));
      return Collections.singletonList(node);
    }

    if (containsDiffFunction(node.getPredicate())) {
      node.setChild(mergeChildrenViaCollectOrMergeSort(childOrdering, childrenNodes));
      return Collections.singletonList(node);
    }

    List<PlanNode> resultNodeList = new ArrayList<>();
    for (PlanNode child : childrenNodes) {
      FilterNode subFilterNode =
          new FilterNode(queryId.genPlanNodeId(), child, node.getPredicate());
      resultNodeList.add(subFilterNode);
      nodeOrderingMap.put(subFilterNode.getPlanNodeId(), childOrdering);
    }
    return resultNodeList;
  }

  @Override
  public List<PlanNode> visitJoin(JoinNode node, PlanContext context) {

    List<PlanNode> leftChildrenNodes = node.getLeftChild().accept(this, context);
    List<PlanNode> rightChildrenNodes = node.getRightChild().accept(this, context);
    if (!node.isCrossJoin()) {
      // child of JoinNode(excluding CrossJoin) must be SortNode, so after rewritten, the child must
      // be MergeSortNode or
      // SortNode
      checkArgument(
          leftChildrenNodes.size() == 1, "The size of left children node of JoinNode should be 1");
      checkArgument(
          rightChildrenNodes.size() == 1,
          "The size of right children node of JoinNode should be 1");
    }

    OrderingScheme leftChildOrdering = nodeOrderingMap.get(node.getLeftChild().getPlanNodeId());
    OrderingScheme rightChildOrdering = nodeOrderingMap.get(node.getRightChild().getPlanNodeId());

    // For CrossJoinNode, we need to merge children nodes(It's safe for other JoinNodes here since
    // the size of their children is always 1.)
    node.setLeftChild(mergeChildrenViaCollectOrMergeSort(leftChildOrdering, leftChildrenNodes));
    node.setRightChild(mergeChildrenViaCollectOrMergeSort(rightChildOrdering, rightChildrenNodes));

    // Now the join implement but CROSS is MergeSortJoin, so it can keep order
    if (!node.isCrossJoin() && !node.getAsofCriteria().isPresent()) {
      switch (node.getJoinType()) {
        case FULL:
          // If join type is FULL Join, we will process SortProperties in ProjectNode above this
          // node.
          break;
        case INNER:
        case LEFT:
          if (ImmutableSet.copyOf(node.getLeftOutputSymbols())
              .containsAll(leftChildOrdering.getOrderBy())) {
            nodeOrderingMap.put(node.getPlanNodeId(), leftChildOrdering);
          }
          break;
        case RIGHT:
          throw new IllegalStateException(
              "RIGHT Join should be transformed to LEFT Join in previous process");
        default:
          throw new UnsupportedOperationException("Unsupported Join Type: " + node.getJoinType());
      }
    }
    return Collections.singletonList(node);
  }

  @Override
  public List<PlanNode> visitPatternRecognition(PatternRecognitionNode node, PlanContext context) {
    context.clearExpectedOrderingScheme();
    boolean canSplitPushDown = (node.getChild() instanceof GroupNode);
    List<PlanNode> childrenNodes = node.getChild().accept(this, context);
    if (childrenNodes.size() == 1) {
      node.setChild(childrenNodes.get(0));
      return Collections.singletonList(node);
    } else if (!canSplitPushDown) {
      CollectNode collectNode =
          new CollectNode(queryId.genPlanNodeId(), node.getChildren().get(0).getOutputSymbols());
      childrenNodes.forEach(collectNode::addChild);
      node.setChild(collectNode);
      return Collections.singletonList(node);
    } else {
      return splitForEachChild(node, childrenNodes);
    }
  }

  @Override
  public List<PlanNode> visitSemiJoin(SemiJoinNode node, PlanContext context) {
    List<PlanNode> leftChildrenNodes = node.getLeftChild().accept(this, context);
    List<PlanNode> rightChildrenNodes = node.getRightChild().accept(this, context);
    checkArgument(
        leftChildrenNodes.size() == 1,
        "The size of left children node of SemiJoinNode should be 1");
    checkArgument(
        rightChildrenNodes.size() == 1,
        "The size of right children node of SemiJoinNode should be 1");
    node.setLeftChild(leftChildrenNodes.get(0));
    node.setRightChild(rightChildrenNodes.get(0));
    return Collections.singletonList(node);
  }

  @Override
  public List<PlanNode> visitDeviceTableScan(
      final DeviceTableScanNode node, final PlanContext context) {
    if (context.isPushDownGrouping()) {
      return constructDeviceTableScanByTags(node, context);
    } else {
      return constructDeviceTableScanByRegionReplicaSet(node, context);
    }
  }

  private List<PlanNode> constructDeviceTableScanByTags(
      final DeviceTableScanNode node, final PlanContext context) {
    DataPartition dataPartition = analysis.getDataPartitionInfo();
    if (dataPartition == null) {
      node.setRegionReplicaSet(NOT_ASSIGNED);
      return Collections.singletonList(node);
    }

    String dbName = node.getQualifiedObjectName().getDatabaseName();
    Map<TSeriesPartitionSlot, Map<TTimePartitionSlot, List<TRegionReplicaSet>>> seriesSlotMap =
        dataPartition.getDataPartitionMap().get(dbName);
    if (seriesSlotMap == null) {
      throw new SemanticException(
          String.format("Given queried database: %s is not exist!", dbName));
    }
    Map<Integer, List<TRegionReplicaSet>> cachedSeriesSlotWithRegions = new HashMap<>();

    List<DeviceEntry> crossRegionDevices = new ArrayList<>();

    final Map<TRegionReplicaSet, DeviceTableScanNode> tableScanNodeMap = new HashMap<>();
    final Map<TRegionReplicaSet, Integer> regionDeviceCount = new HashMap<>();
    for (final DeviceEntry deviceEntry : node.getDeviceEntries()) {
      final List<TRegionReplicaSet> regionReplicaSets =
          getDeviceReplicaSets(
              dataPartition,
              seriesSlotMap,
              deviceEntry.getDeviceID(),
              node.getTimeFilter(),
              cachedSeriesSlotWithRegions);
      regionReplicaSets.forEach(
          regionReplicaSet ->
              regionDeviceCount.put(
                  regionReplicaSet, regionDeviceCount.getOrDefault(regionReplicaSet, 0) + 1));
      if (regionReplicaSets.size() != 1) {
        crossRegionDevices.add(deviceEntry);
        context.deviceCrossRegion = true;
        continue;
      }
      final DeviceTableScanNode deviceTableScanNode =
          tableScanNodeMap.computeIfAbsent(
              regionReplicaSets.get(0),
              k -> {
                final DeviceTableScanNode scanNode =
                    new DeviceTableScanNode(
                        queryId.genPlanNodeId(),
                        node.getQualifiedObjectName(),
                        node.getOutputSymbols(),
                        node.getAssignments(),
                        new ArrayList<>(),
                        node.getTagAndAttributeIndexMap(),
                        node.getScanOrder(),
                        node.getTimePredicate().orElse(null),
                        node.getPushDownPredicate(),
                        node.getPushDownLimit(),
                        node.getPushDownOffset(),
                        node.isPushLimitToEachDevice(),
                        node.containsNonAlignedDevice());
                scanNode.setRegionReplicaSet(regionReplicaSets.get(0));
                return scanNode;
              });
      deviceTableScanNode.appendDeviceEntry(deviceEntry);
    }
    List<PlanNode> result = new ArrayList<>(tableScanNodeMap.values());
    if (context.hasSortProperty) {
      processSortProperty(node, result, context);
    }
    context.mostUsedRegion =
        regionDeviceCount.entrySet().stream()
            .max(Comparator.comparingInt(Map.Entry::getValue))
            .map(Map.Entry::getKey)
            .orElse(null);
    if (!crossRegionDevices.isEmpty()) {
      node.setDeviceEntries(crossRegionDevices);
      MergeSortNode mergeSortNode =
          new MergeSortNode(
              queryId.genPlanNodeId(), context.expectedOrderingScheme, node.getOutputSymbols());
      for (PlanNode node1 : constructDeviceTableScanByRegionReplicaSet(node, context)) {
        if (canSortEliminated(
            mergeSortNode.getOrderingScheme(), nodeOrderingMap.get(node1.getPlanNodeId()))) {
          mergeSortNode.addChild(node1);
        } else {
          SortNode subSortNode =
              new SortNode(
                  queryId.genPlanNodeId(), node1, mergeSortNode.getOrderingScheme(), false, false);
          mergeSortNode.addChild(subSortNode);
        }
      }
      nodeOrderingMap.put(mergeSortNode.getPlanNodeId(), mergeSortNode.getOrderingScheme());
      result.add(mergeSortNode);
    }
    return result;
  }

  private List<PlanNode> constructDeviceTableScanByRegionReplicaSet(
      final DeviceTableScanNode node, final PlanContext context) {
    DataPartition dataPartition = analysis.getDataPartitionInfo();
    if (dataPartition == null) {
      node.setRegionReplicaSet(NOT_ASSIGNED);
      return Collections.singletonList(node);
    }

    String dbName = node.getQualifiedObjectName().getDatabaseName();
    Map<TSeriesPartitionSlot, Map<TTimePartitionSlot, List<TRegionReplicaSet>>> seriesSlotMap =
        dataPartition.getDataPartitionMap().get(dbName);
    if (seriesSlotMap == null) {
      throw new SemanticException(
          String.format("Given queried database: %s is not exist!", dbName));
    }

    final Map<TRegionReplicaSet, DeviceTableScanNode> tableScanNodeMap = new HashMap<>();
    Map<Integer, List<TRegionReplicaSet>> cachedSeriesSlotWithRegions = new HashMap<>();

    for (final DeviceEntry deviceEntry : node.getDeviceEntries()) {
      List<TRegionReplicaSet> regionReplicaSets =
          getDeviceReplicaSets(
              dataPartition,
              seriesSlotMap,
              deviceEntry.getDeviceID(),
              node.getTimeFilter(),
              cachedSeriesSlotWithRegions);
      if (regionReplicaSets.size() > 1) {
        context.deviceCrossRegion = true;
      }
      for (final TRegionReplicaSet regionReplicaSet : regionReplicaSets) {
        final DeviceTableScanNode deviceTableScanNode =
            tableScanNodeMap.computeIfAbsent(
                regionReplicaSet,
                k -> {
                  final DeviceTableScanNode scanNode =
                      new DeviceTableScanNode(
                          queryId.genPlanNodeId(),
                          node.getQualifiedObjectName(),
                          node.getOutputSymbols(),
                          node.getAssignments(),
                          new ArrayList<>(),
                          node.getTagAndAttributeIndexMap(),
                          node.getScanOrder(),
                          node.getTimePredicate().orElse(null),
                          node.getPushDownPredicate(),
                          node.getPushDownLimit(),
                          node.getPushDownOffset(),
                          node.isPushLimitToEachDevice(),
                          node.containsNonAlignedDevice());
                  scanNode.setRegionReplicaSet(regionReplicaSet);
                  return scanNode;
                });
        deviceTableScanNode.appendDeviceEntry(deviceEntry);
      }
    }
    if (tableScanNodeMap.isEmpty()) {
      node.setRegionReplicaSet(NOT_ASSIGNED);
      return Collections.singletonList(node);
    }

    final List<PlanNode> resultTableScanNodeList = new ArrayList<>();
    TRegionReplicaSet mostUsedDataRegion = null;
    int maxDeviceEntrySizeOfTableScan = 0;
    for (final Map.Entry<TRegionReplicaSet, DeviceTableScanNode> entry :
        topology.filterReachableCandidates(tableScanNodeMap.entrySet())) {
      final DeviceTableScanNode subDeviceTableScanNode = entry.getValue();
      resultTableScanNodeList.add(subDeviceTableScanNode);

      if (mostUsedDataRegion == null
          || subDeviceTableScanNode.getDeviceEntries().size() > maxDeviceEntrySizeOfTableScan) {
        mostUsedDataRegion = entry.getKey();
        maxDeviceEntrySizeOfTableScan = subDeviceTableScanNode.getDeviceEntries().size();
      }
    }
    if (mostUsedDataRegion == null) {
      throw new RootFIPlacementException(tableScanNodeMap.keySet());
    }
    context.mostUsedRegion = mostUsedDataRegion;

    if (!context.hasSortProperty) {
      return resultTableScanNodeList;
    }

    processSortProperty(node, resultTableScanNodeList, context);
    return resultTableScanNodeList;
  }

  @Override
  public List<PlanNode> visitTreeDeviceViewScan(TreeDeviceViewScanNode node, PlanContext context) {
    DataPartition dataPartition = analysis.getDataPartitionInfo();
    if (dataPartition == null || node.getTreeDBName() == null) {
      node.setRegionReplicaSet(NOT_ASSIGNED);
      node.setDeviceEntries(Collections.emptyList());
      node.setTreeDBName(null);
      return Collections.singletonList(node);
    }

    String dbName = node.getTreeDBName();
    Map<TSeriesPartitionSlot, Map<TTimePartitionSlot, List<TRegionReplicaSet>>> seriesSlotMap =
        dataPartition.getDataPartitionMap().get(dbName);
    if (seriesSlotMap == null) {
      throw new SemanticException(
          String.format("Given queried database: %s is not exist!", dbName));
    }

    Map<TRegionReplicaSet, Pair<TreeAlignedDeviceViewScanNode, TreeNonAlignedDeviceViewScanNode>>
        tableScanNodeMap = new HashMap<>();
    Map<Integer, List<TRegionReplicaSet>> cachedSeriesSlotWithRegions = new HashMap<>();
    for (DeviceEntry deviceEntry : node.getDeviceEntries()) {
      List<TRegionReplicaSet> regionReplicaSets =
          getDeviceReplicaSets(
              dataPartition,
              seriesSlotMap,
              deviceEntry.getDeviceID(),
              node.getTimeFilter(),
              cachedSeriesSlotWithRegions);

      if (regionReplicaSets.size() > 1) {
        context.deviceCrossRegion = true;
      }
      for (TRegionReplicaSet regionReplicaSet : regionReplicaSets) {
        boolean aligned = deviceEntry instanceof AlignedDeviceEntry;
        Pair<TreeAlignedDeviceViewScanNode, TreeNonAlignedDeviceViewScanNode> pair =
            tableScanNodeMap.computeIfAbsent(regionReplicaSet, k -> new Pair<>(null, null));

        if (pair.left == null && aligned) {
          TreeAlignedDeviceViewScanNode scanNode =
              new TreeAlignedDeviceViewScanNode(
                  queryId.genPlanNodeId(),
                  node.getQualifiedObjectName(),
                  node.getOutputSymbols(),
                  node.getAssignments(),
                  new ArrayList<>(),
                  node.getTagAndAttributeIndexMap(),
                  node.getScanOrder(),
                  node.getTimePredicate().orElse(null),
                  node.getPushDownPredicate(),
                  node.getPushDownLimit(),
                  node.getPushDownOffset(),
                  node.isPushLimitToEachDevice(),
                  node.containsNonAlignedDevice(),
                  node.getTreeDBName(),
                  node.getMeasurementColumnNameMap());
          scanNode.setRegionReplicaSet(regionReplicaSet);
          pair.left = scanNode;
        }

        if (pair.right == null && !aligned) {
          TreeNonAlignedDeviceViewScanNode scanNode =
              new TreeNonAlignedDeviceViewScanNode(
                  queryId.genPlanNodeId(),
                  node.getQualifiedObjectName(),
                  node.getOutputSymbols(),
                  node.getAssignments(),
                  new ArrayList<>(),
                  node.getTagAndAttributeIndexMap(),
                  node.getScanOrder(),
                  node.getTimePredicate().orElse(null),
                  node.getPushDownPredicate(),
                  node.getPushDownLimit(),
                  node.getPushDownOffset(),
                  node.isPushLimitToEachDevice(),
                  node.containsNonAlignedDevice(),
                  node.getTreeDBName(),
                  node.getMeasurementColumnNameMap());
          scanNode.setRegionReplicaSet(regionReplicaSet);
          pair.right = scanNode;
        }

        if (aligned) {
          pair.left.appendDeviceEntry(deviceEntry);
        } else {
          pair.right.appendDeviceEntry(deviceEntry);
        }
      }
    }

    if (tableScanNodeMap.isEmpty()) {
      node.setRegionReplicaSet(NOT_ASSIGNED);
      node.setDeviceEntries(Collections.emptyList());
      node.setTreeDBName(null);
      return Collections.singletonList(node);
    }

    List<PlanNode> resultTableScanNodeList = new ArrayList<>();
    TRegionReplicaSet mostUsedDataRegion = null;
    int maxDeviceEntrySizeOfTableScan = 0;
    for (Map.Entry<
            TRegionReplicaSet,
            Pair<TreeAlignedDeviceViewScanNode, TreeNonAlignedDeviceViewScanNode>>
        entry : topology.filterReachableCandidates(tableScanNodeMap.entrySet())) {
      TRegionReplicaSet regionReplicaSet = entry.getKey();
      Pair<TreeAlignedDeviceViewScanNode, TreeNonAlignedDeviceViewScanNode> pair = entry.getValue();
      int currentDeviceEntrySize = 0;

      if (pair.left != null) {
        currentDeviceEntrySize += pair.left.getDeviceEntries().size();
        resultTableScanNodeList.add(pair.left);
      }

      if (pair.right != null) {
        currentDeviceEntrySize += pair.right.getDeviceEntries().size();
        resultTableScanNodeList.add(pair.right);
      }

      if (mostUsedDataRegion == null || currentDeviceEntrySize > maxDeviceEntrySizeOfTableScan) {
        mostUsedDataRegion = regionReplicaSet;
        maxDeviceEntrySizeOfTableScan = currentDeviceEntrySize;
      }
    }
    if (mostUsedDataRegion == null) {
      throw new RootFIPlacementException(tableScanNodeMap.keySet());
    }
    context.mostUsedRegion = mostUsedDataRegion;

    if (!context.hasSortProperty) {
      return resultTableScanNodeList;
    }

    processSortProperty(node, resultTableScanNodeList, context);
    return resultTableScanNodeList;
  }

  @Override
  public List<PlanNode> visitInformationSchemaTableScan(
      InformationSchemaTableScanNode node, PlanContext context) {
    List<TDataNodeLocation> dataNodeLocations =
        dataNodeLocationSupplier.getDataNodeLocations(
            node.getQualifiedObjectName().getObjectName());
    if (dataNodeLocations.isEmpty()) {
      throw new IoTDBRuntimeException(
          "No available dataNodes, may be the cluster is closing",
          TSStatusCode.NO_AVAILABLE_REPLICA.getStatusCode());
    }

    List<PlanNode> resultTableScanNodeList = new ArrayList<>();
    dataNodeLocations.forEach(
        dataNodeLocation ->
            resultTableScanNodeList.add(
                new InformationSchemaTableScanNode(
                    queryId.genPlanNodeId(),
                    node.getQualifiedObjectName(),
                    node.getOutputSymbols(),
                    node.getAssignments(),
                    node.getPushDownPredicate(),
                    node.getPushDownLimit(),
                    node.getPushDownOffset(),
                    new TRegionReplicaSet(null, ImmutableList.of(dataNodeLocation)))));
    return resultTableScanNodeList;
  }

  @Override
  public List<PlanNode> visitAggregation(AggregationNode node, PlanContext context) {
    List<Symbol> preGroupedSymbols = node.getPreGroupedSymbols();
    OrderingScheme expectedOrderingSchema;
    if (node.isStreamable()) {
      expectedOrderingSchema = constructOrderingSchema(preGroupedSymbols);
      context.setExpectedOrderingScheme(expectedOrderingSchema);
    } else {
      expectedOrderingSchema = null;
      context.clearExpectedOrderingScheme();
    }

    List<PlanNode> childrenNodes = node.getChild().accept(this, context);
    OrderingScheme childOrdering = nodeOrderingMap.get(childrenNodes.get(0).getPlanNodeId());
    if (node.isStreamable()) {
      // Child has Ordering, we need to check if it is the Ordering we expected
      if (childOrdering != null) {
        if (prefixMatched(childOrdering, node.getPreGroupedSymbols())) {
          nodeOrderingMap.put(node.getPlanNodeId(), expectedOrderingSchema);
        } else {
          throw new IllegalStateException(
              String.format(
                  "Should never reach here. Child ordering: %s. PreGroupedSymbols: %s",
                  childOrdering.getOrderBy(), node.getPreGroupedSymbols()));
        }
      } else if (context.deviceCrossRegion) {
        // Child has no Ordering and the device cross region, the grouped property of child is not
        // ensured, so we need to clear the attribute of AggNode
        node.setPreGroupedSymbols(ImmutableList.of());
        context.deviceCrossRegion = false;
      }
      // Child has no Ordering and the device doesn't cross region, do nothing here because the
      // logical optimizer 'TransformAggregationToStreamable' will ensure the grouped property of
      // child
    }

    //  push down aggregation if the child of aggregation node only has the union Node
    if (childrenNodes.size() == 1) {
      node.setChild(childrenNodes.get(0));

      if (childrenNodes.get(0) instanceof UnionNode
          && node.getAggregations().values().stream()
              .noneMatch(aggregation -> aggregation.isDistinct() || aggregation.hasMask())) {
        UnionNode unionNode = (UnionNode) childrenNodes.get(0);
        List<PlanNode> children = unionNode.getChildren();

        //  1. add the project Node above the children of the union node
        List<PlanNode> newProjectNodes = new ArrayList<>();

        Map<Symbol, Collection<Symbol>> symbolMapping = unionNode.getSymbolMapping().asMap();
        for (int i = 0; i < children.size(); i++) {
          Assignments.Builder assignmentsBuilder = Assignments.builder();
          for (Map.Entry<Symbol, Collection<Symbol>> symbolEntry : symbolMapping.entrySet()) {
            List<Symbol> symbolList = (ImmutableList<Symbol>) symbolEntry.getValue();
            assignmentsBuilder.put(symbolEntry.getKey(), symbolList.get(i).toSymbolReference());
          }
          newProjectNodes.add(
              new ProjectNode(
                  queryId.genPlanNodeId(), children.get(i), assignmentsBuilder.build()));
        }

        // 2. split the aggregation into partial and final
        Pair<AggregationNode, AggregationNode> splitResult = split(node, symbolAllocator, queryId);
        AggregationNode intermediate = splitResult.right;

        // 3. add the aggregation node above the project node
        List<PlanNode> aggregationNodes =
            newProjectNodes.stream()
                .map(
                    child -> {
                      PlanNodeId planNodeId = queryId.genPlanNodeId();
                      AggregationNode aggregationNode =
                          new AggregationNode(
                              planNodeId,
                              child,
                              intermediate.getAggregations(),
                              intermediate.getGroupingSets(),
                              intermediate.getPreGroupedSymbols(),
                              intermediate.getStep(),
                              intermediate.getHashSymbol(),
                              intermediate.getGroupIdSymbol());
                      if (node.isStreamable() && childOrdering != null) {
                        nodeOrderingMap.put(planNodeId, expectedOrderingSchema);
                      }
                      return aggregationNode;
                    })
                .collect(Collectors.toList());

        // 4. Add a Collect Node under the final Aggregation Node, and add the partial Aggregation
        // nodes as its children
        CollectNode collectNode =
            new CollectNode(queryId.genPlanNodeId(), aggregationNodes.get(0).getOutputSymbols());
        collectNode.setChildren(aggregationNodes);
        splitResult.left.setChild(collectNode);
        return Collections.singletonList(splitResult.left);
      }

      return Collections.singletonList(node);
    }

    // We cannot do multi-stage Aggregate if any aggregation-function is distinct.
    // For Aggregation with mask, there is no need to do multi-stage Aggregate because the
    // MarkDistinctNode will merge all data from different child.
    if (node.getAggregations().values().stream()
        .anyMatch(aggregation -> aggregation.isDistinct() || aggregation.hasMask())) {
      node.setChild(
          mergeChildrenViaCollectOrMergeSort(
              nodeOrderingMap.get(childrenNodes.get(0).getPlanNodeId()), childrenNodes));
      return Collections.singletonList(node);
    }
    Pair<AggregationNode, AggregationNode> splitResult = split(node, symbolAllocator, queryId);
    AggregationNode intermediate = splitResult.right;

    childrenNodes =
        childrenNodes.stream()
            .map(
                child -> {
                  PlanNodeId planNodeId = queryId.genPlanNodeId();
                  AggregationNode aggregationNode =
                      new AggregationNode(
                          planNodeId,
                          child,
                          intermediate.getAggregations(),
                          intermediate.getGroupingSets(),
                          intermediate.getPreGroupedSymbols(),
                          intermediate.getStep(),
                          intermediate.getHashSymbol(),
                          intermediate.getGroupIdSymbol());
                  if (node.isStreamable() && childOrdering != null) {
                    nodeOrderingMap.put(planNodeId, expectedOrderingSchema);
                  }
                  return aggregationNode;
                })
            .collect(Collectors.toList());
    splitResult.left.setChild(
        mergeChildrenViaCollectOrMergeSort(
            nodeOrderingMap.get(childrenNodes.get(0).getPlanNodeId()), childrenNodes));
    return Collections.singletonList(splitResult.left);
  }

  private boolean prefixMatched(OrderingScheme childOrdering, List<Symbol> preGroupedSymbols) {
    List<Symbol> orderKeys = childOrdering.getOrderBy();
    if (orderKeys.size() < preGroupedSymbols.size()) {
      return false;
    }

    for (int i = 0; i < preGroupedSymbols.size(); i++) {
      if (!orderKeys.get(i).equals(preGroupedSymbols.get(i))) {
        return false;
      }
    }
    return true;
  }

  @Override
  public List<PlanNode> visitAggregationTableScan(
      AggregationTableScanNode node, PlanContext context) {
    String dbName =
        node instanceof AggregationTreeDeviceViewScanNode
            ? ((AggregationTreeDeviceViewScanNode) node).getTreeDBName()
            : node.getQualifiedObjectName().getDatabaseName();
    DataPartition dataPartition = analysis.getDataPartitionInfo();
    if (dbName == null || dataPartition == null) {
      node.setRegionReplicaSet(NOT_ASSIGNED);
      return Collections.singletonList(node);
    }
    boolean needSplit = false;
    List<List<TRegionReplicaSet>> regionReplicaSetsList = new ArrayList<>();
    if (dataPartition != null) {
      Map<TSeriesPartitionSlot, Map<TTimePartitionSlot, List<TRegionReplicaSet>>> seriesSlotMap =
          dataPartition.getDataPartitionMap().get(dbName);
      if (seriesSlotMap == null) {
        throw new SemanticException(
            String.format("Given queried database: %s is not exist!", dbName));
      }

      Map<Integer, List<TRegionReplicaSet>> cachedSeriesSlotWithRegions = new HashMap<>();
      for (DeviceEntry deviceEntry : node.getDeviceEntries()) {
        List<TRegionReplicaSet> regionReplicaSets =
            getDeviceReplicaSets(
                dataPartition,
                seriesSlotMap,
                deviceEntry.getDeviceID(),
                node.getTimeFilter(),
                cachedSeriesSlotWithRegions);
        if (regionReplicaSets.size() > 1) {
          needSplit = true;
          context.deviceCrossRegion = true;
          queryContext.setNeedUpdateScanNumForLastQuery(node.mayUseLastCache());
        }
        regionReplicaSetsList.add(regionReplicaSets);
      }
    }

    if (regionReplicaSetsList.isEmpty()) {
      regionReplicaSetsList = Collections.singletonList(Collections.singletonList(NOT_ASSIGNED));
    }

    Map<TRegionReplicaSet, AggregationTableScanNode> regionNodeMap = new HashMap<>();
    // Step is SINGLE and device data in more than one region, we need to final aggregate the result
    // from different region here, so split
    // this node into two-stage
    needSplit = needSplit && node.getStep() == SINGLE;
    AggregationNode finalAggregation = null;
    if (needSplit) {
      Pair<AggregationNode, AggregationTableScanNode> splitResult =
          split(node, symbolAllocator, queryId);
      finalAggregation = splitResult.left;
      AggregationTableScanNode partialAggregation = splitResult.right;

      // cover case: complete push-down + group by + streamable
      if (!context.hasSortProperty && finalAggregation.isStreamable()) {
        OrderingScheme expectedOrderingSchema =
            constructOrderingSchema(node.getPreGroupedSymbols());
        context.setExpectedOrderingScheme(expectedOrderingSchema);
      }

      buildRegionNodeMap(node, regionReplicaSetsList, regionNodeMap, partialAggregation);
    } else {
      buildRegionNodeMap(node, regionReplicaSetsList, regionNodeMap, node);
    }

    List<PlanNode> resultTableScanNodeList = new ArrayList<>();
    TRegionReplicaSet mostUsedDataRegion = null;
    int maxDeviceEntrySizeOfTableScan = 0;
    for (Map.Entry<TRegionReplicaSet, AggregationTableScanNode> entry :
        topology.filterReachableCandidates(regionNodeMap.entrySet())) {
      DeviceTableScanNode subDeviceTableScanNode = entry.getValue();
      resultTableScanNodeList.add(subDeviceTableScanNode);

      if (mostUsedDataRegion == null
          || subDeviceTableScanNode.getDeviceEntries().size() > maxDeviceEntrySizeOfTableScan) {
        mostUsedDataRegion = entry.getKey();
        maxDeviceEntrySizeOfTableScan = subDeviceTableScanNode.getDeviceEntries().size();
      }
    }
    if (mostUsedDataRegion == null) {
      throw new RootFIPlacementException(regionNodeMap.keySet());
    }
    context.mostUsedRegion = mostUsedDataRegion;

    if (context.hasSortProperty) {
      processSortProperty(node, resultTableScanNodeList, context);
    }

    if (needSplit) {
      if (resultTableScanNodeList.size() == 1) {
        finalAggregation.setChild(resultTableScanNodeList.get(0));
      } else if (resultTableScanNodeList.size() > 1) {
        OrderingScheme childOrdering =
            nodeOrderingMap.get(resultTableScanNodeList.get(0).getPlanNodeId());
        finalAggregation.setChild(
            mergeChildrenViaCollectOrMergeSort(childOrdering, resultTableScanNodeList));
      } else {
        throw new IllegalStateException("List<PlanNode>.size should >= 1, but now is 0");
      }
      resultTableScanNodeList = Collections.singletonList(finalAggregation);
    }

    return resultTableScanNodeList;
  }

  private List<TRegionReplicaSet> getDeviceReplicaSets(
      DataPartition dataPartition,
      Map<TSeriesPartitionSlot, Map<TTimePartitionSlot, List<TRegionReplicaSet>>> seriesSlotMap,
      IDeviceID deviceId,
      Filter timeFilter,
      Map<Integer, List<TRegionReplicaSet>> cachedSeriesSlotWithRegions) {

    // given seriesPartitionSlot has already been calculated
    final TSeriesPartitionSlot seriesPartitionSlot = dataPartition.calculateDeviceGroupId(deviceId);
    List<TRegionReplicaSet> regionReplicaSets =
        cachedSeriesSlotWithRegions.get(seriesPartitionSlot.getSlotId());
    if (regionReplicaSets != null) {
      return regionReplicaSets;
    }

    // given seriesPartitionSlot has not been calculated
    Map<TTimePartitionSlot, List<TRegionReplicaSet>> timeSlotMap =
        seriesSlotMap.get(seriesPartitionSlot);
    if (timeSlotMap == null) {
      List<TRegionReplicaSet> cachedReplicaSets = Collections.singletonList(NOT_ASSIGNED);
      cachedSeriesSlotWithRegions.put(seriesPartitionSlot.getSlotId(), cachedReplicaSets);
      return cachedReplicaSets;
    }
    if (timeSlotMap.size() == 1) {
      TTimePartitionSlot timePartitionSlot = timeSlotMap.keySet().iterator().next();
      if (TimePartitionUtils.satisfyPartitionStartTime(timeFilter, timePartitionSlot.startTime)) {
        cachedSeriesSlotWithRegions.put(
            seriesPartitionSlot.getSlotId(), timeSlotMap.values().iterator().next());
        return timeSlotMap.values().iterator().next();
      } else {
        cachedSeriesSlotWithRegions.put(seriesPartitionSlot.getSlotId(), Collections.emptyList());
        return Collections.emptyList();
      }
    }

    Set<TRegionReplicaSet> resultSet = new HashSet<>();
    for (Map.Entry<TTimePartitionSlot, List<TRegionReplicaSet>> entry : timeSlotMap.entrySet()) {
      TTimePartitionSlot timePartitionSlot = entry.getKey();
      if (TimePartitionUtils.satisfyPartitionStartTime(timeFilter, timePartitionSlot.startTime)) {
        resultSet.addAll(entry.getValue());
      }
    }
    List<TRegionReplicaSet> resultList = new ArrayList<>(resultSet);
    cachedSeriesSlotWithRegions.put(seriesPartitionSlot.getSlotId(), resultList);
    return resultList;
  }

  @Override
  public List<PlanNode> visitEnforceSingleRow(EnforceSingleRowNode node, PlanContext context) {
    return dealWithPlainSingleChildNode(node, context);
  }

  @Override
  public List<PlanNode> visitAssignUniqueId(AssignUniqueId node, PlanContext context) {
    return dealWithPlainSingleChildNode(node, context);
  }

  @Override
  public List<PlanNode> visitMarkDistinct(MarkDistinctNode node, PlanContext context) {
    return dealWithPlainSingleChildNode(node, context);
  }

  private List<PlanNode> dealWithPlainSingleChildNode(
      SingleChildProcessNode node, PlanContext context) {
    List<PlanNode> childrenNodes = node.getChild().accept(this, context);
    OrderingScheme childOrdering = nodeOrderingMap.get(childrenNodes.get(0).getPlanNodeId());
    if (childOrdering != null) {
      nodeOrderingMap.put(node.getPlanNodeId(), childOrdering);
    }

    node.setChild(mergeChildrenViaCollectOrMergeSort(childOrdering, childrenNodes));
    return Collections.singletonList(node);
  }

  private List<PlanNode> splitForEachChild(PlanNode node, List<PlanNode> childrenNodes) {
    ImmutableList.Builder<PlanNode> result = ImmutableList.builder();
    for (PlanNode child : childrenNodes) {
      PlanNode subNode = node.clone();
      subNode.addChild(child);
      subNode.setPlanNodeId(queryId.genPlanNodeId());
      result.add(subNode);
    }
    return result.build();
  }

  @Override
  public List<PlanNode> visitTableFunctionProcessor(
      TableFunctionProcessorNode node, PlanContext context) {
    context.clearExpectedOrderingScheme();
    if (node.getChildren().isEmpty()) {
      return Collections.singletonList(node);
    }
    boolean canSplitPushDown = node.isRowSemantic() || (node.getChild() instanceof GroupNode);
    List<PlanNode> childrenNodes = node.getChild().accept(this, context);
    if (childrenNodes.size() == 1) {
      node.setChild(childrenNodes.get(0));
      return Collections.singletonList(node);
    } else if (!canSplitPushDown) {
      CollectNode collectNode =
          new CollectNode(queryId.genPlanNodeId(), node.getChildren().get(0).getOutputSymbols());
      childrenNodes.forEach(collectNode::addChild);
      node.setChild(collectNode);
      return Collections.singletonList(node);
    } else {
      return splitForEachChild(node, childrenNodes);
    }
  }

  private void buildRegionNodeMap(
      AggregationTableScanNode originalAggTableScanNode,
      List<List<TRegionReplicaSet>> regionReplicaSetsList,
      Map<TRegionReplicaSet, AggregationTableScanNode> regionNodeMap,
      AggregationTableScanNode partialAggTableScanNode) {
    AggregationTreeDeviceViewScanNode aggregationTreeDeviceViewScanNode;
    if (originalAggTableScanNode instanceof AggregationTreeDeviceViewScanNode) {
      aggregationTreeDeviceViewScanNode =
          (AggregationTreeDeviceViewScanNode) originalAggTableScanNode;
    } else {
      aggregationTreeDeviceViewScanNode = null;
    }

    for (int i = 0; i < regionReplicaSetsList.size(); i++) {
      for (TRegionReplicaSet regionReplicaSet : regionReplicaSetsList.get(i)) {
        AggregationTableScanNode aggregationTableScanNode =
            regionNodeMap.computeIfAbsent(
                regionReplicaSet,
                k -> {
                  AggregationTableScanNode scanNode =
                      (aggregationTreeDeviceViewScanNode == null)
                          ? new AggregationTableScanNode(
                              queryId.genPlanNodeId(),
                              partialAggTableScanNode.getQualifiedObjectName(),
                              partialAggTableScanNode.getOutputSymbols(),
                              partialAggTableScanNode.getAssignments(),
                              new ArrayList<>(),
                              partialAggTableScanNode.getTagAndAttributeIndexMap(),
                              partialAggTableScanNode.getScanOrder(),
                              partialAggTableScanNode.getTimePredicate().orElse(null),
                              partialAggTableScanNode.getPushDownPredicate(),
                              partialAggTableScanNode.getPushDownLimit(),
                              partialAggTableScanNode.getPushDownOffset(),
                              partialAggTableScanNode.isPushLimitToEachDevice(),
                              partialAggTableScanNode.containsNonAlignedDevice(),
                              partialAggTableScanNode.getProjection(),
                              partialAggTableScanNode.getAggregations(),
                              partialAggTableScanNode.getGroupingSets(),
                              partialAggTableScanNode.getPreGroupedSymbols(),
                              partialAggTableScanNode.getStep(),
                              partialAggTableScanNode.getGroupIdSymbol())
                          : new AggregationTreeDeviceViewScanNode(
                              queryId.genPlanNodeId(),
                              partialAggTableScanNode.getQualifiedObjectName(),
                              partialAggTableScanNode.getOutputSymbols(),
                              partialAggTableScanNode.getAssignments(),
                              new ArrayList<>(),
                              partialAggTableScanNode.getTagAndAttributeIndexMap(),
                              partialAggTableScanNode.getScanOrder(),
                              partialAggTableScanNode.getTimePredicate().orElse(null),
                              partialAggTableScanNode.getPushDownPredicate(),
                              partialAggTableScanNode.getPushDownLimit(),
                              partialAggTableScanNode.getPushDownOffset(),
                              partialAggTableScanNode.isPushLimitToEachDevice(),
                              partialAggTableScanNode.containsNonAlignedDevice(),
                              partialAggTableScanNode.getProjection(),
                              partialAggTableScanNode.getAggregations(),
                              partialAggTableScanNode.getGroupingSets(),
                              partialAggTableScanNode.getPreGroupedSymbols(),
                              partialAggTableScanNode.getStep(),
                              partialAggTableScanNode.getGroupIdSymbol(),
                              aggregationTreeDeviceViewScanNode.getTreeDBName(),
                              aggregationTreeDeviceViewScanNode.getMeasurementColumnNameMap());
                  scanNode.setRegionReplicaSet(regionReplicaSet);
                  return scanNode;
                });
        if (originalAggTableScanNode.getDeviceEntries().size() > i
            && originalAggTableScanNode.getDeviceEntries().get(i) != null) {
          aggregationTableScanNode.appendDeviceEntry(
              originalAggTableScanNode.getDeviceEntries().get(i));
        }
      }
    }
  }

  private static OrderingScheme constructOrderingSchema(List<Symbol> symbols) {
    Map<Symbol, SortOrder> orderings = new HashMap<>();
    symbols.forEach(symbol -> orderings.put(symbol, SortOrder.ASC_NULLS_LAST));
    return new OrderingScheme(symbols, orderings);
  }

  private PlanNode mergeChildrenViaCollectOrMergeSort(
      final OrderingScheme childOrdering, final List<PlanNode> childrenNodes) {
    checkArgument(childrenNodes != null, "childrenNodes should not be null.");
    checkArgument(!childrenNodes.isEmpty(), "childrenNodes should not be empty.");

    if (childrenNodes.size() == 1) {
      return childrenNodes.get(0);
    }

    final PlanNode firstChild = childrenNodes.get(0);

    // children has sort property, use MergeSort to merge children
    if (childOrdering != null) {
      final MergeSortNode mergeSortNode =
          new MergeSortNode(queryId.genPlanNodeId(), childOrdering, firstChild.getOutputSymbols());
      childrenNodes.forEach(mergeSortNode::addChild);
      nodeOrderingMap.put(mergeSortNode.getPlanNodeId(), childOrdering);
      return mergeSortNode;
    }

    // children has no sort property, use CollectNode to merge children
    final CollectNode collectNode =
        new CollectNode(queryId.genPlanNodeId(), firstChild.getOutputSymbols());
    childrenNodes.forEach(collectNode::addChild);
    return collectNode;
  }

  private void processSortProperty(
      final DeviceTableScanNode deviceTableScanNode,
      final List<PlanNode> resultTableScanNodeList,
      final PlanContext context) {
    final List<Symbol> newOrderingSymbols = new ArrayList<>();
    final List<SortOrder> newSortOrders = new ArrayList<>();
    final OrderingScheme expectedOrderingScheme = context.expectedOrderingScheme;

    boolean lastIsTimeRelated = false;
    for (final Symbol symbol : expectedOrderingScheme.getOrderBy()) {
      if (timeRelatedSymbol(symbol, deviceTableScanNode)) {
        if (!expectedOrderingScheme.getOrderings().get(symbol).isAscending()) {
          // TODO(beyyes) move scan order judgement into logical plan optimizer
          resultTableScanNodeList.forEach(
              node -> ((DeviceTableScanNode) node).setScanOrder(Ordering.DESC));
        }
        newOrderingSymbols.add(symbol);
        newSortOrders.add(expectedOrderingScheme.getOrdering(symbol));
        lastIsTimeRelated = true;
        break;
      } else if (!deviceTableScanNode.getTagAndAttributeIndexMap().containsKey(symbol)) {
        break;
      }

      newOrderingSymbols.add(symbol);
      newSortOrders.add(expectedOrderingScheme.getOrdering(symbol));
    }

    // no sort property can be pushed down into DeviceTableScanNode
    if (newOrderingSymbols.isEmpty()) {
      return;
    }

    Optional<IDeviceID.TreeDeviceIdColumnValueExtractor> extractor =
        createTreeDeviceIdColumnValueExtractor(deviceTableScanNode);
    final List<Function<DeviceEntry, String>> orderingRules = new ArrayList<>();
    for (final Symbol symbol : newOrderingSymbols) {
      final Integer idx = deviceTableScanNode.getTagAndAttributeIndexMap().get(symbol);
      if (idx == null) {
        // time column or date_bin column
        break;
      }
      if (deviceTableScanNode.getAssignments().get(symbol).getColumnCategory()
          == TsTableColumnCategory.TAG) {

        // segments[0] is always tableName for table model
        Function<DeviceEntry, String> iDColumnFunction =
            extractor
                .<Function<DeviceEntry, String>>map(
                    treeDeviceIdColumnValueExtractor ->
                        deviceEntry ->
                            (String)
                                treeDeviceIdColumnValueExtractor.extract(
                                    deviceEntry.getDeviceID(), idx))
                .orElseGet(() -> deviceEntry -> (String) deviceEntry.getNthSegment(idx + 1));
        orderingRules.add(iDColumnFunction);
      } else {
        orderingRules.add(
            deviceEntry ->
                deviceEntry.getAttributeColumnValues()[idx] == null
                    ? null
                    : deviceEntry.getAttributeColumnValues()[idx].getStringValue(
                        TSFileConfig.STRING_CHARSET));
      }
    }
    Comparator<DeviceEntry> comparator = null;

    if (!orderingRules.isEmpty()) {
      if (newSortOrders.get(0).isNullsFirst()) {
        comparator =
            newSortOrders.get(0).isAscending()
                ? Comparator.comparing(
                    orderingRules.get(0), Comparator.nullsFirst(Comparator.naturalOrder()))
                : Comparator.comparing(
                        orderingRules.get(0), Comparator.nullsFirst(Comparator.naturalOrder()))
                    .reversed();
      } else {
        comparator =
            newSortOrders.get(0).isAscending()
                ? Comparator.comparing(
                    orderingRules.get(0), Comparator.nullsLast(Comparator.naturalOrder()))
                : Comparator.comparing(
                        orderingRules.get(0), Comparator.nullsLast(Comparator.naturalOrder()))
                    .reversed();
      }
      for (int i = 1; i < orderingRules.size(); i++) {
        final Comparator<DeviceEntry> thenComparator;
        if (newSortOrders.get(i).isNullsFirst()) {
          thenComparator =
              newSortOrders.get(i).isAscending()
                  ? Comparator.comparing(
                      orderingRules.get(i), Comparator.nullsFirst(Comparator.naturalOrder()))
                  : Comparator.comparing(
                          orderingRules.get(i), Comparator.nullsFirst(Comparator.naturalOrder()))
                      .reversed();
        } else {
          thenComparator =
              newSortOrders.get(i).isAscending()
                  ? Comparator.comparing(
                      orderingRules.get(i), Comparator.nullsLast(Comparator.naturalOrder()))
                  : Comparator.comparing(
                          orderingRules.get(i), Comparator.nullsLast(Comparator.naturalOrder()))
                      .reversed();
        }
        comparator = comparator.thenComparing(thenComparator);
      }
    }

    final Optional<OrderingScheme> newOrderingScheme =
        tableScanOrderingSchema(
            analysis.getTableColumnSchema(deviceTableScanNode.getQualifiedObjectName()),
            deviceTableScanNode.getAssignments(),
            newOrderingSymbols,
            newSortOrders,
            lastIsTimeRelated,
            deviceTableScanNode.getDeviceEntries().size() == 1);
    for (final PlanNode planNode : resultTableScanNodeList) {
      final DeviceTableScanNode scanNode = (DeviceTableScanNode) planNode;
      newOrderingScheme.ifPresent(
          orderingScheme -> nodeOrderingMap.put(scanNode.getPlanNodeId(), orderingScheme));
      if (comparator != null) {
        scanNode.getDeviceEntries().sort(comparator);
      }
    }
  }

  private Optional<IDeviceID.TreeDeviceIdColumnValueExtractor>
      createTreeDeviceIdColumnValueExtractor(DeviceTableScanNode node) {
    if (node instanceof TreeDeviceViewScanNode
        || node instanceof AggregationTreeDeviceViewScanNode) {
      QualifiedObjectName qualifiedObjectName = node.getQualifiedObjectName();
      TsTable table =
          DataNodeTableCache.getInstance()
              .getTable(qualifiedObjectName.getDatabaseName(), qualifiedObjectName.getObjectName());
      return Optional.of(
          TableOperatorGenerator.createTreeDeviceIdColumnValueExtractor(
              DataNodeTreeViewSchemaUtils.getPrefixPath(table)));
    } else {
      return Optional.empty();
    }
  }

  private Optional<OrderingScheme> tableScanOrderingSchema(
      Map<Symbol, ColumnSchema> tableColumnSchema,
      Map<Symbol, ColumnSchema> nodeColumnSchema,
      List<Symbol> newOrderingSymbols,
      List<SortOrder> newSortOrders,
      boolean lastIsTimeRelated,
      boolean isSingleDevice) {

    if (isSingleDevice || !lastIsTimeRelated) {
      return Optional.of(
          new OrderingScheme(
              newOrderingSymbols,
              IntStream.range(0, newOrderingSymbols.size())
                  .boxed()
                  .collect(Collectors.toMap(newOrderingSymbols::get, newSortOrders::get))));
    } else { // table scan node has more than one device and last order item is time related
      int size = newOrderingSymbols.size();
      if (size == 1) {
        return Optional.empty();
      }
      OrderingScheme orderingScheme =
          new OrderingScheme(
              newOrderingSymbols.subList(0, size - 1),
              IntStream.range(0, size - 1)
                  .boxed()
                  .collect(Collectors.toMap(newOrderingSymbols::get, newSortOrders::get)));
      if (isOrderByAllIdsAndTime(
          tableColumnSchema,
          nodeColumnSchema,
          orderingScheme,
          size - 2)) { // all id columns included
        return Optional.of(
            new OrderingScheme(
                newOrderingSymbols,
                IntStream.range(0, newOrderingSymbols.size())
                    .boxed()
                    .collect(Collectors.toMap(newOrderingSymbols::get, newSortOrders::get))));
      } else { // remove the last time column related
        return Optional.of(orderingScheme);
      }
    }
  }

  // time column or push down date_bin function call in agg which should only have one such column
  private boolean timeRelatedSymbol(Symbol symbol, DeviceTableScanNode deviceTableScanNode) {
    if (deviceTableScanNode.isTimeColumn(symbol)) {
      return true;
    }

    if (deviceTableScanNode instanceof AggregationTableScanNode) {
      AggregationTableScanNode aggregationTableScanNode =
          (AggregationTableScanNode) deviceTableScanNode;
      if (aggregationTableScanNode.getProjection() != null
          && !aggregationTableScanNode.getProjection().getMap().isEmpty()) {
        Expression expression = aggregationTableScanNode.getProjection().get(symbol);
        // For now, if there is FunctionCall in AggregationTableScanNode, it must be date_bin
        // function of time. See PushAggregationIntoTableScan#isDateBinFunctionOfTime
        return expression instanceof FunctionCall;
      }
    }

    return false;
  }

  // ------------------- schema related interface ---------------------------------------------
  @Override
  public List<PlanNode> visitTableDeviceQueryScan(
      final TableDeviceQueryScanNode node, final PlanContext context) {
    return visitAbstractTableDeviceQuery(node, context);
  }

  @Override
  public List<PlanNode> visitTableDeviceQueryCount(
      final TableDeviceQueryCountNode node, final PlanContext context) {
    return visitAbstractTableDeviceQuery(node, context);
  }

  private List<PlanNode> visitAbstractTableDeviceQuery(
      final AbstractTableDeviceQueryNode node, final PlanContext context) {
    final Set<TRegionReplicaSet> schemaRegionSet = new HashSet<>();
    analysis
        .getSchemaPartitionInfo()
        .getSchemaPartitionMap()
        .values()
        .forEach(
            replicaSetMap ->
                replicaSetMap.forEach(
                    (deviceGroupId, schemaRegionReplicaSet) ->
                        schemaRegionSet.add(schemaRegionReplicaSet)));

    context.mostUsedRegion = schemaRegionSet.iterator().next();
    if (schemaRegionSet.size() == 1) {
      node.setRegionReplicaSet(schemaRegionSet.iterator().next());
      return Collections.singletonList(node);
    } else {
      final List<PlanNode> res = new ArrayList<>(schemaRegionSet.size());
      for (final TRegionReplicaSet schemaRegion : schemaRegionSet) {
        final AbstractTableDeviceQueryNode clonedChild =
            (AbstractTableDeviceQueryNode) node.clone();
        clonedChild.setPlanNodeId(queryId.genPlanNodeId());
        clonedChild.setRegionReplicaSet(schemaRegion);
        res.add(clonedChild);
      }
      return res;
    }
  }

  @Override
  public List<PlanNode> visitTableDeviceFetch(
      final TableDeviceFetchNode node, final PlanContext context) {
    final String database = node.getDatabase();
    final Set<TRegionReplicaSet> schemaRegionSet = new HashSet<>();
    final SchemaPartition schemaPartition = analysis.getSchemaPartitionInfo();
    final Map<TSeriesPartitionSlot, TRegionReplicaSet> databaseMap =
        schemaPartition.getSchemaPartitionMap().get(database);

    databaseMap.forEach(
        (deviceGroupId, schemaRegionReplicaSet) -> schemaRegionSet.add(schemaRegionReplicaSet));

    if (schemaRegionSet.size() == 1) {
      context.mostUsedRegion = schemaRegionSet.iterator().next();
      node.setRegionReplicaSet(context.mostUsedRegion);
      return Collections.singletonList(node);
    } else {
      final Map<TRegionReplicaSet, TableDeviceFetchNode> tableDeviceFetchMap = new HashMap<>();

      final List<IDeviceID> partitionKeyList = node.getPartitionKeyList();
      final List<Object[]> deviceIDArray = node.getDeviceIdList();
      for (int i = 0; i < node.getPartitionKeyList().size(); ++i) {
        final TRegionReplicaSet regionReplicaSet =
            databaseMap.get(schemaPartition.calculateDeviceGroupId(partitionKeyList.get(i)));
        if (Objects.nonNull(regionReplicaSet)) {
          tableDeviceFetchMap
              .computeIfAbsent(
                  regionReplicaSet,
                  k -> {
                    final TableDeviceFetchNode clonedNode =
                        (TableDeviceFetchNode) node.cloneForDistribution();
                    clonedNode.setPlanNodeId(queryId.genPlanNodeId());
                    clonedNode.setRegionReplicaSet(regionReplicaSet);
                    return clonedNode;
                  })
              .addDeviceId(deviceIDArray.get(i));
        }
      }

      final List<PlanNode> res = new ArrayList<>();
      TRegionReplicaSet mostUsedSchemaRegion = null;
      int maxDeviceEntrySizeOfTableScan = 0;
      for (final Map.Entry<TRegionReplicaSet, TableDeviceFetchNode> entry :
          tableDeviceFetchMap.entrySet()) {
        final TRegionReplicaSet regionReplicaSet = entry.getKey();
        final TableDeviceFetchNode subTableDeviceFetchNode = entry.getValue();
        res.add(subTableDeviceFetchNode);

        if (subTableDeviceFetchNode.getDeviceIdList().size() > maxDeviceEntrySizeOfTableScan) {
          mostUsedSchemaRegion = regionReplicaSet;
          maxDeviceEntrySizeOfTableScan = subTableDeviceFetchNode.getDeviceIdList().size();
        }
      }
      if (mostUsedSchemaRegion == null) {
        throw new RootFIPlacementException(tableDeviceFetchMap.keySet());
      }
      context.mostUsedRegion = mostUsedSchemaRegion;
      return res;
    }
  }

  @Override
  public List<PlanNode> visitWindowFunction(WindowNode node, PlanContext context) {
    context.clearExpectedOrderingScheme();
    if (node.getSpecification().getPartitionBy().isEmpty()) {
      Optional<OrderingScheme> orderingScheme = node.getSpecification().getOrderingScheme();
      orderingScheme.ifPresent(scheme -> nodeOrderingMap.put(node.getPlanNodeId(), scheme));
    }

    if (node.getChildren().isEmpty()) {
      return Collections.singletonList(node);
    }

    boolean canSplitPushDown = node.getChild() instanceof GroupNode;
    List<PlanNode> childrenNodes = node.getChild().accept(this, context);
    if (childrenNodes.size() == 1) {
      node.setChild(childrenNodes.get(0));
      return Collections.singletonList(node);
    } else if (!canSplitPushDown) {
      CollectNode collectNode =
          new CollectNode(queryId.genPlanNodeId(), node.getChildren().get(0).getOutputSymbols());
      childrenNodes.forEach(collectNode::addChild);
      node.setChild(collectNode);
      return Collections.singletonList(node);
    } else {
      return splitForEachChild(node, childrenNodes);
    }
  }

  @Override
  public List<PlanNode> visitUnion(UnionNode node, PlanContext context) {
    context.clearExpectedOrderingScheme();

    List<List<PlanNode>> children =
        node.getChildren().stream()
            .map(child -> child.accept(this, context))
            .collect(toImmutableList());

    List<PlanNode> newUnionChildren =
        children.stream().flatMap(Collection::stream).collect(toImmutableList());

    // after rewrite, we need to reconstruct SymbolMapping
    ListMultimap<Symbol, Symbol> oldSymbolMapping = node.getSymbolMapping();
    ImmutableListMultimap.Builder<Symbol, Symbol> newSymbolMapping =
        ImmutableListMultimap.builder();
    for (Symbol symbol : oldSymbolMapping.keySet()) {
      List<Symbol> oldSymbols = oldSymbolMapping.get(symbol);
      for (int i = 0; i < oldSymbols.size(); i++) {
        Symbol target = oldSymbols.get(i);
        int duplicateSize = children.get(i).size();
        // add the same Symbol for all children spilt from one original node
        while (duplicateSize > 0) {
          newSymbolMapping.put(symbol, target);
          duplicateSize--;
        }
      }
    }
    return Collections.singletonList(
        new UnionNode(
            node.getPlanNodeId(),
            newUnionChildren,
            newSymbolMapping.build(),
            node.getOutputSymbols()));
  }

  public static class PlanContext {
    final Map<PlanNodeId, NodeDistribution> nodeDistributionMap;
    boolean hasExchangeNode = false;
    boolean hasSortProperty = false;
    boolean pushDownGrouping = false;
    OrderingScheme expectedOrderingScheme;
    TRegionReplicaSet mostUsedRegion;
    boolean deviceCrossRegion;

    public PlanContext() {
      this.nodeDistributionMap = new HashMap<>();
    }

    public NodeDistribution getNodeDistribution(PlanNodeId nodeId) {
      return this.nodeDistributionMap.get(nodeId);
    }

    public void clearExpectedOrderingScheme() {
      expectedOrderingScheme = null;
      hasSortProperty = false;
    }

    public void setExpectedOrderingScheme(OrderingScheme expectedOrderingScheme) {
      this.expectedOrderingScheme = expectedOrderingScheme;
      hasSortProperty = true;
    }

    public void setPushDownGrouping(boolean pushDownGrouping) {
      this.pushDownGrouping = pushDownGrouping;
    }

    public boolean isPushDownGrouping() {
      return pushDownGrouping;
    }
  }
}
