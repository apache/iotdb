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

package org.apache.iotdb.db.queryengine.plan.relational.planner.distribute;

import org.apache.iotdb.common.rpc.thrift.TRegionReplicaSet;
import org.apache.iotdb.common.rpc.thrift.TSeriesPartitionSlot;
import org.apache.iotdb.commons.partition.SchemaPartition;
import org.apache.iotdb.commons.schema.table.column.TsTableColumnCategory;
import org.apache.iotdb.commons.utils.PathUtils;
import org.apache.iotdb.db.queryengine.common.MPPQueryContext;
import org.apache.iotdb.db.queryengine.common.QueryId;
import org.apache.iotdb.db.queryengine.plan.planner.distribution.NodeDistribution;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanVisitor;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.WritePlanNode;
import org.apache.iotdb.db.queryengine.plan.relational.analyzer.Analysis;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.ColumnSchema;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.DeviceEntry;
import org.apache.iotdb.db.queryengine.plan.relational.planner.OrderingScheme;
import org.apache.iotdb.db.queryengine.plan.relational.planner.SortOrder;
import org.apache.iotdb.db.queryengine.plan.relational.planner.Symbol;
import org.apache.iotdb.db.queryengine.plan.relational.planner.SymbolAllocator;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.AggregationNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.AggregationTableScanNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.CollectNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.ExplainAnalyzeNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.FillNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.FilterNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.GapFillNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.JoinNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.LimitNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.MergeSortNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.OffsetNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.OutputNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.ProjectNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.SortNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.StreamSortNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.TableScanNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.TopKNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.ValueFillNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.schema.AbstractTableDeviceQueryNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.schema.TableDeviceFetchNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.schema.TableDeviceQueryCountNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.schema.TableDeviceQueryScanNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.optimizations.PushPredicateIntoTableScan;
import org.apache.iotdb.db.queryengine.plan.statement.component.Ordering;

import com.google.common.collect.ImmutableSet;
import org.apache.tsfile.common.conf.TSFileConfig;
import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.utils.Pair;

import javax.annotation.Nonnull;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
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
import static org.apache.iotdb.db.queryengine.plan.relational.planner.SymbolAllocator.GROUP_KEY_SUFFIX;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.SymbolAllocator.SEPARATOR;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.node.AggregationNode.Step.SINGLE;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.optimizations.PushPredicateIntoTableScan.containsDiffFunction;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.optimizations.TransformSortToStreamSort.isOrderByAllIdsAndTime;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.optimizations.Util.split;
import static org.apache.iotdb.db.queryengine.transformation.dag.column.unary.scalar.TableBuiltinScalarFunction.DATE_BIN;
import static org.apache.tsfile.utils.Preconditions.checkArgument;

/** This class is used to generate distributed plan for table model. */
public class TableDistributedPlanGenerator
    extends PlanVisitor<List<PlanNode>, TableDistributedPlanGenerator.PlanContext> {
  private static final String PUSH_DOWN_DATE_BIN_SYMBOL_NAME =
      DATE_BIN.getFunctionName() + SEPARATOR + GROUP_KEY_SUFFIX;
  private final QueryId queryId;
  private final Analysis analysis;
  private final SymbolAllocator symbolAllocator;
  private final Map<PlanNodeId, OrderingScheme> nodeOrderingMap = new HashMap<>();

  public TableDistributedPlanGenerator(
      MPPQueryContext queryContext, Analysis analysis, SymbolAllocator symbolAllocator) {
    this.queryId = queryContext.getQueryId();
    this.analysis = analysis;
    this.symbolAllocator = symbolAllocator;
  }

  public List<PlanNode> genResult(PlanNode node, PlanContext context) {
    List<PlanNode> res = node.accept(this, context);
    if (res.size() == 1) {
      return res;
    } else if (res.size() > 1) {
      CollectNode collectNode =
          new CollectNode(queryId.genPlanNodeId(), res.get(0).getOutputSymbols());
      res.forEach(collectNode::addChild);
      return Collections.singletonList(collectNode);
    } else {
      throw new IllegalStateException("List<PlanNode>.size should >= 1, but now is 0");
    }
  }

  @Override
  public List<PlanNode> visitPlan(
      PlanNode node, TableDistributedPlanGenerator.PlanContext context) {
    if (node instanceof WritePlanNode) {
      return Collections.singletonList(node);
    }

    List<List<PlanNode>> children =
        node.getChildren().stream()
            .map(child -> child.accept(this, context))
            .collect(toImmutableList());

    PlanNode newNode = node.clone();
    for (List<PlanNode> planNodes : children) {
      planNodes.forEach(newNode::addChild);
    }
    return Collections.singletonList(newNode);
  }

  @Override
  public List<PlanNode> visitExplainAnalyze(ExplainAnalyzeNode node, PlanContext context) {
    List<PlanNode> children = genResult(node.getChild(), context);
    node.setChild(children.get(0));
    return Collections.singletonList(node);
  }

  @Override
  public List<PlanNode> visitOutput(OutputNode node, PlanContext context) {
    List<PlanNode> childrenNodes = node.getChild().accept(this, context);
    OrderingScheme childOrdering = nodeOrderingMap.get(childrenNodes.get(0).getPlanNodeId());
    if (childOrdering != null) {
      nodeOrderingMap.put(node.getPlanNodeId(), childOrdering);
    }

    if (childrenNodes.size() == 1) {
      node.setChild(childrenNodes.get(0));
      return Collections.singletonList(node);
    }

    node.setChild(mergeChildrenViaCollectOrMergeSort(childOrdering, childrenNodes));
    return Collections.singletonList(node);
  }

  @Override
  public List<PlanNode> visitFill(FillNode node, PlanContext context) {
    if (!(node instanceof ValueFillNode)) {
      context.clearExpectedOrderingScheme();
    }
    List<PlanNode> childrenNodes = node.getChild().accept(this, context);
    OrderingScheme childOrdering = nodeOrderingMap.get(childrenNodes.get(0).getPlanNodeId());
    if (childOrdering != null) {
      nodeOrderingMap.put(node.getPlanNodeId(), childOrdering);
    }

    if (childrenNodes.size() == 1) {
      node.setChild(childrenNodes.get(0));
      return Collections.singletonList(node);
    }

    node.setChild(mergeChildrenViaCollectOrMergeSort(childOrdering, childrenNodes));
    return Collections.singletonList(node);
  }

  @Override
  public List<PlanNode> visitGapFill(GapFillNode node, PlanContext context) {
    context.clearExpectedOrderingScheme();
    List<PlanNode> childrenNodes = node.getChild().accept(this, context);
    OrderingScheme childOrdering = nodeOrderingMap.get(childrenNodes.get(0).getPlanNodeId());
    if (childOrdering != null) {
      nodeOrderingMap.put(node.getPlanNodeId(), childOrdering);
    }

    if (childrenNodes.size() == 1) {
      node.setChild(childrenNodes.get(0));
      return Collections.singletonList(node);
    }

    node.setChild(mergeChildrenViaCollectOrMergeSort(childOrdering, childrenNodes));
    return Collections.singletonList(node);
  }

  @Override
  public List<PlanNode> visitLimit(LimitNode node, PlanContext context) {
    List<PlanNode> childrenNodes = node.getChild().accept(this, context);
    OrderingScheme childOrdering = nodeOrderingMap.get(childrenNodes.get(0).getPlanNodeId());
    if (childOrdering != null) {
      nodeOrderingMap.put(node.getPlanNodeId(), childOrdering);
    }

    if (childrenNodes.size() == 1) {
      node.setChild(childrenNodes.get(0));
      return Collections.singletonList(node);
    }

    // push down LimitNode in distributed plan optimize rule
    node.setChild(mergeChildrenViaCollectOrMergeSort(childOrdering, childrenNodes));
    return Collections.singletonList(node);
  }

  @Override
  public List<PlanNode> visitOffset(OffsetNode node, PlanContext context) {
    List<PlanNode> childrenNodes = node.getChild().accept(this, context);
    OrderingScheme childOrdering = nodeOrderingMap.get(childrenNodes.get(0).getPlanNodeId());
    if (childOrdering != null) {
      nodeOrderingMap.put(node.getPlanNodeId(), childOrdering);
    }

    if (childrenNodes.size() == 1) {
      node.setChild(childrenNodes.get(0));
      return Collections.singletonList(node);
    }

    node.setChild(mergeChildrenViaCollectOrMergeSort(childOrdering, childrenNodes));
    return Collections.singletonList(node);
  }

  @Override
  public List<PlanNode> visitProject(ProjectNode node, PlanContext context) {
    List<PlanNode> childrenNodes = node.getChild().accept(this, context);
    OrderingScheme childOrdering = nodeOrderingMap.get(childrenNodes.get(0).getPlanNodeId());
    if (childOrdering != null) {
      nodeOrderingMap.put(node.getPlanNodeId(), childOrdering);
    }

    if (childrenNodes.size() == 1) {
      node.setChild(childrenNodes.get(0));
      return Collections.singletonList(node);
    }

    boolean canCopyThis = true;
    if (childOrdering != null) {
      // the column used for order by has been pruned, we can't copy this node to sub nodeTrees.
      canCopyThis =
          ImmutableSet.copyOf(node.getOutputSymbols()).containsAll(childOrdering.getOrderBy());
    }
    canCopyThis =
        canCopyThis
            && node.getAssignments().getMap().values().stream()
                .noneMatch(PushPredicateIntoTableScan::containsDiffFunction);

    if (!canCopyThis) {
      node.setChild(mergeChildrenViaCollectOrMergeSort(childOrdering, childrenNodes));
      return Collections.singletonList(node);
    }

    List<PlanNode> resultNodeList = new ArrayList<>();
    for (int i = 0; i < childrenNodes.size(); i++) {
      PlanNode child = childrenNodes.get(i);
      ProjectNode subProjectNode =
          new ProjectNode(queryId.genPlanNodeId(), child, node.getAssignments());
      resultNodeList.add(subProjectNode);
      if (i == 0) {
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
      node.setChildren(Collections.singletonList(childrenNodes.get(0)));
      return Collections.singletonList(node);
    }

    TopKNode newTopKNode = (TopKNode) node.clone();
    for (int i = 0; i < childrenNodes.size(); i++) {
      PlanNode child = childrenNodes.get(i);
      TopKNode subTopKNode =
          new TopKNode(
              queryId.genPlanNodeId(),
              Collections.singletonList(child),
              node.getOrderingScheme(),
              node.getCount(),
              node.getOutputSymbols(),
              node.isChildrenDataInOrder());
      newTopKNode.addChild(subTopKNode);
    }
    nodeOrderingMap.put(newTopKNode.getPlanNodeId(), newTopKNode.getOrderingScheme());

    return Collections.singletonList(newTopKNode);
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
    for (int i = 0; i < childrenNodes.size(); i++) {
      PlanNode child = childrenNodes.get(i);
      FilterNode subFilterNode =
          new FilterNode(queryId.genPlanNodeId(), child, node.getPredicate());
      resultNodeList.add(subFilterNode);
      if (i == 0) {
        nodeOrderingMap.put(subFilterNode.getPlanNodeId(), childOrdering);
      }
    }
    return resultNodeList;
  }

  @Override
  public List<PlanNode> visitJoin(JoinNode node, PlanContext context) {
    // child of JoinNode must be SortNode, so after rewritten, the child must be MergeSortNode or
    // SortNode
    List<PlanNode> leftChildrenNodes = node.getLeftChild().accept(this, context);
    checkArgument(
        leftChildrenNodes.size() == 1, "The size of left children node of JoinNode should be 1");
    node.setLeftChild(leftChildrenNodes.get(0));

    List<PlanNode> rightChildrenNodes = node.getRightChild().accept(this, context);
    checkArgument(
        rightChildrenNodes.size() == 1, "The size of right children node of JoinNode should be 1");
    node.setRightChild(rightChildrenNodes.get(0));

    return Collections.singletonList(node);
  }

  @Override
  public List<PlanNode> visitTableScan(TableScanNode node, PlanContext context) {
    Map<TRegionReplicaSet, TableScanNode> tableScanNodeMap = new HashMap<>();

    for (DeviceEntry deviceEntry : node.getDeviceEntries()) {
      List<TRegionReplicaSet> regionReplicaSets =
          analysis.getDataRegionReplicaSetWithTimeFilter(
              node.getQualifiedObjectName().getDatabaseName(),
              deviceEntry.getDeviceID(),
              node.getTimeFilter());

      for (TRegionReplicaSet regionReplicaSet : regionReplicaSets) {
        TableScanNode tableScanNode =
            tableScanNodeMap.computeIfAbsent(
                regionReplicaSet,
                k -> {
                  TableScanNode scanNode =
                      new TableScanNode(
                          queryId.genPlanNodeId(),
                          node.getQualifiedObjectName(),
                          node.getOutputSymbols(),
                          node.getAssignments(),
                          new ArrayList<>(),
                          node.getIdAndAttributeIndexMap(),
                          node.getScanOrder(),
                          node.getTimePredicate().orElse(null),
                          node.getPushDownPredicate(),
                          node.getPushDownLimit(),
                          node.getPushDownOffset(),
                          node.isPushLimitToEachDevice());
                  scanNode.setRegionReplicaSet(regionReplicaSet);
                  return scanNode;
                });
        tableScanNode.appendDeviceEntry(deviceEntry);
      }
    }

    if (tableScanNodeMap.isEmpty()) {
      node.setRegionReplicaSet(NOT_ASSIGNED);
      return Collections.singletonList(node);
    }

    List<PlanNode> resultTableScanNodeList = new ArrayList<>();
    TRegionReplicaSet mostUsedDataRegion = null;
    int maxDeviceEntrySizeOfTableScan = 0;
    for (Map.Entry<TRegionReplicaSet, TableScanNode> entry : tableScanNodeMap.entrySet()) {
      TRegionReplicaSet regionReplicaSet = entry.getKey();
      TableScanNode subTableScanNode = entry.getValue();
      subTableScanNode.setPlanNodeId(queryId.genPlanNodeId());
      subTableScanNode.setRegionReplicaSet(regionReplicaSet);
      resultTableScanNodeList.add(subTableScanNode);

      if (mostUsedDataRegion == null
          || subTableScanNode.getDeviceEntries().size() > maxDeviceEntrySizeOfTableScan) {
        mostUsedDataRegion = regionReplicaSet;
        maxDeviceEntrySizeOfTableScan = subTableScanNode.getDeviceEntries().size();
      }
    }
    context.mostUsedRegion = mostUsedDataRegion;

    if (!context.hasSortProperty) {
      return resultTableScanNodeList;
    }

    processSortProperty(node, resultTableScanNodeList, context);
    return resultTableScanNodeList;
  }

  @Override
  public List<PlanNode> visitAggregation(AggregationNode node, PlanContext context) {
    if (node.isStreamable()) {
      OrderingScheme expectedOrderingSchema = constructOrderingSchema(node.getPreGroupedSymbols());
      context.setExpectedOrderingScheme(expectedOrderingSchema);
    }
    List<PlanNode> childrenNodes = node.getChild().accept(this, context);
    OrderingScheme childOrdering = nodeOrderingMap.get(childrenNodes.get(0).getPlanNodeId());
    if (childOrdering != null) {
      nodeOrderingMap.put(node.getPlanNodeId(), childOrdering);
    }

    if (childrenNodes.size() == 1) {
      node.setChild(childrenNodes.get(0));
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
                  if (node.isStreamable()) {
                    nodeOrderingMap.put(planNodeId, childOrdering);
                  }
                  return aggregationNode;
                })
            .collect(Collectors.toList());
    splitResult.left.setChild(
        mergeChildrenViaCollectOrMergeSort(
            nodeOrderingMap.get(childrenNodes.get(0).getPlanNodeId()), childrenNodes));
    return Collections.singletonList(splitResult.left);
  }

  @Override
  public List<PlanNode> visitAggregationTableScan(
      AggregationTableScanNode node, PlanContext context) {
    boolean needSplit = false;
    List<List<TRegionReplicaSet>> regionReplicaSetsList = new ArrayList<>();
    for (DeviceEntry deviceEntry : node.getDeviceEntries()) {
      List<TRegionReplicaSet> regionReplicaSets =
          analysis.getDataRegionReplicaSetWithTimeFilter(
              node.getQualifiedObjectName().getDatabaseName(),
              deviceEntry.getDeviceID(),
              node.getTimeFilter());
      if (regionReplicaSets.size() > 1) {
        needSplit = true;
      }
      regionReplicaSetsList.add(regionReplicaSets);
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
    for (Map.Entry<TRegionReplicaSet, AggregationTableScanNode> entry : regionNodeMap.entrySet()) {
      TRegionReplicaSet regionReplicaSet = entry.getKey();
      TableScanNode subTableScanNode = entry.getValue();
      subTableScanNode.setPlanNodeId(queryId.genPlanNodeId());
      subTableScanNode.setRegionReplicaSet(regionReplicaSet);
      resultTableScanNodeList.add(subTableScanNode);

      if (mostUsedDataRegion == null
          || subTableScanNode.getDeviceEntries().size() > maxDeviceEntrySizeOfTableScan) {
        mostUsedDataRegion = regionReplicaSet;
        maxDeviceEntrySizeOfTableScan = subTableScanNode.getDeviceEntries().size();
      }
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

  private void buildRegionNodeMap(
      AggregationTableScanNode originalAggTableScanNode,
      List<List<TRegionReplicaSet>> regionReplicaSetsList,
      Map<TRegionReplicaSet, AggregationTableScanNode> regionNodeMap,
      AggregationTableScanNode partialAggTableScanNode) {
    for (int i = 0; i < regionReplicaSetsList.size(); i++) {
      for (TRegionReplicaSet regionReplicaSet : regionReplicaSetsList.get(i)) {
        AggregationTableScanNode aggregationTableScanNode =
            regionNodeMap.computeIfAbsent(
                regionReplicaSet,
                k -> {
                  AggregationTableScanNode scanNode =
                      new AggregationTableScanNode(
                          queryId.genPlanNodeId(),
                          partialAggTableScanNode.getQualifiedObjectName(),
                          partialAggTableScanNode.getOutputSymbols(),
                          partialAggTableScanNode.getAssignments(),
                          new ArrayList<>(),
                          partialAggTableScanNode.getIdAndAttributeIndexMap(),
                          partialAggTableScanNode.getScanOrder(),
                          partialAggTableScanNode.getTimePredicate().orElse(null),
                          partialAggTableScanNode.getPushDownPredicate(),
                          partialAggTableScanNode.getPushDownLimit(),
                          partialAggTableScanNode.getPushDownOffset(),
                          partialAggTableScanNode.isPushLimitToEachDevice(),
                          partialAggTableScanNode.getProjection(),
                          partialAggTableScanNode.getAggregations(),
                          partialAggTableScanNode.getGroupingSets(),
                          partialAggTableScanNode.getPreGroupedSymbols(),
                          partialAggTableScanNode.getStep(),
                          partialAggTableScanNode.getGroupIdSymbol());
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
      OrderingScheme childOrdering, List<PlanNode> childrenNodes) {
    PlanNode firstChild = childrenNodes.get(0);

    // children has sort property, use MergeSort to merge children
    if (childOrdering != null) {
      MergeSortNode mergeSortNode =
          new MergeSortNode(queryId.genPlanNodeId(), childOrdering, firstChild.getOutputSymbols());
      childrenNodes.forEach(mergeSortNode::addChild);
      nodeOrderingMap.put(mergeSortNode.getPlanNodeId(), childOrdering);
      return mergeSortNode;
    }

    // children has no sort property, use CollectNode to merge children
    CollectNode collectNode =
        new CollectNode(queryId.genPlanNodeId(), firstChild.getOutputSymbols());
    childrenNodes.forEach(collectNode::addChild);
    return collectNode;
  }

  private void processSortProperty(
      final TableScanNode tableScanNode,
      final List<PlanNode> resultTableScanNodeList,
      final PlanContext context) {
    final List<Symbol> newOrderingSymbols = new ArrayList<>();
    final List<SortOrder> newSortOrders = new ArrayList<>();
    final OrderingScheme expectedOrderingScheme = context.expectedOrderingScheme;

    boolean lastIsTimeRelated = false;
    for (final Symbol symbol : expectedOrderingScheme.getOrderBy()) {
      if (timeRelatedSymbol(symbol, tableScanNode)) {
        if (!expectedOrderingScheme.getOrderings().get(symbol).isAscending()) {
          // TODO(beyyes) move scan order judgement into logical plan optimizer
          resultTableScanNodeList.forEach(
              node -> ((TableScanNode) node).setScanOrder(Ordering.DESC));
        }
        newOrderingSymbols.add(symbol);
        newSortOrders.add(expectedOrderingScheme.getOrdering(symbol));
        lastIsTimeRelated = true;
        break;
      } else if (!tableScanNode.getIdAndAttributeIndexMap().containsKey(symbol)) {
        break;
      }

      newOrderingSymbols.add(symbol);
      newSortOrders.add(expectedOrderingScheme.getOrdering(symbol));
    }

    // no sort property can be pushed down into TableScanNode
    if (newOrderingSymbols.isEmpty()) {
      return;
    }

    final List<Function<DeviceEntry, String>> orderingRules = new ArrayList<>();
    for (final Symbol symbol : newOrderingSymbols) {
      final Integer idx = tableScanNode.getIdAndAttributeIndexMap().get(symbol);
      if (idx == null) {
        // time column or date_bin column
        break;
      }
      if (tableScanNode.getAssignments().get(symbol).getColumnCategory()
          == TsTableColumnCategory.ID) {
        // segments[0] is always tableName
        orderingRules.add(deviceEntry -> (String) deviceEntry.getNthSegment(idx + 1));
      } else {
        orderingRules.add(
            deviceEntry ->
                deviceEntry.getAttributeColumnValues().get(idx) == null
                    ? null
                    : deviceEntry
                        .getAttributeColumnValues()
                        .get(idx)
                        .getStringValue(TSFileConfig.STRING_CHARSET));
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
            analysis.getTableColumnSchema(tableScanNode.getQualifiedObjectName()),
            newOrderingSymbols,
            newSortOrders,
            lastIsTimeRelated,
            tableScanNode.getDeviceEntries().size() == 1);
    for (final PlanNode planNode : resultTableScanNodeList) {
      final TableScanNode scanNode = (TableScanNode) planNode;
      newOrderingScheme.ifPresent(
          orderingScheme -> nodeOrderingMap.put(scanNode.getPlanNodeId(), orderingScheme));
      if (comparator != null) {
        scanNode.getDeviceEntries().sort(comparator);
      }
    }
  }

  private Optional<OrderingScheme> tableScanOrderingSchema(
      Map<Symbol, ColumnSchema> tableColumnSchema,
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
          tableColumnSchema, orderingScheme, size - 2)) { // all id columns included
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
  private boolean timeRelatedSymbol(Symbol symbol, TableScanNode tableScanNode) {
    return tableScanNode.isTimeColumn(symbol)
        || PUSH_DOWN_DATE_BIN_SYMBOL_NAME.equals(symbol.getName());
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
    final String database = PathUtils.qualifyDatabaseName(node.getDatabase());
    final Set<TRegionReplicaSet> schemaRegionSet = new HashSet<>();
    analysis
        .getSchemaPartitionInfo()
        .getSchemaPartitionMap()
        .get(database)
        .forEach(
            (deviceGroupId, schemaRegionReplicaSet) -> schemaRegionSet.add(schemaRegionReplicaSet));

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
    final String database = PathUtils.qualifyDatabaseName(node.getDatabase());
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
      context.mostUsedRegion = mostUsedSchemaRegion;
      return res;
    }
  }

  public static class PlanContext {
    final Map<PlanNodeId, NodeDistribution> nodeDistributionMap;
    boolean hasExchangeNode = false;
    boolean hasSortProperty = false;
    OrderingScheme expectedOrderingScheme;
    TRegionReplicaSet mostUsedRegion;

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
  }
}
