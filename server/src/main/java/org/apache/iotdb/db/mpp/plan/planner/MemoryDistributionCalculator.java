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

package org.apache.iotdb.db.mpp.plan.planner;

import org.apache.iotdb.db.mpp.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.PlanVisitor;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.metedata.read.CountSchemaMergeNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.metedata.read.DevicesCountNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.metedata.read.DevicesSchemaScanNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.metedata.read.LevelTimeSeriesCountNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.metedata.read.NodeManagementMemoryMergeNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.metedata.read.NodePathsConvertNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.metedata.read.NodePathsCountNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.metedata.read.NodePathsSchemaScanNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.metedata.read.SchemaFetchMergeNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.metedata.read.SchemaFetchScanNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.metedata.read.SchemaQueryMergeNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.metedata.read.SchemaQueryOrderByHeatNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.metedata.read.SchemaQueryScanNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.metedata.read.TimeSeriesCountNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.metedata.read.TimeSeriesSchemaScanNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.metedata.write.ConstructSchemaBlackListNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.metedata.write.RollbackSchemaBlackListNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.process.AggregationNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.process.DeviceMergeNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.process.DeviceViewIntoNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.process.DeviceViewNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.process.ExchangeNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.process.FillNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.process.FilterNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.process.GroupByLevelNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.process.GroupByTagNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.process.IntoNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.process.LimitNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.process.MergeSortNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.process.OffsetNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.process.ProjectNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.process.SingleDeviceViewNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.process.SlidingWindowAggregationNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.process.SortNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.process.TimeJoinNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.process.TransformNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.process.VerticallyConcatNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.process.last.LastQueryCollectNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.process.last.LastQueryMergeNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.process.last.LastQueryNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.sink.FragmentSinkNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.source.AlignedLastQueryScanNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.source.AlignedSeriesAggregationScanNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.source.AlignedSeriesScanNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.source.LastQueryScanNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.source.SeriesAggregationScanNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.source.SeriesScanNode;

import static org.apache.iotdb.db.mpp.plan.constant.DataNodeEndPoints.isSameNode;

public class MemoryDistributionCalculator
    extends PlanVisitor<Void, MemoryDistributionCalculator.MemoryDistributionContext> {
  private int exchangeNum = 0;

  public MemoryDistributionCalculator() {}

  public long calculateTotalSplit() {
    return exchangeNum;
  }

  @Override
  public Void visitPlan(PlanNode node, MemoryDistributionContext context) {
    // Throw exception here because we want to ensure that all new PlanNodes implement
    // this method correctly if necessary.
    throw new UnsupportedOperationException("Should call concrete visitXX method");
  }

  // TODO remove allowPipeline if we support pipeline for all operators
  private void processConsumeChildrenOneByOneNode(PlanNode node) {
    MemoryDistributionContext context =
        new MemoryDistributionContext(
            node.getPlanNodeId(), MemoryDistributionType.CONSUME_CHILDREN_ONE_BY_ONE);
    node.getChildren()
        .forEach(
            child -> {
              if (child != null) {
                child.accept(this, context);
              }
            });
  }

  private void processConsumeAllChildrenAtTheSameTime(PlanNode node) {
    MemoryDistributionContext context =
        new MemoryDistributionContext(
            node.getPlanNodeId(), MemoryDistributionType.CONSUME_ALL_CHILDREN_AT_THE_SAME_TIME);
    node.getChildren()
        .forEach(
            child -> {
              if (child != null) {
                child.accept(this, context);
              }
            });
    // exchangeNum += node.getChildren().size()
    // child.accept(this, context);
  }

  @Override
  public Void visitExchange(ExchangeNode node, MemoryDistributionContext context) {
    // we do not distinguish LocalSourceHandle/SourceHandle by not letting LocalSinkHandle update
    // the map
    // TODO why use a map, just +1, +1, if(context.notAdded) +1
    if (context == null) {
      // context == null means this ExchangeNode has no father
      exchangeNum += 1;
    } else {
      if (context.memoryDistributionType.equals(
          MemoryDistributionType.CONSUME_ALL_CHILDREN_AT_THE_SAME_TIME)) {
        exchangeNum += 1;
      } else if (context.memoryDistributionType.equals(
              MemoryDistributionType.CONSUME_CHILDREN_ONE_BY_ONE)
          && !context.exchangeAdded) {
        context.exchangeAdded = true;
        exchangeNum += 1;
      }
    }
    return null;
  }

  @Override
  public Void visitFragmentSink(FragmentSinkNode node, MemoryDistributionContext context) {
    // LocalSinkHandle and LocalSourceHandle are one-to-one mapped and only LocalSourceHandle do the
    // update
    if (!isSameNode(node.getDownStreamEndpoint())) {
      this.exchangeNum += 1;
    }
    node.getChild().accept(this, context);
    return null;
  }

  @Override
  public Void visitSeriesScan(SeriesScanNode node, MemoryDistributionContext context) {
    // do nothing since SourceNode will not have Exchange/FragmentSink as child
    return null;
  }

  @Override
  public Void visitSeriesAggregationScan(
      SeriesAggregationScanNode node, MemoryDistributionContext context) {
    // do nothing since SourceNode will not have Exchange/FragmentSink as child
    return null;
  }

  @Override
  public Void visitAlignedSeriesScan(
      AlignedSeriesScanNode node, MemoryDistributionContext context) {
    // do nothing since SourceNode will not have Exchange/FragmentSink as child
    return null;
  }

  @Override
  public Void visitAlignedSeriesAggregationScan(
      AlignedSeriesAggregationScanNode node, MemoryDistributionContext context) {
    // do nothing since SourceNode will not have Exchange/FragmentSink as child
    return null;
  }

  @Override
  public Void visitDeviceView(DeviceViewNode node, MemoryDistributionContext context) {
    // consume children one by one
    processConsumeChildrenOneByOneNode(node);
    return null;
  }

  @Override
  public Void visitDeviceMerge(DeviceMergeNode node, MemoryDistributionContext context) {
    processConsumeAllChildrenAtTheSameTime(node);
    return null;
  }

  @Override
  public Void visitFill(FillNode node, MemoryDistributionContext context) {
    processConsumeAllChildrenAtTheSameTime(node);
    return null;
  }

  @Override
  public Void visitFilter(FilterNode node, MemoryDistributionContext context) {
    processConsumeAllChildrenAtTheSameTime(node);
    return null;
  }

  @Override
  public Void visitGroupByLevel(GroupByLevelNode node, MemoryDistributionContext context) {
    processConsumeAllChildrenAtTheSameTime(node);
    return null;
  }

  @Override
  public Void visitGroupByTag(GroupByTagNode node, MemoryDistributionContext context) {
    processConsumeAllChildrenAtTheSameTime(node);
    return null;
  }

  @Override
  public Void visitSlidingWindowAggregation(
      SlidingWindowAggregationNode node, MemoryDistributionContext context) {
    processConsumeAllChildrenAtTheSameTime(node);
    return null;
  }

  @Override
  public Void visitLimit(LimitNode node, MemoryDistributionContext context) {
    // TODO it's a one-to-one node, just child.accept(visitor, context)
    // TODO And this way will create a new context by this node, what if it occupied the father node
    processConsumeAllChildrenAtTheSameTime(node);
    // DeviceView(ConsumeOneChild) - TimeJoin(nodeId 1) - exchange (TimeJoinContext)
    //                                                  - exchange (TimeJoinContext)
    //                             - TimeJoin(nodeId 2) - exchange
    //                                                  - exchange
    return null;
  }

  @Override
  public Void visitOffset(OffsetNode node, MemoryDistributionContext context) {
    processConsumeAllChildrenAtTheSameTime(node);
    return null;
  }

  @Override
  public Void visitAggregation(AggregationNode node, MemoryDistributionContext context) {
    processConsumeAllChildrenAtTheSameTime(node);
    return null;
  }

  @Override
  public Void visitSort(SortNode node, MemoryDistributionContext context) {
    processConsumeAllChildrenAtTheSameTime(node);
    return null;
  }

  @Override
  public Void visitProject(ProjectNode node, MemoryDistributionContext context) {
    processConsumeAllChildrenAtTheSameTime(node);
    return null;
  }

  @Override
  public Void visitTimeJoin(TimeJoinNode node, MemoryDistributionContext context) {
    processConsumeAllChildrenAtTheSameTime(node);
    return null;
  }

  @Override
  public Void visitTransform(TransformNode node, MemoryDistributionContext context) {
    processConsumeAllChildrenAtTheSameTime(node);
    return null;
  }

  @Override
  public Void visitLastQueryScan(LastQueryScanNode node, MemoryDistributionContext context) {
    // do nothing since SourceNode will not have Exchange/FragmentSink as child
    return null;
  }

  @Override
  public Void visitAlignedLastQueryScan(
      AlignedLastQueryScanNode node, MemoryDistributionContext context) {
    // do nothing since SourceNode will not have Exchange/FragmentSink as child
    return null;
  }

  @Override
  public Void visitLastQuery(LastQueryNode node, MemoryDistributionContext context) {
    processConsumeChildrenOneByOneNode(node);
    return null;
  }

  @Override
  public Void visitLastQueryMerge(LastQueryMergeNode node, MemoryDistributionContext context) {
    processConsumeAllChildrenAtTheSameTime(node);
    return null;
  }

  @Override
  public Void visitLastQueryCollect(LastQueryCollectNode node, MemoryDistributionContext context) {
    processConsumeChildrenOneByOneNode(node);
    return null;
  }

  @Override
  public Void visitInto(IntoNode node, MemoryDistributionContext context) {
    processConsumeAllChildrenAtTheSameTime(node);
    return null;
  }

  @Override
  public Void visitDeviceViewInto(DeviceViewIntoNode node, MemoryDistributionContext context) {
    processConsumeAllChildrenAtTheSameTime(node);
    return null;
  }

  @Override
  public Void visitVerticallyConcat(VerticallyConcatNode node, MemoryDistributionContext context) {
    processConsumeAllChildrenAtTheSameTime(node);
    return null;
  }

  // TODO how to handle schema query
  @Override
  public Void visitSchemaQueryMerge(SchemaQueryMergeNode node, MemoryDistributionContext context) {
    processConsumeChildrenOneByOneNode(node);
    return null;
  }

  @Override
  public Void visitSchemaQueryScan(SchemaQueryScanNode node, MemoryDistributionContext context) {
    // do nothing since SourceNode will not have Exchange/FragmentSink as child
    return null;
  }

  @Override
  public Void visitSchemaQueryOrderByHeat(
      SchemaQueryOrderByHeatNode node, MemoryDistributionContext context) {
    processConsumeAllChildrenAtTheSameTime(node);
    return null;
  }

  @Override
  public Void visitTimeSeriesSchemaScan(
      TimeSeriesSchemaScanNode node, MemoryDistributionContext context) {
    // do nothing since SourceNode will not have Exchange/FragmentSink as child
    return null;
  }

  @Override
  public Void visitDevicesSchemaScan(
      DevicesSchemaScanNode node, MemoryDistributionContext context) {
    // do nothing since SourceNode will not have Exchange/FragmentSink as child
    return null;
  }

  @Override
  public Void visitDevicesCount(DevicesCountNode node, MemoryDistributionContext context) {
    // do nothing since SourceNode will not have Exchange/FragmentSink as child
    return null;
  }

  @Override
  public Void visitTimeSeriesCount(TimeSeriesCountNode node, MemoryDistributionContext context) {
    // do nothing since SourceNode will not have Exchange/FragmentSink as child
    return null;
  }

  @Override
  public Void visitLevelTimeSeriesCount(
      LevelTimeSeriesCountNode node, MemoryDistributionContext context) {
    // do nothing since SourceNode will not have Exchange/FragmentSink as child
    return null;
  }

  @Override
  public Void visitCountMerge(CountSchemaMergeNode node, MemoryDistributionContext context) {
    processConsumeChildrenOneByOneNode(node);
    return null;
  }

  @Override
  public Void visitSchemaFetchMerge(SchemaFetchMergeNode node, MemoryDistributionContext context) {
    processConsumeChildrenOneByOneNode(node);
    return null;
  }

  @Override
  public Void visitSchemaFetchScan(SchemaFetchScanNode node, MemoryDistributionContext context) {
    // do nothing since SourceNode will not have Exchange/FragmentSink as child
    return null;
  }

  @Override
  public Void visitNodePathsSchemaScan(
      NodePathsSchemaScanNode node, MemoryDistributionContext context) {
    // do nothing since SourceNode will not have Exchange/FragmentSink as child
    return null;
  }

  @Override
  public Void visitNodeManagementMemoryMerge(
      NodeManagementMemoryMergeNode node, MemoryDistributionContext context) {
    processConsumeAllChildrenAtTheSameTime(node);
    return null;
  }

  @Override
  public Void visitNodePathConvert(NodePathsConvertNode node, MemoryDistributionContext context) {
    processConsumeAllChildrenAtTheSameTime(node);
    return null;
  }

  @Override
  public Void visitNodePathsCount(NodePathsCountNode node, MemoryDistributionContext context) {
    processConsumeAllChildrenAtTheSameTime(node);
    return null;
  }

  @Override
  public Void visitConstructSchemaBlackList(
      ConstructSchemaBlackListNode node, MemoryDistributionContext context) {
    processConsumeAllChildrenAtTheSameTime(node);
    return null;
  }

  @Override
  public Void visitRollbackSchemaBlackList(
      RollbackSchemaBlackListNode node, MemoryDistributionContext context) {
    processConsumeAllChildrenAtTheSameTime(node);
    return null;
  }

  @Override
  public Void visitSingleDeviceView(SingleDeviceViewNode node, MemoryDistributionContext context) {
    processConsumeAllChildrenAtTheSameTime(node);
    return null;
  }

  @Override
  public Void visitMergeSort(MergeSortNode node, MemoryDistributionContext context) {
    processConsumeAllChildrenAtTheSameTime(node);
    return null;
  }

  enum MemoryDistributionType {
    /**
     * This type means that this node needs data from all the children. For example, TimeJoinNode.
     * If the type of the father node of an ExchangeNode is CONSUME_ALL_CHILDREN_AT_THE_SAME_TIME,
     * the ExchangeNode needs one split of the memory.
     */
    CONSUME_ALL_CHILDREN_AT_THE_SAME_TIME(0),

    /**
     * This type means that this node consumes data of the children one by one. For example,
     * DeviceMergeNode. If the type of the father node of an ExchangeNode is
     * CONSUME_CHILDREN_ONE_BY_ONE, all the ExchangeNodes of that father node share one split of the
     * memory.
     */
    CONSUME_CHILDREN_ONE_BY_ONE(1);

    private final int id;

    MemoryDistributionType(int id) {
      this.id = id;
    }

    public int getId() {
      return id;
    }
  }

  static class MemoryDistributionContext {
    final PlanNodeId planNodeId;
    // Indicating whether an exchange node has been added as a child
    boolean exchangeAdded = false;
    final MemoryDistributionType memoryDistributionType;

    MemoryDistributionContext(
        PlanNodeId planNodeId, MemoryDistributionType memoryDistributionType) {
      this.planNodeId = planNodeId;
      this.memoryDistributionType = memoryDistributionType;
    }
  }
}
