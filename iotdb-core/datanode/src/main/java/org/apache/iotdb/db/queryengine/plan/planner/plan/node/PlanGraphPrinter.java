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

package org.apache.iotdb.db.queryengine.plan.planner.plan.node;

import org.apache.iotdb.common.rpc.thrift.TRegionReplicaSet;
import org.apache.iotdb.commons.conf.CommonDescriptor;
import org.apache.iotdb.commons.partition.DataPartition;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.commons.queryengine.plan.planner.plan.node.TableScanNode;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.DataOrganizationSpecification;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.Symbol;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.node.AssignUniqueId;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.node.CorrelatedJoinNode;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.node.EnforceSingleRowNode;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.node.ExceptNode;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.node.GapFillNode;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.node.GroupNode;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.node.IntersectNode;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.node.JoinNode;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.node.LinearFillNode;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.node.MarkDistinctNode;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.node.NextFillNode;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.node.OutputNode;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.node.PatternRecognitionNode;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.node.PreviousFillNode;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.node.RowNumberNode;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.node.SemiJoinNode;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.node.StreamSortNode;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.node.TableFunctionProcessorNode;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.node.TopKRankingNode;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.node.UnionNode;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.node.ValueFillNode;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.node.ValuesNode;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.node.WindowNode;
import org.apache.iotdb.db.i18n.DataNodeQueryMessages;
import org.apache.iotdb.db.queryengine.execution.operator.process.copyto.tsfile.CopyToTsFileOptions;
import org.apache.iotdb.db.queryengine.plan.analyze.TemplatedInfo;
import org.apache.iotdb.db.queryengine.plan.expression.Expression;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.AggregationMergeSortNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.AggregationNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.CollectNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.ColumnInjectNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.DeviceMergeNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.DeviceViewIntoNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.DeviceViewNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.FillNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.FilterNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.GroupByLevelNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.GroupByTagNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.HorizontallyConcatNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.IntoNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.LimitNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.MergeSortNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.OffsetNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.ProjectNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.RawDataAggregationNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.SingleDeviceViewNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.SlidingWindowAggregationNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.SortNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.TopKNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.TransformNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.join.FullOuterTimeJoinNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.join.InnerTimeJoinNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.join.LeftOuterTimeJoinNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.last.LastQueryCollectNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.last.LastQueryMergeNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.last.LastQueryNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.last.LastQueryTransformNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.sink.IdentitySinkNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.sink.ShuffleSinkNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.source.AlignedSeriesAggregationScanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.source.AlignedSeriesScanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.source.LastQueryScanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.source.SeriesAggregationScanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.source.SeriesScanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.parameter.AggregationDescriptor;
import org.apache.iotdb.db.queryengine.plan.planner.plan.parameter.CrossSeriesAggregationDescriptor;
import org.apache.iotdb.db.queryengine.plan.planner.plan.parameter.DeviceViewIntoPathDescriptor;
import org.apache.iotdb.db.queryengine.plan.planner.plan.parameter.IntoPathDescriptor;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.AggregationTreeDeviceViewScanNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.CopyToNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.CteScanNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.DeviceTableScanNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.ExchangeNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.ExplainAnalyzeNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.ExternalTsFileScanNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.TableDiskUsageInformationSchemaTableScanNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.TreeDeviceViewScanNode;

import com.google.common.base.Joiner;
import org.apache.tsfile.external.commons.lang3.Validate;
import org.apache.tsfile.utils.Pair;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;

public class PlanGraphPrinter implements PlanVisitor<List<String>, PlanGraphPrinter.GraphContext> {

  private static final String INDENT = " ";
  private static final String HORIZONTAL = "─";
  private static final String VERTICAL = "│";
  private static final String LEFT_BOTTOM = "└";
  private static final String RIGHT_BOTTOM = "┘";
  private static final String LEFT_TOP = "┌";
  private static final String RIGHT_TOP = "┐";
  private static final String UP = "┴";
  private static final String DOWN = "┬";
  private static final String CROSS = "┼";

  private static final int BOX_MARGIN = 1;
  private static final int CONNECTION_LINE_HEIGHT = 2;

  private static final String REGION_NOT_ASSIGNED =
      DataNodeQueryMessages.EXCEPTION_NOT_ASSIGNED_F5DCFB95;
  public static final String DEVICE_NUMBER = "DeviceNumber";
  public static final String CACHED_DEVICE_NUMBER = "CachedDeviceNumber";
  public static final String REGIONS_OF_CURRENT_SUB_TASK = "RegionsOfCurrentSubTask";
  public static final String PREPARE_CACHE_READER_COST = "PrepareCacheReaderCost";
  public static final String LOAD_OBJECT_FILE_COST = "LoadObjectFileCost";
  public static final String PREPARE_CACHED_TSFILE_ID_COST = "PrepareCachedTsFileIdCost";
  public static final String CHECK_ALL_FILES_IN_TSFILE_MANAGER_COST =
      "CheckAllFilesInTSFileManagerCost";
  public static final String READ_TSFILE_CACHE_VALUE_FILES_COST = "ReadTsFileCacheValueFilesCost";

  @Override
  public List<String> visitPlan(PlanNode node, GraphContext context) {
    List<String> boxValue = new ArrayList<>();
    boxValue.add(String.format("PlanNode-%s", node.getPlanNodeId().getId()));
    return render(node, boxValue, context);
  }

  @Override
  public List<String> visitSeriesScan(SeriesScanNode node, GraphContext context) {
    List<String> boxValue = new ArrayList<>();
    boxValue.add(String.format("SeriesScan-%s", node.getPlanNodeId().getId()));
    boxValue.add(
        String.format(DataNodeQueryMessages.EXCEPTION_SERIES_ARG_F5564925, node.getSeriesPath()));

    long limit = node.getPushDownLimit();
    long offset = node.getPushDownOffset();
    if (limit > 0) {
      boxValue.add(String.format(DataNodeQueryMessages.EXCEPTION_LIMIT_ARG_C4FECCC2, limit));
    }
    if (offset > 0) {
      boxValue.add(String.format(DataNodeQueryMessages.EXCEPTION_OFFSET_ARG_7D3690BE, offset));
    }

    Expression predicate = node.getPushDownPredicate();
    if (predicate != null) {
      boxValue.add(
          String.format(DataNodeQueryMessages.EXCEPTION_PREDICATE_ARG_5DFE6605, predicate));
    }

    boxValue.add(
        String.format(DataNodeQueryMessages.EXCEPTION_SCANORDER_ARG_AB0CB81A, node.getScanOrder()));

    boxValue.add(printRegion(node.getRegionReplicaSet()));
    return render(node, boxValue, context);
  }

  @Override
  public List<String> visitAlignedSeriesScan(AlignedSeriesScanNode node, GraphContext context) {
    List<String> boxValue = new ArrayList<>();
    boxValue.add(String.format("AlignedSeriesScan-%s", node.getPlanNodeId().getId()));
    boxValue.add(
        String.format(
            DataNodeQueryMessages.EXCEPTION_SERIES_ARG_ARG_4AD75872,
            node.getAlignedPath().getIDeviceID(),
            node.getAlignedPath().getMeasurementList()));

    long limit = node.getPushDownLimit();
    long offset = node.getPushDownOffset();
    if (limit > 0) {
      boxValue.add(String.format(DataNodeQueryMessages.EXCEPTION_LIMIT_ARG_C4FECCC2, limit));
    }
    if (offset > 0) {
      boxValue.add(String.format(DataNodeQueryMessages.EXCEPTION_OFFSET_ARG_7D3690BE, offset));
    }

    Expression predicate = node.getPushDownPredicate();
    if (predicate != null) {
      boxValue.add(
          String.format(DataNodeQueryMessages.EXCEPTION_PREDICATE_ARG_5DFE6605, predicate));
    }

    boxValue.add(
        String.format(
            DataNodeQueryMessages.EXCEPTION_QUERYALLSENSORS_ARG_ADCB5FCA,
            node.isQueryAllSensors()));
    boxValue.add(
        String.format(DataNodeQueryMessages.EXCEPTION_SCANORDER_ARG_AB0CB81A, node.getScanOrder()));
    boxValue.add(printRegion(node.getRegionReplicaSet()));
    return render(node, boxValue, context);
  }

  @Override
  public List<String> visitSeriesAggregationScan(
      SeriesAggregationScanNode node, GraphContext context) {
    List<String> boxValue = new ArrayList<>();
    boxValue.add(String.format("SeriesAggregationScan-%s", node.getPlanNodeId().getId()));
    boxValue.add(
        String.format(DataNodeQueryMessages.EXCEPTION_SERIES_ARG_F5564925, node.getSeriesPath()));
    for (int i = 0; i < node.getAggregationDescriptorList().size(); i++) {
      AggregationDescriptor descriptor = node.getAggregationDescriptorList().get(i);
      boxValue.add(
          String.format(
              DataNodeQueryMessages.EXCEPTION_AGGREGATOR_ARG_ARG_ARG_8C1D8DC9,
              i,
              descriptor.getAggregationType(),
              descriptor.getStep()));
    }
    Expression predicate = node.getPushDownPredicate();
    if (predicate != null) {
      boxValue.add(
          String.format(DataNodeQueryMessages.EXCEPTION_PREDICATE_ARG_5DFE6605, predicate));
    }
    boxValue.add(
        String.format(DataNodeQueryMessages.EXCEPTION_SCANORDER_ARG_AB0CB81A, node.getScanOrder()));
    boxValue.add(printRegion(node.getRegionReplicaSet()));
    return render(node, boxValue, context);
  }

  @Override
  public List<String> visitAlignedSeriesAggregationScan(
      AlignedSeriesAggregationScanNode node, GraphContext context) {
    List<String> boxValue = new ArrayList<>();
    boxValue.add(String.format("AlignedSeriesAggregationScan-%s", node.getPlanNodeId().getId()));
    boxValue.add(
        String.format(
            DataNodeQueryMessages.EXCEPTION_SERIES_ARG_ARG_4AD75872,
            node.getAlignedPath().getIDeviceID(),
            node.getAlignedPath().getMeasurementList()));
    for (int i = 0; i < node.getAggregationDescriptorList().size(); i++) {
      AggregationDescriptor descriptor = node.getAggregationDescriptorList().get(i);
      boxValue.add(
          String.format(
              DataNodeQueryMessages.EXCEPTION_AGGREGATOR_ARG_ARG_ARG_8C1D8DC9,
              i,
              descriptor.getAggregationType(),
              descriptor.getStep()));
    }
    Expression predicate = node.getPushDownPredicate();
    if (predicate != null) {
      boxValue.add(
          String.format(DataNodeQueryMessages.EXCEPTION_PREDICATE_ARG_5DFE6605, predicate));
    }
    boxValue.add(
        String.format(DataNodeQueryMessages.EXCEPTION_SCANORDER_ARG_AB0CB81A, node.getScanOrder()));
    boxValue.add(printRegion(node.getRegionReplicaSet()));
    return render(node, boxValue, context);
  }

  @Override
  public List<String> visitSingleDeviceView(SingleDeviceViewNode node, GraphContext context) {
    List<String> boxValue = new ArrayList<>();
    boxValue.add(String.format("SingleDeviceView-%s", node.getPlanNodeId().getId()));
    boxValue.add(
        String.format(DataNodeQueryMessages.EXCEPTION_DEVICENAME_ARG_59E457FD, node.getDevice()));
    return render(node, boxValue, context);
  }

  @Override
  public List<String> visitDeviceView(DeviceViewNode node, GraphContext context) {
    List<String> boxValue = new ArrayList<>();
    boxValue.add(String.format("DeviceView-%s", node.getPlanNodeId().getId()));
    boxValue.add(
        String.format(
            DataNodeQueryMessages.EXCEPTION_DEVICECOUNT_ARG_665654AC, node.getDevices().size()));
    return render(node, boxValue, context);
  }

  @Override
  public List<String> visitAggregationMergeSort(
      AggregationMergeSortNode node, GraphContext context) {
    List<String> boxValue = new ArrayList<>();
    boxValue.add(String.format("AggregationMergeSort-%s", node.getPlanNodeId().getId()));
    return render(node, boxValue, context);
  }

  @Override
  public List<String> visitMergeSort(MergeSortNode node, GraphContext context) {
    List<String> boxValue = new ArrayList<>();
    boxValue.add(String.format("MergeSort-%s", node.getPlanNodeId().getId()));
    boxValue.add(
        String.format(
            DataNodeQueryMessages.EXCEPTION_CHILDRENCOUNT_ARG_08EB5B4D, node.getChildren().size()));
    boxValue.add(node.getMergeOrderParameter().toString());
    return render(node, boxValue, context);
  }

  @Override
  public List<String> visitCollect(CollectNode node, GraphContext context) {
    List<String> boxValue = new ArrayList<>();
    boxValue.add(String.format("Collect-%s", node.getPlanNodeId().getId()));
    boxValue.add(
        String.format(
            DataNodeQueryMessages.EXCEPTION_CHILDRENCOUNT_ARG_08EB5B4D, node.getChildren().size()));
    return render(node, boxValue, context);
  }

  @Override
  public List<String> visitTopK(TopKNode node, GraphContext context) {
    List<String> boxValue = new ArrayList<>();
    boxValue.add(String.format("TopK-%s", node.getPlanNodeId().getId()));
    boxValue.add(
        String.format(DataNodeQueryMessages.EXCEPTION_LIMITVALUE_ARG_A0AB58C2, node.getTopValue()));
    boxValue.add(node.getMergeOrderParameter().toString());
    return render(node, boxValue, context);
  }

  @Override
  public List<String> visitDeviceMerge(DeviceMergeNode node, GraphContext context) {
    List<String> boxValue = new ArrayList<>();
    boxValue.add(String.format("DeviceMerge-%s", node.getPlanNodeId().getId()));
    boxValue.add(
        String.format(
            DataNodeQueryMessages.EXCEPTION_DEVICECOUNT_ARG_665654AC, node.getDevices().size()));
    return render(node, boxValue, context);
  }

  @Override
  public List<String> visitFill(FillNode node, GraphContext context) {
    List<String> boxValue = new ArrayList<>();
    boxValue.add(String.format("Fill-%s", node.getPlanNodeId().getId()));
    boxValue.add(
        String.format(
            DataNodeQueryMessages.EXCEPTION_POLICY_ARG_08266A9A,
            node.getFillDescriptor().getFillPolicy()));
    if (node.getFillDescriptor().getTimeDurationThreshold() != null) {
      boxValue.add(
          String.format(
              DataNodeQueryMessages.EXCEPTION_TIMEDURATIONTHRESHOLD_ARG_D521A764,
              node.getFillDescriptor().getTimeDurationThreshold()));
    }
    return render(node, boxValue, context);
  }

  @Override
  public List<String> visitFilter(FilterNode node, GraphContext context) {
    List<String> boxValue = new ArrayList<>();
    boxValue.add(String.format("Filter-%s", node.getPlanNodeId().getId()));
    boxValue.add(
        String.format(DataNodeQueryMessages.EXCEPTION_PREDICATE_ARG_5DFE6605, node.getPredicate()));
    return render(node, boxValue, context);
  }

  @Override
  public List<String> visitGroupByLevel(GroupByLevelNode node, GraphContext context) {
    List<String> boxValue = new ArrayList<>();
    boxValue.add(String.format("GroupByLevel-%s", node.getPlanNodeId().getId()));
    for (int i = 0; i < node.getGroupByLevelDescriptors().size(); i++) {
      AggregationDescriptor descriptor = node.getGroupByLevelDescriptors().get(i);
      boxValue.add(
          String.format(
              DataNodeQueryMessages.EXCEPTION_AGGREGATOR_ARG_ARG_ARG_8C1D8DC9,
              i,
              descriptor.getAggregationType(),
              descriptor.getStep()));
      boxValue.add(
          String.format(
              DataNodeQueryMessages.EXCEPTION_OUTPUT_ARG_1B85810F,
              descriptor.getOutputColumnNames()));
      boxValue.add(
          String.format(
              DataNodeQueryMessages.EXCEPTION_INPUT_ARG_C70AA19F,
              descriptor.getInputExpressions()));
    }
    return render(node, boxValue, context);
  }

  @Override
  public List<String> visitGroupByTag(GroupByTagNode node, GraphContext context) {
    List<String> boxValue = new ArrayList<>();
    boxValue.add(String.format("GroupByTag-%s", node.getPlanNodeId().getId()));
    boxValue.add(
        String.format(DataNodeQueryMessages.EXCEPTION_TAG_KEYS_ARG_DFFA4DCF, node.getTagKeys()));
    int bucketIdx = 0;
    for (Entry<List<String>, List<CrossSeriesAggregationDescriptor>> entry :
        node.getTagValuesToAggregationDescriptors().entrySet()) {
      boxValue.add(
          String.format(
              DataNodeQueryMessages.EXCEPTION_BUCKET_ARG_ARG_D52292C8, bucketIdx, entry.getKey()));
      int aggregatorIdx = 0;
      for (CrossSeriesAggregationDescriptor descriptor : entry.getValue()) {
        if (descriptor == null) {
          boxValue.add(
              String.format(
                  DataNodeQueryMessages.EXCEPTION_AGGREGATOR_ARG_NULL_B5E4F862, aggregatorIdx));
        } else {
          boxValue.add(
              String.format(
                  DataNodeQueryMessages.EXCEPTION_AGGREGATOR_ARG_ARG_ARG_C65C36A7,
                  aggregatorIdx,
                  descriptor.getAggregationType(),
                  descriptor.getStep()));
          boxValue.add(
              String.format(
                  DataNodeQueryMessages.EXCEPTION_OUTPUT_ARG_893AC7DA,
                  descriptor.getOutputColumnNames()));
          boxValue.add(
              String.format(
                  DataNodeQueryMessages.EXCEPTION_INPUT_ARG_F57C6A9B,
                  descriptor.getInputExpressions()));
        }
        aggregatorIdx += 1;
      }
      bucketIdx += 1;
    }
    return render(node, boxValue, context);
  }

  @Override
  public List<String> visitSlidingWindowAggregation(
      SlidingWindowAggregationNode node, GraphContext context) {
    List<String> boxValue = new ArrayList<>();
    boxValue.add(String.format("SlidingWindowAggregation-%s", node.getPlanNodeId().getId()));
    for (int i = 0; i < node.getAggregationDescriptorList().size(); i++) {
      AggregationDescriptor descriptor = node.getAggregationDescriptorList().get(i);
      boxValue.add(
          String.format(
              DataNodeQueryMessages.EXCEPTION_AGGREGATOR_ARG_ARG_ARG_8C1D8DC9,
              i,
              descriptor.getAggregationType(),
              descriptor.getStep()));
    }
    return render(node, boxValue, context);
  }

  @Override
  public List<String> visitOffset(OffsetNode node, GraphContext context) {
    List<String> boxValue = new ArrayList<>();
    boxValue.add(String.format("Offset-%s", node.getPlanNodeId().getId()));
    boxValue.add(
        String.format(DataNodeQueryMessages.EXCEPTION_VALUE_ARG_CD522509, node.getOffset()));
    return render(node, boxValue, context);
  }

  @Override
  public List<String> visitAggregation(AggregationNode node, GraphContext context) {
    List<String> boxValue = new ArrayList<>();
    boxValue.add(String.format("Aggregation-%s", node.getPlanNodeId().getId()));
    for (int i = 0; i < node.getAggregationDescriptorList().size(); i++) {
      AggregationDescriptor descriptor = node.getAggregationDescriptorList().get(i);
      boxValue.add(
          String.format(
              DataNodeQueryMessages.EXCEPTION_AGGREGATOR_ARG_ARG_ARG_8C1D8DC9,
              i,
              descriptor.getAggregationType(),
              descriptor.getStep()));
    }
    return render(node, boxValue, context);
  }

  @Override
  public List<String> visitRawDataAggregation(RawDataAggregationNode node, GraphContext context) {
    List<String> boxValue = new ArrayList<>();
    boxValue.add(String.format("RawDataAggregation-%s", node.getPlanNodeId().getId()));
    for (int i = 0; i < node.getAggregationDescriptorList().size(); i++) {
      AggregationDescriptor descriptor = node.getAggregationDescriptorList().get(i);
      boxValue.add(
          String.format(
              DataNodeQueryMessages.EXCEPTION_AGGREGATOR_ARG_ARG_ARG_8C1D8DC9,
              i,
              descriptor.getAggregationType(),
              descriptor.getStep()));
    }
    return render(node, boxValue, context);
  }

  @Override
  public List<String> visitSort(SortNode node, GraphContext context) {
    List<String> boxValue = new ArrayList<>();
    boxValue.add(String.format("Sort-%s", node.getPlanNodeId().getId()));
    boxValue.add(node.getOrderByParameter().toString());
    return render(node, boxValue, context);
  }

  @Override
  public List<String> visitExchange(
      org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.ExchangeNode node,
      GraphContext context) {
    List<String> boxValue = new ArrayList<>();
    boxValue.add(String.format("Exchange-%s", node.getPlanNodeId().getId()));
    return render(node, boxValue, context);
  }

  @Override
  public List<String> visitLeftOuterTimeJoin(LeftOuterTimeJoinNode node, GraphContext context) {
    List<String> boxValue = new ArrayList<>();
    boxValue.add(String.format("LeftOuterTimeJoin-%s", node.getPlanNodeId().getId()));
    boxValue.add(
        String.format(DataNodeQueryMessages.EXCEPTION_ORDER_ARG_5AC5D051, node.getMergeOrder()));
    return render(node, boxValue, context);
  }

  @Override
  public List<String> visitInnerTimeJoin(InnerTimeJoinNode node, GraphContext context) {
    List<String> boxValue = new ArrayList<>();
    boxValue.add(String.format("InnerTimeJoin-%s", node.getPlanNodeId().getId()));
    boxValue.add(
        String.format(DataNodeQueryMessages.EXCEPTION_ORDER_ARG_5AC5D051, node.getMergeOrder()));
    Optional<List<Long>> timePartitions = node.getTimePartitions();
    if (timePartitions.isPresent()) {
      int size = timePartitions.get().size();
      if (size > 0) {
        StringBuilder builder =
            new StringBuilder("TimePartitions: [").append(timePartitions.get().get(0));
        for (int i = 1; i < Math.min(size, 4); i++) {
          builder.append(",").append(timePartitions.get().get(i));
        }
        for (int i = 4; i < size; i += 4) {
          builder.append(",").append(System.lineSeparator());
          builder.append(timePartitions.get().get(i));
          int j = i + 1;
          while (j < Math.min(i + 4, size)) {
            builder.append(",").append(timePartitions.get().get(j));
          }
        }
        builder.append("]");
        boxValue.add(builder.toString());
      } else {
        boxValue.add("TimePartitions: []");
      }
    } else {
      boxValue.add("TimePartitions: ALL");
    }

    return render(node, boxValue, context);
  }

  @Override
  public List<String> visitFullOuterTimeJoin(FullOuterTimeJoinNode node, GraphContext context) {
    List<String> boxValue = new ArrayList<>();
    boxValue.add(String.format("FullOuterTimeJoin-%s", node.getPlanNodeId().getId()));
    boxValue.add(
        String.format(DataNodeQueryMessages.EXCEPTION_ORDER_ARG_5AC5D051, node.getMergeOrder()));
    return render(node, boxValue, context);
  }

  @Override
  public List<String> visitLimit(LimitNode node, GraphContext context) {
    List<String> boxValue = new ArrayList<>();
    boxValue.add(String.format("Limit-%s", node.getPlanNodeId().getId()));
    boxValue.add(
        String.format(DataNodeQueryMessages.EXCEPTION_COUNT_ARG_4716A93F, node.getLimit()));
    return render(node, boxValue, context);
  }

  @Override
  public List<String> visitTransform(TransformNode node, GraphContext context) {
    List<String> boxValue = new ArrayList<>();
    boxValue.add(String.format("Transform-%s", node.getPlanNodeId().getId()));
    for (int i = 0; i < node.getOutputExpressions().length; i++) {
      Expression exp = node.getOutputExpressions()[i];
      boxValue.add(
          String.format(
              DataNodeQueryMessages.EXCEPTION_EXP_ARG_ARG_ARG_DA510DF0,
              i,
              exp.getExpressionType(),
              exp.getExpressionString()));
    }
    return render(node, boxValue, context);
  }

  @Override
  public List<String> visitProject(ProjectNode node, GraphContext context) {
    List<String> boxValue = new ArrayList<>();
    boxValue.add(String.format("Project-%s", node.getPlanNodeId().getId()));
    List<String> outputColumns = node.getOutputColumnNames();
    if (outputColumns == null) {
      checkArgument(context.getTemplatedInfo() != null);
      outputColumns = context.getTemplatedInfo().getDeviceViewOutputNames();
      // skip device column
      outputColumns = outputColumns.subList(1, outputColumns.size());
    }

    for (int i = 0; i < outputColumns.size(); i++) {
      String outputColumn = outputColumns.get(i);
      boxValue.add(
          String.format(
              DataNodeQueryMessages.EXCEPTION_OUTPUTCOLUMN_ARG_ARG_E768E3E4, i, outputColumn));
    }
    return render(node, boxValue, context);
  }

  @Override
  public List<String> visitInto(IntoNode node, GraphContext context) {
    List<String> boxValue = new ArrayList<>();
    boxValue.add(String.format("Into-%s", node.getPlanNodeId().getId()));
    IntoPathDescriptor descriptor = node.getIntoPathDescriptor();
    drawSourceTargetPath(
        boxValue,
        descriptor.getSourceTargetPathPairList(),
        descriptor.getTargetDeviceToAlignedMap());
    return render(node, boxValue, context);
  }

  @Override
  public List<String> visitDeviceViewInto(DeviceViewIntoNode node, GraphContext context) {
    List<String> boxValue = new ArrayList<>();
    boxValue.add(String.format("DeviceViewInto-%s", node.getPlanNodeId().getId()));
    DeviceViewIntoPathDescriptor descriptor = node.getDeviceViewIntoPathDescriptor();
    Map<String, List<Pair<String, PartialPath>>> deviceToSourceTargetPathPairListMap =
        descriptor.getDeviceToSourceTargetPathPairListMap();
    for (Map.Entry<String, List<Pair<String, PartialPath>>> entry :
        deviceToSourceTargetPathPairListMap.entrySet()) {
      String deviceName = entry.getKey();
      boxValue.add(String.format("Device [%s]:", deviceName));
      drawSourceTargetPath(boxValue, entry.getValue(), descriptor.getTargetDeviceToAlignedMap());
    }
    return render(node, boxValue, context);
  }

  private void drawSourceTargetPath(
      List<String> boxValue,
      List<Pair<String, PartialPath>> sourceTargetPathPairList,
      Map<String, Boolean> targetDeviceToAlignedMap) {
    for (Pair<String, PartialPath> sourceTargetPathPair : sourceTargetPathPairList) {
      boxValue.add(
          String.format(
              "%s -> %s %s",
              sourceTargetPathPair.left,
              sourceTargetPathPair.right,
              targetDeviceToAlignedMap.get(sourceTargetPathPair.right.getIDeviceID().toString())
                  ? "[ALIGNED]"
                  : ""));
    }
  }

  @Override
  public List<String> visitLastQueryScan(LastQueryScanNode node, GraphContext context) {
    List<String> boxValue = new ArrayList<>();
    boxValue.add(String.format("LastQueryScan-%s", node.getPlanNodeId().getId()));
    boxValue.add(
        String.format(DataNodeQueryMessages.EXCEPTION_ALIGNED_ARG_E48AEDF2, node.isAligned()));
    boxValue.add(
        String.format(
            DataNodeQueryMessages.EXCEPTION_SERIES_ARG_ARG_4AD75872,
            node.getDevicePath().getIDeviceID(),
            node.getMeasurementSchemas()));

    List<String> outputPaths = node.getOutputPaths();
    if (outputPaths != null) {
      boxValue.add(
          String.format(DataNodeQueryMessages.EXCEPTION_OUTPUTPATHS_ARG_0EC28B1F, outputPaths));
    }
    boxValue.add(printRegion(node.getRegionReplicaSet()));
    return render(node, boxValue, context);
  }

  @Override
  public List<String> visitLastQuery(LastQueryNode node, GraphContext context) {
    List<String> boxValue = new ArrayList<>();
    boxValue.add(String.format("LastQuery-%s", node.getPlanNodeId().getId()));
    return render(node, boxValue, context);
  }

  @Override
  public List<String> visitLastQueryMerge(LastQueryMergeNode node, GraphContext context) {
    List<String> boxValue = new ArrayList<>();
    boxValue.add(String.format("LastQueryMerge-%s", node.getPlanNodeId().getId()));
    return render(node, boxValue, context);
  }

  @Override
  public List<String> visitLastQueryCollect(LastQueryCollectNode node, GraphContext context) {
    List<String> boxValue = new ArrayList<>();
    boxValue.add(String.format("LastQueryCollect-%s", node.getPlanNodeId().getId()));
    return render(node, boxValue, context);
  }

  @Override
  public List<String> visitLastQueryTransform(LastQueryTransformNode node, GraphContext context) {
    List<String> boxValue = new ArrayList<>();
    boxValue.add(String.format("LastQueryTransform-%s", node.getPlanNodeId().getId()));
    boxValue.add(
        String.format(DataNodeQueryMessages.EXCEPTION_VIEWPATH_ARG_066D73F6, node.getViewPath()));
    return render(node, boxValue, context);
  }

  @Override
  public List<String> visitHorizontallyConcat(HorizontallyConcatNode node, GraphContext context) {
    List<String> boxValue = new ArrayList<>();
    boxValue.add(String.format("HorizontallyConcat-%s", node.getPlanNodeId().getId()));
    return render(node, boxValue, context);
  }

  @Override
  public List<String> visitIdentitySink(IdentitySinkNode node, GraphContext context) {
    List<String> boxValue = new ArrayList<>();
    boxValue.add(String.format("IdentitySink-%s", node.getPlanNodeId().getId()));
    return render(node, boxValue, context);
  }

  @Override
  public List<String> visitShuffleSink(ShuffleSinkNode node, GraphContext context) {
    List<String> boxValue = new ArrayList<>();
    boxValue.add(String.format("ShuffleSink-%s", node.getPlanNodeId().getId()));
    return render(node, boxValue, context);
  }

  @Override
  public List<String> visitColumnInject(ColumnInjectNode node, GraphContext context) {
    List<String> boxValue = new ArrayList<>();
    boxValue.add(String.format("ColumnInject-%s", node.getPlanNodeId().getId()));
    boxValue.add(
        String.format(
            DataNodeQueryMessages.EXCEPTION_TARGETINDEX_ARG_38B82D51, node.getTargetIndex()));
    boxValue.add(
        String.format(
            DataNodeQueryMessages.EXCEPTION_COLUMNGENERATORPARAMETERTYPE_ARG_74D7D221,
            node.getColumnGeneratorParameter().getGeneratorType()));
    return render(node, boxValue, context);
  }

  // =============== Methods below are used for table model ================
  @Override
  public List<String> visitCteScan(CteScanNode node, GraphContext context) {
    List<String> boxValue = new ArrayList<>();
    boxValue.add(String.format("CteScan-%s", node.getPlanNodeId().getId()));
    boxValue.add(
        String.format(
            DataNodeQueryMessages.EXCEPTION_OUTPUTSYMBOLS_ARG_2CDA67EC, node.getOutputSymbols()));
    return render(node, boxValue, context);
  }

  @Override
  public List<String> visitTableScan(TableScanNode node, GraphContext context) {
    DeviceTableScanNode deviceTableScanNode = null;
    if (node instanceof DeviceTableScanNode) {
      deviceTableScanNode = (DeviceTableScanNode) node;
    }
    ExternalTsFileScanNode externalTsFileScanNode = null;
    if (node instanceof ExternalTsFileScanNode) {
      externalTsFileScanNode = (ExternalTsFileScanNode) node;
    }

    List<String> boxValue = new ArrayList<>();
    boxValue.add(node.toString());
    boxValue.add(
        String.format(
            DataNodeQueryMessages.EXCEPTION_QUALIFIEDTABLENAME_ARG_111FC756,
            node.getQualifiedObjectName().toString()));
    boxValue.add(
        String.format(
            DataNodeQueryMessages.EXCEPTION_OUTPUTSYMBOLS_ARG_2CDA67EC, node.getOutputSymbols()));

    if (deviceTableScanNode != null) {
      boxValue.add(
          String.format(
              DataNodeQueryMessages.EXCEPTION_DEVICENUMBER_ARG_446C345C,
              deviceTableScanNode.getDeviceEntryCount()));
      boxValue.add(
          String.format(
              DataNodeQueryMessages.EXCEPTION_SCANORDER_ARG_AB0CB81A,
              deviceTableScanNode.getScanOrder()));
      if (deviceTableScanNode.getTimePredicate().isPresent()) {
        boxValue.add(
            String.format(
                DataNodeQueryMessages.EXCEPTION_TIMEPREDICATE_ARG_26915CBF,
                deviceTableScanNode.getTimePredicate().get()));
      }
    }
    if (externalTsFileScanNode != null) {
      boxValue.add(
          String.format(
              DataNodeQueryMessages.EXCEPTION_TSFILENUMBER_ARG_0D17D680,
              externalTsFileScanNode.getTsFilePaths().size()));
    }

    if (node.getPushDownPredicate() != null) {
      boxValue.add(
          String.format(
              DataNodeQueryMessages.EXCEPTION_PUSHDOWNPREDICATE_ARG_196CC2DC,
              node.getPushDownPredicate()));
    }
    boxValue.add(
        String.format(
            DataNodeQueryMessages.EXCEPTION_PUSHDOWNOFFSET_ARG_00E1070C, node.getPushDownOffset()));
    boxValue.add(
        String.format(
            DataNodeQueryMessages.EXCEPTION_PUSHDOWNLIMIT_ARG_E1420251, node.getPushDownLimit()));

    if (deviceTableScanNode != null) {
      boxValue.add(
          String.format(
              DataNodeQueryMessages.EXCEPTION_PUSHDOWNLIMITTOEACHDEVICE_ARG_2DD716F0,
              deviceTableScanNode.isPushLimitToEachDevice()));
      String topKRuntimeFilterSourceId = deviceTableScanNode.getTopKRuntimeFilterSourceId();
      if (topKRuntimeFilterSourceId != null) {
        boxValue.add(
            String.format(
                DataNodeQueryMessages.EXCEPTION_TOPN_OPT_ARG_6358B1C6, topKRuntimeFilterSourceId));
      }
    }

    boxValue.add(
        String.format(
            DataNodeQueryMessages.EXCEPTION_REGIONID_ARG_190371FB,
            node.getRegionReplicaSet() == null || node.getRegionReplicaSet().getRegionId() == null
                ? REGION_NOT_ASSIGNED
                : node.getRegionReplicaSet().getRegionId().getId()));

    if (deviceTableScanNode instanceof TreeDeviceViewScanNode) {
      TreeDeviceViewScanNode treeDeviceViewScanNode = (TreeDeviceViewScanNode) deviceTableScanNode;
      boxValue.add(
          String.format(
              DataNodeQueryMessages.EXCEPTION_TREEDB_ARG_409FDB70,
              treeDeviceViewScanNode.getTreeDBName()));
      boxValue.add(
          String.format(
              DataNodeQueryMessages.EXCEPTION_MEASUREMENTTOCOLUMNNAME_ARG_E2BC3300,
              treeDeviceViewScanNode.getMeasurementColumnNameMap()));
    }
    if (node instanceof TableDiskUsageInformationSchemaTableScanNode) {
      boxValue.add(
          String.format(
              "%s: %s",
              REGIONS_OF_CURRENT_SUB_TASK,
              ((TableDiskUsageInformationSchemaTableScanNode) node).getRegions()));
    }
    return render(node, boxValue, context);
  }

  @Override
  public List<String> visitAggregation(
      org.apache.iotdb.commons.queryengine.plan.relational.planner.node.AggregationNode node,
      GraphContext context) {
    List<String> boxValue = new ArrayList<>();
    boxValue.add(String.format("Aggregation-%s", node.getPlanNodeId().getId()));
    boxValue.add(
        String.format(
            DataNodeQueryMessages.EXCEPTION_OUTPUTSYMBOLS_ARG_2CDA67EC, node.getOutputSymbols()));
    int i = 0;
    for (org.apache.iotdb.commons.queryengine.plan.relational.planner.node.AggregationNode
            .Aggregation
        aggregation : node.getAggregations().values()) {
      StringBuilder aggregator =
          new StringBuilder(
              String.format(
                  DataNodeQueryMessages.EXCEPTION_AGGREGATOR_ARG_ARG_B03FFEF0,
                  i++,
                  aggregation.getResolvedFunction().toString()));
      if (aggregation.hasMask()) {
        aggregator.append(
            String.format(
                DataNodeQueryMessages.EXCEPTION_MASK_ARG_452DF02A, aggregation.getMask().get()));
      }

      if (aggregation.isDistinct()) {
        aggregator.append("  distinct: true");
      }
      boxValue.add(aggregator.toString());
    }
    boxValue.add(
        String.format(
            DataNodeQueryMessages.EXCEPTION_GROUPINGKEYS_ARG_4D5DC588, node.getGroupingKeys()));
    if (node.isStreamable()) {
      boxValue.add("Streamable: true");
      boxValue.add(
          String.format(
              DataNodeQueryMessages.EXCEPTION_PREGROUPEDSYMBOLS_ARG_93C21065,
              node.getPreGroupedSymbols()));
    }
    boxValue.add(String.format(DataNodeQueryMessages.EXCEPTION_STEP_ARG_ACB0A043, node.getStep()));
    return render(node, boxValue, context);
  }

  @Override
  public List<String> visitAggregationTableScan(
      org.apache.iotdb.db.queryengine.plan.relational.planner.node.AggregationTableScanNode node,
      GraphContext context) {
    List<String> boxValue = new ArrayList<>();
    boxValue.add(node.toString());
    boxValue.add(
        String.format(
            DataNodeQueryMessages.EXCEPTION_QUALIFIEDTABLENAME_ARG_111FC756,
            node.getQualifiedObjectName().toString()));
    boxValue.add(
        String.format(
            DataNodeQueryMessages.EXCEPTION_OUTPUTSYMBOLS_ARG_2CDA67EC, node.getOutputSymbols()));
    int i = 0;
    for (org.apache.iotdb.commons.queryengine.plan.relational.planner.node.AggregationNode
            .Aggregation
        aggregation : node.getAggregations().values()) {
      StringBuilder aggregator =
          new StringBuilder(
              String.format(
                  DataNodeQueryMessages.EXCEPTION_AGGREGATOR_ARG_ARG_B03FFEF0,
                  i++,
                  aggregation.getResolvedFunction().toString()));
      if (aggregation.hasMask()) {
        aggregator.append(
            String.format(
                DataNodeQueryMessages.EXCEPTION_MASK_ARG_452DF02A, aggregation.getMask().get()));
      }

      if (aggregation.isDistinct()) {
        aggregator.append("  distinct: true");
      }
      boxValue.add(aggregator.toString());
    }
    boxValue.add(
        String.format(
            DataNodeQueryMessages.EXCEPTION_GROUPINGKEYS_ARG_4D5DC588, node.getGroupingKeys()));
    if (node.isStreamable()) {
      boxValue.add("Streamable: true");
      boxValue.add(
          String.format(
              DataNodeQueryMessages.EXCEPTION_PREGROUPEDSYMBOLS_ARG_93C21065,
              node.getPreGroupedSymbols()));
    }
    boxValue.add(String.format(DataNodeQueryMessages.EXCEPTION_STEP_ARG_ACB0A043, node.getStep()));

    if (node.getProjection() != null) {
      boxValue.add(
          String.format(
              DataNodeQueryMessages.EXCEPTION_PROJECT_EXPRESSIONS_ARG_B86BB5E3,
              node.getProjection().getMap().values()));
    }

    boxValue.add(
        String.format(
            DataNodeQueryMessages.EXCEPTION_DEVICENUMBER_ARG_446C345C, node.getDeviceEntryCount()));
    boxValue.add(
        String.format(DataNodeQueryMessages.EXCEPTION_SCANORDER_ARG_AB0CB81A, node.getScanOrder()));
    if (node.getPushDownPredicate() != null) {
      boxValue.add(
          String.format(
              DataNodeQueryMessages.EXCEPTION_PUSHDOWNPREDICATE_ARG_196CC2DC,
              node.getPushDownPredicate()));
    }
    boxValue.add(
        String.format(
            DataNodeQueryMessages.EXCEPTION_PUSHDOWNOFFSET_ARG_00E1070C, node.getPushDownOffset()));
    boxValue.add(
        String.format(
            DataNodeQueryMessages.EXCEPTION_PUSHDOWNLIMIT_ARG_E1420251, node.getPushDownLimit()));
    boxValue.add(
        String.format(
            DataNodeQueryMessages.EXCEPTION_PUSHDOWNLIMITTOEACHDEVICE_ARG_2DD716F0,
            node.isPushLimitToEachDevice()));
    boxValue.add(
        String.format(
            DataNodeQueryMessages.EXCEPTION_REGIONID_ARG_190371FB,
            node.getRegionReplicaSet() == null || node.getRegionReplicaSet().getRegionId() == null
                ? REGION_NOT_ASSIGNED
                : node.getRegionReplicaSet().getRegionId().getId()));

    if (node instanceof AggregationTreeDeviceViewScanNode) {
      AggregationTreeDeviceViewScanNode aggregationTreeDeviceViewScanNode =
          (AggregationTreeDeviceViewScanNode) node;
      boxValue.add(
          String.format(
              DataNodeQueryMessages.EXCEPTION_TREEDB_ARG_409FDB70,
              aggregationTreeDeviceViewScanNode.getTreeDBName()));
      boxValue.add(
          String.format(
              DataNodeQueryMessages.EXCEPTION_MEASUREMENTTOCOLUMNNAME_ARG_E2BC3300,
              aggregationTreeDeviceViewScanNode.getMeasurementColumnNameMap()));
    }
    return render(node, boxValue, context);
  }

  @Override
  public List<String> visitMarkDistinct(MarkDistinctNode node, GraphContext context) {
    List<String> boxValue = new ArrayList<>();
    boxValue.add(String.format("MarkDistinct-%s", node.getPlanNodeId()));
    boxValue.add(String.format("MarkerSymbol-%s", node.getMarkerSymbol()));
    boxValue.add(String.format("DistinctSymbols-%s", node.getDistinctSymbols()));
    Optional<Symbol> hashSymbol = node.getHashSymbol();
    hashSymbol.ifPresent(symbol -> boxValue.add(String.format("HashSymbol-%s", symbol)));
    return render(node, boxValue, context);
  }

  @Override
  public List<String> visitFilter(
      org.apache.iotdb.commons.queryengine.plan.relational.planner.node.FilterNode node,
      GraphContext context) {
    List<String> boxValue = new ArrayList<>();
    boxValue.add(String.format("Filter-%s", node.getPlanNodeId().getId()));
    boxValue.add(
        String.format(DataNodeQueryMessages.EXCEPTION_PREDICATE_ARG_5DFE6605, node.getPredicate()));
    return render(node, boxValue, context);
  }

  @Override
  public List<String> visitProject(
      org.apache.iotdb.commons.queryengine.plan.relational.planner.node.ProjectNode node,
      GraphContext context) {
    List<String> boxValue = new ArrayList<>();
    boxValue.add(String.format("Project-%s", node.getPlanNodeId().getId()));
    boxValue.add(
        String.format(
            DataNodeQueryMessages.EXCEPTION_OUTPUTSYMBOLS_ARG_2CDA67EC, node.getOutputSymbols()));
    boxValue.add(
        String.format(
            DataNodeQueryMessages.EXCEPTION_EXPRESSIONS_ARG_7A24068A,
            node.getAssignments().getMap().values()));
    return render(node, boxValue, context);
  }

  @Override
  public List<String> visitInto(
      org.apache.iotdb.db.queryengine.plan.relational.planner.node.IntoNode node,
      GraphContext context) {
    List<String> boxValue = new ArrayList<>();
    boxValue.add(String.format("Into-%s", node.getPlanNodeId().getId()));
    boxValue.add(
        String.format(
            DataNodeQueryMessages.EXCEPTION_OUTPUTSYMBOLS_ARG_2CDA67EC, node.getOutputSymbols()));
    return render(node, boxValue, context);
  }

  @Override
  public List<String> visitCopyTo(CopyToNode node, GraphContext context) {
    List<String> boxValue = new ArrayList<>();
    boxValue.add(
        String.format(
            DataNodeQueryMessages.MESSAGE_COPYTO_ARG_38343199, node.getPlanNodeId().getId()));
    boxValue.add(
        String.format(
            DataNodeQueryMessages.MESSAGE_TARGETFILE_ARG_EC484B24, node.getTargetFilePath()));
    boxValue.add(
        String.format(
            DataNodeQueryMessages.MESSAGE_FORMAT_ARG_CE5DC4EE,
            node.getCopyToOptions().getFormat()));
    if (node.getCopyToOptions() instanceof CopyToTsFileOptions options) {
      boxValue.add(
          String.format(
              DataNodeQueryMessages.MESSAGE_TARGETTABLE_ARG_00BE4714,
              options.getTargetTableName()));
      boxValue.add(
          String.format(
              DataNodeQueryMessages.MESSAGE_TIMECOLUMN_ARG_54F6A2A6,
              options.getTargetTimeColumn()));
      boxValue.add(
          String.format(
              DataNodeQueryMessages.MESSAGE_TAGCOLUMNS_ARG_6520A0C2,
              options.getTargetTagColumns()));
      boxValue.add(
          String.format(
              DataNodeQueryMessages.MESSAGE_MEMORYTHRESHOLD_ARG_3676AB46,
              options.getTargetMemoryThreshold()));
    }
    return render(node, boxValue, context);
  }

  @Override
  public List<String> visitOutput(OutputNode node, GraphContext context) {
    List<String> boxValue = new ArrayList<>();
    boxValue.add(String.format("OutputNode-%s", node.getPlanNodeId().getId()));
    boxValue.add(String.format("OutputColumns-%s", node.getOutputColumnNames()));
    boxValue.add(
        String.format(
            DataNodeQueryMessages.EXCEPTION_OUTPUTSYMBOLS_ARG_2CDA67EC, node.getOutputSymbols()));
    return render(node, boxValue, context);
  }

  @Override
  public List<String> visitExplainAnalyze(ExplainAnalyzeNode node, GraphContext context) {
    List<String> boxValue = new ArrayList<>();
    boxValue.add(String.format("ExplainAnalyze-%s", node.getPlanNodeId().getId()));
    boxValue.add(
        String.format(
            DataNodeQueryMessages.EXCEPTION_CHILDPERMITTEDOUTPUTS_ARG_206350BD,
            node.getChildPermittedOutputs()));
    return render(node, boxValue, context);
  }

  @Override
  public List<String> visitGapFill(GapFillNode node, GraphContext context) {
    List<String> boxValue = new ArrayList<>();
    boxValue.add(String.format("GapFill-%s", node.getPlanNodeId().getId()));
    boxValue.add(
        String.format(DataNodeQueryMessages.EXCEPTION_STARTTIME_ARG_51654DAE, node.getStartTime()));
    boxValue.add(
        String.format(DataNodeQueryMessages.EXCEPTION_ENDTIME_ARG_BE3FB95D, node.getEndTime()));
    if (node.getMonthDuration() != 0) {
      boxValue.add(
          String.format(
              DataNodeQueryMessages.EXCEPTION_INTERVAL_ARG_MO_73E81203, node.getMonthDuration()));
    } else {
      boxValue.add(
          String.format(
              String.format(
                  DataNodeQueryMessages.EXCEPTION_INTERVAL_ARG_S_ARG_F9714CBA,
                  Objects.toString(
                      CommonDescriptor.getInstance().getConfig().getTimestampPrecision())),
              node.getNonMonthDuration()));
    }
    boxValue.add(
        String.format(
            DataNodeQueryMessages.EXCEPTION_GAPFILLCOLUMN_ARG_845A4BA2, node.getGapFillColumn()));
    if (!node.getGapFillGroupingKeys().isEmpty()) {
      boxValue.add(
          String.format(
              DataNodeQueryMessages.EXCEPTION_GAPFILLGROUPINGKEYS_ARG_DBA19008,
              node.getGapFillGroupingKeys()));
    }
    return render(node, boxValue, context);
  }

  @Override
  public List<String> visitPreviousFill(PreviousFillNode node, GraphContext context) {
    List<String> boxValue = new ArrayList<>();
    boxValue.add(String.format("PreviousFill-%s", node.getPlanNodeId().getId()));
    node.getTimeBound()
        .ifPresent(
            timeBound ->
                boxValue.add(
                    String.format(
                        DataNodeQueryMessages.EXCEPTION_TIME_BOUND_ARG_DA9D4812, timeBound)));
    node.getHelperColumn()
        .ifPresent(
            timeColumn ->
                boxValue.add(
                    String.format(
                        DataNodeQueryMessages.EXCEPTION_TIME_COLUMN_ARG_3A2B0F89, timeColumn)));
    node.getGroupingKeys()
        .ifPresent(
            groupingKeys ->
                boxValue.add(
                    String.format(
                        DataNodeQueryMessages.EXCEPTION_FILL_GROUP_ARG_91675AC6, groupingKeys)));

    return render(node, boxValue, context);
  }

  @Override
  public List<String> visitLinearFill(LinearFillNode node, GraphContext context) {
    List<String> boxValue = new ArrayList<>();
    boxValue.add(String.format("LinearFill-%s", node.getPlanNodeId().getId()));
    boxValue.add(
        String.format(
            DataNodeQueryMessages.EXCEPTION_TIME_COLUMN_ARG_3A2B0F89, node.getHelperColumn()));
    node.getGroupingKeys()
        .ifPresent(
            groupingKeys ->
                boxValue.add(
                    String.format(
                        DataNodeQueryMessages.EXCEPTION_FILL_GROUP_ARG_91675AC6, groupingKeys)));
    return render(node, boxValue, context);
  }

  @Override
  public List<String> visitNextFill(NextFillNode node, GraphContext context) {
    List<String> boxValue = new ArrayList<>();
    boxValue.add(String.format("NextFill-%s", node.getPlanNodeId().getId()));
    node.getTimeBound()
        .ifPresent(
            timeBound ->
                boxValue.add(
                    String.format(
                        DataNodeQueryMessages.EXCEPTION_TIME_BOUND_ARG_DA9D4812, timeBound)));
    node.getHelperColumn()
        .ifPresent(
            timeColumn ->
                boxValue.add(
                    String.format(
                        DataNodeQueryMessages.EXCEPTION_TIME_COLUMN_ARG_3A2B0F89, timeColumn)));
    node.getGroupingKeys()
        .ifPresent(
            groupingKeys ->
                boxValue.add(
                    String.format(
                        DataNodeQueryMessages.EXCEPTION_FILL_GROUP_ARG_91675AC6, groupingKeys)));

    return render(node, boxValue, context);
  }

  @Override
  public List<String> visitValueFill(ValueFillNode node, GraphContext context) {
    List<String> boxValue = new ArrayList<>();
    boxValue.add(String.format("ValueFill-%s", node.getPlanNodeId().getId()));
    boxValue.add(
        String.format(
            DataNodeQueryMessages.EXCEPTION_FILLEDVALUE_ARG_E9F0E84D, node.getFilledValue()));
    return render(node, boxValue, context);
  }

  @Override
  public List<String> visitTableExchange(ExchangeNode node, GraphContext context) {
    List<String> boxValue = new ArrayList<>();
    boxValue.add(String.format("Exchange-%s", node.getPlanNodeId().getId()));
    return render(node, boxValue, context);
  }

  @Override
  public List<String> visitLimit(
      org.apache.iotdb.commons.queryengine.plan.relational.planner.node.LimitNode node,
      GraphContext context) {
    List<String> boxValue = new ArrayList<>();
    boxValue.add(String.format("Limit-%s", node.getPlanNodeId().getId()));
    boxValue.add(
        String.format(DataNodeQueryMessages.EXCEPTION_COUNT_ARG_1E0B36E2, node.getCount()));
    return render(node, boxValue, context);
  }

  @Override
  public List<String> visitEnforceSingleRow(EnforceSingleRowNode node, GraphContext context) {
    List<String> boxValue = new ArrayList<>();
    boxValue.add(String.format("EnforceSingleRow-%s", node.getPlanNodeId().getId()));
    return render(node, boxValue, context);
  }

  @Override
  public List<String> visitOffset(
      org.apache.iotdb.commons.queryengine.plan.relational.planner.node.OffsetNode node,
      GraphContext context) {
    List<String> boxValue = new ArrayList<>();
    boxValue.add(String.format("Offset-%s", node.getPlanNodeId().getId()));
    boxValue.add(
        String.format(DataNodeQueryMessages.EXCEPTION_COUNT_ARG_1E0B36E2, node.getCount()));
    return render(node, boxValue, context);
  }

  @Override
  public List<String> visitSort(
      org.apache.iotdb.commons.queryengine.plan.relational.planner.node.SortNode node,
      GraphContext context) {
    List<String> boxValue = new ArrayList<>();
    boxValue.add(String.format("Sort-%s", node.getPlanNodeId().getId()));
    boxValue.add(
        String.format(
            DataNodeQueryMessages.EXCEPTION_ORDERINGSCHEME_ARG_88E8678A, node.getOrderingScheme()));
    return render(node, boxValue, context);
  }

  @Override
  public List<String> visitStreamSort(StreamSortNode node, GraphContext context) {
    List<String> boxValue = new ArrayList<>();
    boxValue.add(String.format("StreamSort-%s", node.getPlanNodeId().getId()));
    boxValue.add(
        String.format(
            DataNodeQueryMessages.EXCEPTION_ORDERINGSCHEME_ARG_88E8678A, node.getOrderingScheme()));
    boxValue.add(
        String.format(
            DataNodeQueryMessages.EXCEPTION_STREAMCOMPAREKEYENDINDEX_ARG_B0B5E239,
            node.getStreamCompareKeyEndIndex()));
    boxValue.add(
        String.format(
            DataNodeQueryMessages.EXCEPTION_ORDERBYALLIDSANDTIME_ARG_55959D0E,
            node.isOrderByAllIdsAndTime()));
    return render(node, boxValue, context);
  }

  @Override
  public List<String> visitGroup(GroupNode node, GraphContext context) {
    List<String> boxValue = new ArrayList<>();
    boxValue.add(String.format("GroupNode-%s", node.getPlanNodeId().getId()));
    boxValue.add(
        String.format(
            DataNodeQueryMessages.EXCEPTION_PARTITIONKEY_ARG_DD8CE0EC,
            node.getOrderingScheme().getOrderBy().subList(0, node.getPartitionKeyCount())));
    List<String> orderKey = new ArrayList<>();
    for (int i = node.getPartitionKeyCount();
        i < node.getOrderingScheme().getOrderBy().size();
        i++) {
      Symbol symbol = node.getOrderingScheme().getOrderBy().get(i);
      orderKey.add(symbol + " " + node.getOrderingScheme().getOrdering(symbol));
    }
    boxValue.add(String.format(DataNodeQueryMessages.EXCEPTION_ORDERKEY_ARG_BAC08FCF, orderKey));
    return render(node, boxValue, context);
  }

  @Override
  public List<String> visitMergeSort(
      org.apache.iotdb.commons.queryengine.plan.relational.planner.node.MergeSortNode node,
      GraphContext context) {
    List<String> boxValue = new ArrayList<>();
    boxValue.add(String.format("MergeSort-%s", node.getPlanNodeId().getId()));
    boxValue.add(
        String.format(
            DataNodeQueryMessages.EXCEPTION_OUTPUTSYMBOLS_ARG_2CDA67EC, node.getOutputSymbols()));
    boxValue.add(
        String.format(
            DataNodeQueryMessages.EXCEPTION_ORDERINGSCHEME_ARG_88E8678A, node.getOrderingScheme()));
    return render(node, boxValue, context);
  }

  @Override
  public List<String> visitCollect(
      org.apache.iotdb.commons.queryengine.plan.relational.planner.node.CollectNode node,
      GraphContext context) {
    List<String> boxValue = new ArrayList<>();
    boxValue.add(String.format("Collect-%s", node.getPlanNodeId().getId()));
    boxValue.add(
        String.format(
            DataNodeQueryMessages.EXCEPTION_OUTPUTSYMBOLS_ARG_2CDA67EC, node.getOutputSymbols()));
    return render(node, boxValue, context);
  }

  @Override
  public List<String> visitTopK(
      org.apache.iotdb.commons.queryengine.plan.relational.planner.node.TopKNode node,
      GraphContext context) {
    List<String> boxValue = new ArrayList<>();
    boxValue.add(String.format("TopK-%s", node.getPlanNodeId().getId()));
    boxValue.add(
        String.format(
            DataNodeQueryMessages.EXCEPTION_ORDERINGSCHEME_ARG_88E8678A, node.getOrderingScheme()));
    boxValue.add(
        String.format(DataNodeQueryMessages.EXCEPTION_COUNT_ARG_1E0B36E2, node.getCount()));
    if (node.getTopKRuntimeFilterSourceId() != null) {
      boxValue.add(DataNodeQueryMessages.EXCEPTION_TOPN_OPT_D3D72301);
    }
    return render(node, boxValue, context);
  }

  @Override
  public List<String> visitCorrelatedJoin(CorrelatedJoinNode node, GraphContext context) {
    List<String> boxValue = new ArrayList<>();
    boxValue.add(String.format("CorrelatedJoin-%s", node.getPlanNodeId().getId()));
    boxValue.add(
        String.format(DataNodeQueryMessages.EXCEPTION_JOINTYPE_ARG_971DC3F9, node.getJoinType()));
    boxValue.add(
        String.format(
            DataNodeQueryMessages.EXCEPTION_CORRELATION_ARG_E71B97B0, node.getCorrelation()));
    boxValue.add(
        String.format(DataNodeQueryMessages.EXCEPTION_FILTER_ARG_DC936398, node.getFilter()));
    return render(node, boxValue, context);
  }

  @Override
  public List<String> visitJoin(JoinNode node, GraphContext context) {
    List<String> boxValue = new ArrayList<>();
    boxValue.add(String.format("Join-%s", node.getPlanNodeId().getId()));
    if (node.getAsofCriteria().isPresent()) {
      boxValue.add(
          String.format(
              DataNodeQueryMessages.EXCEPTION_ASOF_JOINTYPE_ARG_1F69241C, node.getJoinType()));
      boxValue.add(
          String.format(
              DataNodeQueryMessages.EXCEPTION_ASOF_JOINCRITERIA_ARG_E313FAAD,
              node.getAsofCriteria().get()));
    } else {
      boxValue.add(
          String.format(DataNodeQueryMessages.EXCEPTION_JOINTYPE_ARG_971DC3F9, node.getJoinType()));
    }
    boxValue.add(
        String.format(
            DataNodeQueryMessages.EXCEPTION_JOINCRITERIA_ARG_EE225A01, node.getCriteria()));
    boxValue.add(
        String.format(
            DataNodeQueryMessages.EXCEPTION_LEFTOUTPUTSYMBOLS_ARG_D2BE06AE,
            node.getLeftOutputSymbols()));
    boxValue.add(
        String.format(
            DataNodeQueryMessages.EXCEPTION_RIGHTOUTPUTSYMBOLS_ARG_90AEAE8B,
            node.getRightOutputSymbols()));
    if (node.getFilter().isPresent()) {
      boxValue.add(
          String.format(
              DataNodeQueryMessages.EXCEPTION_FILTER_ARG_DC936398,
              node.getFilter().map(v -> v.toString()).orElse(null)));
    }
    return render(node, boxValue, context);
  }

  @Override
  public List<String> visitSemiJoin(SemiJoinNode node, GraphContext context) {
    List<String> boxValue = new ArrayList<>();
    boxValue.add(String.format("SemiJoin-%s", node.getPlanNodeId().getId()));
    boxValue.add(
        String.format(
            DataNodeQueryMessages.EXCEPTION_OUTPUTSYMBOLS_ARG_2CDA67EC, node.getOutputSymbols()));
    boxValue.add(
        String.format(
            DataNodeQueryMessages.EXCEPTION_SOURCEJOINSYMBOL_ARG_35F54565,
            node.getSourceJoinSymbol()));
    boxValue.add(
        String.format(
            DataNodeQueryMessages.EXCEPTION_FILTERINGSOURCEJOINSYMBOL_ARG_7A864AF7,
            node.getFilteringSourceJoinSymbol()));
    return render(node, boxValue, context);
  }

  @Override
  public List<String> visitPatternRecognition(PatternRecognitionNode node, GraphContext context) {
    List<String> boxValue = new ArrayList<>();
    boxValue.add(String.format("PatternRecognition-%s", node.getPlanNodeId().getId()));
    boxValue.add(
        String.format(
            DataNodeQueryMessages.EXCEPTION_PARTITIONBY_ARG_E97DFDCD, node.getPartitionBy()));
    node.getOrderingScheme()
        .ifPresent(
            orderingScheme ->
                boxValue.add(
                    String.format(
                        DataNodeQueryMessages.EXCEPTION_ORDERINGSCHEME_ARG_88E8678A,
                        orderingScheme)));
    node.getHashSymbol()
        .ifPresent(
            hashSymbol ->
                boxValue.add(
                    String.format(
                        DataNodeQueryMessages.EXCEPTION_HASHSYMBOL_ARG_CB1619A6, hashSymbol)));
    // boxValue.add(String.format("Measures: %s", node.getMeasures()));
    boxValue.add(
        String.format(
            DataNodeQueryMessages.EXCEPTION_ROWSPERMATCH_ARG_CFCB6FAC, node.getRowsPerMatch()));
    boxValue.add(
        String.format(
            DataNodeQueryMessages.EXCEPTION_SKIPTOLABELS_ARG_27BC57E1, node.getSkipToLabels()));
    boxValue.add(
        String.format(
            DataNodeQueryMessages.EXCEPTION_SKIPTOPOSITION_ARG_27FB2E49, node.getSkipToPosition()));
    boxValue.add(
        String.format(DataNodeQueryMessages.EXCEPTION_PATTERN_ARG_F93B8CF0, node.getPattern()));
    // boxValue.add(String.format("VariableDefinitions: %s", node.getVariableDefinitions()));
    return render(node, boxValue, context);
  }

  @Override
  public List<String> visitAssignUniqueId(AssignUniqueId node, GraphContext context) {
    List<String> boxValue = new ArrayList<>();
    boxValue.add(String.format("AssignUniqueId-%s", node.getPlanNodeId().getId()));
    boxValue.add(
        String.format(
            DataNodeQueryMessages.EXCEPTION_OUTPUTSYMBOLS_ARG_2CDA67EC, node.getOutputSymbols()));
    boxValue.add(
        String.format(
            DataNodeQueryMessages.EXCEPTION_IDCOLUMNSYMBOL_ARG_E37ADCC7, node.getIdColumn()));
    return render(node, boxValue, context);
  }

  @Override
  public List<String> visitTableFunctionProcessor(
      TableFunctionProcessorNode node, GraphContext context) {
    List<String> boxValue = new ArrayList<>();
    boxValue.add(String.format("TableFunctionProcessor-%s", node.getPlanNodeId().getId()));
    boxValue.add(
        String.format(DataNodeQueryMessages.EXCEPTION_FUNCTION_ARG_D328EC66, node.getName()));
    boxValue.add(
        String.format(
            DataNodeQueryMessages.EXCEPTION_PROPEROUTPUTS_ARG_D9578E71, node.getProperOutputs()));
    boxValue.add(
        String.format(
            DataNodeQueryMessages.EXCEPTION_OUTPUTSYMBOLS_ARG_2CDA67EC, node.getOutputSymbols()));
    boxValue.add(
        String.format(
            DataNodeQueryMessages.EXCEPTION_REQUIREDSYMBOLS_ARG_111D7FC1,
            node.getRequiredSymbols()));
    node.getDataOrganizationSpecification()
        .ifPresent(
            specification -> {
              if (!specification.getPartitionBy().isEmpty()) {
                boxValue.add(
                    String.format(
                        DataNodeQueryMessages.EXCEPTION_PARTITION_BY_ARG_4F7F521F,
                        Objects.toString(Joiner.on(", ").join(specification.getPartitionBy()))));
              }
              specification
                  .getOrderingScheme()
                  .ifPresent(
                      orderingScheme ->
                          boxValue.add(
                              String.format(
                                  DataNodeQueryMessages.EXCEPTION_ORDER_BY_ARG_D26FF4AA,
                                  Objects.toString(orderingScheme))));
            });
    boxValue.add("TableFunctionHandle: " + node.getTableFunctionHandle());
    return render(node, boxValue, context);
  }

  @Override
  public List<String> visitWindowFunction(WindowNode node, GraphContext context) {
    List<String> boxValue = new ArrayList<>();
    boxValue.add(String.format("WindowFunction-%s", node.getPlanNodeId().getId()));
    boxValue.add(
        String.format(
            DataNodeQueryMessages.EXCEPTION_OUTPUTSYMBOLS_ARG_2CDA67EC, node.getOutputSymbols()));

    DataOrganizationSpecification specification = node.getSpecification();
    if (!specification.getPartitionBy().isEmpty()) {
      boxValue.add(
          String.format(
              DataNodeQueryMessages.EXCEPTION_PARTITION_BY_ARG_4F7F521F,
              Objects.toString(Joiner.on(", ").join(specification.getPartitionBy()))));
    }
    specification
        .getOrderingScheme()
        .ifPresent(
            orderingScheme ->
                boxValue.add(
                    String.format(
                        DataNodeQueryMessages.EXCEPTION_ORDER_BY_ARG_D26FF4AA,
                        Objects.toString(orderingScheme))));

    return render(node, boxValue, context);
  }

  @Override
  public List<String> visitValuesNode(ValuesNode node, GraphContext context) {
    List<String> boxValue = new ArrayList<>();
    boxValue.add(String.format("ValuesNode-%s", node.getPlanNodeId().getId()));
    boxValue.add(
        String.format(DataNodeQueryMessages.EXCEPTION_ROWCOUNT_ARG_25F64C0A, node.getRowCount()));
    boxValue.add(
        String.format(
            DataNodeQueryMessages.EXCEPTION_OUTPUTSYMBOLS_ARG_2CDA67EC, node.getOutputSymbols()));

    return render(node, boxValue, context);
  }

  @Override
  public List<String> visitRowNumber(RowNumberNode node, GraphContext context) {
    List<String> boxValue = new ArrayList<>();
    boxValue.add(String.format("RowNumber-%s", node.getPlanNodeId().getId()));
    boxValue.add(
        String.format(
            DataNodeQueryMessages.EXCEPTION_ROWNUMBERSYMBOL_ARG_39FD3A50,
            node.getRowNumberSymbol()));
    boxValue.add(
        String.format(
            DataNodeQueryMessages.EXCEPTION_ISORDERINGSENSITIVE_ARG_96687CE9,
            node.isOrderSensitive()));
    boxValue.add(
        String.format(
            DataNodeQueryMessages.EXCEPTION_PARTITION_BY_ARG_4F7F521F,
            Objects.toString(Joiner.on(", ").join(node.getPartitionBy()))));

    return render(node, boxValue, context);
  }

  @Override
  public List<String> visitTopKRanking(TopKRankingNode node, GraphContext context) {
    List<String> boxValue = new ArrayList<>();

    boxValue.add(String.format("TopKRanking-%s", node.getPlanNodeId().getId()));
    boxValue.add(
        String.format(
            DataNodeQueryMessages.EXCEPTION_RANKINGTYPE_ARG_48037AC8, node.getRankingType()));
    boxValue.add(
        String.format(
            DataNodeQueryMessages.EXCEPTION_RANKINGSYMBOL_ARG_EC2D08FD, node.getRankingSymbol()));
    DataOrganizationSpecification specification = node.getSpecification();
    if (!specification.getPartitionBy().isEmpty()) {
      boxValue.add(
          String.format(
              DataNodeQueryMessages.EXCEPTION_PARTITION_BY_ARG_4F7F521F,
              Objects.toString(Joiner.on(", ").join(specification.getPartitionBy()))));
    }
    specification
        .getOrderingScheme()
        .ifPresent(
            orderingScheme ->
                boxValue.add(
                    String.format(
                        DataNodeQueryMessages.EXCEPTION_ORDER_BY_ARG_D26FF4AA,
                        Objects.toString(orderingScheme))));

    return render(node, boxValue, context);
  }

  private String printRegion(TRegionReplicaSet regionReplicaSet) {
    return String.format(
        DataNodeQueryMessages.EXCEPTION_PARTITION_ARG_975DEE2D,
        regionReplicaSet == null || regionReplicaSet == DataPartition.NOT_ASSIGNED
            ? REGION_NOT_ASSIGNED
            : String.valueOf(regionReplicaSet.getRegionId().id));
  }

  @Override
  public List<String> visitUnion(UnionNode node, GraphContext context) {
    List<String> boxValue = new ArrayList<>();
    boxValue.add(String.format("Union-%s", node.getPlanNodeId().getId()));
    boxValue.add(
        String.format(
            DataNodeQueryMessages.EXCEPTION_OUTPUTSYMBOLS_ARG_2CDA67EC, node.getOutputSymbols()));
    return render(node, boxValue, context);
  }

  @Override
  public List<String> visitIntersect(IntersectNode node, GraphContext context) {
    List<String> boxValue = new ArrayList<>();
    boxValue.add(String.format("Intersect-%s", node.getPlanNodeId().getId()));
    boxValue.add(
        String.format(
            DataNodeQueryMessages.EXCEPTION_OUTPUTSYMBOLS_ARG_2CDA67EC, node.getOutputSymbols()));
    boxValue.add(
        String.format(DataNodeQueryMessages.EXCEPTION_ISDISTINCT_ARG_8F38F5E3, node.isDistinct()));
    return render(node, boxValue, context);
  }

  @Override
  public List<String> visitExcept(ExceptNode node, GraphContext context) {
    List<String> boxValue = new ArrayList<>();
    boxValue.add(String.format("Except-%s", node.getPlanNodeId().getId()));
    boxValue.add(
        String.format(
            DataNodeQueryMessages.EXCEPTION_OUTPUTSYMBOLS_ARG_2CDA67EC, node.getOutputSymbols()));
    boxValue.add(
        String.format(DataNodeQueryMessages.EXCEPTION_ISDISTINCT_ARG_8F38F5E3, node.isDistinct()));
    return render(node, boxValue, context);
  }

  private List<String> render(PlanNode node, List<String> nodeBoxString, GraphContext context) {
    Box box = new Box(nodeBoxString);
    List<List<String>> children = new ArrayList<>();
    node.getChildren().forEach(child -> children.add(child.accept(this, context)));
    box.calculateBoxParams(children);

    box.lines.add(printBoxEdge(box, true));
    for (String valueLine : nodeBoxString) {
      StringBuilder line = new StringBuilder();
      for (int i = 0; i < box.lineWidth; i++) {
        if (i < box.startPosition) {
          line.append(INDENT);
          continue;
        }
        if (i > box.endPosition) {
          line.append(INDENT);
          continue;
        }
        if (i == box.startPosition || i == box.endPosition) {
          line.append(VERTICAL);
          continue;
        }
        if (i - box.startPosition - 1 < valueLine.length()) {
          line.append(valueLine.charAt(i - box.startPosition - 1));
        } else {
          line.append(INDENT);
        }
      }
      box.lines.add(line.toString());
    }
    box.lines.add(printBoxEdge(box, false));

    if (children.isEmpty()) {
      return box.lines;
    }

    addConnectionLine(box, children);

    for (int i = 0; i < getChildrenLineCount(children); i++) {
      StringBuilder line = new StringBuilder();
      for (int j = 0; j < children.size(); j++) {
        line.append(getLine(children, j, i, box.childExtraSpace));
        if (j != children.size() - 1) {
          for (int m = 0; m < BOX_MARGIN; m++) {
            line.append(INDENT);
          }
        }
      }
      box.lines.add(line.toString());
    }
    return box.lines;
  }

  private void addConnectionLine(Box box, List<List<String>> children) {
    // Print Connection Line
    if (children.size() == 1) {
      for (int i = 0; i < CONNECTION_LINE_HEIGHT; i++) {
        StringBuilder line = new StringBuilder();
        for (int j = 0; j < box.lineWidth; j++) {
          line.append(j == box.midPosition ? VERTICAL : INDENT);
        }
        box.lines.add(line.toString());
      }
    } else {
      Map<Integer, String> symbolMap = new HashMap<>();
      Map<Integer, Boolean> childMidPositionMap = new HashMap<>();
      symbolMap.put(box.midPosition, UP);
      for (int i = 0; i < children.size(); i++) {
        int childMidPosition = getChildMidPosition(children, i);
        childMidPositionMap.put(childMidPosition, true);
        if (childMidPosition == box.midPosition) {
          symbolMap.put(box.midPosition, CROSS);
          continue;
        }
        symbolMap.put(
            childMidPosition, i == 0 ? LEFT_TOP : i == children.size() - 1 ? RIGHT_TOP : DOWN);
      }
      StringBuilder line1 = new StringBuilder();
      for (int i = 0; i < box.lineWidth; i++) {
        if (i < getChildMidPosition(children, 0)
            || i > getChildMidPosition(children, children.size() - 1)) {
          line1.append(INDENT);
          continue;
        }
        line1.append(symbolMap.getOrDefault(i, HORIZONTAL));
      }
      box.lines.add(line1.toString());

      for (int row = 1; row < CONNECTION_LINE_HEIGHT; row++) {
        StringBuilder nextLine = new StringBuilder();
        for (int i = 0; i < box.lineWidth; i++) {
          nextLine.append(childMidPositionMap.containsKey(i) ? VERTICAL : INDENT);
        }
        box.lines.add(nextLine.toString());
      }
    }
  }

  private String printBoxEdge(Box box, boolean isTopEdge) {
    StringBuilder line = new StringBuilder();
    for (int i = 0; i < box.lineWidth; i++) {
      if (i < box.startPosition) {
        line.append(INDENT);
      } else if (i > box.endPosition) {
        line.append(INDENT);
      } else if (i == box.startPosition) {
        line.append(isTopEdge ? LEFT_TOP : LEFT_BOTTOM);
      } else if (i == box.endPosition) {
        line.append(isTopEdge ? RIGHT_TOP : RIGHT_BOTTOM);
      } else {
        line.append(HORIZONTAL);
      }
    }
    return line.toString();
  }

  private String getLine(List<List<String>> children, int child, int line, int extraSpace) {
    if (line < children.get(child).size()) {
      StringBuilder ret = new StringBuilder();
      for (int i = 0; i < extraSpace; i++) {
        ret.append(INDENT);
      }
      ret.append(children.get(child).get(line));
      for (int i = 0; i < extraSpace; i++) {
        ret.append(INDENT);
      }
      return ret.toString();
    }
    return genEmptyLine(children.get(child).get(0).length());
  }

  private String genEmptyLine(int lineWidth) {
    StringBuilder line = new StringBuilder();
    for (int i = 0; i < lineWidth; i++) {
      line.append(INDENT);
    }
    return line.toString();
  }

  private int getChildrenLineCount(List<List<String>> children) {
    int count = 0;
    for (List<String> child : children) {
      count = Math.max(count, child.size());
    }
    return count;
  }

  private static int getChildMidPosition(List<List<String>> children, int idx) {
    int left = 0;
    for (int i = 0; i < idx; i++) {
      left += children.get(i).get(0).length();
      left += BOX_MARGIN;
    }
    left += children.get(idx).get(0).length() / 2;
    return left;
  }

  private static class Box {
    private final List<String> boxString;
    private final int boxWidth;
    private int lineWidth;
    private final List<String> lines;
    private int startPosition;
    private int endPosition;
    private int midPosition;
    private int childExtraSpace;

    public Box(List<String> boxString) {
      this.boxString = boxString;
      this.boxWidth = getBoxWidth();
      this.lines = new ArrayList<>();
    }

    public int getBoxWidth() {
      int width = 0;
      for (String line : boxString) {
        width = Math.max(width, line.length());
      }
      return width + 2;
    }

    public void calculateBoxParams(List<List<String>> childBoxStrings) {
      int childrenWidth = 0;
      for (List<String> childBoxString : childBoxStrings) {
        Validate.isTrue(
            !childBoxString.isEmpty(),
            DataNodeQueryMessages.EXCEPTION_LINES_OF_BOX_STRING_SHOULD_BE_GREATER_THAN_0_5DB8C047);
        childrenWidth += childBoxString.get(0).length();
      }
      childrenWidth += childBoxStrings.size() > 1 ? (childBoxStrings.size() - 1) * BOX_MARGIN : 0;
      this.lineWidth = Math.max(this.boxWidth, childrenWidth);
      this.startPosition = (this.lineWidth - this.boxWidth) / 2;
      this.endPosition = this.startPosition + this.boxWidth - 1;
      this.midPosition = this.lineWidth / 2;

      // fix the situation that current line width is longer than width of children
      int extraSpace = this.lineWidth - childrenWidth;
      if (extraSpace > 0) {
        this.childExtraSpace = extraSpace / 2;
      }
    }
  }

  public static class GraphContext {
    private final TemplatedInfo templatedInfo;

    public GraphContext(TemplatedInfo templatedInfo) {
      this.templatedInfo = templatedInfo;
    }

    public TemplatedInfo getTemplatedInfo() {
      return templatedInfo;
    }
  }

  public static List<String> getGraph(PlanNode node) {
    return node.accept(new PlanGraphPrinter(), new PlanGraphPrinter.GraphContext(null));
  }

  public static void print(PlanNode node) {
    List<String> lines = getGraph(node);
    lines.forEach(System.out::println);
  }
}
