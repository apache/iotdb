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
import org.apache.iotdb.commons.partition.DataPartition;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.queryengine.plan.analyze.TemplatedInfo;
import org.apache.iotdb.db.queryengine.plan.expression.Expression;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.AggregationMergeSortNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.AggregationNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.ColumnInjectNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.DeviceMergeNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.DeviceViewIntoNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.DeviceViewNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.ExchangeNode;
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
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.source.AlignedLastQueryScanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.source.AlignedSeriesAggregationScanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.source.AlignedSeriesScanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.source.LastQueryScanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.source.SeriesAggregationScanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.source.SeriesScanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.parameter.AggregationDescriptor;
import org.apache.iotdb.db.queryengine.plan.planner.plan.parameter.CrossSeriesAggregationDescriptor;
import org.apache.iotdb.db.queryengine.plan.planner.plan.parameter.DeviceViewIntoPathDescriptor;
import org.apache.iotdb.db.queryengine.plan.planner.plan.parameter.IntoPathDescriptor;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.TableScanNode;

import org.apache.commons.lang3.Validate;
import org.apache.tsfile.utils.Pair;
import org.eclipse.jetty.util.StringUtil;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;

public class PlanGraphPrinter extends PlanVisitor<List<String>, PlanGraphPrinter.GraphContext> {

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
    boxValue.add(String.format("Series: %s", node.getSeriesPath()));

    long limit = node.getPushDownLimit();
    long offset = node.getPushDownOffset();
    if (limit > 0) {
      boxValue.add(String.format("Limit: %s", limit));
    }
    if (offset > 0) {
      boxValue.add(String.format("Offset: %s", offset));
    }

    Expression predicate = node.getPushDownPredicate();
    if (predicate != null) {
      boxValue.add(String.format("Predicate: %s", predicate));
    }

    boxValue.add(String.format("ScanOrder: %s", node.getScanOrder()));

    boxValue.add(printRegion(node.getRegionReplicaSet()));
    return render(node, boxValue, context);
  }

  @Override
  public List<String> visitAlignedSeriesScan(AlignedSeriesScanNode node, GraphContext context) {
    List<String> boxValue = new ArrayList<>();
    boxValue.add(String.format("AlignedSeriesScan-%s", node.getPlanNodeId().getId()));
    boxValue.add(
        String.format(
            "Series: %s%s",
            node.getAlignedPath().getIDeviceID(), node.getAlignedPath().getMeasurementList()));

    long limit = node.getPushDownLimit();
    long offset = node.getPushDownOffset();
    if (limit > 0) {
      boxValue.add(String.format("Limit: %s", limit));
    }
    if (offset > 0) {
      boxValue.add(String.format("Offset: %s", offset));
    }

    Expression predicate = node.getPushDownPredicate();
    if (predicate != null) {
      boxValue.add(String.format("Predicate: %s", predicate));
    }

    boxValue.add(String.format("QueryAllSensors: %s", node.isQueryAllSensors()));
    boxValue.add(String.format("ScanOrder: %s", node.getScanOrder()));
    boxValue.add(printRegion(node.getRegionReplicaSet()));
    return render(node, boxValue, context);
  }

  @Override
  public List<String> visitSeriesAggregationScan(
      SeriesAggregationScanNode node, GraphContext context) {
    List<String> boxValue = new ArrayList<>();
    boxValue.add(String.format("SeriesAggregationScan-%s", node.getPlanNodeId().getId()));
    boxValue.add(String.format("Series: %s", node.getSeriesPath()));
    for (int i = 0; i < node.getAggregationDescriptorList().size(); i++) {
      AggregationDescriptor descriptor = node.getAggregationDescriptorList().get(i);
      boxValue.add(
          String.format(
              "Aggregator-%d: %s, %s", i, descriptor.getAggregationType(), descriptor.getStep()));
    }
    Expression predicate = node.getPushDownPredicate();
    if (predicate != null) {
      boxValue.add(String.format("Predicate: %s", predicate));
    }
    boxValue.add(String.format("ScanOrder: %s", node.getScanOrder()));
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
            "Series: %s%s",
            node.getAlignedPath().getIDeviceID(), node.getAlignedPath().getMeasurementList()));
    for (int i = 0; i < node.getAggregationDescriptorList().size(); i++) {
      AggregationDescriptor descriptor = node.getAggregationDescriptorList().get(i);
      boxValue.add(
          String.format(
              "Aggregator-%d: %s, %s", i, descriptor.getAggregationType(), descriptor.getStep()));
    }
    Expression predicate = node.getPushDownPredicate();
    if (predicate != null) {
      boxValue.add(String.format("Predicate: %s", predicate));
    }
    boxValue.add(String.format("ScanOrder: %s", node.getScanOrder()));
    boxValue.add(printRegion(node.getRegionReplicaSet()));
    return render(node, boxValue, context);
  }

  @Override
  public List<String> visitSingleDeviceView(SingleDeviceViewNode node, GraphContext context) {
    List<String> boxValue = new ArrayList<>();
    boxValue.add(String.format("SingleDeviceView-%s", node.getPlanNodeId().getId()));
    boxValue.add(String.format("DeviceName: %s", node.getDevice()));
    return render(node, boxValue, context);
  }

  @Override
  public List<String> visitDeviceView(DeviceViewNode node, GraphContext context) {
    List<String> boxValue = new ArrayList<>();
    boxValue.add(String.format("DeviceView-%s", node.getPlanNodeId().getId()));
    boxValue.add(String.format("DeviceCount: %d", node.getDevices().size()));
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
    boxValue.add(String.format("ChildrenCount: %d", node.getChildren().size()));
    boxValue.add(node.getMergeOrderParameter().toString());
    return render(node, boxValue, context);
  }

  @Override
  public List<String> visitTopK(TopKNode node, GraphContext context) {
    List<String> boxValue = new ArrayList<>();
    boxValue.add(String.format("TopK-%s", node.getPlanNodeId().getId()));
    boxValue.add(String.format("LimitValue: %d", node.getTopValue()));
    boxValue.add(node.getMergeOrderParameter().toString());
    return render(node, boxValue, context);
  }

  @Override
  public List<String> visitDeviceMerge(DeviceMergeNode node, GraphContext context) {
    List<String> boxValue = new ArrayList<>();
    boxValue.add(String.format("DeviceMerge-%s", node.getPlanNodeId().getId()));
    boxValue.add(String.format("DeviceCount: %d", node.getDevices().size()));
    return render(node, boxValue, context);
  }

  @Override
  public List<String> visitFill(FillNode node, GraphContext context) {
    List<String> boxValue = new ArrayList<>();
    boxValue.add(String.format("Fill-%s", node.getPlanNodeId().getId()));
    boxValue.add(String.format("Policy: %s", node.getFillDescriptor().getFillPolicy()));
    if (node.getFillDescriptor().getTimeDurationThreshold() != null) {
      boxValue.add(
          String.format(
              "TimeDurationThreshold: %s", node.getFillDescriptor().getTimeDurationThreshold()));
    }
    return render(node, boxValue, context);
  }

  @Override
  public List<String> visitFilter(FilterNode node, GraphContext context) {
    List<String> boxValue = new ArrayList<>();
    boxValue.add(String.format("Filter-%s", node.getPlanNodeId().getId()));
    boxValue.add(String.format("Predicate: %s", node.getPredicate()));
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
              "Aggregator-%d: %s, %s", i, descriptor.getAggregationType(), descriptor.getStep()));
      boxValue.add(String.format("  Output: %s", descriptor.getOutputColumnNames()));
      boxValue.add(String.format("  Input: %s", descriptor.getInputExpressions()));
    }
    return render(node, boxValue, context);
  }

  @Override
  public List<String> visitGroupByTag(GroupByTagNode node, GraphContext context) {
    List<String> boxValue = new ArrayList<>();
    boxValue.add(String.format("GroupByTag-%s", node.getPlanNodeId().getId()));
    boxValue.add(String.format("Tag keys: %s", node.getTagKeys()));
    int bucketIdx = 0;
    for (Entry<List<String>, List<CrossSeriesAggregationDescriptor>> entry :
        node.getTagValuesToAggregationDescriptors().entrySet()) {
      boxValue.add(String.format("Bucket-%d: %s", bucketIdx, entry.getKey()));
      int aggregatorIdx = 0;
      for (CrossSeriesAggregationDescriptor descriptor : entry.getValue()) {
        if (descriptor == null) {
          boxValue.add(String.format("    Aggregator-%d: NULL", aggregatorIdx));
        } else {
          boxValue.add(
              String.format(
                  "    Aggregator-%d: %s, %s",
                  aggregatorIdx, descriptor.getAggregationType(), descriptor.getStep()));
          boxValue.add(String.format("      Output: %s", descriptor.getOutputColumnNames()));
          boxValue.add(String.format("      Input: %s", descriptor.getInputExpressions()));
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
              "Aggregator-%d: %s, %s", i, descriptor.getAggregationType(), descriptor.getStep()));
    }
    return render(node, boxValue, context);
  }

  @Override
  public List<String> visitOffset(OffsetNode node, GraphContext context) {
    List<String> boxValue = new ArrayList<>();
    boxValue.add(String.format("Offset-%s", node.getPlanNodeId().getId()));
    boxValue.add(String.format("value: %d", node.getOffset()));
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
              "Aggregator-%d: %s, %s", i, descriptor.getAggregationType(), descriptor.getStep()));
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
              "Aggregator-%d: %s, %s", i, descriptor.getAggregationType(), descriptor.getStep()));
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
  public List<String> visitExchange(ExchangeNode node, GraphContext context) {
    List<String> boxValue = new ArrayList<>();
    boxValue.add(String.format("Exchange-%s", node.getPlanNodeId().getId()));
    return render(node, boxValue, context);
  }

  @Override
  public List<String> visitLeftOuterTimeJoin(LeftOuterTimeJoinNode node, GraphContext context) {
    List<String> boxValue = new ArrayList<>();
    boxValue.add(String.format("LeftOuterTimeJoin-%s", node.getPlanNodeId().getId()));
    boxValue.add(String.format("Order: %s", node.getMergeOrder()));
    return render(node, boxValue, context);
  }

  @Override
  public List<String> visitInnerTimeJoin(InnerTimeJoinNode node, GraphContext context) {
    List<String> boxValue = new ArrayList<>();
    boxValue.add(String.format("InnerTimeJoin-%s", node.getPlanNodeId().getId()));
    boxValue.add(String.format("Order: %s", node.getMergeOrder()));
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
    boxValue.add(String.format("Order: %s", node.getMergeOrder()));
    return render(node, boxValue, context);
  }

  @Override
  public List<String> visitLimit(LimitNode node, GraphContext context) {
    List<String> boxValue = new ArrayList<>();
    boxValue.add(String.format("Limit-%s", node.getPlanNodeId().getId()));
    boxValue.add(String.format("Count: %d", node.getLimit()));
    return render(node, boxValue, context);
  }

  @Override
  public List<String> visitTransform(TransformNode node, GraphContext context) {
    List<String> boxValue = new ArrayList<>();
    boxValue.add(String.format("Transform-%s", node.getPlanNodeId().getId()));
    for (int i = 0; i < node.getOutputExpressions().length; i++) {
      Expression exp = node.getOutputExpressions()[i];
      boxValue.add(
          String.format("Exp-%d[%s]: %s", i, exp.getExpressionType(), exp.getExpressionString()));
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
      boxValue.add(String.format("OutputColumn-%d: %s", i, outputColumn));
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
    boxValue.add(String.format("Series: %s", node.getSeriesPath()));
    if (StringUtil.isNotBlank(node.getOutputViewPath())) {
      boxValue.add(String.format("ViewPath: %s", node.getOutputViewPath()));
    }
    boxValue.add(printRegion(node.getRegionReplicaSet()));
    return render(node, boxValue, context);
  }

  @Override
  public List<String> visitAlignedLastQueryScan(
      AlignedLastQueryScanNode node, GraphContext context) {
    List<String> boxValue = new ArrayList<>();
    boxValue.add(String.format("AlignedLastQueryScan-%s", node.getPlanNodeId().getId()));
    boxValue.add(
        String.format(
            "Series: %s%s",
            node.getSeriesPath().getIDeviceID(), node.getSeriesPath().getMeasurementList()));
    if (StringUtil.isNotBlank(node.getOutputViewPath())) {
      boxValue.add(String.format("ViewPath: %s", node.getOutputViewPath()));
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
    boxValue.add(String.format("ViewPath: %s", node.getViewPath()));
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
    boxValue.add(String.format("TargetIndex: %d", node.getTargetIndex()));
    boxValue.add(
        String.format(
            "ColumnGeneratorParameterType: %s",
            node.getColumnGeneratorParameter().getGeneratorType()));
    return render(node, boxValue, context);
  }

  // =============== Methods below are used for table model ================
  @Override
  public List<String> visitTableScan(TableScanNode node, GraphContext context) {
    List<String> boxValue = new ArrayList<>();
    boxValue.add(String.format("TableScan-%s", node.getPlanNodeId().getId()));
    boxValue.add(String.format("QualifiedTableName: %s", node.getQualifiedObjectName().toString()));
    boxValue.add(String.format("OutputSymbols: %s", node.getOutputSymbols()));
    boxValue.add(String.format("DeviceEntriesSize: %s", node.getDeviceEntries().size()));
    boxValue.add(String.format("ScanOrder: %s", node.getScanOrder()));
    if (node.getPushDownPredicate() != null) {
      boxValue.add(String.format("PushDownPredicate: %s", node.getPushDownPredicate()));
    }
    boxValue.add(String.format("PushDownOffset: %s", node.getPushDownOffset()));
    boxValue.add(String.format("PushDownLimit: %s", node.getPushDownLimit()));
    boxValue.add(String.format("PushDownLimitToEachDevice: %s", node.isPushLimitToEachDevice()));
    boxValue.add(String.format("RegionId: %s", node.getRegionReplicaSet().getRegionId().getId()));
    return render(node, boxValue, context);
  }

  @Override
  public List<String> visitAggregation(
      org.apache.iotdb.db.queryengine.plan.relational.planner.node.AggregationNode node,
      GraphContext context) {
    List<String> boxValue = new ArrayList<>();
    boxValue.add(String.format("Aggregation-%s", node.getPlanNodeId().getId()));
    boxValue.add(String.format("OutputSymbols: %s", node.getOutputSymbols()));
    if (node.isStreamable()) {
      boxValue.add("Streamable: true");
    }
    int i = 0;
    for (org.apache.iotdb.db.queryengine.plan.relational.planner.node.AggregationNode.Aggregation
        aggregation : node.getAggregations().values()) {
      boxValue.add(
          String.format("Aggregator-%d: %s", i++, aggregation.getResolvedFunction().toString()));
    }
    boxValue.add(String.format("GroupingKeys: %s", node.getGroupingKeys()));
    boxValue.add(String.format("Step: %s", node.getStep()));
    return render(node, boxValue, context);
  }

  @Override
  public List<String> visitAggregationTableScan(
      org.apache.iotdb.db.queryengine.plan.relational.planner.node.AggregationTableScanNode node,
      GraphContext context) {
    List<String> boxValue = new ArrayList<>();
    boxValue.add(String.format("AggregationTableScan-%s", node.getPlanNodeId().getId()));
    boxValue.add(String.format("QualifiedTableName: %s", node.getQualifiedObjectName().toString()));
    boxValue.add(String.format("OutputSymbols: %s", node.getOutputSymbols()));
    if (node.isStreamable()) {
      boxValue.add("Streamable: true");
    }
    int i = 0;
    for (org.apache.iotdb.db.queryengine.plan.relational.planner.node.AggregationNode.Aggregation
        aggregation : node.getAggregations().values()) {
      boxValue.add(
          String.format("Aggregator-%d: %s", i++, aggregation.getResolvedFunction().toString()));
    }
    boxValue.add(String.format("GroupingKeys: %s", node.getGroupingKeys()));
    boxValue.add(String.format("Step: %s", node.getStep()));

    if (node.getProjection() != null) {
      boxValue.add(
          String.format("Project-Expressions: %s", node.getProjection().getMap().values()));
    }

    boxValue.add(String.format("DeviceEntriesSize: %s", node.getDeviceEntries().size()));
    boxValue.add(String.format("ScanOrder: %s", node.getScanOrder()));
    if (node.getPushDownPredicate() != null) {
      boxValue.add(String.format("PushDownPredicate: %s", node.getPushDownPredicate()));
    }
    boxValue.add(String.format("PushDownOffset: %s", node.getPushDownOffset()));
    boxValue.add(String.format("PushDownLimit: %s", node.getPushDownLimit()));
    boxValue.add(String.format("PushDownLimitToEachDevice: %s", node.isPushLimitToEachDevice()));
    boxValue.add(String.format("RegionId: %s", node.getRegionReplicaSet().getRegionId().getId()));
    return render(node, boxValue, context);
  }

  @Override
  public List<String> visitFilter(
      org.apache.iotdb.db.queryengine.plan.relational.planner.node.FilterNode node,
      GraphContext context) {
    List<String> boxValue = new ArrayList<>();
    boxValue.add(String.format("Filter-%s", node.getPlanNodeId().getId()));
    boxValue.add(String.format("Predicate: %s", node.getPredicate()));
    return render(node, boxValue, context);
  }

  @Override
  public List<String> visitProject(
      org.apache.iotdb.db.queryengine.plan.relational.planner.node.ProjectNode node,
      GraphContext context) {
    List<String> boxValue = new ArrayList<>();
    boxValue.add(String.format("Project-%s", node.getPlanNodeId().getId()));
    boxValue.add(String.format("OutputSymbols: %s", node.getOutputSymbols()));
    boxValue.add(String.format("Expressions: %s", node.getAssignments().getMap().values()));
    return render(node, boxValue, context);
  }

  @Override
  public List<String> visitOutput(
      org.apache.iotdb.db.queryengine.plan.relational.planner.node.OutputNode node,
      GraphContext context) {
    List<String> boxValue = new ArrayList<>();
    boxValue.add(String.format("OutputNode-%s", node.getPlanNodeId().getId()));
    boxValue.add(String.format("OutputColumns-%s", node.getOutputColumnNames()));
    boxValue.add(String.format("OutputSymbols: %s", node.getOutputSymbols()));
    return render(node, boxValue, context);
  }

  @Override
  public List<String> visitLimit(
      org.apache.iotdb.db.queryengine.plan.relational.planner.node.LimitNode node,
      GraphContext context) {
    List<String> boxValue = new ArrayList<>();
    boxValue.add(String.format("Limit-%s", node.getPlanNodeId().getId()));
    boxValue.add(String.format("Count: %s", node.getCount()));
    return render(node, boxValue, context);
  }

  @Override
  public List<String> visitOffset(
      org.apache.iotdb.db.queryengine.plan.relational.planner.node.OffsetNode node,
      GraphContext context) {
    List<String> boxValue = new ArrayList<>();
    boxValue.add(String.format("Offset-%s", node.getPlanNodeId().getId()));
    boxValue.add(String.format("Count: %s", node.getCount()));
    return render(node, boxValue, context);
  }

  @Override
  public List<String> visitSort(
      org.apache.iotdb.db.queryengine.plan.relational.planner.node.SortNode node,
      GraphContext context) {
    List<String> boxValue = new ArrayList<>();
    boxValue.add(String.format("Sort-%s", node.getPlanNodeId().getId()));
    boxValue.add(String.format("OrderingScheme: %s", node.getOrderingScheme()));
    return render(node, boxValue, context);
  }

  @Override
  public List<String> visitStreamSort(
      org.apache.iotdb.db.queryengine.plan.relational.planner.node.StreamSortNode node,
      GraphContext context) {
    List<String> boxValue = new ArrayList<>();
    boxValue.add(String.format("StreamSort-%s", node.getPlanNodeId().getId()));
    boxValue.add(String.format("OrderingScheme: %s", node.getOrderingScheme()));
    boxValue.add(String.format("StreamCompareKeyEndIndex: %s", node.getStreamCompareKeyEndIndex()));
    boxValue.add(String.format("OrderByAllIdsAndTime: %s", node.isOrderByAllIdsAndTime()));
    return render(node, boxValue, context);
  }

  @Override
  public List<String> visitMergeSort(
      org.apache.iotdb.db.queryengine.plan.relational.planner.node.MergeSortNode node,
      GraphContext context) {
    List<String> boxValue = new ArrayList<>();
    boxValue.add(String.format("MergeSort-%s", node.getPlanNodeId().getId()));
    boxValue.add(String.format("OutputSymbols: %s", node.getOutputSymbols()));
    boxValue.add(String.format("OrderingScheme: %s", node.getOrderingScheme()));
    return render(node, boxValue, context);
  }

  @Override
  public List<String> visitCollect(
      org.apache.iotdb.db.queryengine.plan.relational.planner.node.CollectNode node,
      GraphContext context) {
    List<String> boxValue = new ArrayList<>();
    boxValue.add(String.format("Collect-%s", node.getPlanNodeId().getId()));
    boxValue.add(String.format("OutputSymbols: %s", node.getOutputSymbols()));
    return render(node, boxValue, context);
  }

  @Override
  public List<String> visitTopK(
      org.apache.iotdb.db.queryengine.plan.relational.planner.node.TopKNode node,
      GraphContext context) {
    List<String> boxValue = new ArrayList<>();
    boxValue.add(String.format("TopK-%s", node.getPlanNodeId().getId()));
    boxValue.add(String.format("OrderingScheme: %s", node.getOrderingScheme()));
    boxValue.add(String.format("Count: %s", node.getCount()));
    return render(node, boxValue, context);
  }

  @Override
  public List<String> visitJoin(
      org.apache.iotdb.db.queryengine.plan.relational.planner.node.JoinNode node,
      GraphContext context) {
    List<String> boxValue = new ArrayList<>();
    boxValue.add(String.format("Join-%s", node.getPlanNodeId().getId()));
    boxValue.add(String.format("JoinType: %s", node.getJoinType()));
    boxValue.add(String.format("JoinCriteria: %s", node.getCriteria()));
    boxValue.add(String.format("LeftOutputSymbols: %s", node.getLeftOutputSymbols()));
    boxValue.add(String.format("RightOutputSymbols: %s", node.getRightOutputSymbols()));
    if (node.getFilter().isPresent()) {
      boxValue.add(
          String.format("Filter: %s", node.getFilter().map(v -> v.toString()).orElse(null)));
    }
    return render(node, boxValue, context);
  }

  private String printRegion(TRegionReplicaSet regionReplicaSet) {
    return String.format(
        "Partition: %s",
        regionReplicaSet == null || regionReplicaSet == DataPartition.NOT_ASSIGNED
            ? "Not Assigned"
            : String.valueOf(regionReplicaSet.getRegionId().id));
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
        Validate.isTrue(!childBoxString.isEmpty(), "Lines of box string should be greater than 0");
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
