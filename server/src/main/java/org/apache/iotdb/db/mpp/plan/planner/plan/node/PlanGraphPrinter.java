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

package org.apache.iotdb.db.mpp.plan.planner.plan.node;

import org.apache.iotdb.common.rpc.thrift.TRegionReplicaSet;
import org.apache.iotdb.commons.partition.DataPartition;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.mpp.plan.expression.Expression;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.process.AggregationNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.process.DeviceMergeNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.process.DeviceViewIntoNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.process.DeviceViewNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.process.ExchangeNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.process.FillNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.process.FilterNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.process.GroupByLevelNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.process.GroupByTagNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.process.HorizontallyConcatNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.process.IntoNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.process.LimitNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.process.MergeSortNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.process.OffsetNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.process.SingleDeviceViewNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.process.SlidingWindowAggregationNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.process.SortNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.process.TimeJoinNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.process.TransformNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.process.last.LastQueryCollectNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.process.last.LastQueryMergeNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.process.last.LastQueryNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.sink.IdentitySinkNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.sink.ShuffleSinkNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.source.AlignedLastQueryScanNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.source.AlignedSeriesAggregationScanNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.source.AlignedSeriesScanNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.source.LastQueryScanNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.source.SeriesAggregationScanNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.source.SeriesScanNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.parameter.AggregationDescriptor;
import org.apache.iotdb.db.mpp.plan.planner.plan.parameter.CrossSeriesAggregationDescriptor;
import org.apache.iotdb.db.mpp.plan.planner.plan.parameter.DeviceViewIntoPathDescriptor;
import org.apache.iotdb.db.mpp.plan.planner.plan.parameter.IntoPathDescriptor;
import org.apache.iotdb.tsfile.utils.Pair;

import org.apache.commons.lang3.Validate;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

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
    boxValue.add(String.format("TimeFilter: %s", node.getTimeFilter()));

    long limit = node.getLimit(), offset = node.getOffset();
    if (limit > 0) {
      boxValue.add(String.format("Limit: %s", limit));
    }
    if (offset > 0) {
      boxValue.add(String.format("Offset: %s", offset));
    }
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
            node.getAlignedPath().getDevice(), node.getAlignedPath().getMeasurementList()));
    boxValue.add(String.format("TimeFilter: %s", node.getTimeFilter()));

    long limit = node.getLimit(), offset = node.getOffset();
    if (limit > 0) {
      boxValue.add(String.format("Limit: %s", limit));
    }
    if (offset > 0) {
      boxValue.add(String.format("Offset: %s", offset));
    }
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
            node.getAlignedPath().getDevice(), node.getAlignedPath().getMeasurementList()));
    for (int i = 0; i < node.getAggregationDescriptorList().size(); i++) {
      AggregationDescriptor descriptor = node.getAggregationDescriptorList().get(i);
      boxValue.add(
          String.format(
              "Aggregator-%d: %s, %s", i, descriptor.getAggregationType(), descriptor.getStep()));
    }
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
  public List<String> visitMergeSort(MergeSortNode node, GraphContext context) {
    List<String> boxValue = new ArrayList<>();
    boxValue.add(String.format("MergeSort-%s", node.getPlanNodeId().getId()));
    boxValue.add(String.format("ChildrenCount: %d", node.getChildren().size()));
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
  public List<String> visitTimeJoin(TimeJoinNode node, GraphContext context) {
    List<String> boxValue = new ArrayList<>();
    boxValue.add(String.format("TimeJoin-%s", node.getPlanNodeId().getId()));
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
              targetDeviceToAlignedMap.get(sourceTargetPathPair.right.getDevice())
                  ? "[ALIGNED]"
                  : ""));
    }
  }

  @Override
  public List<String> visitLastQueryScan(LastQueryScanNode node, GraphContext context) {
    List<String> boxValue = new ArrayList<>();
    boxValue.add(String.format("LastQueryScan-%s", node.getPlanNodeId().getId()));
    boxValue.add(String.format("Series: %s", node.getSeriesPath()));
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
            node.getSeriesPath().getDevice(), node.getSeriesPath().getMeasurementList()));
    boxValue.add(printRegion(node.getRegionReplicaSet()));
    return render(node, boxValue, context);
  }

  @Override
  public List<String> visitLastQuery(LastQueryNode node, GraphContext context) {
    List<String> boxValue = new ArrayList<>();
    boxValue.add(String.format("LastQuery-%s", node.getPlanNodeId().getId()));
    boxValue.add(String.format("TimeFilter: %s", node.getTimeFilter()));
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
    for (PlanNode child : node.getChildren()) {
      children.add(child.accept(this, context));
    }
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

    for (int i = 0; i < getChildrenLineCount(children); i++) {
      StringBuilder line = new StringBuilder();
      for (int j = 0; j < children.size(); j++) {
        line.append(getLine(children, j, i));
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

  private String getLine(List<List<String>> children, int child, int line) {
    if (line < children.get(child).size()) {
      return children.get(child).get(line);
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
    private List<String> boxString;
    private int boxWidth;
    private int lineWidth;
    private List<String> lines;
    private int startPosition;
    private int endPosition;
    private int midPosition;

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

    public String getLine(int idx) {
      if (idx < lines.size()) {
        return lines.get(idx);
      }
      return genEmptyLine(lineWidth);
    }

    private String genEmptyLine(int lineWidth) {
      StringBuilder line = new StringBuilder();
      for (int i = 0; i < lineWidth; i++) {
        line.append(INDENT);
      }
      return line.toString();
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
    }
  }

  public static class GraphContext {}

  public static List<String> getGraph(PlanNode node) {
    return node.accept(new PlanGraphPrinter(), new PlanGraphPrinter.GraphContext());
  }

  public static void print(PlanNode node) {
    List<String> lines = getGraph(node);
    for (String line : lines) {
      System.out.println(line);
    }
  }
}
