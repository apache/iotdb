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

package org.apache.iotdb.db.queryengine.plan.optimization;

import org.apache.iotdb.commons.path.AlignedPath;
import org.apache.iotdb.commons.path.MeasurementPath;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.queryengine.plan.expression.Expression;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.AggregationNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.ColumnInjectNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.DeviceViewNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.FillNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.FilterNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.IntoNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.LimitNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.OffsetNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.SlidingWindowAggregationNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.TransformNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.join.FullOuterTimeJoinNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.source.AlignedSeriesAggregationScanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.source.AlignedSeriesScanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.source.SeriesAggregationScanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.source.SeriesScanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.parameter.AggregationDescriptor;
import org.apache.iotdb.db.queryengine.plan.planner.plan.parameter.FillDescriptor;
import org.apache.iotdb.db.queryengine.plan.planner.plan.parameter.GroupByTimeParameter;
import org.apache.iotdb.db.queryengine.plan.planner.plan.parameter.IntoPathDescriptor;
import org.apache.iotdb.db.queryengine.plan.planner.plan.parameter.OrderByParameter;
import org.apache.iotdb.db.queryengine.plan.statement.component.FillPolicy;
import org.apache.iotdb.db.queryengine.plan.statement.component.OrderByKey;
import org.apache.iotdb.db.queryengine.plan.statement.component.Ordering;
import org.apache.iotdb.db.queryengine.plan.statement.component.SortItem;
import org.apache.iotdb.db.queryengine.plan.statement.literal.LongLiteral;
import org.apache.iotdb.db.utils.columngenerator.parameter.SlidingTimeColumnGeneratorParameter;

import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.iotdb.db.queryengine.common.header.ColumnHeaderConstant.DEVICE;

public class TestPlanBuilder {

  private PlanNode root;

  public TestPlanBuilder() {}

  public PlanNode getRoot() {
    return root;
  }

  public TestPlanBuilder scan(String id, PartialPath path) {
    this.root = new SeriesScanNode(new PlanNodeId(id), (MeasurementPath) path);
    return this;
  }

  public TestPlanBuilder scanAligned(String id, PartialPath path) {
    this.root = new AlignedSeriesScanNode(new PlanNodeId(id), (AlignedPath) path);
    return this;
  }

  public TestPlanBuilder scan(String id, PartialPath path, int limit, int offset) {
    SeriesScanNode node = new SeriesScanNode(new PlanNodeId(id), (MeasurementPath) path);
    node.setPushDownLimit(limit);
    node.setPushDownOffset(offset);
    this.root = node;
    return this;
  }

  public TestPlanBuilder scanAligned(String id, PartialPath path, int limit, int offset) {
    AlignedSeriesScanNode node = new AlignedSeriesScanNode(new PlanNodeId(id), (AlignedPath) path);
    node.setPushDownLimit(limit);
    node.setPushDownOffset(offset);
    this.root = node;
    return this;
  }

  public TestPlanBuilder aggregationScan(
      String id,
      PartialPath path,
      List<AggregationDescriptor> aggregationDescriptors,
      GroupByTimeParameter groupByTimeParameter,
      boolean outputEndTime) {
    SeriesAggregationScanNode aggregationScanNode =
        new SeriesAggregationScanNode(
            new PlanNodeId(id),
            (MeasurementPath) path,
            aggregationDescriptors,
            Ordering.ASC,
            groupByTimeParameter);
    aggregationScanNode.setOutputEndTime(outputEndTime);
    this.root = aggregationScanNode;
    return this;
  }

  public TestPlanBuilder alignedAggregationScan(
      String id,
      PartialPath path,
      List<AggregationDescriptor> aggregationDescriptors,
      GroupByTimeParameter groupByTimeParameter,
      boolean outputEndTime) {
    AlignedSeriesAggregationScanNode aggregationScanNode =
        new AlignedSeriesAggregationScanNode(
            new PlanNodeId(id),
            (AlignedPath) path,
            aggregationDescriptors,
            Ordering.ASC,
            groupByTimeParameter);
    aggregationScanNode.setOutputEndTime(outputEndTime);
    this.root = aggregationScanNode;
    return this;
  }

  public TestPlanBuilder rawDataAggregation(
      String id,
      List<AggregationDescriptor> aggregationDescriptors,
      GroupByTimeParameter groupByTimeParameter,
      boolean outputEndTime) {
    AggregationNode aggregationNode =
        new AggregationNode(
            new PlanNodeId(String.valueOf(id)),
            Collections.singletonList(getRoot()),
            aggregationDescriptors,
            groupByTimeParameter,
            Ordering.ASC);
    aggregationNode.setOutputEndTime(outputEndTime);
    this.root = aggregationNode;
    return this;
  }

  public TestPlanBuilder slidingWindow(
      String id,
      List<AggregationDescriptor> aggregationDescriptors,
      GroupByTimeParameter groupByTimeParameter,
      boolean outputEndTime) {
    SlidingWindowAggregationNode slidingWindowAggregationNode =
        new SlidingWindowAggregationNode(
            new PlanNodeId(id),
            getRoot(),
            aggregationDescriptors,
            groupByTimeParameter,
            Ordering.ASC);
    slidingWindowAggregationNode.setOutputEndTime(outputEndTime);
    this.root = slidingWindowAggregationNode;
    return this;
  }

  public TestPlanBuilder aggregationTimeJoin(
      int startId,
      List<PartialPath> paths,
      List<List<AggregationDescriptor>> aggregationDescriptorsList,
      GroupByTimeParameter groupByTimeParameter) {
    int planId = startId;

    List<PlanNode> seriesSourceNodes = new ArrayList<>();
    for (int i = 0; i < paths.size(); i++) {
      PartialPath path = paths.get(i);
      if (path instanceof MeasurementPath) {
        seriesSourceNodes.add(
            new SeriesAggregationScanNode(
                new PlanNodeId(String.valueOf(planId)),
                (MeasurementPath) paths.get(i),
                aggregationDescriptorsList.get(i),
                Ordering.ASC,
                groupByTimeParameter));
      } else {
        seriesSourceNodes.add(
            new AlignedSeriesAggregationScanNode(
                new PlanNodeId(String.valueOf(planId)),
                (AlignedPath) paths.get(i),
                aggregationDescriptorsList.get(i),
                Ordering.ASC,
                groupByTimeParameter));
      }
      planId++;
    }

    this.root =
        new FullOuterTimeJoinNode(
            new PlanNodeId(String.valueOf(planId)), Ordering.ASC, seriesSourceNodes);
    return this;
  }

  public TestPlanBuilder timeJoin(List<PartialPath> paths) {
    int planId = 0;

    List<PlanNode> seriesSourceNodes = new ArrayList<>();
    for (PartialPath path : paths) {
      seriesSourceNodes.add(
          new SeriesScanNode(new PlanNodeId(String.valueOf(planId)), (MeasurementPath) path));
      planId++;
    }
    this.root =
        new FullOuterTimeJoinNode(
            new PlanNodeId(String.valueOf(planId)), Ordering.ASC, seriesSourceNodes);
    return this;
  }

  public TestPlanBuilder transform(String id, List<Expression> expressions) {
    this.root =
        new TransformNode(
            new PlanNodeId(id),
            getRoot(),
            expressions.toArray(new Expression[0]),
            false,
            ZonedDateTime.now().getOffset(),
            Ordering.ASC);
    return this;
  }

  public TestPlanBuilder limit(String id, long limit) {
    this.root = new LimitNode(new PlanNodeId(id), getRoot(), limit);
    return this;
  }

  public TestPlanBuilder offset(String id, long offset) {
    this.root = new OffsetNode(new PlanNodeId(id), getRoot(), offset);
    return this;
  }

  public TestPlanBuilder fill(String id, FillPolicy fillPolicy) {
    this.root =
        new FillNode(
            new PlanNodeId(id),
            getRoot(),
            new FillDescriptor(fillPolicy, null, null),
            Ordering.ASC);
    return this;
  }

  public TestPlanBuilder fill(String id, String intValue) {
    this.root =
        new FillNode(
            new PlanNodeId(id),
            getRoot(),
            new FillDescriptor(FillPolicy.VALUE, new LongLiteral(intValue), null),
            Ordering.ASC);
    return this;
  }

  public TestPlanBuilder singleDeviceView(String id, String device, String measurement) {
    Map<String, List<Integer>> deviceToMeasurementIndexesMap = new HashMap<>();
    deviceToMeasurementIndexesMap.put(device, Collections.singletonList(1));
    DeviceViewNode deviceViewNode =
        new DeviceViewNode(
            new PlanNodeId(id),
            new OrderByParameter(
                Arrays.asList(
                    new SortItem(OrderByKey.DEVICE, Ordering.ASC),
                    new SortItem(OrderByKey.TIME, Ordering.ASC))),
            Arrays.asList(DEVICE, measurement),
            deviceToMeasurementIndexesMap);
    deviceViewNode.addChildDeviceNode(device, getRoot());
    this.root = deviceViewNode;
    return this;
  }

  public TestPlanBuilder filter(
      String id, List<Expression> expressions, Expression predicate, boolean isGroupByTime) {
    this.root =
        new FilterNode(
            new PlanNodeId(id),
            getRoot(),
            expressions.toArray(new Expression[0]),
            predicate,
            isGroupByTime,
            ZonedDateTime.now().getOffset(),
            Ordering.ASC);
    return this;
  }

  public TestPlanBuilder into(String id, PartialPath sourcePath, PartialPath intoPath) {
    IntoPathDescriptor intoPathDescriptor = new IntoPathDescriptor();
    intoPathDescriptor.specifyTargetPath(sourcePath.toString(), "", intoPath);
    intoPathDescriptor.specifyDeviceAlignment(intoPath.getDevice(), false);
    intoPathDescriptor.recordSourceColumnDataType(
        sourcePath.toString(), sourcePath.getSeriesType());
    this.root = new IntoNode(new PlanNodeId(id), getRoot(), intoPathDescriptor);
    return this;
  }

  public TestPlanBuilder columnInject(String id, GroupByTimeParameter groupByTimeParameter) {
    this.root =
        new ColumnInjectNode(
            new PlanNodeId(id),
            getRoot(),
            0,
            new SlidingTimeColumnGeneratorParameter(groupByTimeParameter, true));
    return this;
  }

  public TestPlanBuilder deviceView(
      String id,
      List<String> outputColumnNames,
      List<String> devices,
      Map<String, List<Integer>> deviceToMeasurementIndexesMap,
      List<PlanNode> children) {
    DeviceViewNode deviceViewNode =
        new DeviceViewNode(
            new PlanNodeId(id),
            new OrderByParameter(
                Arrays.asList(
                    new SortItem(OrderByKey.DEVICE, Ordering.ASC),
                    new SortItem(OrderByKey.TIME, Ordering.ASC))),
            outputColumnNames,
            devices,
            deviceToMeasurementIndexesMap);
    deviceViewNode.setChildren(children);
    this.root = deviceViewNode;
    return this;
  }
}
