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
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.ColumnInjectNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.DeviceViewNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.FillNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.FilterNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.IntoNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.LimitNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.OffsetNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.ProjectNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.RawDataAggregationNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.SlidingWindowAggregationNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.SortNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.TopKNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.TransformNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.join.FullOuterTimeJoinNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.join.InnerTimeJoinNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.join.LeftOuterTimeJoinNode;
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

import org.apache.tsfile.file.metadata.IDeviceID;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.iotdb.commons.schema.column.ColumnHeaderConstant.DEVICE;

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

  public TestPlanBuilder scan(String id, PartialPath path, long pushDownLimit) {
    SeriesScanNode node = new SeriesScanNode(new PlanNodeId(id), (MeasurementPath) path);
    node.setPushDownLimit(pushDownLimit);
    this.root = node;
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

  public TestPlanBuilder scan(String id, PartialPath path, Expression predicate) {
    SeriesScanNode node = new SeriesScanNode(new PlanNodeId(id), (MeasurementPath) path);
    node.setPushDownPredicate(predicate);
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

  public TestPlanBuilder scanAligned(String id, PartialPath path, Expression predicate) {
    AlignedSeriesScanNode node = new AlignedSeriesScanNode(new PlanNodeId(id), (AlignedPath) path);
    node.setPushDownPredicate(predicate);
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

  public TestPlanBuilder aggregationScan(
      String id,
      PartialPath path,
      List<AggregationDescriptor> aggregationDescriptors,
      GroupByTimeParameter groupByTimeParameter,
      boolean outputEndTime,
      Expression pushDownPredicate) {
    SeriesAggregationScanNode aggregationScanNode =
        new SeriesAggregationScanNode(
            new PlanNodeId(id),
            (MeasurementPath) path,
            aggregationDescriptors,
            Ordering.ASC,
            groupByTimeParameter);
    aggregationScanNode.setOutputEndTime(outputEndTime);
    aggregationScanNode.setPushDownPredicate(pushDownPredicate);
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

  public TestPlanBuilder alignedAggregationScan(
      String id,
      PartialPath path,
      List<AggregationDescriptor> aggregationDescriptors,
      GroupByTimeParameter groupByTimeParameter,
      boolean outputEndTime,
      Expression pushDownPredicate) {
    AlignedSeriesAggregationScanNode aggregationScanNode =
        new AlignedSeriesAggregationScanNode(
            new PlanNodeId(id),
            (AlignedPath) path,
            aggregationDescriptors,
            Ordering.ASC,
            groupByTimeParameter);
    aggregationScanNode.setOutputEndTime(outputEndTime);
    aggregationScanNode.setPushDownPredicate(pushDownPredicate);
    this.root = aggregationScanNode;
    return this;
  }

  public TestPlanBuilder rawDataAggregation(
      String id,
      List<AggregationDescriptor> aggregationDescriptors,
      GroupByTimeParameter groupByTimeParameter,
      boolean outputEndTime) {
    RawDataAggregationNode aggregationNode =
        new RawDataAggregationNode(
            new PlanNodeId(String.valueOf(id)),
            getRoot(),
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

  public TestPlanBuilder fullOuterTimeJoin(List<PartialPath> paths) {
    int planId = 0;
    List<PlanNode> seriesSourceNodes = new ArrayList<>();
    for (PartialPath path : paths) {
      if (path instanceof AlignedPath) {
        seriesSourceNodes.add(
            new AlignedSeriesScanNode(new PlanNodeId(String.valueOf(planId)), (AlignedPath) path));
      } else {
        seriesSourceNodes.add(
            new SeriesScanNode(new PlanNodeId(String.valueOf(planId)), (MeasurementPath) path));
      }
      planId++;
    }
    this.root =
        new FullOuterTimeJoinNode(
            new PlanNodeId(String.valueOf(planId)), Ordering.ASC, seriesSourceNodes);
    return this;
  }

  public TestPlanBuilder fullOuterTimeJoin(
      String rootId, Ordering mergeOrder, List<String> ids, List<PartialPath> paths) {
    List<PlanNode> seriesSourceNodes = new ArrayList<>();
    for (int i = 0; i < ids.size(); i++) {
      PartialPath path = paths.get(i);
      if (path instanceof AlignedPath) {
        seriesSourceNodes.add(
            new AlignedSeriesScanNode(new PlanNodeId(ids.get(i)), (AlignedPath) paths.get(i)));
      } else {
        seriesSourceNodes.add(
            new SeriesScanNode(new PlanNodeId(ids.get(i)), (MeasurementPath) paths.get(i)));
      }
    }
    this.root =
        new FullOuterTimeJoinNode(
            new PlanNodeId(String.valueOf(rootId)), mergeOrder, seriesSourceNodes);
    return this;
  }

  public TestPlanBuilder transform(String id, List<Expression> expressions) {
    this.root =
        new TransformNode(
            new PlanNodeId(id),
            getRoot(),
            expressions.toArray(new Expression[0]),
            false,
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
            new FillDescriptor(FillPolicy.CONSTANT, new LongLiteral(intValue), null),
            Ordering.ASC);
    return this;
  }

  public TestPlanBuilder singleDeviceView(String id, String device, String measurement) {
    IDeviceID deviceID = IDeviceID.Factory.DEFAULT_FACTORY.create(device);
    Map<IDeviceID, List<Integer>> deviceToMeasurementIndexesMap = new HashMap<>();
    deviceToMeasurementIndexesMap.put(deviceID, Collections.singletonList(1));
    DeviceViewNode deviceViewNode =
        new DeviceViewNode(
            new PlanNodeId(id),
            new OrderByParameter(
                Arrays.asList(
                    new SortItem(OrderByKey.DEVICE, Ordering.ASC),
                    new SortItem(OrderByKey.TIME, Ordering.ASC))),
            Arrays.asList(DEVICE, measurement),
            deviceToMeasurementIndexesMap);
    deviceViewNode.addChildDeviceNode(deviceID, getRoot());
    this.root = deviceViewNode;
    return this;
  }

  public TestPlanBuilder sort(String id, OrderByParameter orderParameter) {
    this.root = new SortNode(new PlanNodeId(id), getRoot(), orderParameter);
    return this;
  }

  public TestPlanBuilder topK(
      String id, int topKValue, OrderByParameter mergeOrderParameter, List<String> outputColumns) {
    this.root =
        new TopKNode(
            new PlanNodeId(id),
            topKValue,
            Collections.singletonList(getRoot()),
            mergeOrderParameter,
            outputColumns);
    return this;
  }

  public TestPlanBuilder singleOrderedDeviceView(
      String id, String device, OrderByParameter orderByParameter, String... measurement) {
    IDeviceID deviceID = IDeviceID.Factory.DEFAULT_FACTORY.create(device);
    Map<IDeviceID, List<Integer>> deviceToMeasurementIndexesMap = new HashMap<>();
    deviceToMeasurementIndexesMap.put(
        deviceID, measurement.length == 1 ? Collections.singletonList(1) : Arrays.asList(1, 2));
    DeviceViewNode deviceViewNode =
        new DeviceViewNode(
            new PlanNodeId(id),
            orderByParameter,
            measurement.length == 1
                ? Arrays.asList(DEVICE, measurement[0])
                : Arrays.asList(DEVICE, measurement[0], measurement[1]),
            deviceToMeasurementIndexesMap);
    deviceViewNode.addChildDeviceNode(deviceID, getRoot());
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
            Ordering.ASC,
            true);
    return this;
  }

  public TestPlanBuilder transform(String id, List<Expression> expressions, boolean isGroupByTime) {
    this.root =
        new TransformNode(
            new PlanNodeId(id),
            getRoot(),
            expressions.toArray(new Expression[0]),
            isGroupByTime,
            Ordering.ASC);
    return this;
  }

  public TestPlanBuilder into(String id, PartialPath sourcePath, PartialPath intoPath) {
    IntoPathDescriptor intoPathDescriptor = new IntoPathDescriptor();
    intoPathDescriptor.specifyTargetPath(sourcePath.toString(), "", intoPath);
    intoPathDescriptor.specifyDeviceAlignment(intoPath.getIDeviceID().toString(), false);
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
    Map<IDeviceID, List<Integer>> map = new HashMap<>();
    deviceToMeasurementIndexesMap.forEach(
        (key, value) -> map.put(IDeviceID.Factory.DEFAULT_FACTORY.create(key), value));
    DeviceViewNode deviceViewNode =
        new DeviceViewNode(
            new PlanNodeId(id),
            new OrderByParameter(
                Arrays.asList(
                    new SortItem(OrderByKey.DEVICE, Ordering.ASC),
                    new SortItem(OrderByKey.TIME, Ordering.ASC))),
            outputColumnNames,
            devices.stream()
                .map(IDeviceID.Factory.DEFAULT_FACTORY::create)
                .collect(Collectors.toList()),
            map);
    deviceViewNode.setChildren(children);
    this.root = deviceViewNode;
    return this;
  }

  public TestPlanBuilder leftOuterTimeJoin(
      String id, Ordering mergeOrder, PlanNode leftChild, PlanNode rightChild) {
    this.root = new LeftOuterTimeJoinNode(new PlanNodeId(id), mergeOrder, leftChild, rightChild);
    return this;
  }

  public TestPlanBuilder innerTimeJoin(
      String rootId,
      Ordering mergeOrder,
      List<String> ids,
      List<PartialPath> paths,
      List<Expression> predicates) {
    List<PlanNode> seriesSourceNodes = new ArrayList<>();
    for (int i = 0; i < ids.size(); i++) {
      SeriesScanNode seriesScanNode =
          new SeriesScanNode(
              new PlanNodeId(String.valueOf(ids.get(i))), (MeasurementPath) paths.get(i));
      seriesScanNode.setPushDownPredicate(predicates.get(i));
      seriesSourceNodes.add(seriesScanNode);
    }

    this.root =
        new InnerTimeJoinNode(
            new PlanNodeId(String.valueOf(rootId)), seriesSourceNodes, mergeOrder);
    return this;
  }

  public TestPlanBuilder project(String id, List<String> outputColumnNames) {
    this.root = new ProjectNode(new PlanNodeId(id), getRoot(), outputColumnNames);
    return this;
  }
}
