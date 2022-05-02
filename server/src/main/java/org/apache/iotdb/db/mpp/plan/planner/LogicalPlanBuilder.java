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

import org.apache.iotdb.db.metadata.path.MeasurementPath;
import org.apache.iotdb.db.metadata.path.PartialPath;
import org.apache.iotdb.db.mpp.common.MPPQueryContext;
import org.apache.iotdb.db.mpp.common.schematree.PathPatternTree;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.metedata.read.CountSchemaMergeNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.metedata.read.DevicesCountNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.metedata.read.DevicesSchemaScanNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.metedata.read.LevelTimeSeriesCountNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.metedata.read.SchemaFetchNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.metedata.read.SeriesSchemaMergeNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.metedata.read.TimeSeriesCountNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.metedata.read.TimeSeriesSchemaScanNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.process.DeviceViewNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.process.LimitNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.process.OffsetNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.process.TimeJoinNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.source.SeriesScanNode;
import org.apache.iotdb.db.mpp.plan.statement.component.OrderBy;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class LogicalPlanBuilder {

  private PlanNode root;

  private final MPPQueryContext context;

  public LogicalPlanBuilder(MPPQueryContext context) {
    this.context = context;
  }

  public PlanNode getRoot() {
    return root;
  }

  public LogicalPlanBuilder planRawDataQuerySource(
      Map<String, Set<PartialPath>> deviceNameToPathsMap,
      OrderBy scanOrder,
      boolean isAlignByDevice) {
    Map<String, List<PlanNode>> deviceNameToSourceNodesMap = new HashMap<>();

    for (Map.Entry<String, Set<PartialPath>> entry : deviceNameToPathsMap.entrySet()) {
      String deviceName = entry.getKey();
      Set<String> allSensors =
          entry.getValue().stream().map(PartialPath::getMeasurement).collect(Collectors.toSet());
      for (PartialPath path : entry.getValue()) {
        deviceNameToSourceNodesMap
            .computeIfAbsent(deviceName, k -> new ArrayList<>())
            .add(
                new SeriesScanNode(
                    context.getQueryId().genPlanNodeId(),
                    (MeasurementPath) path,
                    allSensors,
                    scanOrder));
      }
    }

    if (isAlignByDevice) {
      planDeviceMerge(deviceNameToSourceNodesMap, scanOrder);
    } else {
      planTimeJoin(deviceNameToSourceNodesMap, scanOrder);
    }

    return this;
  }

  public void planTimeJoin(
      Map<String, List<PlanNode>> deviceNameToSourceNodesMap, OrderBy mergeOrder) {
    List<PlanNode> sourceNodes =
        deviceNameToSourceNodesMap.entrySet().stream()
            .flatMap(entry -> entry.getValue().stream())
            .collect(Collectors.toList());
    this.root = convergeWithTimeJoin(sourceNodes, mergeOrder);
  }

  public void planDeviceMerge(
      Map<String, List<PlanNode>> deviceNameToSourceNodesMap, OrderBy mergeOrder) {
    List<String> measurements =
        deviceNameToSourceNodesMap.values().stream()
            .flatMap(List::stream)
            .map(node -> ((SeriesScanNode) node).getSeriesPath().getMeasurement())
            .distinct()
            .collect(Collectors.toList());
    DeviceViewNode deviceViewNode =
        new DeviceViewNode(
            context.getQueryId().genPlanNodeId(),
            Arrays.asList(OrderBy.DEVICE_ASC, mergeOrder),
            measurements);
    for (Map.Entry<String, List<PlanNode>> entry : deviceNameToSourceNodesMap.entrySet()) {
      String deviceName = entry.getKey();
      List<PlanNode> planNodes = new ArrayList<>(entry.getValue());
      deviceViewNode.addChildDeviceNode(deviceName, convergeWithTimeJoin(planNodes, mergeOrder));
    }
    this.root = deviceViewNode;
  }

  private PlanNode convergeWithTimeJoin(List<PlanNode> sourceNodes, OrderBy mergeOrder) {
    PlanNode tmpNode;
    if (sourceNodes.size() == 1) {
      tmpNode = sourceNodes.get(0);
    } else {
      tmpNode = new TimeJoinNode(context.getQueryId().genPlanNodeId(), mergeOrder, sourceNodes);
    }
    return tmpNode;
  }

  public LogicalPlanBuilder planLimit(int rowLimit) {
    if (rowLimit == 0) {
      return this;
    }

    this.root = new LimitNode(context.getQueryId().genPlanNodeId(), this.getRoot(), rowLimit);
    return this;
  }

  public LogicalPlanBuilder planOffset(int rowOffset) {
    if (rowOffset == 0) {
      return this;
    }

    this.root = new OffsetNode(context.getQueryId().genPlanNodeId(), this.getRoot(), rowOffset);
    return this;
  }

  /** Meta Query* */
  public LogicalPlanBuilder planTimeSeriesMetaSource(
      PartialPath pathPattern,
      String key,
      String value,
      int limit,
      int offset,
      boolean orderByHeat,
      boolean contains,
      boolean prefixPath) {
    TimeSeriesSchemaScanNode timeSeriesMetaScanNode =
        new TimeSeriesSchemaScanNode(
            context.getQueryId().genPlanNodeId(),
            pathPattern,
            key,
            value,
            limit,
            offset,
            orderByHeat,
            contains,
            prefixPath);
    this.root = timeSeriesMetaScanNode;
    return this;
  }

  public LogicalPlanBuilder planDeviceSchemaSource(
      PartialPath pathPattern, int limit, int offset, boolean prefixPath, boolean hasSgCol) {
    DevicesSchemaScanNode devicesSchemaScanNode =
        new DevicesSchemaScanNode(
            context.getQueryId().genPlanNodeId(), pathPattern, limit, offset, prefixPath, hasSgCol);
    this.root = devicesSchemaScanNode;
    return this;
  }

  public LogicalPlanBuilder planSchemaMerge(boolean orderByHeat) {
    SeriesSchemaMergeNode schemaMergeNode =
        new SeriesSchemaMergeNode(context.getQueryId().genPlanNodeId(), orderByHeat);
    schemaMergeNode.addChild(this.getRoot());
    this.root = schemaMergeNode;
    return this;
  }

  public LogicalPlanBuilder planSchemaFetchSource(PathPatternTree patternTree) {
    this.root = new SchemaFetchNode(context.getQueryId().genPlanNodeId(), patternTree);
    return this;
  }

  public LogicalPlanBuilder planCountMerge() {
    CountSchemaMergeNode countMergeNode =
        new CountSchemaMergeNode(context.getQueryId().genPlanNodeId());
    countMergeNode.addChild(this.getRoot());
    this.root = countMergeNode;
    return this;
  }

  public LogicalPlanBuilder planDevicesCountSource(PartialPath partialPath, boolean prefixPath) {
    this.root = new DevicesCountNode(context.getQueryId().genPlanNodeId(), partialPath, prefixPath);
    return this;
  }

  public LogicalPlanBuilder planTimeSeriesCountSource(PartialPath partialPath, boolean prefixPath) {
    this.root =
        new TimeSeriesCountNode(context.getQueryId().genPlanNodeId(), partialPath, prefixPath);
    return this;
  }

  public LogicalPlanBuilder planLevelTimeSeriesCountSource(
      PartialPath partialPath, boolean prefixPath, int level) {
    this.root =
        new LevelTimeSeriesCountNode(
            context.getQueryId().genPlanNodeId(), partialPath, prefixPath, level);
    return this;
  }
}
