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

package org.apache.iotdb.db.mpp.sql.planner;

import org.apache.iotdb.db.metadata.path.PartialPath;
import org.apache.iotdb.db.mpp.common.MPPQueryContext;
import org.apache.iotdb.db.mpp.common.schematree.PathPatternTree;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.PlanNode;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.metedata.read.DevicesSchemaScanNode;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.metedata.read.SchemaFetchNode;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.metedata.read.SchemaMergeNode;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.metedata.read.TimeSeriesSchemaScanNode;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.process.DeviceViewNode;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.process.FilterNode;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.process.LimitNode;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.process.OffsetNode;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.process.TimeJoinNode;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.source.SeriesScanNode;
import org.apache.iotdb.db.mpp.sql.statement.component.OrderBy;
import org.apache.iotdb.tsfile.read.expression.IExpression;

import java.util.ArrayList;
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
      boolean isAlignByDevice,
      IExpression queryFilter,
      List<String> selectedPathList) {
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
                    context.getQueryId().genPlanNodeId(), path, allSensors, scanOrder));
      }
    }

    if (isAlignByDevice) {
      planDeviceMerge(deviceNameToSourceNodesMap, scanOrder, queryFilter, selectedPathList);
    } else {
      planTimeJoin(deviceNameToSourceNodesMap, scanOrder, queryFilter, selectedPathList);
    }

    return this;
  }

  public void planTimeJoin(
      Map<String, List<PlanNode>> deviceNameToSourceNodesMap,
      OrderBy mergeOrder,
      IExpression queryFilter,
      List<String> selectedPathList) {
    List<PlanNode> sourceNodes =
        deviceNameToSourceNodesMap.entrySet().stream()
            .flatMap(entry -> entry.getValue().stream())
            .collect(Collectors.toList());
    this.root = convergeWithTimeJoin(sourceNodes, mergeOrder, queryFilter, selectedPathList);
  }

  public void planDeviceMerge(
      Map<String, List<PlanNode>> deviceNameToSourceNodesMap,
      OrderBy mergeOrder,
      IExpression queryFilter,
      List<String> selectedPathList) {
    DeviceViewNode deviceViewNode =
        new DeviceViewNode(context.getQueryId().genPlanNodeId(), mergeOrder);
    for (Map.Entry<String, List<PlanNode>> entry : deviceNameToSourceNodesMap.entrySet()) {
      String deviceName = entry.getKey();
      List<PlanNode> planNodes = new ArrayList<>(entry.getValue());
      deviceViewNode.addChildDeviceNode(
          deviceName, convergeWithTimeJoin(planNodes, mergeOrder, queryFilter, selectedPathList));
    }
    this.root = deviceViewNode;
  }

  private PlanNode convergeWithTimeJoin(
      List<PlanNode> sourceNodes,
      OrderBy mergeOrder,
      IExpression queryFilter,
      List<String> outputColumnNames) {
    PlanNode tmpNode;
    if (sourceNodes.size() == 1) {
      tmpNode = sourceNodes.get(0);
    } else {
      tmpNode = new TimeJoinNode(context.getQueryId().genPlanNodeId(), mergeOrder, sourceNodes);
    }

    if (queryFilter != null) {
      tmpNode =
          new FilterNode(
              context.getQueryId().genPlanNodeId(), tmpNode, queryFilter, outputColumnNames);
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
    SchemaMergeNode schemaMergeNode =
        new SchemaMergeNode(context.getQueryId().genPlanNodeId(), orderByHeat);
    schemaMergeNode.addChild(this.getRoot());
    this.root = schemaMergeNode;
    return this;
  }

  public LogicalPlanBuilder planSchemaFetchSource(PathPatternTree patternTree) {
    this.root = new SchemaFetchNode(context.getQueryId().genPlanNodeId(), patternTree);
    return this;
  }
}
