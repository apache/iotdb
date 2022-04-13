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
import org.apache.iotdb.db.mpp.sql.planner.plan.node.metedata.read.SchemaMergeNode;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.metedata.read.TimeSeriesSchemaScanNode;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.metedata.read.SchemaFetchNode;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.process.*;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.source.SeriesAggregateScanNode;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.source.SeriesScanNode;
import org.apache.iotdb.db.mpp.sql.statement.component.FilterNullComponent;
import org.apache.iotdb.db.mpp.sql.statement.component.GroupByLevelComponent;
import org.apache.iotdb.db.mpp.sql.statement.component.OrderBy;
import org.apache.iotdb.db.query.aggregation.AggregationType;
import org.apache.iotdb.db.query.expression.Expression;
import org.apache.iotdb.tsfile.read.expression.IExpression;
import org.apache.iotdb.tsfile.read.expression.impl.GlobalTimeExpression;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class QueryPlanBuilder {

  private PlanNode root;

  private final MPPQueryContext context;

  public QueryPlanBuilder(MPPQueryContext context) {
    this.context = context;
  }

  public PlanNode getRoot() {
    return root;
  }

  public void planRawDataQuerySource(
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
  }

  public void planAggregationSourceWithoutValueFilter(
      Map<String, Map<PartialPath, Set<AggregationType>>> deviceNameToAggregationsMap,
      OrderBy scanOrder,
      boolean isAlignByDevice,
      IExpression queryFilter) {
    Filter timeFilter = null;
    if (queryFilter != null) {
      timeFilter = ((GlobalTimeExpression) queryFilter).getFilter();
    }

    Map<String, List<PlanNode>> deviceNameToSourceNodesMap = new HashMap<>();
    for (Map.Entry<String, Map<PartialPath, Set<AggregationType>>> entry :
        deviceNameToAggregationsMap.entrySet()) {
      String deviceName = entry.getKey();

      for (PartialPath path : entry.getValue().keySet()) {
        deviceNameToSourceNodesMap
            .computeIfAbsent(deviceName, k -> new ArrayList<>())
            .add(
                new SeriesAggregateScanNode(
                    context.getQueryId().genPlanNodeId(),
                    path,
                    new ArrayList<>(entry.getValue().get(path)),
                    scanOrder,
                    timeFilter,
                    null));
      }
    }

    if (isAlignByDevice) {
      planDeviceMerge(deviceNameToSourceNodesMap, scanOrder, null, null);
    } else {
      planTimeJoin(deviceNameToSourceNodesMap, scanOrder, null, null);
    }
  }

  public void planAggregationSourceWithValueFilter(
      Map<String, Map<PartialPath, Set<AggregationType>>> deviceNameToAggregationsMap,
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
      planDeviceMergeForAggregation(
          deviceNameToAggregationsMap,
          deviceNameToSourceNodesMap,
          scanOrder,
          queryFilter,
          selectedPathList);
    } else {
      planTimeJoin(deviceNameToSourceNodesMap, scanOrder, queryFilter, selectedPathList);
      Map<PartialPath, Set<AggregationType>> aggregateFuncMap = new HashMap<>();
      deviceNameToAggregationsMap.values().forEach(aggregateFuncMap::putAll);
      this.root =
          new AggregateNode(
              context.getQueryId().genPlanNodeId(), this.getRoot(), aggregateFuncMap, null);
    }
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
    DeviceMergeNode deviceMergeNode =
        new DeviceMergeNode(context.getQueryId().genPlanNodeId(), mergeOrder);
    for (Map.Entry<String, List<PlanNode>> entry : deviceNameToSourceNodesMap.entrySet()) {
      String deviceName = entry.getKey();
      List<PlanNode> planNodes = new ArrayList<>(entry.getValue());
      deviceMergeNode.addChildDeviceNode(
          deviceName, convergeWithTimeJoin(planNodes, mergeOrder, queryFilter, selectedPathList));
    }
    this.root = deviceMergeNode;
  }

  private void planDeviceMergeForAggregation(
      Map<String, Map<PartialPath, Set<AggregationType>>> deviceNameToAggregationsMap,
      Map<String, List<PlanNode>> deviceNameToSourceNodesMap,
      OrderBy mergeOrder,
      IExpression queryFilter,
      List<String> selectedPathList) {
    DeviceMergeNode deviceMergeNode =
        new DeviceMergeNode(context.getQueryId().genPlanNodeId(), mergeOrder);
    for (Map.Entry<String, List<PlanNode>> entry : deviceNameToSourceNodesMap.entrySet()) {
      String deviceName = entry.getKey();
      List<PlanNode> planNodes = new ArrayList<>(entry.getValue());
      PlanNode timeJoinNode =
          convergeWithTimeJoin(planNodes, mergeOrder, queryFilter, selectedPathList);
      AggregateNode aggregateNode =
          new AggregateNode(
              context.getQueryId().genPlanNodeId(),
              timeJoinNode,
              deviceNameToAggregationsMap.get(deviceName),
              null);
      deviceMergeNode.addChildDeviceNode(deviceName, aggregateNode);
    }
    this.root = deviceMergeNode;
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

  public void planGroupByLevel(GroupByLevelComponent groupByLevelComponent) {
    if (groupByLevelComponent == null) {
      return;
    }

    this.root =
        new GroupByLevelNode(
            context.getQueryId().genPlanNodeId(),
            this.getRoot(),
            groupByLevelComponent.getLevels(),
            groupByLevelComponent.getGroupedHeaderMap());
  }

  public void planFilterNull(FilterNullComponent filterNullComponent) {
    if (filterNullComponent == null) {
      return;
    }

    this.root =
        new FilterNullNode(
            context.getQueryId().genPlanNodeId(),
            this.getRoot(),
            filterNullComponent.getWithoutPolicyType(),
            filterNullComponent.getWithoutNullColumns().stream()
                .map(Expression::getExpressionString)
                .collect(Collectors.toList()));
  }

  public void planLimit(int rowLimit) {
    if (rowLimit == 0) {
      return;
    }

    this.root = new LimitNode(context.getQueryId().genPlanNodeId(), this.getRoot(), rowLimit);
  }

  public void planOffset(int rowOffset) {
    if (rowOffset == 0) {
      return;
    }

    this.root = new OffsetNode(context.getQueryId().genPlanNodeId(), this.getRoot(), rowOffset);
  }

  /** Meta Query* */
  public void planTimeSeriesMetaSource(
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
  }

  public void planDeviceMetaSource(
      PartialPath pathPattern, int limit, int offset, boolean prefixPath, boolean hasSgCol) {
    DevicesSchemaScanNode devicesMetaScanNode =
        new DevicesSchemaScanNode(
            context.getQueryId().genPlanNodeId(), pathPattern, limit, offset, prefixPath, hasSgCol);
    this.root = devicesMetaScanNode;
  }

  public void planMetaMerge(boolean orderByHeat) {
    SchemaMergeNode metaMergeNode =
        new SchemaMergeNode(context.getQueryId().genPlanNodeId(), orderByHeat);
    metaMergeNode.addChild(this.getRoot());
    this.root = metaMergeNode;
  }

  public void planSchemaFetchSource(PathPatternTree patternTree) {
    this.root = new SchemaFetchNode(context.getQueryId().genPlanNodeId(), patternTree);
  }
}
