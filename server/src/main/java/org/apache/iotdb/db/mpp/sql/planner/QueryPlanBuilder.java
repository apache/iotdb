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
import org.apache.iotdb.db.mpp.sql.planner.plan.node.PlanNode;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.process.*;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.source.SeriesAggregateScanNode;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.source.SeriesScanNode;
import org.apache.iotdb.db.mpp.sql.statement.component.FilterNullComponent;
import org.apache.iotdb.db.mpp.sql.statement.component.GroupByLevelComponent;
import org.apache.iotdb.db.mpp.sql.statement.component.OrderBy;
import org.apache.iotdb.db.query.aggregation.AggregationType;
import org.apache.iotdb.db.query.expression.Expression;
import org.apache.iotdb.tsfile.read.expression.IExpression;

import java.util.*;
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
      List<String> outputColumnNames) {
    Map<String, List<PlanNode>> deviceNameToSourceNodesMap = new HashMap<>();

    for (Map.Entry<String, Set<PartialPath>> entry : deviceNameToPathsMap.entrySet()) {
      String deviceName = entry.getKey();
      for (PartialPath path : entry.getValue()) {
        deviceNameToSourceNodesMap
            .computeIfAbsent(deviceName, k -> new ArrayList<>())
            .add(new SeriesScanNode(context.getQueryId().genPlanNodeId(), path, scanOrder));
      }
    }

    if (isAlignByDevice) {
      convergeWithDeviceMerge(
          deviceNameToSourceNodesMap, scanOrder, queryFilter, outputColumnNames);
    } else {
      convergeWithTimeJoin(deviceNameToSourceNodesMap, scanOrder, queryFilter, outputColumnNames);
    }
  }

  public void planAggregationQuerySource(
      Map<String, Map<PartialPath, Set<AggregationType>>> deviceNameToAggregationsMap,
      OrderBy scanOrder,
      boolean isAlignByDevice,
      IExpression queryFilter,
      List<String> outputColumnNames) {
    Map<String, List<PlanNode>> deviceNameToSourceNodesMap = new HashMap<>();
    for (Map.Entry<String, Map<PartialPath, Set<AggregationType>>> entry :
        deviceNameToAggregationsMap.entrySet()) {
      String deviceName = entry.getKey();

      for (PartialPath path : entry.getValue().keySet()) {
        SeriesAggregateScanNode aggregateScanNode =
            new SeriesAggregateScanNode(
                context.getQueryId().genPlanNodeId(),
                path,
                new ArrayList<>(entry.getValue().get(path)),
                scanOrder);
        deviceNameToSourceNodesMap
            .computeIfAbsent(deviceName, k -> new ArrayList<>())
            .add(
                new AggregateNode(
                    context.getQueryId().genPlanNodeId(),
                    path,
                    new ArrayList<>(entry.getValue().get(path)),
                    Collections.singletonList(aggregateScanNode)));
      }
    }

    if (isAlignByDevice) {
      convergeWithDeviceMerge(
          deviceNameToSourceNodesMap, scanOrder, queryFilter, outputColumnNames);
    } else {
      convergeWithTimeJoin(deviceNameToSourceNodesMap, scanOrder, queryFilter, outputColumnNames);
    }
  }

  public void convergeWithTimeJoin(
      Map<String, List<PlanNode>> deviceNameToSourceNodesMap,
      OrderBy mergeOrder,
      IExpression queryFilter,
      List<String> outputColumnNames) {
    List<PlanNode> sourceNodes =
        deviceNameToSourceNodesMap.entrySet().stream()
            .flatMap(entry -> entry.getValue().stream())
            .collect(Collectors.toList());
    this.root = convergeWithTimeJoin(sourceNodes, mergeOrder, queryFilter, outputColumnNames);
  }

  public void convergeWithDeviceMerge(
      Map<String, List<PlanNode>> deviceNameToSourceNodesMap,
      OrderBy mergeOrder,
      IExpression queryFilter,
      List<String> outputColumnNames) {
    DeviceMergeNode deviceMergeNode =
        new DeviceMergeNode(context.getQueryId().genPlanNodeId(), mergeOrder);
    for (Map.Entry<String, List<PlanNode>> entry : deviceNameToSourceNodesMap.entrySet()) {
      String deviceName = entry.getKey();
      List<PlanNode> planNodes = new ArrayList<>(entry.getValue());
      deviceMergeNode.addChildDeviceNode(
          deviceName, convergeWithTimeJoin(planNodes, mergeOrder, queryFilter, outputColumnNames));
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

  public void planQueryFilter(IExpression queryFilter, List<String> outputColumnNames) {
    if (queryFilter == null) {
      return;
    }

    this.root =
        new FilterNode(
            context.getQueryId().genPlanNodeId(), this.getRoot(), queryFilter, outputColumnNames);
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
            groupByLevelComponent.getGroupedPathMap());
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
}
