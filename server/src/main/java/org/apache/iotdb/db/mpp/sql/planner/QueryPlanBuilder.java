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
import org.apache.iotdb.db.mpp.common.filter.QueryFilter;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.PlanNode;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.PlanNodeIdAllocator;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.process.*;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.source.SeriesAggregateScanNode;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.source.SeriesScanNode;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.source.SourceNode;
import org.apache.iotdb.db.mpp.sql.statement.component.FilterNullComponent;
import org.apache.iotdb.db.mpp.sql.statement.component.GroupByLevelComponent;
import org.apache.iotdb.db.mpp.sql.statement.component.OrderBy;
import org.apache.iotdb.db.query.aggregation.AggregationType;
import org.apache.iotdb.db.query.expression.Expression;

import java.util.*;
import java.util.stream.Collectors;

public class QueryPlanBuilder {

  private PlanNode root;

  public QueryPlanBuilder() {}

  public QueryPlanBuilder(PlanNode root) {
    this.root = root;
  }

  public PlanNode getRoot() {
    return root;
  }

  public void withNewRoot(PlanNode newRoot) {
    this.root = newRoot;
  }

  public void planRawDataQuerySource(
      Map<String, Set<PartialPath>> deviceNameToPathsMap,
      OrderBy scanOrder,
      boolean isAlignByDevice) {
    Map<String, List<SourceNode>> deviceNameToSourceNodesMap = new HashMap<>();

    for (Map.Entry<String, Set<PartialPath>> entry : deviceNameToPathsMap.entrySet()) {
      String deviceName = entry.getKey();
      for (PartialPath path : entry.getValue()) {
        deviceNameToSourceNodesMap
            .computeIfAbsent(deviceName, k -> new ArrayList<>())
            .add(new SeriesScanNode(PlanNodeIdAllocator.generateId(), path, scanOrder));
      }
    }

    if (isAlignByDevice) {
      DeviceMergeNode deviceMergeNode = new DeviceMergeNode(PlanNodeIdAllocator.generateId());
      for (Map.Entry<String, List<SourceNode>> entry : deviceNameToSourceNodesMap.entrySet()) {
        String deviceName = entry.getKey();
        List<PlanNode> planNodes = new ArrayList<>(entry.getValue());
        if (planNodes.size() == 1) {
          deviceMergeNode.addChildDeviceNode(deviceName, planNodes.get(0));
        } else {
          TimeJoinNode timeJoinNode =
              new TimeJoinNode(PlanNodeIdAllocator.generateId(), scanOrder, null, planNodes);
          deviceMergeNode.addChildDeviceNode(deviceName, timeJoinNode);
        }
      }
      this.withNewRoot(deviceMergeNode);
    } else {
      List<PlanNode> planNodes =
          deviceNameToSourceNodesMap.entrySet().stream()
              .flatMap(entry -> entry.getValue().stream())
              .collect(Collectors.toList());
      TimeJoinNode timeJoinNode =
          new TimeJoinNode(PlanNodeIdAllocator.generateId(), scanOrder, null, planNodes);
      this.withNewRoot(timeJoinNode);
    }
  }

  public void planAggregationQuerySource(
      Map<String, Map<PartialPath, Set<AggregationType>>> deviceNameToAggregationsMap,
      OrderBy scanOrder,
      boolean isAlignByDevice) {
    Map<String, List<PlanNode>> deviceNameToSourceNodesMap = new HashMap<>();
    for (Map.Entry<String, Map<PartialPath, Set<AggregationType>>> entry :
        deviceNameToAggregationsMap.entrySet()) {
      String deviceName = entry.getKey();
      for (PartialPath path : entry.getValue().keySet()) {
        List<SourceNode> childSourceNodeList = new ArrayList<>();
        for (AggregationType aggregationType : entry.getValue().get(path)) {
          childSourceNodeList.add(
              new SeriesAggregateScanNode(PlanNodeIdAllocator.generateId(), path, aggregationType));
        }
        //        deviceNameToSourceNodesMap
        //                .computeIfAbsent(deviceName, k -> new ArrayList<>())
        //                .add(new AggregateNode(PlanNodeIdAllocator.generateId(),
        // childSourceNodeList));
      }
    }
  }

  public void planQueryFilter(QueryFilter queryFilter, List<String> outputColumnNames) {
    if (queryFilter == null) {
      return;
    }

    this.withNewRoot(
        new FilterNode(
            PlanNodeIdAllocator.generateId(), this.getRoot(), queryFilter, outputColumnNames));
  }

  public void planGroupByLevel(GroupByLevelComponent groupByLevelComponent) {
    if (groupByLevelComponent == null) {
      return;
    }

    this.withNewRoot(
        new GroupByLevelNode(
            PlanNodeIdAllocator.generateId(),
            this.getRoot(),
            groupByLevelComponent.getLevels(),
            groupByLevelComponent.getGroupedPathMap()));
  }

  public void planFilterNull(FilterNullComponent filterNullComponent) {
    if (filterNullComponent == null) {
      return;
    }

    this.withNewRoot(
        new FilterNullNode(
            PlanNodeIdAllocator.generateId(),
            this.getRoot(),
            filterNullComponent.getWithoutPolicyType(),
            filterNullComponent.getWithoutNullColumns().stream()
                .map(Expression::getExpressionString)
                .collect(Collectors.toList())));
  }

  public void planSort(OrderBy resultOrder) {
    if (resultOrder == null || resultOrder == OrderBy.TIMESTAMP_ASC) {
      return;
    }

    this.withNewRoot(
        new SortNode(PlanNodeIdAllocator.generateId(), this.getRoot(), null, resultOrder));
  }

  public void planLimit(int rowLimit) {
    if (rowLimit == 0) {
      return;
    }

    this.withNewRoot(new LimitNode(PlanNodeIdAllocator.generateId(), rowLimit, this.getRoot()));
  }

  public void planOffset(int rowOffset) {
    if (rowOffset == 0) {
      return;
    }

    this.withNewRoot(new OffsetNode(PlanNodeIdAllocator.generateId(), this.getRoot(), rowOffset));
  }
}
