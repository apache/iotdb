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

import org.apache.iotdb.db.queryengine.plan.relational.planner.node.DeviceTableScanNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.ExchangeNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.ExplainAnalyzeNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.OutputNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.TableScanNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.TreeDeviceViewScanNode;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;

import java.util.ArrayList;
import java.util.List;

/**
 * Converts a plan tree into a JSON representation. Each plan node becomes a JSON object with: -
 * "name": the node type with its plan node ID - "id": the plan node ID - "properties": key-value
 * pairs - "children": array of child nodes
 */
public class PlanGraphJsonPrinter {

  private static final Gson GSON = new GsonBuilder().setPrettyPrinting().create();

  private static final String REGION_NOT_ASSIGNED = "Not Assigned";

  public static String toPrettyJson(PlanNode node) {
    JsonObject root = buildJsonNode(node);
    return GSON.toJson(root);
  }

  private static JsonObject buildJsonNode(PlanNode node) {
    JsonObject jsonNode = new JsonObject();
    String simpleName = node.getClass().getSimpleName();
    String nodeId = node.getPlanNodeId().getId();

    jsonNode.addProperty("name", simpleName + "-" + nodeId);
    jsonNode.addProperty("id", nodeId);

    JsonObject properties = buildProperties(node);
    if (properties.size() > 0) {
      jsonNode.add("properties", properties);
    }

    List<PlanNode> children = node.getChildren();
    if (children != null && !children.isEmpty()) {
      JsonArray childrenArray = new JsonArray();
      for (PlanNode child : children) {
        childrenArray.add(buildJsonNode(child));
      }
      jsonNode.add("children", childrenArray);
    }

    return jsonNode;
  }

  private static JsonObject buildProperties(PlanNode node) {
    JsonObject properties = new JsonObject();

    if (node instanceof OutputNode) {
      OutputNode n = (OutputNode) node;
      properties.addProperty("OutputColumns", String.valueOf(n.getOutputColumnNames()));
      properties.addProperty("OutputSymbols", String.valueOf(n.getOutputSymbols()));
    } else if (node instanceof ExplainAnalyzeNode) {
      ExplainAnalyzeNode n = (ExplainAnalyzeNode) node;
      properties.addProperty("ChildPermittedOutputs", String.valueOf(n.getChildPermittedOutputs()));
    } else if (node instanceof TableScanNode) {
      buildTableScanProperties(properties, (TableScanNode) node);
    } else if (node instanceof ExchangeNode) {
      // No extra properties needed
    } else if (node
        instanceof org.apache.iotdb.db.queryengine.plan.relational.planner.node.AggregationNode) {
      buildAggregationProperties(
          properties,
          (org.apache.iotdb.db.queryengine.plan.relational.planner.node.AggregationNode) node);
    } else if (node
        instanceof org.apache.iotdb.db.queryengine.plan.relational.planner.node.FilterNode) {
      org.apache.iotdb.db.queryengine.plan.relational.planner.node.FilterNode n =
          (org.apache.iotdb.db.queryengine.plan.relational.planner.node.FilterNode) node;
      properties.addProperty("Predicate", String.valueOf(n.getPredicate()));
    } else if (node
        instanceof org.apache.iotdb.db.queryengine.plan.relational.planner.node.ProjectNode) {
      org.apache.iotdb.db.queryengine.plan.relational.planner.node.ProjectNode n =
          (org.apache.iotdb.db.queryengine.plan.relational.planner.node.ProjectNode) node;
      properties.addProperty("OutputSymbols", String.valueOf(n.getOutputSymbols()));
      properties.addProperty("Expressions", String.valueOf(n.getAssignments().getMap().values()));
    } else if (node
        instanceof org.apache.iotdb.db.queryengine.plan.relational.planner.node.LimitNode) {
      org.apache.iotdb.db.queryengine.plan.relational.planner.node.LimitNode n =
          (org.apache.iotdb.db.queryengine.plan.relational.planner.node.LimitNode) node;
      properties.addProperty("Count", String.valueOf(n.getCount()));
    } else if (node
        instanceof org.apache.iotdb.db.queryengine.plan.relational.planner.node.OffsetNode) {
      org.apache.iotdb.db.queryengine.plan.relational.planner.node.OffsetNode n =
          (org.apache.iotdb.db.queryengine.plan.relational.planner.node.OffsetNode) node;
      properties.addProperty("Count", String.valueOf(n.getCount()));
    } else if (node
        instanceof org.apache.iotdb.db.queryengine.plan.relational.planner.node.SortNode) {
      org.apache.iotdb.db.queryengine.plan.relational.planner.node.SortNode n =
          (org.apache.iotdb.db.queryengine.plan.relational.planner.node.SortNode) node;
      properties.addProperty("OrderBy", String.valueOf(n.getOrderingScheme()));
    } else if (node
        instanceof org.apache.iotdb.db.queryengine.plan.relational.planner.node.MergeSortNode) {
      org.apache.iotdb.db.queryengine.plan.relational.planner.node.MergeSortNode n =
          (org.apache.iotdb.db.queryengine.plan.relational.planner.node.MergeSortNode) node;
      properties.addProperty("OrderBy", String.valueOf(n.getOrderingScheme()));
    } else if (node
        instanceof org.apache.iotdb.db.queryengine.plan.relational.planner.node.JoinNode) {
      org.apache.iotdb.db.queryengine.plan.relational.planner.node.JoinNode n =
          (org.apache.iotdb.db.queryengine.plan.relational.planner.node.JoinNode) node;
      properties.addProperty("JoinType", String.valueOf(n.getJoinType()));
      properties.addProperty("Criteria", String.valueOf(n.getCriteria()));
      properties.addProperty("OutputSymbols", String.valueOf(n.getOutputSymbols()));
    } else if (node
        instanceof org.apache.iotdb.db.queryengine.plan.relational.planner.node.UnionNode) {
      org.apache.iotdb.db.queryengine.plan.relational.planner.node.UnionNode n =
          (org.apache.iotdb.db.queryengine.plan.relational.planner.node.UnionNode) node;
      properties.addProperty("OutputSymbols", String.valueOf(n.getOutputSymbols()));
    }

    return properties;
  }

  private static void buildTableScanProperties(JsonObject properties, TableScanNode node) {
    properties.addProperty("QualifiedTableName", node.getQualifiedObjectName().toString());
    properties.addProperty("OutputSymbols", String.valueOf(node.getOutputSymbols()));

    if (node instanceof DeviceTableScanNode) {
      DeviceTableScanNode deviceNode = (DeviceTableScanNode) node;
      properties.addProperty("DeviceNumber", String.valueOf(deviceNode.getDeviceEntries().size()));
      properties.addProperty("ScanOrder", String.valueOf(deviceNode.getScanOrder()));
      if (deviceNode.getTimePredicate().isPresent()) {
        properties.addProperty(
            "TimePredicate", String.valueOf(deviceNode.getTimePredicate().get()));
      }
    }

    if (node.getPushDownPredicate() != null) {
      properties.addProperty("PushDownPredicate", String.valueOf(node.getPushDownPredicate()));
    }
    properties.addProperty("PushDownOffset", String.valueOf(node.getPushDownOffset()));
    properties.addProperty("PushDownLimit", String.valueOf(node.getPushDownLimit()));

    if (node instanceof DeviceTableScanNode) {
      properties.addProperty(
          "PushDownLimitToEachDevice",
          String.valueOf(((DeviceTableScanNode) node).isPushLimitToEachDevice()));
    }

    properties.addProperty(
        "RegionId",
        node.getRegionReplicaSet() == null || node.getRegionReplicaSet().getRegionId() == null
            ? REGION_NOT_ASSIGNED
            : String.valueOf(node.getRegionReplicaSet().getRegionId().getId()));

    if (node instanceof TreeDeviceViewScanNode) {
      TreeDeviceViewScanNode treeNode = (TreeDeviceViewScanNode) node;
      properties.addProperty("TreeDB", treeNode.getTreeDBName());
      properties.addProperty(
          "MeasurementToColumnName", String.valueOf(treeNode.getMeasurementColumnNameMap()));
    }
  }

  private static void buildAggregationProperties(
      JsonObject properties,
      org.apache.iotdb.db.queryengine.plan.relational.planner.node.AggregationNode node) {
    properties.addProperty("OutputSymbols", String.valueOf(node.getOutputSymbols()));

    JsonArray aggregators = new JsonArray();
    int i = 0;
    for (org.apache.iotdb.db.queryengine.plan.relational.planner.node.AggregationNode.Aggregation
        aggregation : node.getAggregations().values()) {
      JsonObject agg = new JsonObject();
      agg.addProperty("index", i++);
      agg.addProperty("function", aggregation.getResolvedFunction().toString());
      if (aggregation.hasMask()) {
        agg.addProperty("mask", String.valueOf(aggregation.getMask().get()));
      }
      if (aggregation.isDistinct()) {
        agg.addProperty("distinct", true);
      }
      aggregators.add(agg);
    }
    properties.add("Aggregators", aggregators);

    properties.addProperty("GroupingKeys", String.valueOf(node.getGroupingKeys()));
    if (node.isStreamable()) {
      properties.addProperty("Streamable", true);
      properties.addProperty("PreGroupedSymbols", String.valueOf(node.getPreGroupedSymbols()));
    }
    properties.addProperty("Step", String.valueOf(node.getStep()));
  }

  public static List<String> getJsonLines(PlanNode node) {
    List<String> lines = new ArrayList<>();
    lines.add(toPrettyJson(node));
    return lines;
  }
}
