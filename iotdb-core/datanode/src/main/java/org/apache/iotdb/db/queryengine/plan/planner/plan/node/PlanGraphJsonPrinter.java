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

import org.apache.iotdb.db.queryengine.plan.relational.planner.node.AggregationNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.DeviceTableScanNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.ExchangeNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.ExplainAnalyzeNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.FilterNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.JoinNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.LimitNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.MergeSortNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.OffsetNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.OutputNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.ProjectNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.SortNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.TableScanNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.TreeDeviceViewScanNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.UnionNode;

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
    // JsonObject.isEmpty() is not available in all Gson versions
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
      properties.add("OutputColumns", toJsonArray(n.getOutputColumnNames()));
      properties.add("OutputSymbols", toJsonArray(n.getOutputSymbols()));
    } else if (node instanceof ExplainAnalyzeNode) {
      ExplainAnalyzeNode n = (ExplainAnalyzeNode) node;
      properties.add("ChildPermittedOutputs", toJsonArray(n.getChildPermittedOutputs()));
    } else if (node instanceof TableScanNode) {
      buildTableScanProperties(properties, (TableScanNode) node);
    } else if (node instanceof ExchangeNode) {
      // No extra properties needed
    } else if (node instanceof AggregationNode) {
      buildAggregationProperties(properties, (AggregationNode) node);
    } else if (node instanceof FilterNode) {
      FilterNode n = (FilterNode) node;
      properties.addProperty("Predicate", String.valueOf(n.getPredicate()));
    } else if (node instanceof ProjectNode) {
      ProjectNode n = (ProjectNode) node;
      properties.add("OutputSymbols", toJsonArray(n.getOutputSymbols()));
      properties.add("Expressions", toJsonArray(n.getAssignments().getMap().values()));
    } else if (node instanceof LimitNode) {
      LimitNode n = (LimitNode) node;
      properties.addProperty("Count", n.getCount());
    } else if (node instanceof OffsetNode) {
      OffsetNode n = (OffsetNode) node;
      properties.addProperty("Count", n.getCount());
    } else if (node instanceof SortNode) {
      SortNode n = (SortNode) node;
      properties.addProperty("OrderBy", String.valueOf(n.getOrderingScheme()));
    } else if (node instanceof MergeSortNode) {
      MergeSortNode n = (MergeSortNode) node;
      properties.addProperty("OrderBy", String.valueOf(n.getOrderingScheme()));
    } else if (node instanceof JoinNode) {
      JoinNode n = (JoinNode) node;
      properties.addProperty("JoinType", String.valueOf(n.getJoinType()));
      properties.add("Criteria", toJsonArray(n.getCriteria()));
      properties.add("OutputSymbols", toJsonArray(n.getOutputSymbols()));
    } else if (node instanceof UnionNode) {
      UnionNode n = (UnionNode) node;
      properties.add("OutputSymbols", toJsonArray(n.getOutputSymbols()));
    }

    return properties;
  }

  private static <T> JsonArray toJsonArray(java.util.Collection<T> items) {
    JsonArray array = new JsonArray();
    for (T item : items) {
      array.add(String.valueOf(item));
    }
    return array;
  }

  private static <T> JsonArray toJsonArray(List<T> items) {
    JsonArray array = new JsonArray();
    for (T item : items) {
      array.add(String.valueOf(item));
    }
    return array;
  }

  private static void buildTableScanProperties(JsonObject properties, TableScanNode node) {
    properties.addProperty("QualifiedTableName", node.getQualifiedObjectName().toString());
    properties.add("OutputSymbols", toJsonArray(node.getOutputSymbols()));

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

  private static void buildAggregationProperties(JsonObject properties, AggregationNode node) {
    properties.add("OutputSymbols", toJsonArray(node.getOutputSymbols()));

    JsonArray aggregators = new JsonArray();
    int i = 0;
    for (AggregationNode.Aggregation aggregation : node.getAggregations().values()) {
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

    properties.add("GroupingKeys", toJsonArray(node.getGroupingKeys()));
    if (node.isStreamable()) {
      properties.addProperty("Streamable", true);
      properties.add("PreGroupedSymbols", toJsonArray(node.getPreGroupedSymbols()));
    }
    properties.addProperty("Step", String.valueOf(node.getStep()));
  }

  public static List<String> getJsonLines(PlanNode node) {
    List<String> lines = new ArrayList<>();
    lines.add(toPrettyJson(node));
    return lines;
  }
}
