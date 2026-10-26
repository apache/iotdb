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

package org.apache.iotdb.db.queryengine.plan.relational.planner;

import org.apache.iotdb.commons.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.node.AggregationNode;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.node.CollectNode;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.node.GroupNode;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.node.SortNode;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.node.WindowNode;

import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class CollectInputPropertyTest {
  private static List<PlanNode> plans(String sql) {
    PlanTester tester = new PlanTester();
    tester.createPlan(sql);
    List<PlanNode> result = new ArrayList<>();
    for (int i = 0; i < tester.getDistributedPlan().getFragments().size(); i++) {
      result.add(tester.getFragmentPlan(i));
    }
    return result;
  }

  private static <T> List<T> nodes(PlanNode node, Class<T> type) {
    List<T> result = new ArrayList<>();
    if (type.isInstance(node)) result.add(type.cast(node));
    for (PlanNode child : node.getChildren()) result.addAll(nodes(child, type));
    return result;
  }

  private static <T> List<T> nodes(List<PlanNode> plans, Class<T> type) {
    List<T> result = new ArrayList<>();
    for (PlanNode plan : plans) {
      result.addAll(nodes(plan, type));
    }
    return result;
  }

  private static List<PlanNode> allNodes(PlanNode node) {
    List<PlanNode> result = new ArrayList<>();
    result.add(node);
    for (PlanNode child : node.getChildren()) {
      result.addAll(allNodes(child));
    }
    return result;
  }

  @Test
  public void testAggregationAboveGlobalWindowHashesInterleavedGroups() {
    for (String aggregate : new String[] {"SUM(s1)", "COUNT(DISTINCT s1)"}) {
      List<PlanNode> fragments =
          plans(
              "SELECT tag1, tag2, tag3, "
                  + aggregate
                  + " FROM ("
                  + "SELECT tag1, tag2, tag3, s1, COUNT(*) OVER () AS n "
                  + "FROM table1 WHERE tag1 != 'shenzhen') t WHERE n > 0 GROUP BY tag1, tag2, tag3");
      PlanNode root = fragments.get(0);
      List<AggregationNode> aggregations = nodes(fragments, AggregationNode.class);
      assertFalse(aggregations.isEmpty());
      assertFalse(nodes(fragments, CollectNode.class).isEmpty());
      for (AggregationNode aggregation : aggregations) {
        if (!nodes(aggregation.getChild(), CollectNode.class).isEmpty()) {
          assertTrue(aggregation.getPreGroupedSymbols().isEmpty());
          assertFalse(aggregation.isStreamable());
        }
      }
      assertTrue(
          "Hash aggregation should not add a grouping sort", nodes(root, SortNode.class).isEmpty());
    }
  }

  @Test
  public void testPartialPreGroupingAlsoFallsBackToHash() {
    List<PlanNode> fragments =
        plans(
            "SELECT tag1, tag2, tag3, s1, COUNT(*) FROM ("
                + "SELECT tag1, tag2, tag3, s1, COUNT(*) OVER () AS n "
                + "FROM table1 WHERE tag1 != 'shenzhen') t WHERE n > 0 GROUP BY tag1, tag2, tag3, s1");
    List<AggregationNode> aggregations = nodes(fragments, AggregationNode.class);
    assertFalse(aggregations.isEmpty());
    assertFalse(nodes(fragments, CollectNode.class).isEmpty());
    assertTrue(aggregations.get(0).getPreGroupedSymbols().isEmpty());
  }

  @Test
  public void testNestedWindowRestoresPartitionContiguityWithSort() {
    List<PlanNode> fragments =
        plans(
            "SELECT tag1, tag2, tag3, SUM(s1) OVER ("
                + "PARTITION BY tag1, tag2, tag3 ORDER BY s1) AS running_sum "
                + "FROM (SELECT tag1, tag2, tag3, s1, COUNT(*) OVER () AS n FROM table1) t WHERE n > 0");
    PlanNode root = fragments.get(0);
    assertFalse(nodes(fragments, CollectNode.class).isEmpty());
    assertTrue(nodes(root, GroupNode.class).isEmpty());
    List<WindowNode> windows = nodes(root, WindowNode.class);
    assertEquals(2, windows.size());
    WindowNode outer =
        windows.stream()
            .filter(w -> !w.getSpecification().getPartitionBy().isEmpty())
            .findFirst()
            .get();
    assertFalse(nodes(outer.getChild(), SortNode.class).isEmpty());
  }

  @Test
  public void testEveryFragmentHasAtMostOneMultiChildLevel() {
    String[] queries = {
      "SELECT tag1, tag2, tag3, SUM(s1) FROM table1 GROUP BY tag1, tag2, tag3",
      "SELECT tag1, tag2, tag3, SUM(s1) FROM ("
          + "SELECT tag1, tag2, tag3, s1 FROM table1 UNION ALL "
          + "SELECT tag1, tag2, tag3, s1 FROM table2) u GROUP BY tag1, tag2, tag3",
      "SELECT tag1, tag2, tag3, SUM(s1) FROM ("
          + "SELECT tag1, tag2, tag3, s1, COUNT(*) OVER () AS n "
          + "FROM table1 WHERE tag1 != 'shenzhen') t WHERE n > 0 GROUP BY tag1, tag2, tag3"
    };
    for (String sql : queries) {
      for (PlanNode fragment : plans(sql)) {
        int multiChildLevels = 0;
        for (PlanNode node : allNodes(fragment)) {
          if (node.getChildren().size() > 1) {
            multiChildLevels++;
          }
        }
        assertTrue("Fragment contains more than one multi-child level", multiChildLevels <= 1);
      }
    }
  }

  @Test
  public void testOrderedScanBranchesKeepStreamingAggregation() {
    List<PlanNode> fragments =
        plans("SELECT tag1, tag2, tag3, SUM(s1 + 1) FROM table1 GROUP BY tag1, tag2, tag3");
    List<AggregationNode> aggregations = nodes(fragments, AggregationNode.class);
    assertFalse(aggregations.isEmpty());
    assertTrue(aggregations.stream().anyMatch(AggregationNode::isStreamable));
  }
}
