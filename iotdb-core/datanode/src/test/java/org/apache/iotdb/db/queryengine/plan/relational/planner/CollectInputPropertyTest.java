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
import org.apache.iotdb.commons.queryengine.plan.relational.planner.node.StreamSortNode;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.node.TopKNode;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.node.WindowNode;

import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class CollectInputPropertyTest {
  private static PlanNode plan(String sql) {
    PlanTester tester = new PlanTester();
    tester.createPlan(sql);
    return tester.getFragmentPlan(0);
  }

  private static <T> List<T> nodes(PlanNode node, Class<T> type) {
    List<T> result = new ArrayList<>();
    if (type.isInstance(node)) result.add(type.cast(node));
    for (PlanNode child : node.getChildren()) result.addAll(nodes(child, type));
    return result;
  }

  @Test
  public void testAggregationAboveGlobalWindowHashesInterleavedGroups() {
    for (String aggregate : new String[] {"SUM(s1)", "COUNT(DISTINCT s1)"}) {
      PlanNode root =
          plan(
              "SELECT tag1, tag2, tag3, "
                  + aggregate
                  + " FROM ("
                  + "SELECT tag1, tag2, tag3, s1, COUNT(*) OVER () AS n "
                  + "FROM table1 WHERE tag1 != 'shenzhen') t WHERE n > 0 GROUP BY tag1, tag2, tag3");
      List<AggregationNode> aggregations = nodes(root, AggregationNode.class);
      assertFalse(aggregations.isEmpty());
      assertFalse(nodes(root, CollectNode.class).isEmpty());
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
    PlanNode root =
        plan(
            "SELECT tag1, tag2, tag3, s1, COUNT(*) FROM ("
                + "SELECT tag1, tag2, tag3, s1, COUNT(*) OVER () AS n "
                + "FROM table1 WHERE tag1 != 'shenzhen') t WHERE n > 0 GROUP BY tag1, tag2, tag3, s1");
    List<AggregationNode> aggregations = nodes(root, AggregationNode.class);
    assertFalse(aggregations.isEmpty());
    assertFalse(nodes(root, CollectNode.class).isEmpty());
    assertTrue(aggregations.get(0).getPreGroupedSymbols().isEmpty());
  }

  @Test
  public void testNestedWindowRestoresPartitionContiguityWithSort() {
    PlanNode root =
        plan(
            "SELECT tag1, tag2, tag3, SUM(s1) OVER ("
                + "PARTITION BY tag1, tag2, tag3 ORDER BY s1) AS running_sum "
                + "FROM (SELECT tag1, tag2, tag3, s1, COUNT(*) OVER () AS n FROM table1) t WHERE n > 0");
    assertFalse(nodes(root, CollectNode.class).isEmpty());
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
  public void testStreamSortAfterGlobalWindowUsesFullSort() {
    PlanNode root =
        plan(
            "SELECT tag1, tag2, tag3, s1, COUNT(*) OVER () AS n "
                + "FROM table1 ORDER BY tag1, tag2, tag3, s1");
    assertFalse(nodes(root, CollectNode.class).isEmpty());
    assertFalse(nodes(root, SortNode.class).isEmpty());
    assertTrue(nodes(root, StreamSortNode.class).isEmpty());
  }

  @Test
  public void testConstantFillDoesNotRemoveRestoredSort() {
    PlanNode root =
        plan(
            "SELECT tag1, tag2, tag3, s1, COUNT(*) OVER () AS n FROM table1 "
                + "FILL METHOD CONSTANT 0 ORDER BY tag1, tag2, tag3");
    assertFalse(nodes(root, CollectNode.class).isEmpty());
    assertFalse(nodes(root, SortNode.class).isEmpty());
    assertTrue(nodes(root, StreamSortNode.class).isEmpty());
  }

  @Test
  public void testUnionFinalAggregationDoesNotInheritBranchGrouping() {
    PlanNode root =
        plan(
            "SELECT tag1, tag2, tag3, SUM(s1) FROM ("
                + "SELECT tag1, tag2, tag3, s1 FROM table1 UNION ALL "
                + "SELECT tag1, tag2, tag3, s1 FROM table2) u GROUP BY tag1, tag2, tag3");
    List<AggregationNode> aggregations = nodes(root, AggregationNode.class);
    assertFalse(aggregations.isEmpty());
    boolean foundFinalCollect = false;
    for (AggregationNode aggregation : aggregations) {
      if (aggregation.getChild() instanceof CollectNode) {
        foundFinalCollect = true;
        assertTrue(aggregation.getPreGroupedSymbols().isEmpty());
      }
    }
    assertTrue(foundFinalCollect);
  }

  @Test
  public void testTopKAboveGlobalWindowDoesNotAssumeOrderedInput() {
    PlanNode root =
        plan(
            "SELECT tag1, tag2, tag3, s1, COUNT(*) OVER () AS n FROM table1 "
                + "ORDER BY s1 DESC, tag1, tag2, tag3 LIMIT 5");
    List<TopKNode> topKs = nodes(root, TopKNode.class);
    assertFalse(topKs.isEmpty());
    boolean foundUnorderedInput = false;
    for (TopKNode topK : topKs) {
      if (!nodes(topK.getChildren().get(0), CollectNode.class).isEmpty()) {
        foundUnorderedInput = true;
        assertFalse(topK.isChildrenDataInOrder());
      }
    }
    assertTrue(foundUnorderedInput);
  }

  @Test
  public void testOrderedScanBranchesKeepStreamingAggregation() {
    PlanNode root =
        plan("SELECT tag1, tag2, tag3, SUM(s1 + 1) FROM table1 GROUP BY tag1, tag2, tag3");
    List<AggregationNode> aggregations = nodes(root, AggregationNode.class);
    assertFalse(aggregations.isEmpty());
    assertTrue(aggregations.stream().anyMatch(AggregationNode::isStreamable));
  }
}
