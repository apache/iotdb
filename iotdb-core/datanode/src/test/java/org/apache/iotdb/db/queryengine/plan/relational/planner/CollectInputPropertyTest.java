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
import org.apache.iotdb.db.queryengine.plan.planner.SharedExchangeMemoryEligibility;
import org.apache.iotdb.db.queryengine.plan.planner.plan.DistributedQueryPlan;
import org.apache.iotdb.db.queryengine.plan.planner.plan.FragmentInstance;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.ExchangeNode;

import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class CollectInputPropertyTest {
  private static DistributedQueryPlan plan(String sql) {
    PlanTester tester = new PlanTester();
    tester.createPlan(sql);
    return tester.getDistributedPlan();
  }

  private static <T> List<T> nodes(PlanNode node, Class<T> type) {
    List<T> result = new ArrayList<>();
    if (type.isInstance(node)) result.add(type.cast(node));
    if (!(node instanceof ExchangeNode))
      for (PlanNode child : node.getChildren()) result.addAll(nodes(child, type));
    return result;
  }

  private static void assertSequential(DistributedQueryPlan plan) {
    assertTrue(
        plan.getInstances().stream()
            .anyMatch(
                fi -> !nodes(fi.getFragment().getPlanNodeTree(), CollectNode.class).isEmpty()));
    for (FragmentInstance fi : plan.getInstances())
      assertFalse(
          fi.getId().toString(),
          SharedExchangeMemoryEligibility.isEligible(fi.getFragment().getPlanNodeTree()));
  }

  @Test
  public void testTopKWindowKeepsGroupInsteadOfFullSort() {
    DistributedQueryPlan query =
        plan(
            "SELECT tag1, tag2, tag3, s1, SUM(s1) OVER (PARTITION BY tag1, tag2, tag3 ORDER BY s1) rs "
                + "FROM (SELECT tag1, tag2, tag3, s1, ROW_NUMBER() OVER () n FROM table1) t "
                + "WHERE n > 0 ORDER BY rs DESC LIMIT 10");
    assertSequential(query);
    PlanNode root = query.getInstances().get(0).getFragment().getPlanNodeTree();
    assertEquals(1, nodes(root, GroupNode.class).size());
    assertTrue(
        nodes(root, SortNode.class).stream().noneMatch(sort -> sort.getClass() == SortNode.class));
  }

  @Test
  public void testTopKAggregationKeepsStreamingGroups() {
    DistributedQueryPlan query =
        plan(
            "SELECT tag1, tag2, tag3, SUM(s1) AS s FROM (SELECT tag1, tag2, tag3, s1, "
                + "COUNT(*) OVER () AS n FROM table1 WHERE tag1 != 'shenzhen') t "
                + "WHERE n > 0 GROUP BY tag1, tag2, tag3 ORDER BY s DESC LIMIT 10");
    assertSequential(query);
    List<AggregationNode> aggregations =
        nodes(query.getInstances().get(0).getFragment().getPlanNodeTree(), AggregationNode.class);
    assertEquals(1, aggregations.size());
    assertEquals(3, aggregations.get(0).getPreGroupedSymbols().size());
    assertTrue(aggregations.get(0).isStreamable());
  }

  @Test
  public void testNestedWindowKeepsSequentialGroupingPlan() {
    DistributedQueryPlan query =
        plan(
            "SELECT tag1, tag2, tag3, SUM(s1) OVER (PARTITION BY tag1, tag2, tag3 ORDER BY s1) AS rs "
                + "FROM (SELECT tag1, tag2, tag3, s1, COUNT(*) OVER () AS n FROM table1) t WHERE n > 0");
    assertSequential(query);
    assertFalse(
        nodes(query.getInstances().get(0).getFragment().getPlanNodeTree(), GroupNode.class)
            .isEmpty());
  }

  @Test
  public void testUnorderedScansProjectionsAggregationsAndUnionRetainReadyMode() {
    for (String sql :
        new String[] {
          "SELECT * FROM table1",
          "SELECT time, s1 + 1 AS x FROM table1",
          "SELECT time, diff(s1) AS d FROM table1",
          "SELECT time, abs(diff(s1)) AS d FROM table1 "
              + "WHERE tag1='shenzhen' AND tag2='B2' AND tag3='ZZ'",
          "SELECT time, s1 FROM table1 WHERE diff(s1) > 0",
          "SELECT COUNT(*), SUM(s1), MIN(s1), MAX(s1) FROM table1",
          "SELECT COUNT(DISTINCT s1) FROM table1",
          "SELECT s1 FROM table1 UNION ALL SELECT s1 FROM table2"
        }) {
      DistributedQueryPlan query = plan(sql);
      assertTrue(
          sql,
          query.getInstances().stream()
              .anyMatch(
                  fi ->
                      SharedExchangeMemoryEligibility.isEligible(
                          fi.getFragment().getPlanNodeTree())));
    }
  }

  @Test
  public void testOrderedScanBranchesKeepStreamingAggregation() {
    DistributedQueryPlan query =
        plan("SELECT tag1, tag2, tag3, SUM(s1 + 1) FROM table1 GROUP BY tag1, tag2, tag3");
    assertTrue(
        query.getInstances().stream()
            .anyMatch(
                fi ->
                    nodes(fi.getFragment().getPlanNodeTree(), AggregationNode.class).stream()
                        .anyMatch(AggregationNode::isStreamable)));
  }
}
