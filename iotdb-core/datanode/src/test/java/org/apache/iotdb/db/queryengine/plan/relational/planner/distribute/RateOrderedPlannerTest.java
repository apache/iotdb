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

package org.apache.iotdb.db.queryengine.plan.relational.planner.distribute;

import org.apache.iotdb.commons.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.node.AggregationNode;
import org.apache.iotdb.db.queryengine.plan.relational.analyzer.TestMetadata;
import org.apache.iotdb.db.queryengine.plan.relational.planner.PlanTester;

import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class RateOrderedPlannerTest {

  @Test
  public void testSingleDeviceQueryEnablesOrderedInput() {
    AggregationNode aggregation =
        planAggregation(
            "SELECT irate(s1, time) AS ir FROM table1 "
                + "WHERE tag1='shanghai' AND tag2='A3' AND tag3='YY'");
    assertNotNull(aggregation);
    assertTrue(hasOrderedInput(aggregation));
  }

  @Test
  public void testMultiDeviceQueryKeepsNaiveInput() {
    AggregationNode aggregation = planAggregation("SELECT irate(s1, time) AS ir FROM table1");
    assertNotNull(aggregation);
    assertFalse(hasOrderedInput(aggregation));
  }

  @Test
  public void testNonTimeArgumentKeepsNaiveInput() {
    AggregationNode aggregation =
        planAggregation(
            "SELECT irate(s1, s2) AS ir FROM table1 "
                + "WHERE tag1='shanghai' AND tag2='A3' AND tag3='YY'");
    assertNotNull(aggregation);
    assertFalse(hasOrderedInput(aggregation));
  }

  private static AggregationNode planAggregation(String sql) {
    PlanTester planTester = new PlanTester(new TestMetadata());
    planTester.createPlan(sql);
    for (int fragment = 0; fragment < 3; fragment++) {
      AggregationNode aggregation;
      try {
        aggregation = findAggregation(planTester.getFragmentPlan(fragment));
      } catch (RuntimeException e) {
        break;
      }
      if (aggregation != null) {
        return aggregation;
      }
    }
    return null;
  }

  private static AggregationNode findAggregation(PlanNode node) {
    if (node instanceof AggregationNode) {
      return (AggregationNode) node;
    }
    for (PlanNode child : node.getChildren()) {
      AggregationNode aggregation = findAggregation(child);
      if (aggregation != null) {
        return aggregation;
      }
    }
    return null;
  }

  private static boolean hasOrderedInput(AggregationNode aggregation) {
    return aggregation.getAggregations().values().stream()
        .anyMatch(AggregationNode.Aggregation::isInputOrderedByTimeAscending);
  }
}
