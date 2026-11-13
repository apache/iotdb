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

import static org.apache.iotdb.commons.queryengine.plan.relational.planner.node.AggregationNode.Step.SINGLE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class RateOrderedPlannerTest {

  private static final String SINGLE_DEVICE_WHERE =
      " WHERE tag1='shanghai' AND tag2='A3' AND tag3='YY'";
  // DEVICE_3 and DEVICE_4 are both in DATA_REGION_GROUP_2, so this filter still produces a SINGLE
  // aggregation over a two-device scan.
  private static final String TWO_DEVICE_WHERE = " WHERE tag1='shanghai'";

  @Test
  public void testSingleDeviceQueryEnablesOrderedInput() {
    assertOrdered("SELECT irate(s1, time) AS ir FROM table1" + SINGLE_DEVICE_WHERE);
  }

  @Test
  public void testTwoDevicesGroupedByAllTagsEnableOrderedInput() {
    assertOrdered(
        "SELECT irate(s1, time) AS ir FROM table1"
            + TWO_DEVICE_WHERE
            + " GROUP BY tag1, tag2, tag3");
  }

  @Test
  public void testTwoDevicesGroupedBySubsetOfTagsKeepNaiveInput() {
    assertNotOrdered(
        "SELECT irate(s1, time) AS ir FROM table1" + TWO_DEVICE_WHERE + " GROUP BY tag1");
  }

  @Test
  public void testGlobalAggregationOverTwoDevicesKeepsNaiveInput() {
    assertNotOrdered("SELECT irate(s1, time) AS ir FROM table1" + TWO_DEVICE_WHERE);
  }

  @Test
  public void testDescendingSubqueryWithTwoDevicesKeepsNaiveInput() {
    // With two devices the TopK above the descending scan cannot be eliminated, so this covers
    // rejection of an intermediate node that does not preserve the scan order.
    assertNotOrdered(
        "SELECT irate(s1, time) AS ir FROM (SELECT s1, time FROM table1"
            + TWO_DEVICE_WHERE
            + " ORDER BY time DESC LIMIT 10)");
  }

  @Test
  public void testDescendingSingleDeviceScanKeepsNaiveInput() {
    // The limit of a single-device scan is pushed into the scan, so the plan is
    // Aggregation <- DeviceTableScan(DESC) and the descending scan-order check is exercised.
    assertNotOrdered(
        "SELECT irate(s1, time) AS ir FROM (SELECT s1, time FROM table1"
            + SINGLE_DEVICE_WHERE
            + " ORDER BY time DESC LIMIT 10)");
  }

  @Test
  public void testNonTimeArgumentKeepsNaiveInput() {
    assertNotOrdered("SELECT irate(s1, s2) AS ir FROM table1" + SINGLE_DEVICE_WHERE);
  }

  @Test
  public void testFourArgumentRateFunctionsEnableOrderedInput() {
    assertOrdered("SELECT rate(s1, time, s2, s2) AS r FROM table1" + SINGLE_DEVICE_WHERE);
    assertOrdered("SELECT increase(s1, time, s2, s2) AS r FROM table1" + SINGLE_DEVICE_WHERE);
    assertOrdered("SELECT delta(s1, time, s2, s2) AS r FROM table1" + SINGLE_DEVICE_WHERE);
  }

  @Test
  public void testTimeArgumentThroughAliasEnablesOrderedInput() {
    assertOrdered(
        "SELECT irate(v, t) AS ir FROM (SELECT s1 AS v, time AS t FROM table1"
            + SINGLE_DEVICE_WHERE
            + ")");
  }

  private static void assertOrdered(String sql) {
    AggregationNode aggregation = planSingleAggregation(sql);
    assertNotNull(aggregation);
    assertEquals(SINGLE, aggregation.getStep());
    assertTrue(hasOrderedInput(aggregation));
  }

  private static void assertNotOrdered(String sql) {
    AggregationNode aggregation = planSingleAggregation(sql);
    assertNotNull(aggregation);
    assertEquals(SINGLE, aggregation.getStep());
    assertFalse(hasOrderedInput(aggregation));
  }

  private static AggregationNode planSingleAggregation(String sql) {
    PlanTester planTester = new PlanTester(new TestMetadata());
    planTester.createPlan(sql);
    for (int fragment = 0;
        fragment < planTester.getDistributedPlan().getFragments().size();
        fragment++) {
      AggregationNode aggregation = findSingleAggregation(planTester.getFragmentPlan(fragment));
      if (aggregation != null) {
        return aggregation;
      }
    }
    return null;
  }

  private static AggregationNode findSingleAggregation(PlanNode node) {
    if (node instanceof AggregationNode && ((AggregationNode) node).getStep() == SINGLE) {
      return (AggregationNode) node;
    }
    for (PlanNode child : node.getChildren()) {
      AggregationNode aggregation = findSingleAggregation(child);
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
