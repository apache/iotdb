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

package org.apache.iotdb.db.queryengine.plan.relational.planner.optimizations;

import org.apache.iotdb.commons.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.node.TopKNode;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.node.UnionNode;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.queryengine.plan.planner.plan.LogicalQueryPlan;
import org.apache.iotdb.db.queryengine.plan.relational.planner.PlanNodeSearcher;
import org.apache.iotdb.db.queryengine.plan.relational.planner.PlanTester;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import static org.apache.iotdb.db.queryengine.plan.relational.planner.assertions.PlanAssert.assertPlan;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.assertions.PlanMatchPattern.anyTree;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.assertions.PlanMatchPattern.exchange;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.assertions.PlanMatchPattern.output;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.assertions.PlanMatchPattern.tableScanWithRuntimeFilter;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.assertions.PlanMatchPattern.topKWithRuntimeFilterSourceId;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.assertions.PlanMatchPattern.topKWithoutRuntimeFilter;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.assertions.PlanMatchPattern.union;

public class TopKRuntimeFilterDistributedPlanTest {

  private static final String TABLE1 = "testdb.table1";
  private static final String TABLE2 = "testdb.table2";

  private boolean originalEnableTopKRuntimeFilter;

  @Before
  public void setUp() {
    originalEnableTopKRuntimeFilter =
        IoTDBDescriptor.getInstance().getConfig().isEnableTopKRuntimeFilter();
    IoTDBDescriptor.getInstance().getConfig().setEnableTopKRuntimeFilter(true);
    IoTDBDescriptor.getInstance().getConfig().setDataNodeId(1);
  }

  @After
  public void tearDown() {
    IoTDBDescriptor.getInstance()
        .getConfig()
        .setEnableTopKRuntimeFilter(originalEnableTopKRuntimeFilter);
  }

  @Test
  public void singleRegionKeepsLogicalTopKSourceId() {
    String sql = "SELECT time FROM table1 WHERE  tag1 = 'shanghai' ORDER BY time DESC LIMIT 10";
    PlanTester planTester = new PlanTester();
    LogicalQueryPlan logicalPlan = planTester.createPlan(sql);

    TopKNode logicalTopK = findProducerTopK(logicalPlan.getRootNode());
    String logicalSourceId = logicalTopK.getTopKRuntimeFilterSourceId();
    Assert.assertEquals(logicalTopK.getPlanNodeId().getId(), logicalSourceId);

    assertPlan(
        logicalPlan,
        output(
            topKWithRuntimeFilterSourceId(
                logicalSourceId, tableScanWithRuntimeFilter(TABLE1, logicalSourceId))));

    PlanNode distributedPlan = planTester.getFragmentPlan(0);

    assertPlan(
        distributedPlan,
        output(
            topKWithRuntimeFilterSourceId(
                logicalSourceId, tableScanWithRuntimeFilter(TABLE1, logicalSourceId))));
  }

  @Test
  public void multiRegionCopiesSourceIdToRegionTopKAndClearsRoot() {
    String sql = "SELECT time FROM table1 ORDER BY time DESC LIMIT 10";
    PlanTester planTester = new PlanTester();
    LogicalQueryPlan logicalPlan = planTester.createPlan(sql);

    TopKNode logicalTopK = findProducerTopK(logicalPlan.getRootNode());
    String logicalSourceId = logicalTopK.getTopKRuntimeFilterSourceId();
    Assert.assertEquals(logicalTopK.getPlanNodeId().getId(), logicalSourceId);

    assertPlan(
        logicalPlan,
        output(
            topKWithRuntimeFilterSourceId(
                logicalSourceId, tableScanWithRuntimeFilter(TABLE1, logicalSourceId))));

    PlanNode coordinatorFragment = planTester.getFragmentPlan(0);
    assertPlan(
        coordinatorFragment,
        output(anyTree(topKWithoutRuntimeFilter(exchange(), exchange(), exchange()))));
    Assert.assertNull(findProducerTopKOrNull(coordinatorFragment));

    for (int i = 1; i <= 3; i++) {
      PlanNode regionFragment = planTester.getFragmentPlan(i);
      assertPlan(
          regionFragment,
          topKWithRuntimeFilterSourceId(
              logicalSourceId, tableScanWithRuntimeFilter(TABLE1, logicalSourceId)));
    }
  }

  @Test
  public void unionBranchesKeepDistinctSourceIdsAfterDistribution() {
    String sql =
        "(SELECT time FROM table1) UNION ALL (SELECT time FROM table2) ORDER BY time DESC LIMIT 10";
    PlanTester planTester = new PlanTester();
    LogicalQueryPlan logicalPlan = planTester.createPlan(sql);

    UnionNode unionNode =
        (UnionNode)
            PlanNodeSearcher.searchFrom(logicalPlan.getRootNode())
                .where(UnionNode.class::isInstance)
                .findFirst()
                .orElseThrow(AssertionError::new);
    TopKNode leftBranchTopK = (TopKNode) unionNode.getChildren().get(0);
    TopKNode rightBranchTopK = (TopKNode) unionNode.getChildren().get(1);
    String leftSourceId = leftBranchTopK.getTopKRuntimeFilterSourceId();
    String rightSourceId = rightBranchTopK.getTopKRuntimeFilterSourceId();
    Assert.assertNotNull(leftSourceId);
    Assert.assertNotNull(rightSourceId);
    Assert.assertEquals(leftBranchTopK.getPlanNodeId().getId(), leftSourceId);
    Assert.assertEquals(rightBranchTopK.getPlanNodeId().getId(), rightSourceId);
    Assert.assertNotEquals(leftSourceId, rightSourceId);

    assertPlan(
        logicalPlan,
        output(
            topKWithoutRuntimeFilter(
                union(
                    topKWithRuntimeFilterSourceId(
                        leftSourceId, tableScanWithRuntimeFilter(TABLE1, leftSourceId)),
                    topKWithRuntimeFilterSourceId(
                        rightSourceId, tableScanWithRuntimeFilter(TABLE2, rightSourceId))))));

    assertPlan(
        planTester.getFragmentPlan(0),
        output(topKWithoutRuntimeFilter(union(exchange(), exchange()))));

    assertPlan(
        planTester.getFragmentPlan(1),
        topKWithoutRuntimeFilter(exchange(), exchange(), exchange()));
    for (int i = 2; i <= 4; i++) {
      assertPlan(
          planTester.getFragmentPlan(i),
          topKWithRuntimeFilterSourceId(
              leftSourceId, tableScanWithRuntimeFilter(TABLE1, leftSourceId)));
    }
    assertPlan(
        planTester.getFragmentPlan(5),
        topKWithoutRuntimeFilter(exchange(), exchange(), exchange()));
    for (int i = 6; i <= 8; i++) {
      assertPlan(
          planTester.getFragmentPlan(i),
          topKWithRuntimeFilterSourceId(
              rightSourceId, tableScanWithRuntimeFilter(TABLE2, rightSourceId)));
    }
  }

  private static TopKNode findProducerTopK(PlanNode root) {
    return (TopKNode)
        PlanNodeSearcher.searchFrom(root)
            .where(
                node ->
                    node instanceof TopKNode
                        && ((TopKNode) node).getTopKRuntimeFilterSourceId() != null)
            .findFirst()
            .orElseThrow(AssertionError::new);
  }

  private static TopKNode findProducerTopKOrNull(PlanNode root) {
    return (TopKNode)
        PlanNodeSearcher.searchFrom(root)
            .where(
                node ->
                    node instanceof TopKNode
                        && ((TopKNode) node).getTopKRuntimeFilterSourceId() != null)
            .findFirst()
            .orElse(null);
  }
}
