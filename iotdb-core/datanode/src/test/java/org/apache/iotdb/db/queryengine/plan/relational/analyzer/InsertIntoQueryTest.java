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

package org.apache.iotdb.db.queryengine.plan.relational.analyzer;

import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.queryengine.plan.planner.plan.DistributedQueryPlan;
import org.apache.iotdb.db.queryengine.plan.planner.plan.LogicalQueryPlan;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.sink.IdentitySinkNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.SymbolAllocator;
import org.apache.iotdb.db.queryengine.plan.relational.planner.TableLogicalPlanner;
import org.apache.iotdb.db.queryengine.plan.relational.planner.distribute.TableDistributedPlanner;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.DeviceTableScanNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.ExchangeNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.IntoNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.LimitNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.OutputNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.StreamSortNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.TopKNode;

import org.junit.BeforeClass;
import org.junit.Test;

import static org.apache.iotdb.db.queryengine.plan.relational.analyzer.AnalyzerTest.analyzeSQL;
import static org.apache.iotdb.db.queryengine.plan.relational.analyzer.MockTableModelDataPartition.DEVICES_REGION_GROUP;
import static org.apache.iotdb.db.queryengine.plan.relational.analyzer.TestUtils.DEFAULT_WARNING;
import static org.apache.iotdb.db.queryengine.plan.relational.analyzer.TestUtils.QUERY_CONTEXT;
import static org.apache.iotdb.db.queryengine.plan.relational.analyzer.TestUtils.SESSION_INFO;
import static org.apache.iotdb.db.queryengine.plan.relational.analyzer.TestUtils.TEST_MATADATA;
import static org.apache.iotdb.db.queryengine.plan.relational.analyzer.TestUtils.assertTableScan;
import static org.apache.iotdb.db.queryengine.plan.relational.analyzer.TestUtils.getChildrenNode;
import static org.apache.iotdb.db.queryengine.plan.statement.component.Ordering.ASC;
import static org.apache.iotdb.db.queryengine.plan.statement.component.Ordering.DESC;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class InsertIntoQueryTest {
  String sql;
  Analysis analysis;
  LogicalQueryPlan logicalQueryPlan;
  PlanNode logicalPlanNode;
  OutputNode outputNode;
  IntoNode intoNode;
  DeviceTableScanNode deviceTableScanNode;
  TableDistributedPlanner distributionPlanner;
  DistributedQueryPlan distributedQueryPlan;

  @BeforeClass
  public static void setUp() {
    IoTDBDescriptor.getInstance().getConfig().setDataNodeId(1);
  }

  @Test
  public void simpleInsertIntoQuery() {
    sql =
        "INSERT INTO testdb.table3 SELECT * FROM testdb.table2 order by tag2 desc, time desc limit 10";
    analysis = analyzeSQL(sql, TEST_MATADATA, QUERY_CONTEXT);
    SymbolAllocator symbolAllocator = new SymbolAllocator();
    logicalQueryPlan =
        new TableLogicalPlanner(
                QUERY_CONTEXT, TEST_MATADATA, SESSION_INFO, symbolAllocator, DEFAULT_WARNING)
            .plan(analysis);
    logicalPlanNode = logicalQueryPlan.getRootNode();

    // LogicalPlan: `Output-Into-Limit-StreamSort-TableScan
    assertTrue(logicalPlanNode instanceof OutputNode);
    assertTrue(getChildrenNode(logicalPlanNode, 1) instanceof IntoNode);
    intoNode = (IntoNode) getChildrenNode(logicalPlanNode, 1);
    assertEquals(10, intoNode.getColumns().size());
    assertEquals(1, intoNode.getOutputSymbols().size());
    assertEquals("testdb", intoNode.getDatabase());
    assertEquals("table3", intoNode.getTable());
    assertTrue(getChildrenNode(logicalPlanNode, 2) instanceof LimitNode);
    assertTrue(getChildrenNode(logicalPlanNode, 3) instanceof StreamSortNode);
    assertTrue(getChildrenNode(logicalPlanNode, 4) instanceof DeviceTableScanNode);
    deviceTableScanNode = (DeviceTableScanNode) getChildrenNode(logicalPlanNode, 4);
    assertEquals("testdb.table2", deviceTableScanNode.getQualifiedObjectName().toString());
    assertEquals(10, deviceTableScanNode.getAssignments().size());
    assertEquals(6, deviceTableScanNode.getDeviceEntries().size());
    assertEquals(5, deviceTableScanNode.getTagAndAttributeIndexMap().size());
    assertEquals(ASC, deviceTableScanNode.getScanOrder());
    assertEquals(10, deviceTableScanNode.getPushDownLimit());
    assertEquals(0, deviceTableScanNode.getPushDownOffset());
    assertTrue(deviceTableScanNode.isPushLimitToEachDevice());

    // DistributePlan: optimize `Output-Into-TopK-Exchange-Limit-StreamPort-TableScan`
    distributionPlanner =
        new TableDistributedPlanner(
            analysis, symbolAllocator, logicalQueryPlan, TEST_MATADATA, null);
    distributedQueryPlan = distributionPlanner.plan();
    assertEquals(4, distributedQueryPlan.getFragments().size());
    IdentitySinkNode identitySinkNode =
        (IdentitySinkNode) distributedQueryPlan.getFragments().get(0).getPlanNodeTree();
    outputNode = (OutputNode) getChildrenNode(identitySinkNode, 1);
    assertTrue(getChildrenNode(outputNode, 1) instanceof IntoNode);
    assertTrue(getChildrenNode(outputNode, 2) instanceof TopKNode);
    TopKNode topKNode = (TopKNode) getChildrenNode(outputNode, 2);
    assertTrue(topKNode.getChildren().get(0) instanceof ExchangeNode);
    assertTrue(topKNode.getChildren().get(1) instanceof ExchangeNode);
    assertTrue(topKNode.getChildren().get(2) instanceof ExchangeNode);

    for (int i = 1; i <= 3; i++) {
      PlanNode rootOfFI = distributedQueryPlan.getFragments().get(i).getPlanNodeTree();
      assertTrue(rootOfFI instanceof IdentitySinkNode);
      assertTrue(getChildrenNode(rootOfFI, 1) instanceof LimitNode);
      assertTrue(getChildrenNode(rootOfFI, 2) instanceof StreamSortNode);
      assertTrue(getChildrenNode(rootOfFI, 3) instanceof DeviceTableScanNode);
      deviceTableScanNode = (DeviceTableScanNode) getChildrenNode(rootOfFI, 3);
      assertTableScan(deviceTableScanNode, DEVICES_REGION_GROUP.get(i - 1), DESC, 10, 0, true, "");
    }
  }
}
