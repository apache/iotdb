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

import org.apache.iotdb.db.queryengine.common.MPPQueryContext;
import org.apache.iotdb.db.queryengine.execution.warnings.WarningCollector;
import org.apache.iotdb.db.queryengine.plan.planner.plan.DistributedQueryPlan;
import org.apache.iotdb.db.queryengine.plan.planner.plan.LogicalQueryPlan;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.ExchangeNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.sink.IdentitySinkNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.LogicalPlanner;
import org.apache.iotdb.db.queryengine.plan.relational.planner.distribute.TableDistributedPlanner;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.LimitNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.OffsetNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.OutputNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.ProjectNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.StreamSortNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.TableScanNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.TopKNode;

import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static org.apache.iotdb.db.queryengine.plan.relational.analyzer.AnalyzerTest.analyzeSQL;
import static org.apache.iotdb.db.queryengine.plan.relational.analyzer.LimitOffsetPushDownTest.getChildrenNode;
import static org.apache.iotdb.db.queryengine.plan.relational.analyzer.SortTest.assertTableScan;
import static org.apache.iotdb.db.queryengine.plan.relational.analyzer.SortTest.metadata;
import static org.apache.iotdb.db.queryengine.plan.relational.analyzer.SortTest.queryId;
import static org.apache.iotdb.db.queryengine.plan.relational.analyzer.SortTest.sessionInfo;
import static org.apache.iotdb.db.queryengine.plan.statement.component.Ordering.ASC;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class SubQueryTest {

  String sql;
  Analysis analysis;
  MPPQueryContext context;
  WarningCollector warningCollector = WarningCollector.NOOP;
  LogicalQueryPlan logicalQueryPlan;
  PlanNode logicalPlanNode;
  OutputNode outputNode;
  ProjectNode projectNode;
  StreamSortNode streamSortNode;
  TableDistributedPlanner distributionPlanner;
  DistributedQueryPlan distributedQueryPlan;
  TableScanNode tableScanNode;
  List<String> originalDeviceEntries1 =
      Arrays.asList(
          "table1.shanghai.B3.YY",
          "table1.shenzhen.B1.XX",
          "table1.shenzhen.B2.ZZ",
          "table1.shanghai.A3.YY");
  static List<String> originalDeviceEntries2 =
      Arrays.asList("table1.shenzhen.B1.XX", "table1.shenzhen.B2.ZZ");

  @Test
  public void subQueryTest1() {
    // outer query has limit and sort,
    // sub query only has sort,
    // sort in sub query is invalid,
    // limit can be pushed down into TableScan.
    sql =
        "SELECT time, tag2, attr2, CAST(add_s2 as double) "
            + "FROM (SELECT time, SUBSTRING(tag1, 1) as sub_tag1, tag2, attr2, s1, s2+1 as add_s2 FROM table1 WHERE s1>1 ORDER BY tag1 DESC) "
            + "ORDER BY tag2 OFFSET 3 LIMIT 6";
    context = new MPPQueryContext(sql, queryId, sessionInfo, null, null);
    analysis = analyzeSQL(sql, metadata, context);
    logicalQueryPlan =
        new LogicalPlanner(context, metadata, sessionInfo, warningCollector).plan(analysis);
    logicalPlanNode = logicalQueryPlan.getRootNode();

    // LogicalPlan: `Output - Offset - Limit - StreamSort - Project - TableScan`
    assertTrue(logicalPlanNode instanceof OutputNode);
    assertTrue(getChildrenNode(logicalPlanNode, 1) instanceof OffsetNode);
    assertTrue(getChildrenNode(logicalPlanNode, 2) instanceof LimitNode);
    assertTrue(getChildrenNode(logicalPlanNode, 3) instanceof StreamSortNode);
    assertTrue(getChildrenNode(logicalPlanNode, 4) instanceof ProjectNode);
    assertTrue(getChildrenNode(logicalPlanNode, 5) instanceof TableScanNode);
    tableScanNode = (TableScanNode) getChildrenNode(logicalPlanNode, 5);
    assertEquals(9, tableScanNode.getPushDownLimit());
    assertEquals(true, tableScanNode.isPushLimitToEachDevice());
    assertEquals("(\"s1\" > 1)", tableScanNode.getPushDownPredicate().toString());

    /*
     * IdentitySinkNode-163
     *   └──OutputNode-14
     *       └──OffsetNode-10
     *           └──TopKNode-11
     *               ├──ExchangeNode-159: [SourceAddress:192.0.12.1/test_query.2.0/161]
     *               ├──LimitNode-137
     *               │   └──StreamSortNode-122
     *               │       └──ProjectNode-118
     *               │           └──TableScanNode-115
     *               └──ExchangeNode-160: [SourceAddress:192.0.10.1/test_query.3.0/162]
     */
    distributionPlanner = new TableDistributedPlanner(analysis, logicalQueryPlan, context);
    distributedQueryPlan = distributionPlanner.plan();
    assertEquals(3, distributedQueryPlan.getFragments().size());
    IdentitySinkNode identitySinkNode =
        (IdentitySinkNode) distributedQueryPlan.getFragments().get(0).getPlanNodeTree();
    outputNode = (OutputNode) getChildrenNode(identitySinkNode, 1);
    assertTrue(getChildrenNode(outputNode, 1) instanceof OffsetNode);
    assertTrue(getChildrenNode(outputNode, 2) instanceof TopKNode);
    TopKNode topKNode = (TopKNode) getChildrenNode(outputNode, 2);
    assertTrue(topKNode.getChildren().get(0) instanceof ExchangeNode);
    assertTrue(topKNode.getChildren().get(1) instanceof LimitNode);
    assertTrue(topKNode.getChildren().get(2) instanceof ExchangeNode);
    streamSortNode = (StreamSortNode) getChildrenNode(topKNode.getChildren().get(1), 1);
    assertTrue(getChildrenNode(streamSortNode, 1) instanceof ProjectNode);
    assertTrue(getChildrenNode(streamSortNode, 2) instanceof TableScanNode);
    tableScanNode = (TableScanNode) getChildrenNode(streamSortNode, 2);
    assertTableScan(
        tableScanNode,
        Arrays.asList(
            "table1.shanghai.A3.YY",
            "table1.shenzhen.B1.XX",
            "table1.shenzhen.B2.ZZ",
            "table1.shanghai.B3.YY"),
        ASC,
        9,
        0,
        true);
    /*
     * IdentitySinkNode-161
     *   └──LimitNode-136
     *       └──StreamSortNode-121
     *           └──ProjectNode-117
     *               └──TableScanNode-114
     */
    identitySinkNode =
        (IdentitySinkNode) distributedQueryPlan.getFragments().get(1).getPlanNodeTree();
    assertTrue(getChildrenNode(identitySinkNode, 1) instanceof LimitNode);
    assertTrue(getChildrenNode(identitySinkNode, 2) instanceof StreamSortNode);
    assertTrue(getChildrenNode(identitySinkNode, 3) instanceof ProjectNode);
    assertTrue(getChildrenNode(identitySinkNode, 4) instanceof TableScanNode);
    tableScanNode = (TableScanNode) getChildrenNode(identitySinkNode, 4);
    assertTableScan(
        tableScanNode,
        Arrays.asList("table1.shenzhen.B1.XX", "table1.shenzhen.B2.ZZ"),
        ASC,
        9,
        0,
        true);
  }

  @Test
  public void subQueryTest2() {
    // outer query has limit_1(count=3) and sort,
    // sub query has limit_2(count=9) and sort,
    // only the sort in sub query can be pushed down into TableScan.
    sql =
        "SELECT time, tag2, attr2, CAST(add_s2 as double) "
            + "FROM (SELECT time, SUBSTRING(tag1, 1) as sub_tag1, tag2, attr2, s1, s2+1 as add_s2 FROM table1 WHERE s1>1 ORDER BY tag1 DESC limit 3) "
            + "ORDER BY tag2 ASC OFFSET 5 LIMIT 10";
    context = new MPPQueryContext(sql, queryId, sessionInfo, null, null);
    analysis = analyzeSQL(sql, metadata, context);
    logicalQueryPlan =
        new LogicalPlanner(context, metadata, sessionInfo, warningCollector).plan(analysis);
    logicalPlanNode = logicalQueryPlan.getRootNode();

    // LogicalPlan: `Output - Offset - Limit - StreamSort - Project - Limit - Project - StreamSort -
    // Project - TableScan`
    assertTrue(logicalPlanNode instanceof OutputNode);
    assertTrue(getChildrenNode(logicalPlanNode, 1) instanceof OffsetNode);
    assertTrue(getChildrenNode(logicalPlanNode, 2) instanceof LimitNode);
    assertTrue(getChildrenNode(logicalPlanNode, 3) instanceof StreamSortNode);
    assertTrue(getChildrenNode(logicalPlanNode, 4) instanceof ProjectNode);
    assertTrue(getChildrenNode(logicalPlanNode, 5) instanceof LimitNode);
    assertTrue(getChildrenNode(logicalPlanNode, 6) instanceof ProjectNode);
    assertTrue(getChildrenNode(logicalPlanNode, 7) instanceof StreamSortNode);
    assertTrue(getChildrenNode(logicalPlanNode, 8) instanceof ProjectNode);
    assertTrue(getChildrenNode(logicalPlanNode, 9) instanceof TableScanNode);
    tableScanNode = (TableScanNode) getChildrenNode(logicalPlanNode, 9);
    assertEquals(15, tableScanNode.getPushDownLimit());
    assertEquals(true, tableScanNode.isPushLimitToEachDevice());
    assertEquals("(\"s1\" > 1)", tableScanNode.getPushDownPredicate().toString());

    /*
     * IdentitySinkNode-163
     *   └──OutputNode-14
     *       └──OffsetNode-10
     *           └──TopKNode-11
     *               ├──ExchangeNode-159: [SourceAddress:192.0.12.1/test_query.2.0/161]
     *               ├──LimitNode-137
     *               │   └──StreamSortNode-122
     *               │       └──ProjectNode-118
     *               │           └──TableScanNode-115
     *               └──ExchangeNode-160: [SourceAddress:192.0.10.1/test_query.3.0/162]
     */
    distributionPlanner = new TableDistributedPlanner(analysis, logicalQueryPlan, context);
    distributedQueryPlan = distributionPlanner.plan();
    assertEquals(3, distributedQueryPlan.getFragments().size());
    IdentitySinkNode identitySinkNode =
        (IdentitySinkNode) distributedQueryPlan.getFragments().get(0).getPlanNodeTree();
    outputNode = (OutputNode) getChildrenNode(identitySinkNode, 1);
    assertTrue(getChildrenNode(outputNode, 1) instanceof OffsetNode);
    assertTrue(getChildrenNode(outputNode, 2) instanceof TopKNode);
    TopKNode topKNode = (TopKNode) getChildrenNode(outputNode, 2);
    assertTrue(topKNode.getChildren().get(0) instanceof ExchangeNode);
    assertTrue(topKNode.getChildren().get(1) instanceof LimitNode);
    assertTrue(topKNode.getChildren().get(2) instanceof ExchangeNode);
    streamSortNode = (StreamSortNode) getChildrenNode(topKNode.getChildren().get(1), 1);
    assertTrue(getChildrenNode(streamSortNode, 1) instanceof ProjectNode);
    assertTrue(getChildrenNode(streamSortNode, 2) instanceof TableScanNode);
    tableScanNode = (TableScanNode) getChildrenNode(streamSortNode, 2);
    assertTableScan(
        tableScanNode,
        Arrays.asList(
            "table1.shanghai.A3.YY",
            "table1.shenzhen.B1.XX",
            "table1.shenzhen.B2.ZZ",
            "table1.shanghai.B3.YY"),
        ASC,
        9,
        0,
        true);
    /*
     * IdentitySinkNode-161
     *   └──LimitNode-136
     *       └──StreamSortNode-121
     *           └──ProjectNode-117
     *               └──TableScanNode-114
     */
    identitySinkNode =
        (IdentitySinkNode) distributedQueryPlan.getFragments().get(1).getPlanNodeTree();
    assertTrue(getChildrenNode(identitySinkNode, 1) instanceof LimitNode);
    assertTrue(getChildrenNode(identitySinkNode, 2) instanceof StreamSortNode);
    assertTrue(getChildrenNode(identitySinkNode, 3) instanceof ProjectNode);
    assertTrue(getChildrenNode(identitySinkNode, 4) instanceof TableScanNode);
    tableScanNode = (TableScanNode) getChildrenNode(identitySinkNode, 4);
    assertTableScan(
        tableScanNode,
        Arrays.asList("table1.shenzhen.B1.XX", "table1.shenzhen.B2.ZZ"),
        ASC,
        9,
        0,
        true);
  }

  @Test
  public void diffTest() {}
}
