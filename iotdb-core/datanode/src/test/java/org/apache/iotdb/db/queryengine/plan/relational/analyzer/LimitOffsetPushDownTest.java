/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.iotdb.db.queryengine.plan.relational.analyzer;

import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.db.protocol.session.IClientSession;
import org.apache.iotdb.db.queryengine.common.QueryId;
import org.apache.iotdb.db.queryengine.common.SessionInfo;
import org.apache.iotdb.db.queryengine.plan.planner.plan.DistributedQueryPlan;
import org.apache.iotdb.db.queryengine.plan.planner.plan.LogicalQueryPlan;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.Metadata;
import org.apache.iotdb.db.queryengine.plan.relational.planner.SymbolAllocator;
import org.apache.iotdb.db.queryengine.plan.relational.planner.TableLogicalPlanner;
import org.apache.iotdb.db.queryengine.plan.relational.planner.distribute.TableDistributedPlanner;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.CollectNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.ExchangeNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.LimitNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.ProjectNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.StreamSortNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.TableScanNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.TopKNode;

import org.junit.Test;

import java.time.ZoneId;

import static org.apache.iotdb.db.queryengine.plan.relational.analyzer.AnalyzerTest.analyzeSQL;
import static org.apache.iotdb.db.queryengine.plan.relational.analyzer.TestUtils.DEFAULT_WARNING;
import static org.apache.iotdb.db.queryengine.plan.relational.analyzer.TestUtils.QUERY_CONTEXT;
import static org.apache.iotdb.db.queryengine.plan.relational.analyzer.TestUtils.SESSION_INFO;
import static org.apache.iotdb.db.queryengine.plan.relational.analyzer.TestUtils.TEST_MATADATA;
import static org.apache.iotdb.db.queryengine.plan.relational.analyzer.TestUtils.getChildrenNode;
import static org.apache.iotdb.db.queryengine.plan.statement.component.Ordering.ASC;
import static org.apache.iotdb.db.queryengine.plan.statement.component.Ordering.DESC;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class LimitOffsetPushDownTest {
  QueryId queryId = new QueryId("test_query");
  SessionInfo sessionInfo =
      new SessionInfo(
          1L,
          "iotdb-user",
          ZoneId.systemDefault(),
          IoTDBConstant.ClientVersion.V_1_0,
          "db",
          IClientSession.SqlDialect.TABLE);
  Metadata metadata = new TestMatadata();
  String sql;
  Analysis analysis;
  LogicalQueryPlan logicalQueryPlan;
  PlanNode rootNode;
  TableDistributedPlanner distributionPlanner;
  DistributedQueryPlan distributedQueryPlan;
  TableScanNode tableScanNode;

  // without sort operation, limit can be pushed into TableScan, pushLimitToEachDevice==false
  @Test
  public void noOrderByTest() {
    sql = "SELECT time, tag3, cast(s2 AS double) FROM table1 where s1>1 offset 5 limit 10";
    analysis = analyzeSQL(sql, TEST_MATADATA, QUERY_CONTEXT);
    SymbolAllocator symbolAllocator = new SymbolAllocator();
    logicalQueryPlan =
        new TableLogicalPlanner(
                QUERY_CONTEXT, TEST_MATADATA, SESSION_INFO, symbolAllocator, DEFAULT_WARNING)
            .plan(analysis);
    rootNode = logicalQueryPlan.getRootNode();
    // LogicalPlan: `Output - Project - Offset - Limit - TableScan`
    assertTrue(getChildrenNode(rootNode, 3) instanceof LimitNode);
    assertTrue(getChildrenNode(rootNode, 4) instanceof TableScanNode);

    // DistributePlan: `Output - Project - Offset - Limit - Collect - TableScan`
    distributionPlanner =
        new TableDistributedPlanner(
            this.analysis, symbolAllocator, logicalQueryPlan, TEST_MATADATA);
    distributedQueryPlan = distributionPlanner.plan();
    assertEquals(3, distributedQueryPlan.getFragments().size());
    CollectNode collectNode =
        (CollectNode)
            getChildrenNode(distributedQueryPlan.getFragments().get(0).getPlanNodeTree(), 5);
    assertTrue(collectNode.getChildren().get(0) instanceof ExchangeNode);
    assertTrue(collectNode.getChildren().get(1) instanceof TableScanNode);
    assertTrue(collectNode.getChildren().get(2) instanceof ExchangeNode);
    tableScanNode = (TableScanNode) collectNode.getChildren().get(1);
    assertEquals(4, tableScanNode.getDeviceEntries().size());
    assertEquals(ASC, tableScanNode.getScanOrder());
    assertTrue(tableScanNode.getPushDownLimit() == 15 && tableScanNode.getPushDownOffset() == 0);
    assertFalse(tableScanNode.isPushLimitToEachDevice());

    tableScanNode =
        (TableScanNode)
            getChildrenNode(distributedQueryPlan.getFragments().get(1).getPlanNodeTree(), 1);
    assertEquals(2, tableScanNode.getDeviceEntries().size());
    assertEquals(ASC, tableScanNode.getScanOrder());
    assertTrue(tableScanNode.getPushDownLimit() == 15 && tableScanNode.getPushDownOffset() == 0);
    assertFalse(tableScanNode.isPushLimitToEachDevice());
  }

  // order by all IDs, limit can be pushed into TableScan, pushLimitToEachDevice==false
  @Test
  public void orderByAllIDsTest() {
    sql =
        "SELECT time, tag3, cast(s2 AS double) FROM table1 where s1>1 order by tag2 desc, tag1 asc, attr1 desc, tag3 desc, time desc, s1+s3 asc offset 5 limit 10";
    analysis = analyzeSQL(sql, TEST_MATADATA, QUERY_CONTEXT);
    SymbolAllocator symbolAllocator = new SymbolAllocator();
    logicalQueryPlan =
        new TableLogicalPlanner(
                QUERY_CONTEXT, TEST_MATADATA, SESSION_INFO, symbolAllocator, DEFAULT_WARNING)
            .plan(analysis);
    rootNode = logicalQueryPlan.getRootNode();
    // LogicalPlan: `Output - Offset - Limit - Project - StreamSort -  Project - TableScan`
    assertTrue(getChildrenNode(rootNode, 6) instanceof TableScanNode);

    // DistributePlan: `IdentitySink - Output - Offset - Project - TopK - Project - TableScan`
    distributionPlanner =
        new TableDistributedPlanner(
            this.analysis, symbolAllocator, logicalQueryPlan, TEST_MATADATA);
    distributedQueryPlan = distributionPlanner.plan();
    assertEquals(3, distributedQueryPlan.getFragments().size());
    TopKNode topKNode =
        (TopKNode) getChildrenNode(distributedQueryPlan.getFragments().get(0).getPlanNodeTree(), 4);
    assertTrue(topKNode.getChildren().get(0) instanceof ExchangeNode);
    assertTrue(topKNode.getChildren().get(1) instanceof ProjectNode);
    assertTrue(topKNode.getChildren().get(2) instanceof ExchangeNode);
    tableScanNode = (TableScanNode) getChildrenNode(topKNode.getChildren().get(1), 1);
    assertEquals(4, tableScanNode.getDeviceEntries().size());
    assertEquals(DESC, tableScanNode.getScanOrder());
    assertTrue(tableScanNode.getPushDownLimit() == 15 && tableScanNode.getPushDownOffset() == 0);
    assertFalse(tableScanNode.isPushLimitToEachDevice());

    // `Identity - Project - TableScan`
    tableScanNode =
        (TableScanNode)
            getChildrenNode(distributedQueryPlan.getFragments().get(1).getPlanNodeTree(), 2);
    assertEquals(2, tableScanNode.getDeviceEntries().size());
    assertEquals(DESC, tableScanNode.getScanOrder());
    assertTrue(tableScanNode.getPushDownLimit() == 15 && tableScanNode.getPushDownOffset() == 0);
    assertFalse(tableScanNode.isPushLimitToEachDevice());

    sql = "SELECT * FROM table1 order by tag2 desc, tag1 asc, attr1 desc, tag3 desc limit 10";
    analysis = analyzeSQL(sql, TEST_MATADATA, QUERY_CONTEXT);
    symbolAllocator = new SymbolAllocator();
    logicalQueryPlan =
        new TableLogicalPlanner(
                QUERY_CONTEXT, TEST_MATADATA, SESSION_INFO, symbolAllocator, DEFAULT_WARNING)
            .plan(analysis);
    rootNode = logicalQueryPlan.getRootNode();
    // LogicalPlan: `Output - Limit - StreamSort - TableScan`
    assertTrue(getChildrenNode(rootNode, 3) instanceof TableScanNode);

    // DistributePlan: `IdentitySink - Output - TopK - TableScan`
    distributionPlanner =
        new TableDistributedPlanner(
            this.analysis, symbolAllocator, logicalQueryPlan, TEST_MATADATA);
    distributedQueryPlan = distributionPlanner.plan();
    assertEquals(3, distributedQueryPlan.getFragments().size());
    topKNode =
        (TopKNode) getChildrenNode(distributedQueryPlan.getFragments().get(0).getPlanNodeTree(), 2);
    assertTrue(topKNode.getChildren().get(0) instanceof ExchangeNode);
    assertTrue(topKNode.getChildren().get(1) instanceof TableScanNode);
    assertTrue(topKNode.getChildren().get(2) instanceof ExchangeNode);
    tableScanNode = (TableScanNode) topKNode.getChildren().get(1);
    assertEquals(4, tableScanNode.getDeviceEntries().size());
    assertEquals(ASC, tableScanNode.getScanOrder());
    assertTrue(tableScanNode.getPushDownLimit() == 10 && tableScanNode.getPushDownOffset() == 0);
    assertFalse(tableScanNode.isPushLimitToEachDevice());

    // `Identity - TableScan`
    tableScanNode =
        (TableScanNode)
            distributedQueryPlan.getFragments().get(1).getPlanNodeTree().getChildren().get(0);
    assertEquals(2, tableScanNode.getDeviceEntries().size());
    assertEquals(ASC, tableScanNode.getScanOrder());
    assertTrue(tableScanNode.getPushDownLimit() == 10 && tableScanNode.getPushDownOffset() == 0);
    assertFalse(tableScanNode.isPushLimitToEachDevice());
  }

  // order by some tags, limit can be pushed into TableScan, pushLimitToEachDevice==true
  @Test
  public void orderBySomeTagsTest() {
    sql =
        "SELECT time, tag3, cast(s2 AS double) FROM table1 where s1>1 order by tag2 desc, attr1 desc, tag3 desc, time desc, s1+s3 asc offset 5 limit 10";
    analysis = analyzeSQL(sql, TEST_MATADATA, QUERY_CONTEXT);
    SymbolAllocator symbolAllocator = new SymbolAllocator();
    logicalQueryPlan =
        new TableLogicalPlanner(
                QUERY_CONTEXT, TEST_MATADATA, SESSION_INFO, symbolAllocator, DEFAULT_WARNING)
            .plan(analysis);
    rootNode = logicalQueryPlan.getRootNode();
    // LogicalPlan: `Output - Offset - Limit - Project - StreamSort - Project - TableScan`
    assertTrue(getChildrenNode(rootNode, 4) instanceof StreamSortNode);
    assertTrue(getChildrenNode(rootNode, 6) instanceof TableScanNode);

    // DistributePlan: `Identity - Output - Offset - Project - TopK - Limit - StreamSort - Project -
    // TableScan`
    distributionPlanner =
        new TableDistributedPlanner(analysis, symbolAllocator, logicalQueryPlan, TEST_MATADATA);
    distributedQueryPlan = distributionPlanner.plan();
    assertEquals(3, distributedQueryPlan.getFragments().size());
    TopKNode topKNode =
        (TopKNode) getChildrenNode(distributedQueryPlan.getFragments().get(0).getPlanNodeTree(), 4);
    assertTrue(topKNode.getChildren().get(0) instanceof ExchangeNode);
    assertTrue(topKNode.getChildren().get(1) instanceof LimitNode);
    assertTrue(topKNode.getChildren().get(2) instanceof ExchangeNode);
    tableScanNode = (TableScanNode) getChildrenNode(topKNode.getChildren().get(1), 3);
    assertEquals(4, tableScanNode.getDeviceEntries().size());
    assertEquals(DESC, tableScanNode.getScanOrder());
    assertTrue(tableScanNode.getPushDownLimit() == 15 && tableScanNode.getPushDownOffset() == 0);
    assertTrue(tableScanNode.isPushLimitToEachDevice());

    // `IdentitySink - Limit - StreamSort - Project - TableScan`
    tableScanNode =
        (TableScanNode)
            getChildrenNode(distributedQueryPlan.getFragments().get(1).getPlanNodeTree(), 4);
    assertEquals(2, tableScanNode.getDeviceEntries().size());
    assertEquals(DESC, tableScanNode.getScanOrder());
    assertTrue(tableScanNode.getPushDownLimit() == 15 && tableScanNode.getPushDownOffset() == 0);
    assertTrue(tableScanNode.isPushLimitToEachDevice());
  }

  // order by time, limit can be pushed into TableScan, pushLimitToEachDevice==true
  @Test
  public void orderByTimeTest() {
    sql =
        "SELECT time, tag3, cast(s2 AS double) FROM table1 where s1>1 order by time desc, tag2 asc, s1+s3 asc offset 5 limit 10";
    analysis = analyzeSQL(sql, TEST_MATADATA, QUERY_CONTEXT);
    SymbolAllocator symbolAllocator = new SymbolAllocator();
    logicalQueryPlan =
        new TableLogicalPlanner(
                QUERY_CONTEXT, TEST_MATADATA, SESSION_INFO, symbolAllocator, DEFAULT_WARNING)
            .plan(analysis);
    // LogicalPlan: `Output - Offset - Project - TopK - Project - TableScan`
    rootNode = logicalQueryPlan.getRootNode();
    assertTrue(getChildrenNode(rootNode, 3) instanceof TopKNode);
    assertTrue(getChildrenNode(rootNode, 5) instanceof TableScanNode);

    // DistributePlan-1 `Identity - Output - Offset - Project - TopK - {Exchange + TopK + Exchange}
    // DistributePlan-2 `Identity - TopK - Project - TableScan`
    distributionPlanner =
        new TableDistributedPlanner(analysis, symbolAllocator, logicalQueryPlan, TEST_MATADATA);
    distributedQueryPlan = distributionPlanner.plan();
    assertEquals(3, distributedQueryPlan.getFragments().size());
    TopKNode topKNode =
        (TopKNode) getChildrenNode(distributedQueryPlan.getFragments().get(0).getPlanNodeTree(), 4);
    assertTrue(topKNode.getChildren().get(0) instanceof ExchangeNode);
    assertTrue(topKNode.getChildren().get(1) instanceof TopKNode);
    assertTrue(topKNode.getChildren().get(2) instanceof ExchangeNode);
    tableScanNode = (TableScanNode) getChildrenNode(topKNode.getChildren().get(1), 2);
    assertEquals(4, tableScanNode.getDeviceEntries().size());
    assertEquals(DESC, tableScanNode.getScanOrder());
    assertTrue(tableScanNode.getPushDownLimit() == 15 && tableScanNode.getPushDownOffset() == 0);
    assertTrue(tableScanNode.isPushLimitToEachDevice());

    tableScanNode =
        (TableScanNode)
            getChildrenNode(distributedQueryPlan.getFragments().get(1).getPlanNodeTree(), 3);
    assertEquals(2, tableScanNode.getDeviceEntries().size());
    assertEquals(DESC, tableScanNode.getScanOrder());
    assertTrue(tableScanNode.getPushDownLimit() == 15 && tableScanNode.getPushDownOffset() == 0);
    assertTrue(tableScanNode.isPushLimitToEachDevice());
  }

  // order by others, limit can not be pushed into TableScan
  @Test
  public void orderByOthersTest() {
    sql =
        "SELECT time, tag3, cast(s2 AS double) FROM table1 where s1>1 order by s1 desc, tag2 desc, attr1 desc, tag3 desc, time desc, s1+s3 asc offset 5 limit 10";
    analysis = analyzeSQL(sql, TEST_MATADATA, QUERY_CONTEXT);
    SymbolAllocator symbolAllocator = new SymbolAllocator();
    logicalQueryPlan =
        new TableLogicalPlanner(
                QUERY_CONTEXT, TEST_MATADATA, SESSION_INFO, symbolAllocator, DEFAULT_WARNING)
            .plan(analysis);
    // LogicalPlan: `Output - Offset - Project - TopK - Project - TableScan`
    rootNode = logicalQueryPlan.getRootNode();
    assertTrue(getChildrenNode(rootNode, 3) instanceof TopKNode);
    assertTrue(getChildrenNode(rootNode, 5) instanceof TableScanNode);

    // DistributePlan-1 `Identity - Output - Offset - Project - TopK - {Exchange + TopK + Exchange}
    // DistributePlan-2 `Identity - TopK - Project - TableScan`
    distributionPlanner =
        new TableDistributedPlanner(analysis, symbolAllocator, logicalQueryPlan, TEST_MATADATA);
    distributedQueryPlan = distributionPlanner.plan();
    assertEquals(3, distributedQueryPlan.getFragments().size());
    TopKNode topKNode =
        (TopKNode) getChildrenNode(distributedQueryPlan.getFragments().get(0).getPlanNodeTree(), 4);
    assertTrue(topKNode.getChildren().get(0) instanceof ExchangeNode);
    assertTrue(topKNode.getChildren().get(1) instanceof TopKNode);
    assertTrue(topKNode.getChildren().get(2) instanceof ExchangeNode);
    tableScanNode = (TableScanNode) getChildrenNode(topKNode.getChildren().get(1), 2);
    assertEquals(4, tableScanNode.getDeviceEntries().size());
    assertEquals(ASC, tableScanNode.getScanOrder());
    assertTrue(tableScanNode.getPushDownLimit() == 0 && tableScanNode.getPushDownOffset() == 0);

    tableScanNode =
        (TableScanNode)
            getChildrenNode(distributedQueryPlan.getFragments().get(1).getPlanNodeTree(), 3);
    assertEquals(2, tableScanNode.getDeviceEntries().size());
    assertEquals(ASC, tableScanNode.getScanOrder());
    assertTrue(tableScanNode.getPushDownLimit() == 0 && tableScanNode.getPushDownOffset() == 0);
  }

  /** Actually with diff function, LimitNode should be above of ProjectNode. */
  @Test
  public void limitDiffProjectTest() {
    sql = "SELECT time, diff(s1) FROM table1 limit 10";
    analysis = analyzeSQL(sql, TEST_MATADATA, QUERY_CONTEXT);
    SymbolAllocator symbolAllocator = new SymbolAllocator();
    logicalQueryPlan =
        new TableLogicalPlanner(
                QUERY_CONTEXT, TEST_MATADATA, SESSION_INFO, symbolAllocator, DEFAULT_WARNING)
            .plan(analysis);
    // LogicalPlan: `Output - Project - Limit - TableScan`
    rootNode = logicalQueryPlan.getRootNode();
    assertTrue(getChildrenNode(rootNode, 1) instanceof ProjectNode);
    assertTrue(getChildrenNode(rootNode, 2) instanceof LimitNode);
    assertTrue(getChildrenNode(rootNode, 3) instanceof TableScanNode);
  }
}
