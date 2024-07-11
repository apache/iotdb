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
import org.apache.iotdb.db.queryengine.common.MPPQueryContext;
import org.apache.iotdb.db.queryengine.common.QueryId;
import org.apache.iotdb.db.queryengine.common.SessionInfo;
import org.apache.iotdb.db.queryengine.execution.warnings.WarningCollector;
import org.apache.iotdb.db.queryengine.plan.planner.plan.DistributedQueryPlan;
import org.apache.iotdb.db.queryengine.plan.planner.plan.LogicalQueryPlan;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.ExchangeNode;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.Metadata;
import org.apache.iotdb.db.queryengine.plan.relational.planner.LogicalPlanner;
import org.apache.iotdb.db.queryengine.plan.relational.planner.distribute.TableDistributionPlanner;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.CollectNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.MergeSortNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.SortNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.TableScanNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.optimizations.PruneUnUsedColumns;
import org.apache.iotdb.db.queryengine.plan.relational.planner.optimizations.PushPredicateIntoTableScan;
import org.apache.iotdb.db.queryengine.plan.relational.planner.optimizations.RemoveRedundantIdentityProjections;
import org.apache.iotdb.db.queryengine.plan.relational.planner.optimizations.SimplifyExpressions;
import org.apache.iotdb.db.queryengine.plan.relational.planner.optimizations.TablePlanOptimizer;

import org.junit.Test;

import java.time.ZoneId;
import java.util.Arrays;
import java.util.List;

import static org.apache.iotdb.db.queryengine.plan.relational.analyzer.AnalyzerTest.analyzeSQL;
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
  Analysis actualAnalysis;
  MPPQueryContext context;
  LogicalPlanner logicalPlanner;
  LogicalQueryPlan logicalQueryPlan;
  PlanNode rootNode;
  TableDistributionPlanner distributionPlanner;
  DistributedQueryPlan distributedQueryPlan;
  TableScanNode tableScanNode;
  List<TablePlanOptimizer> planOptimizerList =
      Arrays.asList(
          new SimplifyExpressions(),
          new PruneUnUsedColumns(),
          new PushPredicateIntoTableScan(),
          new RemoveRedundantIdentityProjections());

  // without sort operation, limit can be pushed into TableScan, pushLimitToEachDevice==false
  // Output - Project - Limit - Offset - Collect - TableScan
  @Test
  public void noOrderByTest() {
    sql = "SELECT time, tag3, cast(s2 AS double) FROM table1 where s1>1 offset 5 limit 10";
    context = new MPPQueryContext(sql, queryId, sessionInfo, null, null);
    actualAnalysis = analyzeSQL(sql, metadata);
    logicalQueryPlan =
        new LogicalPlanner(context, metadata, sessionInfo, planOptimizerList, WarningCollector.NOOP)
            .plan(actualAnalysis);
    rootNode = logicalQueryPlan.getRootNode();
    assertTrue(getChildrenNode(rootNode, 4) instanceof TableScanNode);

    distributionPlanner = new TableDistributionPlanner(actualAnalysis, logicalQueryPlan, context);
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

  // order by all tags, limit can be pushed into TableScan, pushLimitToEachDevice==false
  // Output - Limit - Offset - MergeSort - Sort - Project - Project - TableScan
  @Test
  public void orderByAllTagsTest() {
    sql =
        "SELECT time, tag3, cast(s2 AS double) FROM table1 where s1>1 order by tag2 desc, tag1 asc, attr1 desc, tag3 desc, time desc, s1+s3 asc offset 5 limit 10";
    context = new MPPQueryContext(sql, queryId, sessionInfo, null, null);
    actualAnalysis = analyzeSQL(sql, metadata);
    logicalQueryPlan =
        new LogicalPlanner(context, metadata, sessionInfo, planOptimizerList, WarningCollector.NOOP)
            .plan(actualAnalysis);
    rootNode = logicalQueryPlan.getRootNode();
    assertTrue(getChildrenNode(rootNode, 6) instanceof TableScanNode);

    distributionPlanner = new TableDistributionPlanner(actualAnalysis, logicalQueryPlan, context);
    distributedQueryPlan = distributionPlanner.plan();
    assertEquals(3, distributedQueryPlan.getFragments().size());
    MergeSortNode mergeSortNode =
        (MergeSortNode)
            getChildrenNode(distributedQueryPlan.getFragments().get(0).getPlanNodeTree(), 4);
    assertTrue(mergeSortNode.getChildren().get(0) instanceof ExchangeNode);
    assertTrue(mergeSortNode.getChildren().get(1) instanceof SortNode);
    assertTrue(mergeSortNode.getChildren().get(2) instanceof ExchangeNode);
    tableScanNode = (TableScanNode) getChildrenNode(mergeSortNode.getChildren().get(1), 3);
    assertEquals(4, tableScanNode.getDeviceEntries().size());
    assertEquals(DESC, tableScanNode.getScanOrder());
    assertTrue(tableScanNode.getPushDownLimit() == 15 && tableScanNode.getPushDownOffset() == 0);
    assertFalse(tableScanNode.isPushLimitToEachDevice());

    tableScanNode =
        (TableScanNode)
            getChildrenNode(distributedQueryPlan.getFragments().get(1).getPlanNodeTree(), 4);
    assertEquals(2, tableScanNode.getDeviceEntries().size());
    assertEquals(DESC, tableScanNode.getScanOrder());
    assertTrue(tableScanNode.getPushDownLimit() == 15 && tableScanNode.getPushDownOffset() == 0);
    assertFalse(tableScanNode.isPushLimitToEachDevice());
  }

  // order by some tags, limit can be pushed into TableScan, pushLimitToEachDevice==true
  // Output - Limit - Offset - MergeSort - Sort - Project - Project - TableScan
  @Test
  public void orderBySomeTagsTest() {
    sql =
        "SELECT time, tag3, cast(s2 AS double) FROM table1 where s1>1 order by tag2 desc, attr1 desc, tag3 desc, time desc, s1+s3 asc offset 5 limit 10";
    context = new MPPQueryContext(sql, queryId, sessionInfo, null, null);
    actualAnalysis = analyzeSQL(sql, metadata);
    logicalQueryPlan =
        new LogicalPlanner(context, metadata, sessionInfo, planOptimizerList, WarningCollector.NOOP)
            .plan(actualAnalysis);
    rootNode = logicalQueryPlan.getRootNode();
    assertTrue(getChildrenNode(rootNode, 6) instanceof TableScanNode);

    distributionPlanner = new TableDistributionPlanner(actualAnalysis, logicalQueryPlan, context);
    distributedQueryPlan = distributionPlanner.plan();
    assertEquals(3, distributedQueryPlan.getFragments().size());
    MergeSortNode mergeSortNode =
        (MergeSortNode)
            getChildrenNode(distributedQueryPlan.getFragments().get(0).getPlanNodeTree(), 4);
    assertTrue(mergeSortNode.getChildren().get(0) instanceof ExchangeNode);
    assertTrue(mergeSortNode.getChildren().get(1) instanceof SortNode);
    assertTrue(mergeSortNode.getChildren().get(2) instanceof ExchangeNode);
    tableScanNode = (TableScanNode) getChildrenNode(mergeSortNode.getChildren().get(1), 3);
    assertEquals(4, tableScanNode.getDeviceEntries().size());
    assertEquals(DESC, tableScanNode.getScanOrder());
    assertTrue(tableScanNode.getPushDownLimit() == 15 && tableScanNode.getPushDownOffset() == 0);
    assertTrue(tableScanNode.isPushLimitToEachDevice());

    tableScanNode =
        (TableScanNode)
            getChildrenNode(distributedQueryPlan.getFragments().get(1).getPlanNodeTree(), 4);
    assertEquals(2, tableScanNode.getDeviceEntries().size());
    assertEquals(DESC, tableScanNode.getScanOrder());
    assertTrue(tableScanNode.getPushDownLimit() == 15 && tableScanNode.getPushDownOffset() == 0);
    assertTrue(tableScanNode.isPushLimitToEachDevice());
  }

  // order by time, limit can be pushed into TableScan, pushLimitToEachDevice==true
  // Output - Limit - Offset - MergeSort - Sort - Project - Project - TableScan
  @Test
  public void orderByTimeTest() {
    sql =
        "SELECT time, tag3, cast(s2 AS double) FROM table1 where s1>1 order by time desc, tag2 asc, s1+s3 asc offset 5 limit 10";
    context = new MPPQueryContext(sql, queryId, sessionInfo, null, null);
    actualAnalysis = analyzeSQL(sql, metadata);
    logicalQueryPlan =
        new LogicalPlanner(context, metadata, sessionInfo, planOptimizerList, WarningCollector.NOOP)
            .plan(actualAnalysis);
    rootNode = logicalQueryPlan.getRootNode();
    assertTrue(getChildrenNode(rootNode, 6) instanceof TableScanNode);

    distributionPlanner = new TableDistributionPlanner(actualAnalysis, logicalQueryPlan, context);
    distributedQueryPlan = distributionPlanner.plan();
    assertEquals(3, distributedQueryPlan.getFragments().size());
    MergeSortNode mergeSortNode =
        (MergeSortNode)
            getChildrenNode(distributedQueryPlan.getFragments().get(0).getPlanNodeTree(), 4);
    assertTrue(mergeSortNode.getChildren().get(0) instanceof ExchangeNode);
    assertTrue(mergeSortNode.getChildren().get(1) instanceof SortNode);
    assertTrue(mergeSortNode.getChildren().get(2) instanceof ExchangeNode);
    tableScanNode = (TableScanNode) getChildrenNode(mergeSortNode.getChildren().get(1), 3);
    assertEquals(4, tableScanNode.getDeviceEntries().size());
    assertEquals(DESC, tableScanNode.getScanOrder());
    assertTrue(tableScanNode.getPushDownLimit() == 15 && tableScanNode.getPushDownOffset() == 0);
    assertTrue(tableScanNode.isPushLimitToEachDevice());

    tableScanNode =
        (TableScanNode)
            getChildrenNode(distributedQueryPlan.getFragments().get(1).getPlanNodeTree(), 4);
    assertEquals(2, tableScanNode.getDeviceEntries().size());
    assertEquals(DESC, tableScanNode.getScanOrder());
    assertTrue(tableScanNode.getPushDownLimit() == 15 && tableScanNode.getPushDownOffset() == 0);
    assertTrue(tableScanNode.isPushLimitToEachDevice());
  }

  // order by others, limit can not be pushed into TableScan
  // Output - Limit - Offset - MergeSort - Sort - Project - Project - TableScan
  @Test
  public void orderByOthersTest() {
    sql =
        "SELECT time, tag3, cast(s2 AS double) FROM table1 where s1>1 order by s1 desc, tag2 desc, attr1 desc, tag3 desc, time desc, s1+s3 asc offset 5 limit 10";
    context = new MPPQueryContext(sql, queryId, sessionInfo, null, null);
    actualAnalysis = analyzeSQL(sql, metadata);
    logicalQueryPlan =
        new LogicalPlanner(context, metadata, sessionInfo, planOptimizerList, WarningCollector.NOOP)
            .plan(actualAnalysis);
    rootNode = logicalQueryPlan.getRootNode();
    assertTrue(getChildrenNode(rootNode, 6) instanceof TableScanNode);

    distributionPlanner = new TableDistributionPlanner(actualAnalysis, logicalQueryPlan, context);
    distributedQueryPlan = distributionPlanner.plan();
    assertEquals(3, distributedQueryPlan.getFragments().size());
    MergeSortNode mergeSortNode =
        (MergeSortNode)
            getChildrenNode(distributedQueryPlan.getFragments().get(0).getPlanNodeTree(), 4);
    assertTrue(mergeSortNode.getChildren().get(0) instanceof ExchangeNode);
    assertTrue(mergeSortNode.getChildren().get(1) instanceof SortNode);
    assertTrue(mergeSortNode.getChildren().get(2) instanceof ExchangeNode);
    tableScanNode = (TableScanNode) getChildrenNode(mergeSortNode.getChildren().get(1), 3);
    assertEquals(4, tableScanNode.getDeviceEntries().size());
    assertEquals(ASC, tableScanNode.getScanOrder());
    assertTrue(tableScanNode.getPushDownLimit() == 0 && tableScanNode.getPushDownOffset() == 0);

    tableScanNode =
        (TableScanNode)
            getChildrenNode(distributedQueryPlan.getFragments().get(1).getPlanNodeTree(), 4);
    assertEquals(2, tableScanNode.getDeviceEntries().size());
    assertEquals(ASC, tableScanNode.getScanOrder());
    assertTrue(tableScanNode.getPushDownLimit() == 0 && tableScanNode.getPushDownOffset() == 0);
  }

  private PlanNode getChildrenNode(PlanNode root, int idx) {
    PlanNode result = root;
    for (int i = 1; i <= idx; i++) {
      result = result.getChildren().get(0);
    }
    return result;
  }
}
