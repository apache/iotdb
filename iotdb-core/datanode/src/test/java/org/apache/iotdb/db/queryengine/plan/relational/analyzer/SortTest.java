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
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.sink.IdentitySinkNode;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.Metadata;
import org.apache.iotdb.db.queryengine.plan.relational.planner.LogicalPlanner;
import org.apache.iotdb.db.queryengine.plan.relational.planner.distribute.TableDistributedPlanner;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.FilterNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.LimitNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.MergeSortNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.OffsetNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.OutputNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.ProjectNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.StreamSortNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.TableScanNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.TopKNode;
import org.apache.iotdb.db.queryengine.plan.statement.component.Ordering;

import org.junit.Test;

import java.time.ZoneId;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.iotdb.db.queryengine.plan.relational.analyzer.AnalyzerTest.analyzeSQL;
import static org.apache.iotdb.db.queryengine.plan.relational.analyzer.LimitOffsetPushDownTest.getChildrenNode;
import static org.apache.iotdb.db.queryengine.plan.statement.component.Ordering.ASC;
import static org.apache.iotdb.db.queryengine.plan.statement.component.Ordering.DESC;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class SortTest {

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
  WarningCollector warningCollector = WarningCollector.NOOP;
  LogicalPlanner logicalPlanner;
  LogicalQueryPlan logicalQueryPlan;
  PlanNode rootNode;
  OutputNode outputNode;
  PlanNode mergeSortNode;
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
  List<String> originalDeviceEntries2 =
      Arrays.asList("table1.shenzhen.B1.XX", "table1.shenzhen.B2.ZZ");

  // order by some_ids, time, others; has filter
  @Test
  public void someIDColumnTimeOthersSortTest() {
    sql =
        "SELECT time, tag3, tag1, cast(s2 as double), s2+s3, attr1 FROM table1 "
            + "where s1>1 and s1+s3>0 and cast(s1 as double)>1.0 order by tag2 desc, tag3 asc, time desc, s1+s2 desc offset 5 limit 10";
    context = new MPPQueryContext(sql, queryId, sessionInfo, null, null);
    actualAnalysis = analyzeSQL(sql, metadata, context);
    logicalQueryPlan =
        new LogicalPlanner(context, metadata, sessionInfo, warningCollector).plan(actualAnalysis);
    rootNode = logicalQueryPlan.getRootNode();

    // LogicalPlan: `Output-Offset-Limit-Project-StreamSort-Project-Filter-TableScan`
    assertTrue(rootNode instanceof OutputNode);
    assertTrue(getChildrenNode(rootNode, 1) instanceof OffsetNode);
    assertTrue(getChildrenNode(rootNode, 2) instanceof LimitNode);
    assertTrue(getChildrenNode(rootNode, 3) instanceof ProjectNode);
    assertTrue(getChildrenNode(rootNode, 4) instanceof StreamSortNode);
    streamSortNode = (StreamSortNode) getChildrenNode(rootNode, 4);
    assertTrue(getChildrenNode(streamSortNode, 1) instanceof ProjectNode);
    assertTrue(getChildrenNode(streamSortNode, 2) instanceof FilterNode);
    assertTrue(getChildrenNode(streamSortNode, 3) instanceof TableScanNode);
    tableScanNode = (TableScanNode) getChildrenNode(streamSortNode, 3);
    assertEquals("testdb.table1", tableScanNode.getQualifiedObjectName().toString());
    assertEquals(8, tableScanNode.getAssignments().size());
    assertEquals(6, tableScanNode.getDeviceEntries().size());
    assertEquals(5, tableScanNode.getIdAndAttributeIndexMap().size());
    // TODO change scan order in logical plan
    assertEquals(ASC, tableScanNode.getScanOrder());
    assertEquals(0, tableScanNode.getPushDownLimit());
    assertEquals(0, tableScanNode.getPushDownOffset());
    assertEquals(false, tableScanNode.isPushLimitToEachDevice());

    // DistributePlan: `Output-Offset-Limit-Project-MergeSort-StreamSort-Project-Filter-TableScan`
    // to
    // `Output-Offset-Project-TopK-Limit-StreamSort-Project-Filter-TableScan`
    distributionPlanner = new TableDistributedPlanner(actualAnalysis, logicalQueryPlan, context);
    distributedQueryPlan = distributionPlanner.plan();
    assertEquals(3, distributedQueryPlan.getFragments().size());
    IdentitySinkNode identitySinkNode =
        (IdentitySinkNode) distributedQueryPlan.getFragments().get(0).getPlanNodeTree();
    outputNode = (OutputNode) getChildrenNode(identitySinkNode, 1);
    assertTrue(getChildrenNode(outputNode, 1) instanceof OffsetNode);
    assertTrue(getChildrenNode(outputNode, 2) instanceof ProjectNode);
    TopKNode topKNode = (TopKNode) getChildrenNode(outputNode, 3);
    assertTrue(topKNode.getChildren().get(0) instanceof ExchangeNode);
    assertTrue(topKNode.getChildren().get(1) instanceof LimitNode);
    assertTrue(topKNode.getChildren().get(2) instanceof ExchangeNode);
    streamSortNode = (StreamSortNode) getChildrenNode(topKNode.getChildren().get(1), 1);
    assertTrue(getChildrenNode(streamSortNode, 1) instanceof ProjectNode);
    assertTrue(getChildrenNode(streamSortNode, 2) instanceof FilterNode);
    tableScanNode = (TableScanNode) getChildrenNode(streamSortNode, 3);
    assertTableScan(
        tableScanNode,
        Arrays.asList(
            "table1.shanghai.B3.YY",
            "table1.shenzhen.B2.ZZ",
            "table1.shenzhen.B1.XX",
            "table1.shanghai.A3.YY"),
        DESC,
        0,
        0,
        false);

    // DistributePlan: `IdentitySink - Limit - StreamSort - Project - Filter - TableScan`
    LimitNode limitNode =
        (LimitNode)
            getChildrenNode(distributedQueryPlan.getFragments().get(1).getPlanNodeTree(), 1);
    streamSortNode = (StreamSortNode) getChildrenNode(limitNode, 1);
    assertTrue(getChildrenNode(streamSortNode, 1) instanceof ProjectNode);
    assertTrue(getChildrenNode(streamSortNode, 2) instanceof FilterNode);
    tableScanNode = (TableScanNode) getChildrenNode(streamSortNode, 3);
    assertEquals(2, tableScanNode.getDeviceEntries().size());
    assertTableScan(
        tableScanNode,
        Arrays.asList("table1.shenzhen.B2.ZZ", "table1.shenzhen.B1.XX"),
        DESC,
        0,
        0,
        false);

    sql = "SELECT * FROM table1 order by tag2 desc, tag3 asc offset 5 limit 10";
    context = new MPPQueryContext(sql, queryId, sessionInfo, null, null);
    actualAnalysis = analyzeSQL(sql, metadata, context);
    logicalQueryPlan =
        new LogicalPlanner(context, metadata, sessionInfo, warningCollector).plan(actualAnalysis);
    rootNode = logicalQueryPlan.getRootNode();
    // LogicalPlan: `Output-Offset-Limit-StreamSort-TableScan`
    assertTrue(getChildrenNode(rootNode, 3) instanceof StreamSortNode);
    distributionPlanner = new TableDistributedPlanner(actualAnalysis, logicalQueryPlan, context);
    distributedQueryPlan = distributionPlanner.plan();
    assertEquals(3, distributedQueryPlan.getFragments().size());
    // DistributedPlan: `Output-Offset-TopK-Limit-StreamSort-TableScan`
    identitySinkNode =
        (IdentitySinkNode) distributedQueryPlan.getFragments().get(0).getPlanNodeTree();
    assertTrue(getChildrenNode(identitySinkNode, 3) instanceof TopKNode);
    topKNode = (TopKNode) getChildrenNode(identitySinkNode, 3);
    assertTrue(topKNode.getChildren().get(1) instanceof LimitNode);
    assertTrue(getChildrenNode(topKNode.getChildren().get(1), 1) instanceof StreamSortNode);
  }

  // order by all_ids, time, others
  // with limit and offset, use TopKNode
  @Test
  public void allIDColumnTimeSortWithLimitTest() {
    sql =
        "SELECT time, tag3, tag1, cast(s2 as double), s2+s3, attr1 FROM table1 "
            + "where s1>1 and s1+s3>0 and cast(s1 as double)>1.0 order by tag2 desc, tag1 desc, tag3 asc, time desc, s1+s2 desc offset 5 limit 10";
    context = new MPPQueryContext(sql, queryId, sessionInfo, null, null);
    actualAnalysis = analyzeSQL(sql, metadata, context);
    logicalQueryPlan =
        new LogicalPlanner(context, metadata, sessionInfo, warningCollector).plan(actualAnalysis);
    rootNode = logicalQueryPlan.getRootNode();

    // LogicalPlan: `Output-Offset-Limit-Project-StreamSort-Project-Filter-TableScan`
    assertTrue(rootNode instanceof OutputNode);
    assertTrue(getChildrenNode(rootNode, 1) instanceof OffsetNode);
    assertTrue(getChildrenNode(rootNode, 2) instanceof LimitNode);
    assertTrue(getChildrenNode(rootNode, 3) instanceof ProjectNode);
    assertTrue(getChildrenNode(rootNode, 4) instanceof StreamSortNode);
    StreamSortNode streamSortNode = (StreamSortNode) getChildrenNode(rootNode, 4);
    assertTrue(getChildrenNode(streamSortNode, 1) instanceof ProjectNode);
    assertTrue(getChildrenNode(streamSortNode, 2) instanceof FilterNode);
    assertTrue(getChildrenNode(streamSortNode, 3) instanceof TableScanNode);
    tableScanNode = (TableScanNode) getChildrenNode(streamSortNode, 3);
    assertEquals("testdb.table1", tableScanNode.getQualifiedObjectName().toString());
    assertEquals(8, tableScanNode.getAssignments().size());
    assertEquals(6, tableScanNode.getDeviceEntries().size());

    // DistributePlan: optimize
    // `Output-Offset-Limit-Project-MergeSort-StreamSort-Project-Filter-TableScan`
    // to `Output-Offset-Project-TopK-Limit-Project-Filter-TableScan`
    distributionPlanner = new TableDistributedPlanner(actualAnalysis, logicalQueryPlan, context);
    distributedQueryPlan = distributionPlanner.plan();
    assertEquals(3, distributedQueryPlan.getFragments().size());
    outputNode =
        (OutputNode)
            distributedQueryPlan.getFragments().get(0).getPlanNodeTree().getChildren().get(0);
    assertTrue(getChildrenNode(outputNode, 1) instanceof OffsetNode);
    assertTrue(getChildrenNode(outputNode, 2) instanceof ProjectNode);
    assertTrue(getChildrenNode(outputNode, 3) instanceof TopKNode);
    TopKNode topKNode = (TopKNode) getChildrenNode(outputNode, 3);
    // childrenDataInOrder==true, and can eliminate the StreamSortNode
    assertTrue(topKNode.isChildrenDataInOrder());
    assertTrue(topKNode.getChildren().get(0) instanceof ExchangeNode);
    assertTrue(topKNode.getChildren().get(1) instanceof LimitNode);
    assertTrue(topKNode.getChildren().get(2) instanceof ExchangeNode);
    projectNode = (ProjectNode) getChildrenNode(topKNode.getChildren().get(1), 1);
    assertTrue(getChildrenNode(projectNode, 1) instanceof FilterNode);
    tableScanNode = (TableScanNode) getChildrenNode(projectNode, 2);
    assertTableScan(
        tableScanNode,
        Arrays.asList(
            "table1.shanghai.B3.YY",
            "table1.shenzhen.B2.ZZ",
            "table1.shenzhen.B1.XX",
            "table1.shanghai.A3.YY"),
        DESC,
        0,
        0,
        false);

    // DistributePlan: `IdentitySink-Limit-Project-Filter-TableScan`
    IdentitySinkNode identitySinkNode =
        (IdentitySinkNode) distributedQueryPlan.getFragments().get(1).getPlanNodeTree();
    assertTrue(getChildrenNode(identitySinkNode, 1) instanceof LimitNode);
    assertTrue(getChildrenNode(identitySinkNode, 2) instanceof ProjectNode);
    assertTrue(getChildrenNode(identitySinkNode, 3) instanceof FilterNode);
    assertTrue(getChildrenNode(identitySinkNode, 4) instanceof TableScanNode);
    tableScanNode = (TableScanNode) getChildrenNode(identitySinkNode, 4);
    assertTableScan(
        tableScanNode,
        Arrays.asList("table1.shenzhen.B2.ZZ", "table1.shenzhen.B1.XX"),
        DESC,
        0,
        0,
        false);
  }

  // order by all_ids, time, others
  // without limit and offset, use MergeSortNode
  @Test
  public void allIDColumnTimeSortNoLimitTest() {
    sql =
        "SELECT time, tag3, tag1, cast(s2 as double), s2+s3, attr1 FROM table1 "
            + "where s1>1 and s1+s3>0 and cast(s1 as double)>1.0 order by tag2 desc, tag1 desc, tag3 asc, time desc, s1+s2 desc";
    context = new MPPQueryContext(sql, queryId, sessionInfo, null, null);
    actualAnalysis = analyzeSQL(sql, metadata, context);
    logicalQueryPlan =
        new LogicalPlanner(context, metadata, sessionInfo, warningCollector).plan(actualAnalysis);
    rootNode = logicalQueryPlan.getRootNode();

    // LogicalPlan: `Output-Project-StreamSort-Project-Filter-TableScan`
    assertTrue(rootNode instanceof OutputNode);
    assertTrue(getChildrenNode(rootNode, 1) instanceof ProjectNode);
    assertTrue(getChildrenNode(rootNode, 2) instanceof StreamSortNode);
    StreamSortNode streamSortNode = (StreamSortNode) getChildrenNode(rootNode, 2);
    assertTrue(getChildrenNode(streamSortNode, 1) instanceof ProjectNode);
    assertTrue(getChildrenNode(streamSortNode, 2) instanceof FilterNode);
    assertTrue(getChildrenNode(streamSortNode, 3) instanceof TableScanNode);
    tableScanNode = (TableScanNode) getChildrenNode(streamSortNode, 3);
    assertEquals("testdb.table1", tableScanNode.getQualifiedObjectName().toString());
    assertEquals(8, tableScanNode.getAssignments().size());
    assertEquals(6, tableScanNode.getDeviceEntries().size());

    // DistributePlan: optimize `Output-Project-MergeSort-Project-Filter-TableScan`
    distributionPlanner = new TableDistributedPlanner(actualAnalysis, logicalQueryPlan, context);
    distributedQueryPlan = distributionPlanner.plan();
    assertEquals(3, distributedQueryPlan.getFragments().size());
    outputNode =
        (OutputNode)
            distributedQueryPlan.getFragments().get(0).getPlanNodeTree().getChildren().get(0);
    assertTrue(getChildrenNode(outputNode, 1) instanceof ProjectNode);
    MergeSortNode mergeSortNode = (MergeSortNode) getChildrenNode(outputNode, 2);
    assertTrue(mergeSortNode.getChildren().get(0) instanceof ExchangeNode);
    assertTrue(mergeSortNode.getChildren().get(1) instanceof ProjectNode);
    assertTrue(mergeSortNode.getChildren().get(2) instanceof ExchangeNode);
    projectNode = (ProjectNode) mergeSortNode.getChildren().get(1);
    assertTrue(getChildrenNode(projectNode, 1) instanceof FilterNode);
    tableScanNode = (TableScanNode) getChildrenNode(projectNode, 2);
    assertEquals(4, tableScanNode.getDeviceEntries().size());
    assertTableScan(
        tableScanNode,
        Arrays.asList(
            "table1.shanghai.B3.YY",
            "table1.shenzhen.B2.ZZ",
            "table1.shenzhen.B1.XX",
            "table1.shanghai.A3.YY"),
        DESC,
        0,
        0,
        false);

    // DistributePlan: `IdentitySink-Project-Filter-TableScan`
    projectNode =
        (ProjectNode)
            distributedQueryPlan.getFragments().get(1).getPlanNodeTree().getChildren().get(0);
    assertTrue(getChildrenNode(projectNode, 1) instanceof FilterNode);
    tableScanNode = (TableScanNode) getChildrenNode(projectNode, 2);
    assertTableScan(
        tableScanNode,
        Arrays.asList("table1.shenzhen.B2.ZZ", "table1.shenzhen.B1.XX"),
        DESC,
        0,
        0,
        false);
  }

  // order by some_ids, others, time
  @Test
  public void someIDColumnOthersTimeSortTest() {
    sql =
        "SELECT time, tag3, tag1, cast(s2 as double), s2+s3, attr1 FROM table1 "
            + "where s1>1 and s1+s3>0 and cast(s1 as double)>1.0 order by tag2 desc, tag1 desc, s1+s2 desc, time desc offset 5 limit 10";
    context = new MPPQueryContext(sql, queryId, sessionInfo, null, null);
    actualAnalysis = analyzeSQL(sql, metadata, context);
    logicalQueryPlan =
        new LogicalPlanner(context, metadata, sessionInfo, warningCollector).plan(actualAnalysis);
    rootNode = logicalQueryPlan.getRootNode();

    // LogicalPlan: `Output-Offset-Limit-Project-StreamSort-Project-Filter-TableScan`
    assertTrue(rootNode instanceof OutputNode);
    assertTrue(getChildrenNode(rootNode, 1) instanceof OffsetNode);
    assertTrue(getChildrenNode(rootNode, 2) instanceof LimitNode);
    assertTrue(getChildrenNode(rootNode, 3) instanceof ProjectNode);
    assertTrue(getChildrenNode(rootNode, 4) instanceof StreamSortNode);
    streamSortNode = (StreamSortNode) getChildrenNode(rootNode, 4);
    assertEquals(1, streamSortNode.getStreamCompareKeyEndIndex());
    assertTrue(getChildrenNode(streamSortNode, 1) instanceof ProjectNode);
    assertTrue(getChildrenNode(streamSortNode, 2) instanceof FilterNode);
    assertTrue(getChildrenNode(streamSortNode, 3) instanceof TableScanNode);
    tableScanNode = (TableScanNode) getChildrenNode(streamSortNode, 3);
    assertEquals("testdb.table1", tableScanNode.getQualifiedObjectName().toString());
    assertEquals(8, tableScanNode.getAssignments().size());
    assertEquals(6, tableScanNode.getDeviceEntries().size());
    assertEquals(5, tableScanNode.getIdAndAttributeIndexMap().size());
    assertEquals(ASC, tableScanNode.getScanOrder());
    assertEquals(0, tableScanNode.getPushDownLimit());
    assertEquals(0, tableScanNode.getPushDownOffset());
    assertEquals(false, tableScanNode.isPushLimitToEachDevice());

    // DistributePlan: optimize
    // `Output-Offset-Limit-Project-MergeSort-StreamSort-Project-Filter-TableScan` to
    // `Output-Offset-Project-TopK-Limit-StreamSort-Project-Filter-TableScan`
    distributionPlanner = new TableDistributedPlanner(actualAnalysis, logicalQueryPlan, context);
    distributedQueryPlan = distributionPlanner.plan();
    assertEquals(3, distributedQueryPlan.getFragments().size());
    outputNode =
        (OutputNode)
            distributedQueryPlan.getFragments().get(0).getPlanNodeTree().getChildren().get(0);
    assertTrue(getChildrenNode(outputNode, 1) instanceof OffsetNode);
    assertTrue(getChildrenNode(outputNode, 2) instanceof ProjectNode);
    TopKNode topKNode = (TopKNode) getChildrenNode(outputNode, 3);
    assertTrue(topKNode.getChildren().get(0) instanceof ExchangeNode);
    assertTrue(topKNode.getChildren().get(1) instanceof LimitNode);
    assertTrue(topKNode.getChildren().get(2) instanceof ExchangeNode);
    streamSortNode = (StreamSortNode) getChildrenNode(topKNode.getChildren().get(1), 1);
    assertTrue(getChildrenNode(streamSortNode, 1) instanceof ProjectNode);
    assertTrue(getChildrenNode(streamSortNode, 2) instanceof FilterNode);
    tableScanNode = (TableScanNode) getChildrenNode(streamSortNode, 3);
    assertTableScan(
        tableScanNode,
        Arrays.asList(
            "table1.shanghai.B3.YY",
            "table1.shenzhen.B2.ZZ",
            "table1.shenzhen.B1.XX",
            "table1.shanghai.A3.YY"),
        ASC,
        0,
        0,
        false);

    // DistributePlan: `IdentitySink - Limit - StreamSort - Project - Filter - TableScan`
    streamSortNode =
        (StreamSortNode)
            getChildrenNode(distributedQueryPlan.getFragments().get(1).getPlanNodeTree(), 2);
    assertTrue(getChildrenNode(streamSortNode, 1) instanceof ProjectNode);
    assertTrue(getChildrenNode(streamSortNode, 2) instanceof FilterNode);
    tableScanNode = (TableScanNode) getChildrenNode(streamSortNode, 3);
    assertTableScan(
        tableScanNode,
        Arrays.asList("table1.shenzhen.B2.ZZ", "table1.shenzhen.B1.XX"),
        ASC,
        0,
        0,
        false);
  }

  // order by all_ids, others, time
  @Test
  public void allIDColumnOthersTimeSortTest() {
    sql =
        "SELECT time, tag3, tag1, cast(s2 as double), s2+s3, attr1 FROM table1 "
            + "where s1>1 and s1+s3>0 and cast(s1 as double)>1.0 order by tag2 desc, tag1 desc, tag3 asc, s1+s2 desc, time desc offset 5 limit 10";
    context = new MPPQueryContext(sql, queryId, sessionInfo, null, null);
    actualAnalysis = analyzeSQL(sql, metadata, context);
    logicalQueryPlan =
        new LogicalPlanner(context, metadata, sessionInfo, warningCollector).plan(actualAnalysis);
    rootNode = logicalQueryPlan.getRootNode();

    // LogicalPlan: `Output-Offset-Limit-Project-StreamSort-Project-Filter-TableScan`
    assertTrue(rootNode instanceof OutputNode);
    assertTrue(getChildrenNode(rootNode, 1) instanceof OffsetNode);
    assertTrue(getChildrenNode(rootNode, 2) instanceof LimitNode);
    assertTrue(getChildrenNode(rootNode, 3) instanceof ProjectNode);
    assertTrue(getChildrenNode(rootNode, 4) instanceof StreamSortNode);
    streamSortNode = (StreamSortNode) getChildrenNode(rootNode, 4);
    assertEquals(2, streamSortNode.getStreamCompareKeyEndIndex());
    assertTrue(getChildrenNode(streamSortNode, 1) instanceof ProjectNode);
    assertTrue(getChildrenNode(streamSortNode, 2) instanceof FilterNode);
    assertTrue(getChildrenNode(streamSortNode, 3) instanceof TableScanNode);
    tableScanNode = (TableScanNode) getChildrenNode(streamSortNode, 3);
    assertEquals("testdb.table1", tableScanNode.getQualifiedObjectName().toString());
    assertEquals(8, tableScanNode.getAssignments().size());
    assertEquals(6, tableScanNode.getDeviceEntries().size());
    assertEquals(5, tableScanNode.getIdAndAttributeIndexMap().size());

    // DistributePlan: optimize
    // `Output-Offset-Limit-Project-MergeSort-StreamSort-Project-Filter-TableScan`
    // to `Output-Offset-Project-TopK-Limit-StreamSort-Project-Filter-TableScan`
    distributionPlanner = new TableDistributedPlanner(actualAnalysis, logicalQueryPlan, context);
    distributedQueryPlan = distributionPlanner.plan();
    assertEquals(3, distributedQueryPlan.getFragments().size());
    outputNode =
        (OutputNode)
            distributedQueryPlan.getFragments().get(0).getPlanNodeTree().getChildren().get(0);
    assertTrue(getChildrenNode(outputNode, 1) instanceof OffsetNode);
    assertTrue(getChildrenNode(outputNode, 2) instanceof ProjectNode);
    assertTrue(getChildrenNode(outputNode, 3) instanceof TopKNode);
    TopKNode topKNode = (TopKNode) getChildrenNode(outputNode, 3);
    assertTrue(topKNode.getChildren().get(0) instanceof ExchangeNode);
    assertTrue(topKNode.getChildren().get(1) instanceof LimitNode);
    assertTrue(topKNode.getChildren().get(2) instanceof ExchangeNode);
    streamSortNode = (StreamSortNode) getChildrenNode(topKNode.getChildren().get(1), 1);
    assertTrue(getChildrenNode(streamSortNode, 1) instanceof ProjectNode);
    assertTrue(getChildrenNode(streamSortNode, 2) instanceof FilterNode);
    tableScanNode = (TableScanNode) getChildrenNode(streamSortNode, 3);
    assertEquals(4, tableScanNode.getDeviceEntries().size());
    assertTableScan(
        tableScanNode,
        Arrays.asList(
            "table1.shanghai.B3.YY",
            "table1.shenzhen.B2.ZZ",
            "table1.shenzhen.B1.XX",
            "table1.shanghai.A3.YY"),
        ASC,
        0,
        0,
        false);

    // DistributePlan: `IdentitySink - Limit - StreamSort - Project - Filter - TableScan`
    IdentitySinkNode sinkNode =
        (IdentitySinkNode) distributedQueryPlan.getFragments().get(1).getPlanNodeTree();
    assertTrue(getChildrenNode(sinkNode, 1) instanceof LimitNode);
    assertTrue(getChildrenNode(sinkNode, 2) instanceof StreamSortNode);
    assertTrue(getChildrenNode(sinkNode, 5) instanceof TableScanNode);
    tableScanNode = (TableScanNode) getChildrenNode(sinkNode, 5);
    assertEquals(2, tableScanNode.getDeviceEntries().size());
    assertTableScan(
        tableScanNode,
        Arrays.asList("table1.shenzhen.B2.ZZ", "table1.shenzhen.B1.XX"),
        ASC,
        0,
        0,
        false);
  }

  @Test
  public void orderByTimeTest() {
    // order by time, some_ids, others; no filter
    sql =
        "SELECT time, tag3, tag1, cast(s2 as double), s2+s3, attr1 FROM table1 order by time desc, tag2 asc, tag3 desc, s1+s2 desc offset 5 limit 10";
    context = new MPPQueryContext(sql, queryId, sessionInfo, null, null);
    actualAnalysis = analyzeSQL(sql, metadata, context);
    logicalQueryPlan =
        new LogicalPlanner(context, metadata, sessionInfo, warningCollector).plan(actualAnalysis);
    rootNode = logicalQueryPlan.getRootNode();

    assertTopKNoFilter(originalDeviceEntries1, originalDeviceEntries2, DESC, 15, 0, true);

    // order by time, others, some_ids; has filter
    sql =
        "SELECT time, tag3, substring(tag1, 1), cast(s2 as double), s2+s3, attr1 FROM table1 "
            + "where s1>1 and s1+s3>0 and cast(s1 as double)>1.0 order by time desc, s1+s2 asc, tag2 asc, tag1 desc offset 5 limit 10";
    context = new MPPQueryContext(sql, queryId, sessionInfo, null, null);
    actualAnalysis = analyzeSQL(sql, metadata, context);
    logicalQueryPlan =
        new LogicalPlanner(context, metadata, sessionInfo, warningCollector).plan(actualAnalysis);
    rootNode = logicalQueryPlan.getRootNode();
    assertTopKWithFilter(originalDeviceEntries1, originalDeviceEntries2, DESC, 0, 0, false);

    // order by time, others, all_ids; has filter
    sql =
        "SELECT time, tag3, tag1, cast(s2 as double), s2+s3, attr1 FROM table1 "
            + "where s1>1 and s1+s3>0 and cast(s1 as double)>1.0 order by time desc, s1+s2 asc, tag2 asc, tag3 desc, tag1 desc offset 5 limit 10";
    context = new MPPQueryContext(sql, queryId, sessionInfo, null, null);
    actualAnalysis = analyzeSQL(sql, metadata, context);
    logicalQueryPlan =
        new LogicalPlanner(context, metadata, sessionInfo, warningCollector).plan(actualAnalysis);
    rootNode = logicalQueryPlan.getRootNode();
    assertTopKWithFilter(originalDeviceEntries1, originalDeviceEntries2, DESC, 0, 0, false);

    // order by time, all_ids, others; has filter
    sql =
        "SELECT time, tag3, tag1, cast(s2 as double), s2+s3, attr1 FROM table1 "
            + "where s1>1 and s1+s3>0 and cast(s1 as double)>1.0 order by time desc, tag2 asc, tag3 desc, tag1 asc, s1+s2 desc offset 5 limit 10";
    context = new MPPQueryContext(sql, queryId, sessionInfo, null, null);
    actualAnalysis = analyzeSQL(sql, metadata, context);
    logicalQueryPlan =
        new LogicalPlanner(context, metadata, sessionInfo, warningCollector).plan(actualAnalysis);
    rootNode = logicalQueryPlan.getRootNode();

    assertTopKWithFilter(originalDeviceEntries1, originalDeviceEntries2, DESC, 0, 0, false);
  }

  @Test
  public void orderByOthersTest() {
    // order by others, some_ids, time
    sql =
        "SELECT time, tag3, tag1, cast(s2 as double), s2+s3, attr1 FROM table1 "
            + "order by s1+s2 desc, tag2 desc, tag1 desc, time desc offset 5 limit 10";
    context = new MPPQueryContext(sql, queryId, sessionInfo, null, null);
    actualAnalysis = analyzeSQL(sql, metadata, context);
    logicalQueryPlan =
        new LogicalPlanner(context, metadata, sessionInfo, warningCollector).plan(actualAnalysis);
    rootNode = logicalQueryPlan.getRootNode();
    assertTopKNoFilter(originalDeviceEntries1, originalDeviceEntries2, ASC, 0, 0, false);

    // order by others, all_ids, time
    sql =
        "SELECT time, tag3, tag1, cast(s2 as double), s2+s3, attr1 FROM table1 "
            + "where s1>1 and s1+s3>0 and cast(s1 as double)>1.0 order by s1+s2 desc, tag2 desc, tag1 desc, tag3 desc, time asc offset 5 limit 10";
    context = new MPPQueryContext(sql, queryId, sessionInfo, null, null);
    actualAnalysis = analyzeSQL(sql, metadata, context);
    logicalQueryPlan =
        new LogicalPlanner(context, metadata, sessionInfo, warningCollector).plan(actualAnalysis);
    rootNode = logicalQueryPlan.getRootNode();
    assertTopKWithFilter(originalDeviceEntries1, originalDeviceEntries2, ASC, 0, 0, false);

    // order by others, time, some_ids
    sql =
        "SELECT time, tag3, tag1, cast(s2 as double), s2+s3, attr1 FROM table1 "
            + "where s1>1 and s1+s3>0 and cast(s1 as double)>1.0 order by s1+s2 desc, time desc, tag2 desc, tag1 desc offset 5 limit 10";
    context = new MPPQueryContext(sql, queryId, sessionInfo, null, null);
    actualAnalysis = analyzeSQL(sql, metadata, context);
    logicalQueryPlan =
        new LogicalPlanner(context, metadata, sessionInfo, warningCollector).plan(actualAnalysis);
    rootNode = logicalQueryPlan.getRootNode();
    assertTopKWithFilter(originalDeviceEntries1, originalDeviceEntries2, ASC, 0, 0, false);

    // order by others, time, all_ids
    sql =
        "SELECT time, tag3, tag1, cast(s2 as double), s2+s3, attr1 FROM table1 "
            + "where s1>1 and s1+s3>0 and cast(s1 as double)>1.0 order by s1+s2 desc, time desc, tag2 desc, tag1 desc, tag3 asc offset 5 limit 10";
    context = new MPPQueryContext(sql, queryId, sessionInfo, null, null);
    actualAnalysis = analyzeSQL(sql, metadata, context);
    logicalQueryPlan =
        new LogicalPlanner(context, metadata, sessionInfo, warningCollector).plan(actualAnalysis);
    rootNode = logicalQueryPlan.getRootNode();
    assertTopKWithFilter(originalDeviceEntries1, originalDeviceEntries2, ASC, 0, 0, false);
  }

  public void assertTopKWithFilter(
      List<String> deviceEntries1,
      List<String> deviceEntries2,
      Ordering expectedOrdering,
      long expectedPushDownLimit,
      long expectedPushDownOffset,
      boolean isPushLimitToEachDevice) {
    // LogicalPlan: `Output - Offset - Project - TopK - Project - FilterNode - TableScan`
    assertTrue(rootNode instanceof OutputNode);
    assertTrue(getChildrenNode(rootNode, 1) instanceof OffsetNode);
    assertTrue(getChildrenNode(rootNode, 2) instanceof ProjectNode);
    assertTrue(getChildrenNode(rootNode, 3) instanceof TopKNode);
    assertTrue(getChildrenNode(rootNode, 4) instanceof ProjectNode);
    assertTrue(getChildrenNode(rootNode, 5) instanceof FilterNode);
    assertTrue(getChildrenNode(rootNode, 6) instanceof TableScanNode);
    tableScanNode = (TableScanNode) getChildrenNode(rootNode, 6);
    assertEquals("testdb.table1", tableScanNode.getQualifiedObjectName().toString());
    assertEquals(8, tableScanNode.getAssignments().size());
    assertEquals(6, tableScanNode.getDeviceEntries().size());
    assertEquals(5, tableScanNode.getIdAndAttributeIndexMap().size());
    assertTrue(
        tableScanNode.getPushDownLimit() == expectedPushDownLimit
            && tableScanNode.getPushDownOffset() == expectedPushDownOffset);
    assertEquals(isPushLimitToEachDevice, tableScanNode.isPushLimitToEachDevice());

    // DistributePlan `Identity - Output - Offset - Project - TopK - {Exchange + TopK + Exchange}
    distributionPlanner = new TableDistributedPlanner(actualAnalysis, logicalQueryPlan, context);
    distributedQueryPlan = distributionPlanner.plan();
    assertEquals(3, distributedQueryPlan.getFragments().size());
    IdentitySinkNode identitySinkNode =
        (IdentitySinkNode) distributedQueryPlan.getFragments().get(0).getPlanNodeTree();
    assertTrue(getChildrenNode(identitySinkNode, 1) instanceof OutputNode);
    OutputNode outputNode = (OutputNode) getChildrenNode(identitySinkNode, 1);
    assertTrue(getChildrenNode(outputNode, 1) instanceof OffsetNode);
    assertTrue(getChildrenNode(outputNode, 3) instanceof TopKNode);
    TopKNode topKNode = (TopKNode) getChildrenNode(outputNode, 3);
    assertTrue(topKNode.getChildren().get(0) instanceof ExchangeNode);
    assertTrue(topKNode.getChildren().get(1) instanceof TopKNode);
    assertTrue(topKNode.getChildren().get(2) instanceof ExchangeNode);
    TopKNode topKNode1 = (TopKNode) topKNode.getChildren().get(1);
    assertTrue(getChildrenNode(topKNode1, 1) instanceof ProjectNode);
    assertTrue(getChildrenNode(topKNode1, 2) instanceof FilterNode);
    tableScanNode = (TableScanNode) getChildrenNode(topKNode1, 3);
    assertEquals(4, tableScanNode.getDeviceEntries().size());
    assertTableScan(
        tableScanNode,
        deviceEntries1,
        expectedOrdering,
        expectedPushDownLimit,
        expectedPushDownOffset,
        isPushLimitToEachDevice);

    // IdentitySink - TopK - Project - Filter - TableScan
    identitySinkNode =
        (IdentitySinkNode) distributedQueryPlan.getFragments().get(1).getPlanNodeTree();
    assertTrue(getChildrenNode(identitySinkNode, 1) instanceof TopKNode);
    assertTrue(getChildrenNode(identitySinkNode, 2) instanceof ProjectNode);
    assertTrue(getChildrenNode(identitySinkNode, 3) instanceof FilterNode);
    tableScanNode = (TableScanNode) getChildrenNode(identitySinkNode, 4);
    assertEquals(2, tableScanNode.getDeviceEntries().size());

    assertTableScan(
        tableScanNode,
        deviceEntries2,
        expectedOrdering,
        expectedPushDownLimit,
        expectedPushDownOffset,
        isPushLimitToEachDevice);
  }

  public void assertTopKNoFilter(
      List<String> deviceEntries1,
      List<String> deviceEntries2,
      Ordering expectedOrdering,
      long expectedPushDownLimit,
      long expectedPushDownOffset,
      boolean isPushLimitToEachDevice) {
    // LogicalPlan: `Output - Offset - Project - TopK - Project -  TableScan`
    assertTrue(rootNode instanceof OutputNode);
    assertTrue(getChildrenNode(rootNode, 1) instanceof OffsetNode);
    assertTrue(getChildrenNode(rootNode, 2) instanceof ProjectNode);
    assertTrue(getChildrenNode(rootNode, 3) instanceof TopKNode);
    assertTrue(getChildrenNode(rootNode, 4) instanceof ProjectNode);
    assertTrue(getChildrenNode(rootNode, 5) instanceof TableScanNode);
    tableScanNode = (TableScanNode) getChildrenNode(rootNode, 5);
    assertEquals("testdb.table1", tableScanNode.getQualifiedObjectName().toString());
    assertEquals(8, tableScanNode.getAssignments().size());
    assertEquals(6, tableScanNode.getDeviceEntries().size());
    assertEquals(5, tableScanNode.getIdAndAttributeIndexMap().size());
    assertEquals(expectedPushDownLimit, tableScanNode.getPushDownLimit());
    assertEquals(expectedPushDownOffset, tableScanNode.getPushDownOffset());
    assertEquals(isPushLimitToEachDevice, tableScanNode.isPushLimitToEachDevice());

    // DistributePlan `Identity - Output - Offset - Project - TopK - {Exchange + TopK + Exchange}
    distributionPlanner = new TableDistributedPlanner(actualAnalysis, logicalQueryPlan, context);
    distributedQueryPlan = distributionPlanner.plan();
    assertEquals(3, distributedQueryPlan.getFragments().size());
    IdentitySinkNode identitySinkNode =
        (IdentitySinkNode) distributedQueryPlan.getFragments().get(0).getPlanNodeTree();
    assertTrue(getChildrenNode(identitySinkNode, 1) instanceof OutputNode);
    OutputNode outputNode = (OutputNode) getChildrenNode(identitySinkNode, 1);
    assertTrue(getChildrenNode(outputNode, 1) instanceof OffsetNode);
    assertTrue(getChildrenNode(outputNode, 3) instanceof TopKNode);
    TopKNode topKNode = (TopKNode) getChildrenNode(outputNode, 3);
    assertTrue(topKNode.getChildren().get(0) instanceof ExchangeNode);
    assertTrue(topKNode.getChildren().get(1) instanceof TopKNode);
    assertTrue(topKNode.getChildren().get(2) instanceof ExchangeNode);
    TopKNode topKNode1 = (TopKNode) topKNode.getChildren().get(1);
    assertTrue(getChildrenNode(topKNode1, 1) instanceof ProjectNode);
    tableScanNode = (TableScanNode) getChildrenNode(topKNode1, 2);
    assertEquals(4, tableScanNode.getDeviceEntries().size());
    assertTableScan(
        tableScanNode,
        deviceEntries1,
        expectedOrdering,
        expectedPushDownLimit,
        expectedPushDownOffset,
        isPushLimitToEachDevice);

    // IdentitySink - TopK - Project - TableScan
    identitySinkNode =
        (IdentitySinkNode) distributedQueryPlan.getFragments().get(1).getPlanNodeTree();
    assertTrue(getChildrenNode(identitySinkNode, 1) instanceof TopKNode);
    assertTrue(getChildrenNode(identitySinkNode, 2) instanceof ProjectNode);
    tableScanNode = (TableScanNode) getChildrenNode(identitySinkNode, 3);
    assertEquals(2, tableScanNode.getDeviceEntries().size());
    assertTableScan(
        tableScanNode,
        deviceEntries2,
        expectedOrdering,
        expectedPushDownLimit,
        expectedPushDownOffset,
        isPushLimitToEachDevice);
  }

  public void assertStreamSortWithFilter(
      Ordering expectedOrdering,
      long expectedPushDownLimit,
      long expectedPushDownOffset,
      boolean isPushLimitToEachDevice) {}

  public void assertTableScan(
      TableScanNode tableScanNode,
      List<String> deviceEntries,
      Ordering ordering,
      long pushLimit,
      long pushOffset,
      boolean pushLimitToEachDevice) {
    assertEquals(
        deviceEntries,
        tableScanNode.getDeviceEntries().stream()
            .map(d -> d.getDeviceID().toString())
            .collect(Collectors.toList()));
    assertEquals(ordering, tableScanNode.getScanOrder());
    assertEquals(pushLimit, tableScanNode.getPushDownLimit());
    assertEquals(pushOffset, tableScanNode.getPushDownOffset());
    assertEquals(pushLimitToEachDevice, tableScanNode.isPushLimitToEachDevice());
  }
}
