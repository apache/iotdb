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
import org.apache.iotdb.db.queryengine.plan.relational.planner.distribute.TableDistributionPlanner;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.FilterNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.LimitNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.MergeSortNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.OffsetNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.OutputNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.ProjectNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.SortNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.StreamSortNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.TableScanNode;

import org.junit.Test;

import java.time.ZoneId;
import java.util.Arrays;
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
  LogicalPlanner logicalPlanner;
  LogicalQueryPlan logicalQueryPlan;
  PlanNode rootNode;
  OutputNode outputNode;
  PlanNode mergeSortNode;
  ProjectNode projectNode;
  StreamSortNode streamSortNode;
  TableDistributionPlanner distributionPlanner;
  DistributedQueryPlan distributedQueryPlan;
  TableScanNode tableScanNode;

  /*
   * order by time, others, some_ids
   *
   * IdentitySinkNode-33
   *   └──OutputNode-8
   *       └──LimitNode-7
   *           └──OffsetNode-6
   *             └──ProjectNode
   *               └──MergeSortNode-25
   *                   ├──ExchangeNode-29: [SourceAddress:192.0.12.1/test_query.2.0/31]
   *                   ├──SortNode-27
   *                   │   └──ProjectNode-23
   *                   │           └──FilterNode-17
   *                   │               └──TableScanNode-14
   *                   └──ExchangeNode-30: [SourceAddress:192.0.10.1/test_query.3.0/32]
   *
   * IdentitySinkNode-31
   *   └──SortNode-26
   *       └──ProjectNode-22
   *               └──FilterNode-16
   *                   └──TableScanNode-13
   *
   * IdentitySinkNode-31
   *   └──SortNode-26
   *           └──ProjectNode-19
   *               └──FilterNode-16
   *                   └──TableScanNode-13
   */
  @Test
  public void timeOthersSomeIDColumnSortTest() {
    sql =
        "SELECT time, tag3, substring(tag1, 1), cast(s2 as double), s2+s3, attr1 FROM table1 "
            + "where s1>1 and s1+s3>0 and cast(s1 as double)>1.0 order by time desc, s1+s2 asc, tag2 asc, tag1 desc offset 5 limit 10";
    context = new MPPQueryContext(sql, queryId, sessionInfo, null, null);
    actualAnalysis = analyzeSQL(sql, metadata);
    logicalQueryPlan =
        new LogicalPlanner(context, metadata, sessionInfo, WarningCollector.NOOP)
            .plan(actualAnalysis);
    rootNode = logicalQueryPlan.getRootNode();

    assertTrue(rootNode instanceof OutputNode);
    assertTrue(getChildrenNode(rootNode, 1) instanceof OffsetNode);
    assertTrue(getChildrenNode(rootNode, 2) instanceof LimitNode);
    assertTrue(getChildrenNode(rootNode, 3) instanceof ProjectNode);
    SortNode sortNode =
        (SortNode)
            rootNode
                .getChildren()
                .get(0)
                .getChildren()
                .get(0)
                .getChildren()
                .get(0)
                .getChildren()
                .get(0);
    // TODO(beyyes) merge parent and child ProjectNode into one, parent contains all children
    assertTrue(sortNode.getChildren().get(0) instanceof ProjectNode);
    assertTrue(sortNode.getChildren().get(0).getChildren().get(0) instanceof FilterNode);
    tableScanNode =
        (TableScanNode) sortNode.getChildren().get(0).getChildren().get(0).getChildren().get(0);
    assertEquals("testdb.table1", tableScanNode.getQualifiedObjectName().toString());
    assertEquals(8, tableScanNode.getAssignments().size());
    assertEquals(6, tableScanNode.getDeviceEntries().size());
    assertEquals(5, tableScanNode.getIdAndAttributeIndexMap().size());

    // Output - Limit - Offset -Project - MergeSort - Sort - Project - Filter - TableScan
    distributionPlanner = new TableDistributionPlanner(actualAnalysis, logicalQueryPlan, context);
    distributedQueryPlan = distributionPlanner.plan();
    assertEquals(3, distributedQueryPlan.getFragments().size());
    assertTrue(
        distributedQueryPlan.getFragments().get(0).getPlanNodeTree().getChildren().get(0)
            instanceof OutputNode);
    OutputNode outputNode =
        (OutputNode)
            distributedQueryPlan.getFragments().get(0).getPlanNodeTree().getChildren().get(0);
    assertTrue(getChildrenNode(outputNode, 1) instanceof OffsetNode);
    assertTrue(getChildrenNode(outputNode, 2) instanceof LimitNode);
    assertTrue(
        outputNode.getChildren().get(0).getChildren().get(0).getChildren().get(0)
            instanceof ProjectNode);
    MergeSortNode mergeSortNode =
        (MergeSortNode)
            outputNode
                .getChildren()
                .get(0)
                .getChildren()
                .get(0)
                .getChildren()
                .get(0)
                .getChildren()
                .get(0);
    assertTrue(mergeSortNode.getChildren().get(0) instanceof ExchangeNode);
    assertTrue(mergeSortNode.getChildren().get(1) instanceof SortNode);
    assertTrue(mergeSortNode.getChildren().get(2) instanceof ExchangeNode);
    sortNode = (SortNode) mergeSortNode.getChildren().get(1);
    assertTrue(sortNode.getChildren().get(0) instanceof ProjectNode);
    assertTrue(sortNode.getChildren().get(0).getChildren().get(0) instanceof FilterNode);
    tableScanNode =
        (TableScanNode) sortNode.getChildren().get(0).getChildren().get(0).getChildren().get(0);
    assertEquals(4, tableScanNode.getDeviceEntries().size());
    assertEquals(
        Arrays.asList(
            "table1.shanghai.B3.YY",
            "table1.shenzhen.B1.XX",
            "table1.shenzhen.B2.ZZ",
            "table1.shanghai.A3.YY"),
        tableScanNode.getDeviceEntries().stream()
            .map(d -> d.getDeviceID().toString())
            .collect(Collectors.toList()));
    assertEquals(DESC, tableScanNode.getScanOrder());
    assertTrue(tableScanNode.getPushDownLimit() == 0 && tableScanNode.getPushDownOffset() == 0);

    // IdentitySink - Sort - Project - Filter - TableScan
    assertTrue(
        distributedQueryPlan.getFragments().get(1).getPlanNodeTree() instanceof IdentitySinkNode);
    assertTrue(
        distributedQueryPlan.getFragments().get(1).getPlanNodeTree().getChildren().get(0)
            instanceof SortNode);
    assertTrue(
        distributedQueryPlan
                .getFragments()
                .get(1)
                .getPlanNodeTree()
                .getChildren()
                .get(0)
                .getChildren()
                .get(0)
            instanceof ProjectNode);
    assertTrue(
        distributedQueryPlan
                .getFragments()
                .get(1)
                .getPlanNodeTree()
                .getChildren()
                .get(0)
                .getChildren()
                .get(0)
                .getChildren()
                .get(0)
            instanceof FilterNode);
    tableScanNode =
        (TableScanNode)
            distributedQueryPlan
                .getFragments()
                .get(1)
                .getPlanNodeTree()
                .getChildren()
                .get(0)
                .getChildren()
                .get(0)
                .getChildren()
                .get(0)
                .getChildren()
                .get(0);
    assertEquals(2, tableScanNode.getDeviceEntries().size());
    assertEquals(
        Arrays.asList("table1.shenzhen.B1.XX", "table1.shenzhen.B2.ZZ"),
        tableScanNode.getDeviceEntries().stream()
            .map(d -> d.getDeviceID().toString())
            .collect(Collectors.toList()));
    assertEquals(DESC, tableScanNode.getScanOrder());
    assertTrue(tableScanNode.getPushDownLimit() == 0 && tableScanNode.getPushDownOffset() == 0);

    // TODO(beyyes) add only one device entry optimization and verifies
  }

  // order by time, others, all_ids
  @Test
  public void timeOthersAllIDColumnSortTest() {
    sql =
        "SELECT time, tag3, tag1, cast(s2 as double), s2+s3, attr1 FROM table1 "
            + "where s1>1 and s1+s3>0 and cast(s1 as double)>1.0 order by time desc, s1+s2 asc, tag2 asc, tag3 desc, tag1 desc offset 5 limit 10";
    context = new MPPQueryContext(sql, queryId, sessionInfo, null, null);
    actualAnalysis = analyzeSQL(sql, metadata);
    logicalQueryPlan =
        new LogicalPlanner(context, metadata, sessionInfo, WarningCollector.NOOP)
            .plan(actualAnalysis);
    rootNode = logicalQueryPlan.getRootNode();

    // Output - Offset - Limit - Project - Sort - Project - Filter - TableScan
    assertTrue(rootNode instanceof OutputNode);
    assertTrue(getChildrenNode(rootNode, 1) instanceof OffsetNode);
    assertTrue(getChildrenNode(rootNode, 2) instanceof LimitNode);
    assertTrue(
        rootNode.getChildren().get(0).getChildren().get(0).getChildren().get(0)
            instanceof ProjectNode);
    SortNode sortNode =
        (SortNode)
            rootNode
                .getChildren()
                .get(0)
                .getChildren()
                .get(0)
                .getChildren()
                .get(0)
                .getChildren()
                .get(0);
    assertTrue(sortNode.getChildren().get(0) instanceof ProjectNode);
    assertTrue(sortNode.getChildren().get(0).getChildren().get(0) instanceof FilterNode);
    tableScanNode =
        (TableScanNode) sortNode.getChildren().get(0).getChildren().get(0).getChildren().get(0);
    assertEquals("testdb.table1", tableScanNode.getQualifiedObjectName().toString());
    assertEquals(8, tableScanNode.getAssignments().size());
    assertEquals(6, tableScanNode.getDeviceEntries().size());
    assertEquals(5, tableScanNode.getIdAndAttributeIndexMap().size());
    assertTrue(tableScanNode.getPushDownLimit() == 0 && tableScanNode.getPushDownOffset() == 0);

    // Output - Offset - Limit - Project - MergeSort - Sort - Project - Filter - TableScan
    distributionPlanner = new TableDistributionPlanner(actualAnalysis, logicalQueryPlan, context);
    distributedQueryPlan = distributionPlanner.plan();
    assertEquals(3, distributedQueryPlan.getFragments().size());
    assertTrue(
        distributedQueryPlan.getFragments().get(0).getPlanNodeTree().getChildren().get(0)
            instanceof OutputNode);
    OutputNode outputNode =
        (OutputNode)
            distributedQueryPlan.getFragments().get(0).getPlanNodeTree().getChildren().get(0);
    assertTrue(getChildrenNode(rootNode, 1) instanceof OffsetNode);
    assertTrue(getChildrenNode(rootNode, 2) instanceof LimitNode);
    assertTrue(
        outputNode.getChildren().get(0).getChildren().get(0).getChildren().get(0)
            instanceof ProjectNode);
    MergeSortNode mergeSortNode =
        (MergeSortNode)
            outputNode
                .getChildren()
                .get(0)
                .getChildren()
                .get(0)
                .getChildren()
                .get(0)
                .getChildren()
                .get(0);
    assertTrue(mergeSortNode.getChildren().get(0) instanceof ExchangeNode);
    assertTrue(mergeSortNode.getChildren().get(1) instanceof SortNode);
    assertTrue(mergeSortNode.getChildren().get(2) instanceof ExchangeNode);
    sortNode = (SortNode) mergeSortNode.getChildren().get(1);
    assertTrue(sortNode.getChildren().get(0) instanceof ProjectNode);
    assertTrue(sortNode.getChildren().get(0).getChildren().get(0) instanceof FilterNode);
    assertTrue(
        sortNode.getChildren().get(0).getChildren().get(0).getChildren().get(0)
            instanceof TableScanNode);
    tableScanNode =
        (TableScanNode) sortNode.getChildren().get(0).getChildren().get(0).getChildren().get(0);
    assertEquals(4, tableScanNode.getDeviceEntries().size());
    assertEquals(
        Arrays.asList(
            "table1.shanghai.B3.YY",
            "table1.shenzhen.B1.XX",
            "table1.shenzhen.B2.ZZ",
            "table1.shanghai.A3.YY"),
        tableScanNode.getDeviceEntries().stream()
            .map(d -> d.getDeviceID().toString())
            .collect(Collectors.toList()));
    assertEquals(DESC, tableScanNode.getScanOrder());
    assertTrue(tableScanNode.getPushDownLimit() == 0 && tableScanNode.getPushDownOffset() == 0);

    // IdentitySink - Sort - Project - Filter - TableScan
    assertTrue(
        distributedQueryPlan.getFragments().get(1).getPlanNodeTree() instanceof IdentitySinkNode);
    assertTrue(
        distributedQueryPlan.getFragments().get(1).getPlanNodeTree().getChildren().get(0)
            instanceof SortNode);
    assertTrue(
        distributedQueryPlan
                .getFragments()
                .get(1)
                .getPlanNodeTree()
                .getChildren()
                .get(0)
                .getChildren()
                .get(0)
            instanceof ProjectNode);
    assertTrue(
        distributedQueryPlan
                .getFragments()
                .get(1)
                .getPlanNodeTree()
                .getChildren()
                .get(0)
                .getChildren()
                .get(0)
                .getChildren()
                .get(0)
            instanceof FilterNode);
    tableScanNode =
        (TableScanNode)
            distributedQueryPlan
                .getFragments()
                .get(1)
                .getPlanNodeTree()
                .getChildren()
                .get(0)
                .getChildren()
                .get(0)
                .getChildren()
                .get(0)
                .getChildren()
                .get(0);
    assertEquals(2, tableScanNode.getDeviceEntries().size());
    assertEquals(
        Arrays.asList("table1.shenzhen.B1.XX", "table1.shenzhen.B2.ZZ"),
        tableScanNode.getDeviceEntries().stream()
            .map(d -> d.getDeviceID().toString())
            .collect(Collectors.toList()));
    assertEquals(DESC, tableScanNode.getScanOrder());
    assertTrue(tableScanNode.getPushDownLimit() == 0 && tableScanNode.getPushDownOffset() == 0);
  }

  // order by time, some_ids, others
  @Test
  public void timeSomeIDColumnOthersSortTest() {
    sql =
        "SELECT time, tag3, tag1, cast(s2 as double), s2+s3, attr1 FROM table1 "
            + "where s1>1 and s1+s3>0 and cast(s1 as double)>1.0 order by time desc, tag2 asc, tag3 desc, s1+s2 desc offset 5 limit 10";
    context = new MPPQueryContext(sql, queryId, sessionInfo, null, null);
    actualAnalysis = analyzeSQL(sql, metadata);
    logicalQueryPlan =
        new LogicalPlanner(context, metadata, sessionInfo, WarningCollector.NOOP)
            .plan(actualAnalysis);
    rootNode = logicalQueryPlan.getRootNode();

    // Output - Offset - Limit - Project - Sort - Project - Filter - TableScan
    assertTrue(rootNode instanceof OutputNode);
    assertTrue(getChildrenNode(rootNode, 1) instanceof OffsetNode);
    assertTrue(getChildrenNode(rootNode, 2) instanceof LimitNode);
    assertTrue(
        rootNode.getChildren().get(0).getChildren().get(0).getChildren().get(0)
            instanceof ProjectNode);
    SortNode sortNode =
        (SortNode)
            rootNode
                .getChildren()
                .get(0)
                .getChildren()
                .get(0)
                .getChildren()
                .get(0)
                .getChildren()
                .get(0);
    assertTrue(sortNode.getChildren().get(0) instanceof ProjectNode);
    assertTrue(sortNode.getChildren().get(0).getChildren().get(0) instanceof FilterNode);
    tableScanNode =
        (TableScanNode) sortNode.getChildren().get(0).getChildren().get(0).getChildren().get(0);
    assertEquals("testdb.table1", tableScanNode.getQualifiedObjectName().toString());
    assertEquals(8, tableScanNode.getAssignments().size());
    assertEquals(6, tableScanNode.getDeviceEntries().size());
    assertEquals(5, tableScanNode.getIdAndAttributeIndexMap().size());
    assertTrue(tableScanNode.getPushDownLimit() == 0 && tableScanNode.getPushDownOffset() == 0);

    distributionPlanner = new TableDistributionPlanner(actualAnalysis, logicalQueryPlan, context);
    distributedQueryPlan = distributionPlanner.plan();
    assertEquals(3, distributedQueryPlan.getFragments().size());
    assertTrue(
        distributedQueryPlan.getFragments().get(0).getPlanNodeTree().getChildren().get(0)
            instanceof OutputNode);
    OutputNode outputNode =
        (OutputNode)
            distributedQueryPlan.getFragments().get(0).getPlanNodeTree().getChildren().get(0);
    assertTrue(getChildrenNode(outputNode, 1) instanceof OffsetNode);
    assertTrue(getChildrenNode(outputNode, 2) instanceof LimitNode);
    assertTrue(
        outputNode.getChildren().get(0).getChildren().get(0).getChildren().get(0)
            instanceof ProjectNode);
    MergeSortNode mergeSortNode =
        (MergeSortNode)
            outputNode
                .getChildren()
                .get(0)
                .getChildren()
                .get(0)
                .getChildren()
                .get(0)
                .getChildren()
                .get(0);
    assertTrue(mergeSortNode.getChildren().get(0) instanceof ExchangeNode);
    assertTrue(mergeSortNode.getChildren().get(1) instanceof SortNode);
    assertTrue(mergeSortNode.getChildren().get(2) instanceof ExchangeNode);
    sortNode = (SortNode) mergeSortNode.getChildren().get(1);
    assertTrue(sortNode.getChildren().get(0) instanceof ProjectNode);
    assertTrue(sortNode.getChildren().get(0).getChildren().get(0) instanceof FilterNode);
    tableScanNode =
        (TableScanNode) sortNode.getChildren().get(0).getChildren().get(0).getChildren().get(0);
    assertEquals(4, tableScanNode.getDeviceEntries().size());
    assertEquals(
        Arrays.asList(
            "table1.shanghai.B3.YY",
            "table1.shenzhen.B1.XX",
            "table1.shenzhen.B2.ZZ",
            "table1.shanghai.A3.YY"),
        tableScanNode.getDeviceEntries().stream()
            .map(d -> d.getDeviceID().toString())
            .collect(Collectors.toList()));
    assertEquals(DESC, tableScanNode.getScanOrder());
    assertTrue(tableScanNode.getPushDownLimit() == 0 && tableScanNode.getPushDownOffset() == 0);

    // IdentitySinkNode - SortNode - ProjectNode - FilterNode - TableScanNode
    assertTrue(
        distributedQueryPlan.getFragments().get(1).getPlanNodeTree() instanceof IdentitySinkNode);
    assertTrue(
        distributedQueryPlan.getFragments().get(1).getPlanNodeTree().getChildren().get(0)
            instanceof SortNode);
    assertTrue(
        distributedQueryPlan
                .getFragments()
                .get(1)
                .getPlanNodeTree()
                .getChildren()
                .get(0)
                .getChildren()
                .get(0)
            instanceof ProjectNode);
    assertTrue(
        distributedQueryPlan
                .getFragments()
                .get(1)
                .getPlanNodeTree()
                .getChildren()
                .get(0)
                .getChildren()
                .get(0)
                .getChildren()
                .get(0)
            instanceof FilterNode);
    tableScanNode =
        (TableScanNode)
            distributedQueryPlan
                .getFragments()
                .get(1)
                .getPlanNodeTree()
                .getChildren()
                .get(0)
                .getChildren()
                .get(0)
                .getChildren()
                .get(0)
                .getChildren()
                .get(0);
    assertEquals(2, tableScanNode.getDeviceEntries().size());
    assertEquals(
        Arrays.asList("table1.shenzhen.B1.XX", "table1.shenzhen.B2.ZZ"),
        tableScanNode.getDeviceEntries().stream()
            .map(d -> d.getDeviceID().toString())
            .collect(Collectors.toList()));
    assertEquals(DESC, tableScanNode.getScanOrder());
    assertTrue(tableScanNode.getPushDownLimit() == 0 && tableScanNode.getPushDownOffset() == 0);
  }

  // order by time, all_ids, others
  @Test
  public void timeAllIDColumnOthersSortTest() {
    sql =
        "SELECT time, tag3, tag1, cast(s2 as double), s2+s3, attr1 FROM table1 "
            + "where s1>1 and s1+s3>0 and cast(s1 as double)>1.0 order by time desc, tag2 asc, tag3 desc, tag1 asc, s1+s2 desc offset 5 limit 10";
    context = new MPPQueryContext(sql, queryId, sessionInfo, null, null);
    actualAnalysis = analyzeSQL(sql, metadata);
    logicalQueryPlan =
        new LogicalPlanner(context, metadata, sessionInfo, WarningCollector.NOOP)
            .plan(actualAnalysis);
    rootNode = logicalQueryPlan.getRootNode();

    // Output - Limit - Offset - Project - Sort - Project - Filter - TableScan
    assertTrue(rootNode instanceof OutputNode);
    assertTrue(getChildrenNode(rootNode, 1) instanceof OffsetNode);
    assertTrue(getChildrenNode(rootNode, 2) instanceof LimitNode);
    assertTrue(
        rootNode.getChildren().get(0).getChildren().get(0).getChildren().get(0)
            instanceof ProjectNode);
    SortNode sortNode =
        (SortNode)
            rootNode
                .getChildren()
                .get(0)
                .getChildren()
                .get(0)
                .getChildren()
                .get(0)
                .getChildren()
                .get(0);
    assertTrue(sortNode.getChildren().get(0) instanceof ProjectNode);
    assertTrue(sortNode.getChildren().get(0).getChildren().get(0) instanceof FilterNode);
    tableScanNode =
        (TableScanNode) sortNode.getChildren().get(0).getChildren().get(0).getChildren().get(0);
    assertEquals("testdb.table1", tableScanNode.getQualifiedObjectName().toString());
    assertEquals(8, tableScanNode.getAssignments().size());
    assertEquals(6, tableScanNode.getDeviceEntries().size());
    assertEquals(5, tableScanNode.getIdAndAttributeIndexMap().size());

    // Output - Offset - Limit - Project - MergeSort - Sort - Project - Filter - TableScan
    distributionPlanner = new TableDistributionPlanner(actualAnalysis, logicalQueryPlan, context);
    distributedQueryPlan = distributionPlanner.plan();
    assertEquals(3, distributedQueryPlan.getFragments().size());
    assertTrue(
        distributedQueryPlan.getFragments().get(0).getPlanNodeTree().getChildren().get(0)
            instanceof OutputNode);
    OutputNode outputNode =
        (OutputNode)
            distributedQueryPlan.getFragments().get(0).getPlanNodeTree().getChildren().get(0);
    assertTrue(getChildrenNode(outputNode, 1) instanceof OffsetNode);
    assertTrue(getChildrenNode(outputNode, 2) instanceof LimitNode);
    assertTrue(
        outputNode.getChildren().get(0).getChildren().get(0).getChildren().get(0)
            instanceof ProjectNode);
    MergeSortNode mergeSortNode =
        (MergeSortNode)
            outputNode
                .getChildren()
                .get(0)
                .getChildren()
                .get(0)
                .getChildren()
                .get(0)
                .getChildren()
                .get(0);
    assertTrue(mergeSortNode.getChildren().get(0) instanceof ExchangeNode);
    assertTrue(mergeSortNode.getChildren().get(1) instanceof SortNode);
    assertTrue(mergeSortNode.getChildren().get(2) instanceof ExchangeNode);
    sortNode = (SortNode) mergeSortNode.getChildren().get(1);
    assertTrue(sortNode.getChildren().get(0) instanceof ProjectNode);
    assertTrue(sortNode.getChildren().get(0).getChildren().get(0) instanceof FilterNode);
    tableScanNode =
        (TableScanNode) sortNode.getChildren().get(0).getChildren().get(0).getChildren().get(0);
    assertEquals(4, tableScanNode.getDeviceEntries().size());
    assertEquals(
        Arrays.asList(
            "table1.shanghai.B3.YY",
            "table1.shenzhen.B1.XX",
            "table1.shenzhen.B2.ZZ",
            "table1.shanghai.A3.YY"),
        tableScanNode.getDeviceEntries().stream()
            .map(d -> d.getDeviceID().toString())
            .collect(Collectors.toList()));
    assertEquals(DESC, tableScanNode.getScanOrder());

    // IdentitySinkNode - SortNode - ProjectNode - FilterNode - TableScanNode
    assertTrue(
        distributedQueryPlan.getFragments().get(1).getPlanNodeTree() instanceof IdentitySinkNode);
    assertTrue(
        distributedQueryPlan.getFragments().get(1).getPlanNodeTree().getChildren().get(0)
            instanceof SortNode);
    assertTrue(
        distributedQueryPlan
                .getFragments()
                .get(1)
                .getPlanNodeTree()
                .getChildren()
                .get(0)
                .getChildren()
                .get(0)
            instanceof ProjectNode);
    assertTrue(
        distributedQueryPlan
                .getFragments()
                .get(1)
                .getPlanNodeTree()
                .getChildren()
                .get(0)
                .getChildren()
                .get(0)
                .getChildren()
                .get(0)
            instanceof FilterNode);
    assertTrue(
        distributedQueryPlan
                .getFragments()
                .get(1)
                .getPlanNodeTree()
                .getChildren()
                .get(0)
                .getChildren()
                .get(0)
                .getChildren()
                .get(0)
                .getChildren()
                .get(0)
            instanceof TableScanNode);
    tableScanNode =
        (TableScanNode)
            distributedQueryPlan
                .getFragments()
                .get(1)
                .getPlanNodeTree()
                .getChildren()
                .get(0)
                .getChildren()
                .get(0)
                .getChildren()
                .get(0)
                .getChildren()
                .get(0);
    assertEquals(2, tableScanNode.getDeviceEntries().size());
    assertEquals(
        Arrays.asList("table1.shenzhen.B1.XX", "table1.shenzhen.B2.ZZ"),
        tableScanNode.getDeviceEntries().stream()
            .map(d -> d.getDeviceID().toString())
            .collect(Collectors.toList()));
    assertEquals(DESC, tableScanNode.getScanOrder());
  }

  // order by some_ids, time, others
  @Test
  public void someIDColumnTimeOthersSortTest() {
    sql =
        "SELECT time, tag3, tag1, cast(s2 as double), s2+s3, attr1 FROM table1 "
            + "where s1>1 and s1+s3>0 and cast(s1 as double)>1.0 order by tag2 desc, tag3 asc, time desc, s1+s2 desc offset 5 limit 10";
    context = new MPPQueryContext(sql, queryId, sessionInfo, null, null);
    actualAnalysis = analyzeSQL(sql, metadata);
    logicalQueryPlan =
        new LogicalPlanner(context, metadata, sessionInfo, WarningCollector.NOOP)
            .plan(actualAnalysis);
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

    // DistributePlan: `Output-Offset-Limit-Project-MergeSort-StreamSort-Project-Filter-TableScan`
    distributionPlanner = new TableDistributionPlanner(actualAnalysis, logicalQueryPlan, context);
    distributedQueryPlan = distributionPlanner.plan();
    assertEquals(3, distributedQueryPlan.getFragments().size());
    outputNode =
        (OutputNode)
            distributedQueryPlan.getFragments().get(0).getPlanNodeTree().getChildren().get(0);
    assertTrue(getChildrenNode(outputNode, 1) instanceof OffsetNode);
    assertTrue(getChildrenNode(outputNode, 2) instanceof LimitNode);
    assertTrue(getChildrenNode(outputNode, 3) instanceof ProjectNode);
    MergeSortNode mergeSortNode = (MergeSortNode) getChildrenNode(outputNode, 4);
    assertTrue(mergeSortNode.getChildren().get(0) instanceof ExchangeNode);
    assertTrue(mergeSortNode.getChildren().get(1) instanceof StreamSortNode);
    assertTrue(mergeSortNode.getChildren().get(2) instanceof ExchangeNode);
    streamSortNode = (StreamSortNode) mergeSortNode.getChildren().get(1);
    assertTrue(getChildrenNode(streamSortNode, 1) instanceof ProjectNode);
    assertTrue(getChildrenNode(streamSortNode, 2) instanceof FilterNode);
    tableScanNode = (TableScanNode) getChildrenNode(streamSortNode, 3);
    assertEquals(4, tableScanNode.getDeviceEntries().size());
    assertEquals(
        Arrays.asList(
            "table1.shanghai.B3.YY",
            "table1.shenzhen.B2.ZZ",
            "table1.shenzhen.B1.XX",
            "table1.shanghai.A3.YY"),
        tableScanNode.getDeviceEntries().stream()
            .map(d -> d.getDeviceID().toString())
            .collect(Collectors.toList()));
    assertEquals(DESC, tableScanNode.getScanOrder());

    // DistributePlan: `IdentitySink - StreamSort - Project - Filter - TableScan`
    streamSortNode =
        (StreamSortNode)
            distributedQueryPlan.getFragments().get(1).getPlanNodeTree().getChildren().get(0);
    assertTrue(getChildrenNode(streamSortNode, 1) instanceof ProjectNode);
    assertTrue(getChildrenNode(streamSortNode, 2) instanceof FilterNode);
    tableScanNode = (TableScanNode) getChildrenNode(streamSortNode, 3);
    assertEquals(2, tableScanNode.getDeviceEntries().size());
    assertEquals(
        Arrays.asList("table1.shenzhen.B2.ZZ", "table1.shenzhen.B1.XX"),
        tableScanNode.getDeviceEntries().stream()
            .map(d -> d.getDeviceID().toString())
            .collect(Collectors.toList()));
    assertEquals(DESC, tableScanNode.getScanOrder());
  }

  // order by all_ids, time, others
  @Test
  public void allIDColumnTimeOthersSortTest() {
    sql =
        "SELECT time, tag3, tag1, cast(s2 as double), s2+s3, attr1 FROM table1 "
            + "where s1>1 and s1+s3>0 and cast(s1 as double)>1.0 order by tag2 desc, tag1 desc, tag3 asc, time desc, s1+s2 desc offset 5 limit 10";
    context = new MPPQueryContext(sql, queryId, sessionInfo, null, null);
    actualAnalysis = analyzeSQL(sql, metadata);
    logicalQueryPlan =
        new LogicalPlanner(context, metadata, sessionInfo, WarningCollector.NOOP)
            .plan(actualAnalysis);
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
    assertEquals(5, tableScanNode.getIdAndAttributeIndexMap().size());

    // DistributePlan: `Output-Offset-Limit-Project-MergeSort-Project-Filter-TableScan`
    distributionPlanner = new TableDistributionPlanner(actualAnalysis, logicalQueryPlan, context);
    distributedQueryPlan = distributionPlanner.plan();
    assertEquals(3, distributedQueryPlan.getFragments().size());
    outputNode =
        (OutputNode)
            distributedQueryPlan.getFragments().get(0).getPlanNodeTree().getChildren().get(0);
    assertTrue(getChildrenNode(outputNode, 1) instanceof OffsetNode);
    assertTrue(getChildrenNode(outputNode, 2) instanceof LimitNode);
    assertTrue(getChildrenNode(outputNode, 3) instanceof ProjectNode);
    MergeSortNode mergeSortNode = (MergeSortNode) getChildrenNode(outputNode, 4);
    assertTrue(mergeSortNode.getChildren().get(0) instanceof ExchangeNode);
    assertTrue(mergeSortNode.getChildren().get(1) instanceof ProjectNode);
    assertTrue(mergeSortNode.getChildren().get(2) instanceof ExchangeNode);
    projectNode = (ProjectNode) mergeSortNode.getChildren().get(1);
    assertTrue(getChildrenNode(projectNode, 1) instanceof FilterNode);
    tableScanNode = (TableScanNode) getChildrenNode(projectNode, 2);
    assertEquals(4, tableScanNode.getDeviceEntries().size());
    assertEquals(
        Arrays.asList(
            "table1.shanghai.B3.YY",
            "table1.shenzhen.B2.ZZ",
            "table1.shenzhen.B1.XX",
            "table1.shanghai.A3.YY"),
        tableScanNode.getDeviceEntries().stream()
            .map(d -> d.getDeviceID().toString())
            .collect(Collectors.toList()));
    assertEquals(DESC, tableScanNode.getScanOrder());

    // DistributePlan: `IdentitySink-Project-Filter-TableScan`
    projectNode =
        (ProjectNode)
            distributedQueryPlan.getFragments().get(1).getPlanNodeTree().getChildren().get(0);
    assertTrue(getChildrenNode(projectNode, 1) instanceof FilterNode);
    tableScanNode = (TableScanNode) getChildrenNode(projectNode, 2);
    assertEquals(2, tableScanNode.getDeviceEntries().size());
    assertEquals(
        Arrays.asList("table1.shenzhen.B2.ZZ", "table1.shenzhen.B1.XX"),
        tableScanNode.getDeviceEntries().stream()
            .map(d -> d.getDeviceID().toString())
            .collect(Collectors.toList()));
    assertEquals(DESC, tableScanNode.getScanOrder());
  }

  // order by some_ids, others, time
  @Test
  public void someIDColumnOthersTimeSortTest() {
    sql =
        "SELECT time, tag3, tag1, cast(s2 as double), s2+s3, attr1 FROM table1 "
            + "where s1>1 and s1+s3>0 and cast(s1 as double)>1.0 order by tag2 desc, tag1 desc, s1+s2 desc, time desc offset 5 limit 10";
    context = new MPPQueryContext(sql, queryId, sessionInfo, null, null);
    actualAnalysis = analyzeSQL(sql, metadata);
    logicalQueryPlan =
        new LogicalPlanner(context, metadata, sessionInfo, WarningCollector.NOOP)
            .plan(actualAnalysis);
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

    // DistributePlan: `Output-Offset-Limit-Project-MergeSort-StreamSort-Project-Filter-TableScan`
    distributionPlanner = new TableDistributionPlanner(actualAnalysis, logicalQueryPlan, context);
    distributedQueryPlan = distributionPlanner.plan();
    assertEquals(3, distributedQueryPlan.getFragments().size());
    outputNode =
        (OutputNode)
            distributedQueryPlan.getFragments().get(0).getPlanNodeTree().getChildren().get(0);
    assertTrue(getChildrenNode(outputNode, 1) instanceof OffsetNode);
    assertTrue(getChildrenNode(outputNode, 2) instanceof LimitNode);
    assertTrue(getChildrenNode(outputNode, 3) instanceof ProjectNode);
    MergeSortNode mergeSortNode = (MergeSortNode) getChildrenNode(outputNode, 4);
    assertTrue(mergeSortNode.getChildren().get(0) instanceof ExchangeNode);
    assertTrue(mergeSortNode.getChildren().get(1) instanceof StreamSortNode);
    assertTrue(mergeSortNode.getChildren().get(2) instanceof ExchangeNode);
    streamSortNode = (StreamSortNode) mergeSortNode.getChildren().get(1);
    assertTrue(getChildrenNode(streamSortNode, 1) instanceof ProjectNode);
    assertTrue(getChildrenNode(streamSortNode, 2) instanceof FilterNode);
    tableScanNode = (TableScanNode) getChildrenNode(streamSortNode, 3);
    assertEquals(4, tableScanNode.getDeviceEntries().size());
    assertEquals(
        Arrays.asList(
            "table1.shanghai.B3.YY",
            "table1.shenzhen.B2.ZZ",
            "table1.shenzhen.B1.XX",
            "table1.shanghai.A3.YY"),
        tableScanNode.getDeviceEntries().stream()
            .map(d -> d.getDeviceID().toString())
            .collect(Collectors.toList()));
    assertEquals(ASC, tableScanNode.getScanOrder());

    // DistributePlan: `IdentitySink - StreamSort - Project - Filter - TableScan`
    streamSortNode =
        (StreamSortNode)
            distributedQueryPlan.getFragments().get(1).getPlanNodeTree().getChildren().get(0);
    assertTrue(getChildrenNode(streamSortNode, 1) instanceof ProjectNode);
    assertTrue(getChildrenNode(streamSortNode, 2) instanceof FilterNode);
    tableScanNode = (TableScanNode) getChildrenNode(streamSortNode, 3);
    assertEquals(2, tableScanNode.getDeviceEntries().size());
    assertEquals(
        Arrays.asList("table1.shenzhen.B2.ZZ", "table1.shenzhen.B1.XX"),
        tableScanNode.getDeviceEntries().stream()
            .map(d -> d.getDeviceID().toString())
            .collect(Collectors.toList()));
    assertEquals(ASC, tableScanNode.getScanOrder());
  }

  // order by all_ids, others, time
  @Test
  public void allIDColumnOthersTimeSortTest() {
    sql =
        "SELECT time, tag3, tag1, cast(s2 as double), s2+s3, attr1 FROM table1 "
            + "where s1>1 and s1+s3>0 and cast(s1 as double)>1.0 order by tag2 desc, tag1 desc, tag3 asc, s1+s2 desc, time desc offset 5 limit 10";
    context = new MPPQueryContext(sql, queryId, sessionInfo, null, null);
    actualAnalysis = analyzeSQL(sql, metadata);
    logicalQueryPlan =
        new LogicalPlanner(context, metadata, sessionInfo, WarningCollector.NOOP)
            .plan(actualAnalysis);
    rootNode = logicalQueryPlan.getRootNode();

    context = new MPPQueryContext(sql, queryId, sessionInfo, null, null);
    actualAnalysis = analyzeSQL(sql, metadata);
    logicalQueryPlan =
        new LogicalPlanner(context, metadata, sessionInfo, WarningCollector.NOOP)
            .plan(actualAnalysis);
    rootNode = logicalQueryPlan.getRootNode();

    // LogicalPlan: `Output-Offset-Limit-Project-StreamSort-Project-Filter-TableScan`
    assertTrue(rootNode instanceof OutputNode);
    assertTrue(getChildrenNode(rootNode, 1) instanceof OffsetNode);
    assertTrue(getChildrenNode(rootNode, 2) instanceof LimitNode);
    assertTrue(getChildrenNode(rootNode, 3) instanceof ProjectNode);
    assertTrue(getChildrenNode(rootNode, 4) instanceof StreamSortNode);
    streamSortNode = (StreamSortNode) getChildrenNode(rootNode, 4);
    assertEquals(3, streamSortNode.getStreamCompareKeyEndIndex());
    assertTrue(getChildrenNode(streamSortNode, 1) instanceof ProjectNode);
    assertTrue(getChildrenNode(streamSortNode, 2) instanceof FilterNode);
    assertTrue(getChildrenNode(streamSortNode, 3) instanceof TableScanNode);
    tableScanNode = (TableScanNode) getChildrenNode(streamSortNode, 3);
    assertEquals("testdb.table1", tableScanNode.getQualifiedObjectName().toString());
    assertEquals(8, tableScanNode.getAssignments().size());
    assertEquals(6, tableScanNode.getDeviceEntries().size());
    assertEquals(5, tableScanNode.getIdAndAttributeIndexMap().size());

    // DistributePlan: `Output-Offset-Limit-Project-MergeSort-StreamSort-Project-Filter-TableScan`
    distributionPlanner = new TableDistributionPlanner(actualAnalysis, logicalQueryPlan, context);
    distributedQueryPlan = distributionPlanner.plan();
    assertEquals(3, distributedQueryPlan.getFragments().size());
    outputNode =
        (OutputNode)
            distributedQueryPlan.getFragments().get(0).getPlanNodeTree().getChildren().get(0);
    assertTrue(getChildrenNode(outputNode, 1) instanceof OffsetNode);
    assertTrue(getChildrenNode(outputNode, 2) instanceof LimitNode);
    assertTrue(getChildrenNode(outputNode, 3) instanceof ProjectNode);
    MergeSortNode mergeSortNode = (MergeSortNode) getChildrenNode(outputNode, 4);
    assertTrue(mergeSortNode.getChildren().get(0) instanceof ExchangeNode);
    assertTrue(mergeSortNode.getChildren().get(1) instanceof StreamSortNode);
    assertTrue(mergeSortNode.getChildren().get(2) instanceof ExchangeNode);
    streamSortNode = (StreamSortNode) mergeSortNode.getChildren().get(1);
    assertTrue(getChildrenNode(streamSortNode, 1) instanceof ProjectNode);
    assertTrue(getChildrenNode(streamSortNode, 2) instanceof FilterNode);
    tableScanNode = (TableScanNode) getChildrenNode(streamSortNode, 3);
    assertEquals(4, tableScanNode.getDeviceEntries().size());
    assertEquals(
        Arrays.asList(
            "table1.shanghai.B3.YY",
            "table1.shenzhen.B2.ZZ",
            "table1.shenzhen.B1.XX",
            "table1.shanghai.A3.YY"),
        tableScanNode.getDeviceEntries().stream()
            .map(d -> d.getDeviceID().toString())
            .collect(Collectors.toList()));
    assertEquals(ASC, tableScanNode.getScanOrder());

    // DistributePlan: `IdentitySink - StreamSort - Project - Filter - TableScan`
    streamSortNode =
        (StreamSortNode)
            distributedQueryPlan.getFragments().get(1).getPlanNodeTree().getChildren().get(0);
    assertTrue(getChildrenNode(streamSortNode, 1) instanceof ProjectNode);
    assertTrue(getChildrenNode(streamSortNode, 2) instanceof FilterNode);
    tableScanNode = (TableScanNode) getChildrenNode(streamSortNode, 3);
    assertEquals(2, tableScanNode.getDeviceEntries().size());
    assertEquals(
        Arrays.asList("table1.shenzhen.B2.ZZ", "table1.shenzhen.B1.XX"),
        tableScanNode.getDeviceEntries().stream()
            .map(d -> d.getDeviceID().toString())
            .collect(Collectors.toList()));
    assertEquals(ASC, tableScanNode.getScanOrder());
  }

  // order by others, some_ids, time
  @Test
  public void othersSomeIDColumnTimeSortTest() {
    sql =
        "SELECT time, tag3, tag1, cast(s2 as double), s2+s3, attr1 FROM table1 "
            + "where s1>1 and s1+s3>0 and cast(s1 as double)>1.0 order by s1+s2 desc, tag2 desc, tag1 desc, time asc offset 5 limit 10";
    context = new MPPQueryContext(sql, queryId, sessionInfo, null, null);
    actualAnalysis = analyzeSQL(sql, metadata);
    logicalQueryPlan =
        new LogicalPlanner(context, metadata, sessionInfo, WarningCollector.NOOP)
            .plan(actualAnalysis);
    rootNode = logicalQueryPlan.getRootNode();

    // OutputNode -  OffsetNode - LimitNode - ProjectNode - SortNode - ProjectNode - FilterNode -
    // TableScanNode
    assertTrue(rootNode instanceof OutputNode);
    assertTrue(rootNode.getChildren().get(0) instanceof OffsetNode);
    assertTrue(rootNode.getChildren().get(0).getChildren().get(0) instanceof LimitNode);
    assertTrue(
        rootNode.getChildren().get(0).getChildren().get(0).getChildren().get(0)
            instanceof ProjectNode);
    SortNode sortNode =
        (SortNode)
            rootNode
                .getChildren()
                .get(0)
                .getChildren()
                .get(0)
                .getChildren()
                .get(0)
                .getChildren()
                .get(0);
    assertTrue(sortNode.getChildren().get(0) instanceof ProjectNode);
    assertTrue(sortNode.getChildren().get(0).getChildren().get(0) instanceof FilterNode);
    assertTrue(
        sortNode.getChildren().get(0).getChildren().get(0).getChildren().get(0)
            instanceof TableScanNode);
    tableScanNode =
        (TableScanNode) sortNode.getChildren().get(0).getChildren().get(0).getChildren().get(0);
    assertEquals("testdb.table1", tableScanNode.getQualifiedObjectName().toString());
    assertEquals(8, tableScanNode.getAssignments().size());
    assertEquals(6, tableScanNode.getDeviceEntries().size());
    assertEquals(5, tableScanNode.getIdAndAttributeIndexMap().size());

    // OutputNode - LimitNode - OffsetNode - ProjectNode - MergeSortNode - SortNode - ProjectNode -
    // FilterNode -
    // TableScanNode
    distributionPlanner = new TableDistributionPlanner(actualAnalysis, logicalQueryPlan, context);
    distributedQueryPlan = distributionPlanner.plan();
    assertEquals(3, distributedQueryPlan.getFragments().size());
    assertTrue(
        distributedQueryPlan.getFragments().get(0).getPlanNodeTree().getChildren().get(0)
            instanceof OutputNode);
    OutputNode outputNode =
        (OutputNode)
            distributedQueryPlan.getFragments().get(0).getPlanNodeTree().getChildren().get(0);
    assertTrue(getChildrenNode(outputNode, 1) instanceof OffsetNode);
    assertTrue(getChildrenNode(outputNode, 2) instanceof LimitNode);
    assertTrue(
        outputNode.getChildren().get(0).getChildren().get(0).getChildren().get(0)
            instanceof ProjectNode);
    MergeSortNode mergeSortNode =
        (MergeSortNode)
            outputNode
                .getChildren()
                .get(0)
                .getChildren()
                .get(0)
                .getChildren()
                .get(0)
                .getChildren()
                .get(0);
    assertTrue(mergeSortNode.getChildren().get(0) instanceof ExchangeNode);
    assertTrue(mergeSortNode.getChildren().get(1) instanceof SortNode);
    assertTrue(mergeSortNode.getChildren().get(2) instanceof ExchangeNode);
    sortNode = (SortNode) mergeSortNode.getChildren().get(1);
    assertTrue(sortNode.getChildren().get(0) instanceof ProjectNode);
    assertTrue(sortNode.getChildren().get(0).getChildren().get(0) instanceof FilterNode);
    assertTrue(
        sortNode.getChildren().get(0).getChildren().get(0).getChildren().get(0)
            instanceof TableScanNode);
    tableScanNode =
        (TableScanNode) sortNode.getChildren().get(0).getChildren().get(0).getChildren().get(0);
    assertEquals(4, tableScanNode.getDeviceEntries().size());
    assertEquals(
        Arrays.asList(
            "table1.shanghai.B3.YY",
            "table1.shenzhen.B1.XX",
            "table1.shenzhen.B2.ZZ",
            "table1.shanghai.A3.YY"),
        tableScanNode.getDeviceEntries().stream()
            .map(d -> d.getDeviceID().toString())
            .collect(Collectors.toList()));
    assertEquals(ASC, tableScanNode.getScanOrder());

    // IdentitySinkNode - SortNode - ProjectNode - FilterNode - TableScanNode
    assertTrue(
        distributedQueryPlan.getFragments().get(1).getPlanNodeTree() instanceof IdentitySinkNode);
    assertTrue(
        distributedQueryPlan.getFragments().get(1).getPlanNodeTree().getChildren().get(0)
            instanceof SortNode);
    assertTrue(
        distributedQueryPlan
                .getFragments()
                .get(1)
                .getPlanNodeTree()
                .getChildren()
                .get(0)
                .getChildren()
                .get(0)
            instanceof ProjectNode);
    assertTrue(
        distributedQueryPlan
                .getFragments()
                .get(1)
                .getPlanNodeTree()
                .getChildren()
                .get(0)
                .getChildren()
                .get(0)
                .getChildren()
                .get(0)
            instanceof FilterNode);
    tableScanNode =
        (TableScanNode)
            distributedQueryPlan
                .getFragments()
                .get(1)
                .getPlanNodeTree()
                .getChildren()
                .get(0)
                .getChildren()
                .get(0)
                .getChildren()
                .get(0)
                .getChildren()
                .get(0);
    assertEquals(2, tableScanNode.getDeviceEntries().size());
    assertEquals(
        Arrays.asList("table1.shenzhen.B1.XX", "table1.shenzhen.B2.ZZ"),
        tableScanNode.getDeviceEntries().stream()
            .map(d -> d.getDeviceID().toString())
            .collect(Collectors.toList()));
    assertEquals(ASC, tableScanNode.getScanOrder());
  }

  // order by others, all_ids, time
  @Test
  public void othersAllIDColumnTimeSortTest() {
    sql =
        "SELECT time, tag3, tag1, cast(s2 as double), s2+s3, attr1 FROM table1 "
            + "where s1>1 and s1+s3>0 and cast(s1 as double)>1.0 order by s1+s2 desc, tag2 desc, tag1 desc, tag3 desc, time asc offset 5 limit 10";
    context = new MPPQueryContext(sql, queryId, sessionInfo, null, null);
    actualAnalysis = analyzeSQL(sql, metadata);
    logicalQueryPlan =
        new LogicalPlanner(context, metadata, sessionInfo, WarningCollector.NOOP)
            .plan(actualAnalysis);
    rootNode = logicalQueryPlan.getRootNode();

    // Output-Offset-Limit-Project-Sort-Project-Filter-TableScan
    assertTrue(rootNode instanceof OutputNode);
    assertTrue(getChildrenNode(rootNode, 1) instanceof OffsetNode);
    assertTrue(getChildrenNode(rootNode, 2) instanceof LimitNode);
    assertTrue(getChildrenNode(rootNode, 3) instanceof ProjectNode);
    SortNode sortNode =
        (SortNode)
            rootNode
                .getChildren()
                .get(0)
                .getChildren()
                .get(0)
                .getChildren()
                .get(0)
                .getChildren()
                .get(0);
    assertTrue(sortNode.getChildren().get(0) instanceof ProjectNode);
    assertTrue(sortNode.getChildren().get(0).getChildren().get(0) instanceof FilterNode);
    assertTrue(
        sortNode.getChildren().get(0).getChildren().get(0).getChildren().get(0)
            instanceof TableScanNode);
    tableScanNode =
        (TableScanNode) sortNode.getChildren().get(0).getChildren().get(0).getChildren().get(0);
    assertEquals("testdb.table1", tableScanNode.getQualifiedObjectName().toString());
    assertEquals(8, tableScanNode.getAssignments().size());
    assertEquals(6, tableScanNode.getDeviceEntries().size());
    assertEquals(5, tableScanNode.getIdAndAttributeIndexMap().size());

    // Output-Offset-Limit-Project-MergeSort-Sort-Project-Filter-TableScan
    distributionPlanner = new TableDistributionPlanner(actualAnalysis, logicalQueryPlan, context);
    distributedQueryPlan = distributionPlanner.plan();
    assertEquals(3, distributedQueryPlan.getFragments().size());
    assertTrue(
        distributedQueryPlan.getFragments().get(0).getPlanNodeTree().getChildren().get(0)
            instanceof OutputNode);
    OutputNode outputNode =
        (OutputNode)
            distributedQueryPlan.getFragments().get(0).getPlanNodeTree().getChildren().get(0);
    assertTrue(getChildrenNode(outputNode, 1) instanceof OffsetNode);
    assertTrue(getChildrenNode(outputNode, 2) instanceof LimitNode);
    assertTrue(
        outputNode.getChildren().get(0).getChildren().get(0).getChildren().get(0)
            instanceof ProjectNode);
    assertTrue(
        outputNode
                .getChildren()
                .get(0)
                .getChildren()
                .get(0)
                .getChildren()
                .get(0)
                .getChildren()
                .get(0)
            instanceof MergeSortNode);
    MergeSortNode mergeSortNode =
        (MergeSortNode)
            outputNode
                .getChildren()
                .get(0)
                .getChildren()
                .get(0)
                .getChildren()
                .get(0)
                .getChildren()
                .get(0);
    assertTrue(mergeSortNode.getChildren().get(0) instanceof ExchangeNode);
    assertTrue(mergeSortNode.getChildren().get(1) instanceof SortNode);
    assertTrue(mergeSortNode.getChildren().get(2) instanceof ExchangeNode);
    sortNode = (SortNode) mergeSortNode.getChildren().get(1);
    assertTrue(sortNode.getChildren().get(0) instanceof ProjectNode);
    assertTrue(sortNode.getChildren().get(0).getChildren().get(0) instanceof FilterNode);
    assertTrue(
        sortNode.getChildren().get(0).getChildren().get(0).getChildren().get(0)
            instanceof TableScanNode);
    tableScanNode =
        (TableScanNode) sortNode.getChildren().get(0).getChildren().get(0).getChildren().get(0);
    assertEquals(4, tableScanNode.getDeviceEntries().size());
    assertEquals(
        Arrays.asList(
            "table1.shanghai.B3.YY",
            "table1.shenzhen.B1.XX",
            "table1.shenzhen.B2.ZZ",
            "table1.shanghai.A3.YY"),
        tableScanNode.getDeviceEntries().stream()
            .map(d -> d.getDeviceID().toString())
            .collect(Collectors.toList()));
    assertEquals(ASC, tableScanNode.getScanOrder());

    // IdentitySinkNode - SortNode - ProjectNode - FilterNode - TableScanNode
    assertTrue(
        distributedQueryPlan.getFragments().get(1).getPlanNodeTree() instanceof IdentitySinkNode);
    assertTrue(
        distributedQueryPlan.getFragments().get(1).getPlanNodeTree().getChildren().get(0)
            instanceof SortNode);
    assertTrue(
        distributedQueryPlan
                .getFragments()
                .get(1)
                .getPlanNodeTree()
                .getChildren()
                .get(0)
                .getChildren()
                .get(0)
            instanceof ProjectNode);
    assertTrue(
        distributedQueryPlan
                .getFragments()
                .get(1)
                .getPlanNodeTree()
                .getChildren()
                .get(0)
                .getChildren()
                .get(0)
                .getChildren()
                .get(0)
            instanceof FilterNode);
    tableScanNode =
        (TableScanNode)
            distributedQueryPlan
                .getFragments()
                .get(1)
                .getPlanNodeTree()
                .getChildren()
                .get(0)
                .getChildren()
                .get(0)
                .getChildren()
                .get(0)
                .getChildren()
                .get(0);
    assertEquals(2, tableScanNode.getDeviceEntries().size());
    assertEquals(
        Arrays.asList("table1.shenzhen.B1.XX", "table1.shenzhen.B2.ZZ"),
        tableScanNode.getDeviceEntries().stream()
            .map(d -> d.getDeviceID().toString())
            .collect(Collectors.toList()));
    assertEquals(ASC, tableScanNode.getScanOrder());
  }

  // order by others, time, some_ids
  @Test
  public void othersTimeSomeIDColumnSortTest() {
    sql =
        "SELECT time, tag3, tag1, cast(s2 as double), s2+s3, attr1 FROM table1 "
            + "where s1>1 and s1+s3>0 and cast(s1 as double)>1.0 order by s1+s2 desc, time desc, tag2 desc, tag1 desc offset 5 limit 10";
    context = new MPPQueryContext(sql, queryId, sessionInfo, null, null);
    actualAnalysis = analyzeSQL(sql, metadata);
    logicalQueryPlan =
        new LogicalPlanner(context, metadata, sessionInfo, WarningCollector.NOOP)
            .plan(actualAnalysis);
    rootNode = logicalQueryPlan.getRootNode();

    // Output-Offset-Limit-Project-Sort-Project-Filter-TableScan
    assertTrue(rootNode instanceof OutputNode);
    assertTrue(getChildrenNode(rootNode, 1) instanceof OffsetNode);
    assertTrue(getChildrenNode(rootNode, 2) instanceof LimitNode);
    assertTrue(
        rootNode.getChildren().get(0).getChildren().get(0).getChildren().get(0)
            instanceof ProjectNode);
    SortNode sortNode =
        (SortNode)
            rootNode
                .getChildren()
                .get(0)
                .getChildren()
                .get(0)
                .getChildren()
                .get(0)
                .getChildren()
                .get(0);
    assertTrue(sortNode.getChildren().get(0) instanceof ProjectNode);
    assertTrue(sortNode.getChildren().get(0).getChildren().get(0) instanceof FilterNode);
    tableScanNode =
        (TableScanNode) sortNode.getChildren().get(0).getChildren().get(0).getChildren().get(0);
    assertEquals("testdb.table1", tableScanNode.getQualifiedObjectName().toString());
    assertEquals(8, tableScanNode.getAssignments().size());
    assertEquals(6, tableScanNode.getDeviceEntries().size());
    assertEquals(5, tableScanNode.getIdAndAttributeIndexMap().size());

    // Output-Offset-Limit-Project-MergeSort-Sort-Project-Filter-TableScan
    distributionPlanner = new TableDistributionPlanner(actualAnalysis, logicalQueryPlan, context);
    distributedQueryPlan = distributionPlanner.plan();
    assertEquals(3, distributedQueryPlan.getFragments().size());
    assertTrue(
        distributedQueryPlan.getFragments().get(0).getPlanNodeTree().getChildren().get(0)
            instanceof OutputNode);
    OutputNode outputNode =
        (OutputNode)
            distributedQueryPlan.getFragments().get(0).getPlanNodeTree().getChildren().get(0);
    assertTrue(getChildrenNode(outputNode, 1) instanceof OffsetNode);
    assertTrue(getChildrenNode(outputNode, 2) instanceof LimitNode);
    assertTrue(
        outputNode.getChildren().get(0).getChildren().get(0).getChildren().get(0)
            instanceof ProjectNode);
    MergeSortNode mergeSortNode =
        (MergeSortNode)
            outputNode
                .getChildren()
                .get(0)
                .getChildren()
                .get(0)
                .getChildren()
                .get(0)
                .getChildren()
                .get(0);
    assertTrue(mergeSortNode.getChildren().get(0) instanceof ExchangeNode);
    assertTrue(mergeSortNode.getChildren().get(1) instanceof SortNode);
    assertTrue(mergeSortNode.getChildren().get(2) instanceof ExchangeNode);
    sortNode = (SortNode) mergeSortNode.getChildren().get(1);
    assertTrue(sortNode.getChildren().get(0) instanceof ProjectNode);
    assertTrue(sortNode.getChildren().get(0).getChildren().get(0) instanceof FilterNode);
    tableScanNode =
        (TableScanNode) sortNode.getChildren().get(0).getChildren().get(0).getChildren().get(0);
    assertEquals(4, tableScanNode.getDeviceEntries().size());
    assertEquals(
        Arrays.asList(
            "table1.shanghai.B3.YY",
            "table1.shenzhen.B1.XX",
            "table1.shenzhen.B2.ZZ",
            "table1.shanghai.A3.YY"),
        tableScanNode.getDeviceEntries().stream()
            .map(d -> d.getDeviceID().toString())
            .collect(Collectors.toList()));
    assertEquals(ASC, tableScanNode.getScanOrder());

    // IdentitySinkNode - SortNode - ProjectNode - FilterNode - TableScanNode
    assertTrue(
        distributedQueryPlan.getFragments().get(1).getPlanNodeTree() instanceof IdentitySinkNode);
    assertTrue(
        distributedQueryPlan.getFragments().get(1).getPlanNodeTree().getChildren().get(0)
            instanceof SortNode);
    assertTrue(
        distributedQueryPlan
                .getFragments()
                .get(1)
                .getPlanNodeTree()
                .getChildren()
                .get(0)
                .getChildren()
                .get(0)
            instanceof ProjectNode);
    assertTrue(
        distributedQueryPlan
                .getFragments()
                .get(1)
                .getPlanNodeTree()
                .getChildren()
                .get(0)
                .getChildren()
                .get(0)
                .getChildren()
                .get(0)
            instanceof FilterNode);
    tableScanNode =
        (TableScanNode)
            distributedQueryPlan
                .getFragments()
                .get(1)
                .getPlanNodeTree()
                .getChildren()
                .get(0)
                .getChildren()
                .get(0)
                .getChildren()
                .get(0)
                .getChildren()
                .get(0);
    assertEquals(2, tableScanNode.getDeviceEntries().size());
    assertEquals(
        Arrays.asList("table1.shenzhen.B1.XX", "table1.shenzhen.B2.ZZ"),
        tableScanNode.getDeviceEntries().stream()
            .map(d -> d.getDeviceID().toString())
            .collect(Collectors.toList()));
    assertEquals(ASC, tableScanNode.getScanOrder());
  }

  // order by others, time, all_ids
  @Test
  public void othersTimeAllIDColumnSortTest() {
    sql =
        "SELECT time, tag3, tag1, cast(s2 as double), s2+s3, attr1 FROM table1 "
            + "where s1>1 and s1+s3>0 and cast(s1 as double)>1.0 order by s1+s2 desc, time desc, tag2 desc, tag1 desc, tag3 asc offset 5 limit 10";
    context = new MPPQueryContext(sql, queryId, sessionInfo, null, null);
    actualAnalysis = analyzeSQL(sql, metadata);
    logicalQueryPlan =
        new LogicalPlanner(context, metadata, sessionInfo, WarningCollector.NOOP)
            .plan(actualAnalysis);
    rootNode = logicalQueryPlan.getRootNode();

    // Output-Offset-Limit-Project-Sort-Project-Filter-TableScan
    assertTrue(rootNode instanceof OutputNode);
    assertTrue(getChildrenNode(rootNode, 1) instanceof OffsetNode);
    assertTrue(getChildrenNode(rootNode, 2) instanceof LimitNode);
    assertTrue(
        rootNode.getChildren().get(0).getChildren().get(0).getChildren().get(0)
            instanceof ProjectNode);
    assertTrue(
        rootNode.getChildren().get(0).getChildren().get(0).getChildren().get(0).getChildren().get(0)
            instanceof SortNode);
    SortNode sortNode =
        (SortNode)
            rootNode
                .getChildren()
                .get(0)
                .getChildren()
                .get(0)
                .getChildren()
                .get(0)
                .getChildren()
                .get(0);
    assertTrue(sortNode.getChildren().get(0) instanceof ProjectNode);
    assertTrue(sortNode.getChildren().get(0).getChildren().get(0) instanceof FilterNode);
    assertTrue(
        sortNode.getChildren().get(0).getChildren().get(0).getChildren().get(0)
            instanceof TableScanNode);
    tableScanNode =
        (TableScanNode) sortNode.getChildren().get(0).getChildren().get(0).getChildren().get(0);
    assertEquals("testdb.table1", tableScanNode.getQualifiedObjectName().toString());
    assertEquals(8, tableScanNode.getAssignments().size());
    assertEquals(6, tableScanNode.getDeviceEntries().size());
    assertEquals(5, tableScanNode.getIdAndAttributeIndexMap().size());

    // Output-Offset-Limit-Project-MergeSort-Sort-Project-Filter-TableScan
    distributionPlanner = new TableDistributionPlanner(actualAnalysis, logicalQueryPlan, context);
    distributedQueryPlan = distributionPlanner.plan();
    assertEquals(3, distributedQueryPlan.getFragments().size());
    assertTrue(
        distributedQueryPlan.getFragments().get(0).getPlanNodeTree().getChildren().get(0)
            instanceof OutputNode);
    OutputNode outputNode =
        (OutputNode)
            distributedQueryPlan.getFragments().get(0).getPlanNodeTree().getChildren().get(0);
    assertTrue(getChildrenNode(outputNode, 1) instanceof OffsetNode);
    assertTrue(getChildrenNode(outputNode, 2) instanceof LimitNode);
    assertTrue(
        outputNode.getChildren().get(0).getChildren().get(0).getChildren().get(0)
            instanceof ProjectNode);
    assertTrue(
        outputNode
                .getChildren()
                .get(0)
                .getChildren()
                .get(0)
                .getChildren()
                .get(0)
                .getChildren()
                .get(0)
            instanceof MergeSortNode);
    MergeSortNode mergeSortNode =
        (MergeSortNode)
            outputNode
                .getChildren()
                .get(0)
                .getChildren()
                .get(0)
                .getChildren()
                .get(0)
                .getChildren()
                .get(0);
    assertTrue(mergeSortNode.getChildren().get(0) instanceof ExchangeNode);
    assertTrue(mergeSortNode.getChildren().get(1) instanceof SortNode);
    assertTrue(mergeSortNode.getChildren().get(2) instanceof ExchangeNode);
    sortNode = (SortNode) mergeSortNode.getChildren().get(1);
    assertTrue(sortNode.getChildren().get(0) instanceof ProjectNode);
    assertTrue(sortNode.getChildren().get(0).getChildren().get(0) instanceof FilterNode);
    assertTrue(
        sortNode.getChildren().get(0).getChildren().get(0).getChildren().get(0)
            instanceof TableScanNode);
    tableScanNode =
        (TableScanNode) sortNode.getChildren().get(0).getChildren().get(0).getChildren().get(0);
    assertEquals(4, tableScanNode.getDeviceEntries().size());
    assertEquals(
        Arrays.asList(
            "table1.shanghai.B3.YY",
            "table1.shenzhen.B1.XX",
            "table1.shenzhen.B2.ZZ",
            "table1.shanghai.A3.YY"),
        tableScanNode.getDeviceEntries().stream()
            .map(d -> d.getDeviceID().toString())
            .collect(Collectors.toList()));
    assertEquals(ASC, tableScanNode.getScanOrder());

    // IdentitySinkNode - SortNode - ProjectNode - FilterNode - TableScanNode
    assertTrue(
        distributedQueryPlan.getFragments().get(1).getPlanNodeTree() instanceof IdentitySinkNode);
    assertTrue(
        distributedQueryPlan.getFragments().get(1).getPlanNodeTree().getChildren().get(0)
            instanceof SortNode);
    assertTrue(
        distributedQueryPlan
                .getFragments()
                .get(1)
                .getPlanNodeTree()
                .getChildren()
                .get(0)
                .getChildren()
                .get(0)
            instanceof ProjectNode);
    assertTrue(
        distributedQueryPlan
                .getFragments()
                .get(1)
                .getPlanNodeTree()
                .getChildren()
                .get(0)
                .getChildren()
                .get(0)
                .getChildren()
                .get(0)
            instanceof FilterNode);
    assertTrue(
        distributedQueryPlan
                .getFragments()
                .get(1)
                .getPlanNodeTree()
                .getChildren()
                .get(0)
                .getChildren()
                .get(0)
                .getChildren()
                .get(0)
                .getChildren()
                .get(0)
            instanceof TableScanNode);
    tableScanNode =
        (TableScanNode)
            distributedQueryPlan
                .getFragments()
                .get(1)
                .getPlanNodeTree()
                .getChildren()
                .get(0)
                .getChildren()
                .get(0)
                .getChildren()
                .get(0)
                .getChildren()
                .get(0);
    assertEquals(2, tableScanNode.getDeviceEntries().size());
    assertEquals(
        Arrays.asList("table1.shenzhen.B1.XX", "table1.shenzhen.B2.ZZ"),
        tableScanNode.getDeviceEntries().stream()
            .map(d -> d.getDeviceID().toString())
            .collect(Collectors.toList()));
    assertEquals(ASC, tableScanNode.getScanOrder());
  }
}
