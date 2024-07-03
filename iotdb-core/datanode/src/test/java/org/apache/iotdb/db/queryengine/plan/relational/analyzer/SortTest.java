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
import org.apache.iotdb.db.queryengine.plan.relational.metadata.Metadata;
import org.apache.iotdb.db.queryengine.plan.relational.planner.LogicalPlanner;
import org.apache.iotdb.db.queryengine.plan.relational.planner.Symbol;
import org.apache.iotdb.db.queryengine.plan.relational.planner.distribute.TableDistributionPlanner;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.MergeSortNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.OutputNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.ProjectNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.SortNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.TableScanNode;

import org.junit.Test;

import java.time.ZoneId;
import java.util.Arrays;
import java.util.stream.Collectors;

import static org.apache.iotdb.db.queryengine.plan.relational.analyzer.AnalyzerTest.analyzeSQL;
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
  TableDistributionPlanner distributionPlanner;
  DistributedQueryPlan distributedQueryPlan;
  TableScanNode tableScanNode;

  // order by time, others, some_ids
  @Test
  public void timeOthersSomeIDColumnSortTest() {
    sql =
        "SELECT time, tag1, tag2, s1, s2, attr1 FROM table1 order by time desc, s1+s2 asc, tag2 asc, tag1 desc";
    context = new MPPQueryContext(sql, queryId, sessionInfo, null, null);
    actualAnalysis = analyzeSQL(sql, metadata);
    logicalQueryPlan =
        new LogicalPlanner(context, metadata, sessionInfo, WarningCollector.NOOP)
            .plan(actualAnalysis);
    rootNode = logicalQueryPlan.getRootNode();

    assertTrue(rootNode instanceof OutputNode);
    assertTrue(rootNode.getChildren().get(0) instanceof SortNode);
    assertTrue(rootNode.getChildren().get(0).getChildren().get(0) instanceof ProjectNode);
    assertTrue(
        rootNode.getChildren().get(0).getChildren().get(0).getChildren().get(0)
            instanceof TableScanNode);
    tableScanNode =
        (TableScanNode) rootNode.getChildren().get(0).getChildren().get(0).getChildren().get(0);
    assertEquals("testdb.table1", tableScanNode.getQualifiedObjectName().toString());
    // TODO(beyyes) fix the case prune unUsed Columns when the child of SortNode is ProjectNode
    //    assertEquals(
    //        Arrays.asList("time", "tag1", "tag2", "attr1", "s1", "s2"),
    //        tableScanNode.getOutputColumnNames());
    assertEquals(9, tableScanNode.getAssignments().size());
    assertEquals(6, tableScanNode.getDeviceEntries().size());
    assertEquals(5, tableScanNode.getIdAndAttributeIndexMap().size());

    distributionPlanner = new TableDistributionPlanner(actualAnalysis, logicalQueryPlan, context);
    distributedQueryPlan = distributionPlanner.plan();
    assertEquals(3, distributedQueryPlan.getFragments().size());
    assertTrue(
        distributedQueryPlan.getFragments().get(0).getPlanNodeTree().getChildren().get(0)
            instanceof OutputNode);
    OutputNode outputNode =
        (OutputNode)
            distributedQueryPlan.getFragments().get(0).getPlanNodeTree().getChildren().get(0);
    assertTrue(outputNode.getChildren().get(0) instanceof MergeSortNode);
    MergeSortNode mergeSortNode = (MergeSortNode) outputNode.getChildren().get(0);
    assertEquals(
        Arrays.asList("time", "(s1 + s2)", "tag2", "tag1"),
        mergeSortNode.getOrderingScheme().getOrderBy().stream()
            .map(Symbol::getName)
            .collect(Collectors.toList()));
    assertTrue(mergeSortNode.getChildren().get(0) instanceof ExchangeNode);
    assertTrue(mergeSortNode.getChildren().get(1) instanceof SortNode);
    assertTrue(mergeSortNode.getChildren().get(2) instanceof ExchangeNode);
    SortNode sortNode = (SortNode) mergeSortNode.getChildren().get(1);
    assertTrue(sortNode.getChildren().get(0).getChildren().get(0) instanceof TableScanNode);
    tableScanNode = (TableScanNode) sortNode.getChildren().get(0).getChildren().get(0);
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

    // TODO(beyyes) add only one device entry optimization and verifies
  }

  // order by time, others, all_ids
  @Test
  public void timeOthersAllIDColumnSortTest() {
    // TODO
  }

  // order by time, some_ids, others
  @Test
  public void timeSomeIDColumnOthersSortTest() {
    // TODO
  }

  // order by time, all_ids, others
  @Test
  public void timeAllIDColumnOthersSortTest() {
    // TODO
  }

  // order by some_ids, time, others
  @Test
  public void someIDColumnTimeOthersSortTest() {
    sql =
        "SELECT time, tag1, tag2, s1, s2, attr1 FROM table1 order by tag2 desc, tag1 asc, time desc, s1+s2 asc";
    context = new MPPQueryContext(sql, queryId, sessionInfo, null, null);
    actualAnalysis = analyzeSQL(sql, metadata);
    logicalQueryPlan =
        new LogicalPlanner(context, metadata, sessionInfo, WarningCollector.NOOP)
            .plan(actualAnalysis);
    rootNode = logicalQueryPlan.getRootNode();

    assertTrue(rootNode instanceof OutputNode);
    assertTrue(rootNode.getChildren().get(0) instanceof SortNode);
    assertTrue(rootNode.getChildren().get(0).getChildren().get(0) instanceof ProjectNode);
    assertTrue(
        rootNode.getChildren().get(0).getChildren().get(0).getChildren().get(0)
            instanceof TableScanNode);
    tableScanNode =
        (TableScanNode) rootNode.getChildren().get(0).getChildren().get(0).getChildren().get(0);
    assertEquals("testdb.table1", tableScanNode.getQualifiedObjectName().toString());
    //    assertEquals(
    //        Arrays.asList("time", "tag1", "tag2", "attr1", "s1", "s2"),
    //        tableScanNode.getOutputColumnNames());
    assertEquals(9, tableScanNode.getAssignments().size());
    assertEquals(6, tableScanNode.getDeviceEntries().size());
    assertEquals(5, tableScanNode.getIdAndAttributeIndexMap().size());

    distributionPlanner = new TableDistributionPlanner(actualAnalysis, logicalQueryPlan, context);
    distributedQueryPlan = distributionPlanner.plan();
    assertEquals(3, distributedQueryPlan.getFragments().size());
    assertTrue(
        distributedQueryPlan.getFragments().get(0).getPlanNodeTree().getChildren().get(0)
            instanceof OutputNode);
    OutputNode outputNode =
        (OutputNode)
            distributedQueryPlan.getFragments().get(0).getPlanNodeTree().getChildren().get(0);
    assertTrue(outputNode.getChildren().get(0) instanceof MergeSortNode);
    MergeSortNode mergeSortNode = (MergeSortNode) outputNode.getChildren().get(0);
    assertEquals(
        Arrays.asList("tag2", "tag1", "time", "(s1 + s2)"),
        mergeSortNode.getOrderingScheme().getOrderBy().stream()
            .map(Symbol::getName)
            .collect(Collectors.toList()));
    assertTrue(mergeSortNode.getChildren().get(0) instanceof ExchangeNode);
    assertTrue(mergeSortNode.getChildren().get(1) instanceof SortNode);
    assertTrue(mergeSortNode.getChildren().get(2) instanceof ExchangeNode);
    SortNode sortNode = (SortNode) mergeSortNode.getChildren().get(1);
    assertTrue(sortNode.getChildren().get(0).getChildren().get(0) instanceof TableScanNode);
    tableScanNode = (TableScanNode) sortNode.getChildren().get(0).getChildren().get(0);
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
  }

  // order by all_ids, time, others
  @Test
  public void allIDColumnTimeOthersSortTest() {
    // TODO
  }

  // order by some_ids, others, time
  @Test
  public void someIDColumnOthersTimeSortTest() {
    // TODO
  }

  // order by all_ids, others, time
  @Test
  public void allIDColumnOthersTimeSortTest() {
    // TODO
  }

  // order by others, some_ids, time
  @Test
  public void othersSomeIDColumnTimeSortTest() {
    sql =
        "SELECT time, tag1, tag2, s1, s2, attr1 FROM table1 order by s1+s2 desc, tag2 desc, tag1 asc, time desc";
    context = new MPPQueryContext(sql, queryId, sessionInfo, null, null);
    actualAnalysis = analyzeSQL(sql, metadata);
    logicalQueryPlan =
        new LogicalPlanner(context, metadata, sessionInfo, WarningCollector.NOOP)
            .plan(actualAnalysis);
    rootNode = logicalQueryPlan.getRootNode();

    assertTrue(rootNode instanceof OutputNode);
    assertTrue(rootNode.getChildren().get(0) instanceof SortNode);
    assertTrue(rootNode.getChildren().get(0).getChildren().get(0) instanceof ProjectNode);
    assertTrue(
        rootNode.getChildren().get(0).getChildren().get(0).getChildren().get(0)
            instanceof TableScanNode);
    tableScanNode =
        (TableScanNode) rootNode.getChildren().get(0).getChildren().get(0).getChildren().get(0);
    assertEquals("testdb.table1", tableScanNode.getQualifiedObjectName().toString());
    //    assertEquals(
    //        Arrays.asList("time", "tag1", "tag2", "attr1", "s1", "s2"),
    //        tableScanNode.getOutputColumnNames());
    assertEquals(9, tableScanNode.getAssignments().size());
    assertEquals(6, tableScanNode.getDeviceEntries().size());
    assertEquals(5, tableScanNode.getIdAndAttributeIndexMap().size());

    distributionPlanner = new TableDistributionPlanner(actualAnalysis, logicalQueryPlan, context);
    distributedQueryPlan = distributionPlanner.plan();
    assertEquals(3, distributedQueryPlan.getFragments().size());
    assertTrue(
        distributedQueryPlan.getFragments().get(0).getPlanNodeTree().getChildren().get(0)
            instanceof OutputNode);
    OutputNode outputNode =
        (OutputNode)
            distributedQueryPlan.getFragments().get(0).getPlanNodeTree().getChildren().get(0);
    assertTrue(outputNode.getChildren().get(0) instanceof MergeSortNode);
    MergeSortNode mergeSortNode = (MergeSortNode) outputNode.getChildren().get(0);
    assertEquals(
        Arrays.asList("(s1 + s2)", "tag2", "tag1", "time"),
        mergeSortNode.getOrderingScheme().getOrderBy().stream()
            .map(Symbol::getName)
            .collect(Collectors.toList()));
    assertTrue(mergeSortNode.getChildren().get(0) instanceof ExchangeNode);
    assertTrue(mergeSortNode.getChildren().get(1) instanceof SortNode);
    assertTrue(mergeSortNode.getChildren().get(2) instanceof ExchangeNode);
    SortNode sortNode = (SortNode) mergeSortNode.getChildren().get(1);
    assertTrue(sortNode.getChildren().get(0).getChildren().get(0) instanceof TableScanNode);
    tableScanNode = (TableScanNode) sortNode.getChildren().get(0).getChildren().get(0);
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
  }

  // order by others, all_ids, time
  @Test
  public void othersAllIDColumnTimeSortTest() {
    // TODO
  }

  // order by others, time, some_ids
  @Test
  public void othersTimeSomeIDColumnSortTest() {
    // TODO
  }

  // order by others, time, all_ids
  @Test
  public void othersTimeAllIDColumnSortTest() {
    // TODO
  }
}
