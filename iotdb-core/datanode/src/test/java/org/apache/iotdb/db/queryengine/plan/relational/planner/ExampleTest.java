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

package org.apache.iotdb.db.queryengine.plan.relational.planner;

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
import org.apache.iotdb.db.queryengine.plan.relational.analyzer.Analysis;
import org.apache.iotdb.db.queryengine.plan.relational.analyzer.TestMatadata;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.Metadata;
import org.apache.iotdb.db.queryengine.plan.relational.planner.assertions.PlanMatchPattern;
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
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.ArithmeticBinaryExpression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Cast;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.ComparisonExpression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.DoubleLiteral;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.GenericDataType;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Identifier;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.LogicalExpression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.LongLiteral;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.SymbolReference;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.junit.Test;

import java.time.ZoneId;
import java.util.Arrays;
import java.util.stream.Collectors;

import static org.apache.iotdb.db.queryengine.plan.relational.analyzer.AnalyzerTest.analyzeSQL;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.assertions.PlanAssert.assertPlan;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.assertions.PlanMatchPattern.anyTree;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.assertions.PlanMatchPattern.dataType;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.assertions.PlanMatchPattern.filter;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.assertions.PlanMatchPattern.limit;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.assertions.PlanMatchPattern.offset;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.assertions.PlanMatchPattern.output;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.assertions.PlanMatchPattern.project;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.assertions.PlanMatchPattern.sort;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.assertions.PlanMatchPattern.tableScan;
import static org.apache.iotdb.db.queryengine.plan.relational.sql.ast.ArithmeticBinaryExpression.Operator.ADD;
import static org.apache.iotdb.db.queryengine.plan.relational.sql.ast.ComparisonExpression.Operator.GREATER_THAN;
import static org.apache.iotdb.db.queryengine.plan.relational.sql.ast.LogicalExpression.Operator.AND;
import static org.apache.iotdb.db.queryengine.plan.relational.sql.ast.SortItem.NullOrdering.LAST;
import static org.apache.iotdb.db.queryengine.plan.relational.sql.ast.SortItem.Ordering.ASCENDING;
import static org.apache.iotdb.db.queryengine.plan.relational.sql.ast.SortItem.Ordering.DESCENDING;
import static org.apache.iotdb.db.queryengine.plan.statement.component.Ordering.DESC;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class ExampleTest {

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

  // You can see the same name test in SortTest as a contrast
  /*
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

    PlanMatchPattern tableScan =
        tableScan(
            "testdb.table1",
            ImmutableList.of("time", "tag1", "tag2", "tag3", "attr1", "s1", "s2", "s3"),
            ImmutableSet.of("time", "tag1", "tag2", "tag3", "attr1", "s1", "s2", "s3"));

    assertPlan(
        new PlanTester().createPlan(sql),
        anyTree(
            project(
                filter( // (("s1" + "s3") > 0) AND (CAST("s1" AS double) > 1E0)
                    new LogicalExpression(
                        AND,
                        ImmutableList.of(
                            new ComparisonExpression(
                                GREATER_THAN,
                                new ArithmeticBinaryExpression(
                                    ADD, new SymbolReference("s1"), new SymbolReference("s3")),
                                new LongLiteral("0")),
                            new ComparisonExpression(
                                GREATER_THAN,
                                new Cast(new SymbolReference("s1"), dataType("double")),
                                new DoubleLiteral("1.0")))),
                    tableScan))));

    // Match full Plan: Output - Limit - Offset -Project - MergeSort - Sort - Project - Filter -
    // TableScan
    assertPlan(
        new PlanTester().createPlan(sql),
        anyTree(
            output(
                limit(
                    10,
                    offset(
                        5,
                        project(
                            sort(
                                ImmutableList.of(
                                    sort("tag1", DESCENDING, LAST),
                                    sort("time", DESCENDING, LAST),
                                    sort("expr_1", ASCENDING, LAST),
                                    sort("tag2", ASCENDING, LAST)),
                                project(
                                    filter( // (("s1" + "s3") > 0) AND (CAST("s1" AS double) > 1E0)
                                        new LogicalExpression(
                                            AND,
                                            ImmutableList.of(
                                                new ComparisonExpression(
                                                    GREATER_THAN,
                                                    new ArithmeticBinaryExpression(
                                                        ADD,
                                                        new SymbolReference("s1"),
                                                        new SymbolReference("s3")),
                                                    new LongLiteral("0")),
                                                new ComparisonExpression(
                                                    GREATER_THAN,
                                                    new Cast(
                                                        new SymbolReference("s1"),
                                                        new GenericDataType(
                                                            new Identifier("double"),
                                                            ImmutableList.of())),
                                                    new DoubleLiteral("1.0")))),
                                        tableScan)))))))));

    // You can use anyTree() to match partial Plan
    assertPlan(new PlanTester().createPlan(sql), anyTree(tableScan));

    context = new MPPQueryContext(sql, queryId, sessionInfo, null, null);
    actualAnalysis = analyzeSQL(sql, metadata);
    logicalQueryPlan =
        new LogicalPlanner(context, metadata, sessionInfo, WarningCollector.NOOP)
            .plan(actualAnalysis);
    rootNode = logicalQueryPlan.getRootNode();

    assertTrue(rootNode instanceof OutputNode);
    assertTrue(rootNode.getChildren().get(0) instanceof LimitNode);
    assertTrue(rootNode.getChildren().get(0).getChildren().get(0) instanceof OffsetNode);
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
    assertTrue(outputNode.getChildren().get(0) instanceof LimitNode);
    assertTrue(outputNode.getChildren().get(0).getChildren().get(0) instanceof OffsetNode);
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
}
