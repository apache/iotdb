/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.db.queryengine.plan.relational.analyzer;

import org.apache.iotdb.db.queryengine.plan.planner.plan.DistributedQueryPlan;
import org.apache.iotdb.db.queryengine.plan.planner.plan.LogicalQueryPlan;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.sink.IdentitySinkNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.PlanTester;
import org.apache.iotdb.db.queryengine.plan.relational.planner.Symbol;
import org.apache.iotdb.db.queryengine.plan.relational.planner.SymbolAllocator;
import org.apache.iotdb.db.queryengine.plan.relational.planner.TableLogicalPlanner;
import org.apache.iotdb.db.queryengine.plan.relational.planner.assertions.PlanMatchPattern;
import org.apache.iotdb.db.queryengine.plan.relational.planner.distribute.TableDistributedPlanner;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.DeviceTableScanNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.ExchangeNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.JoinNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.LimitNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.MergeSortNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.OffsetNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.OutputNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.ProjectNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.SortNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.TopKNode;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.ComparisonExpression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Expression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.SymbolReference;
import org.apache.iotdb.db.queryengine.plan.statement.component.Ordering;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.junit.Ignore;
import org.junit.Test;

import java.util.Collections;
import java.util.List;
import java.util.Optional;

import static org.apache.iotdb.db.queryengine.plan.relational.analyzer.AnalyzerTest.analyzeSQL;
import static org.apache.iotdb.db.queryengine.plan.relational.analyzer.TestUtils.ALL_DEVICE_ENTRIES;
import static org.apache.iotdb.db.queryengine.plan.relational.analyzer.TestUtils.BEIJING_A1_DEVICE_ENTRY;
import static org.apache.iotdb.db.queryengine.plan.relational.analyzer.TestUtils.DEFAULT_WARNING;
import static org.apache.iotdb.db.queryengine.plan.relational.analyzer.TestUtils.QUERY_CONTEXT;
import static org.apache.iotdb.db.queryengine.plan.relational.analyzer.TestUtils.SESSION_INFO;
import static org.apache.iotdb.db.queryengine.plan.relational.analyzer.TestUtils.SHENZHEN_DEVICE_ENTRIES;
import static org.apache.iotdb.db.queryengine.plan.relational.analyzer.TestUtils.TEST_MATADATA;
import static org.apache.iotdb.db.queryengine.plan.relational.analyzer.TestUtils.assertJoinNodeEquals;
import static org.apache.iotdb.db.queryengine.plan.relational.analyzer.TestUtils.assertNodeMatches;
import static org.apache.iotdb.db.queryengine.plan.relational.analyzer.TestUtils.assertTableScan;
import static org.apache.iotdb.db.queryengine.plan.relational.analyzer.TestUtils.buildSymbols;
import static org.apache.iotdb.db.queryengine.plan.relational.analyzer.TestUtils.getChildrenNode;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.assertions.PlanAssert.assertPlan;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.assertions.PlanMatchPattern.aggregation;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.assertions.PlanMatchPattern.aggregationFunction;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.assertions.PlanMatchPattern.aggregationTableScan;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.assertions.PlanMatchPattern.exchange;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.assertions.PlanMatchPattern.filter;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.assertions.PlanMatchPattern.join;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.assertions.PlanMatchPattern.mergeSort;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.assertions.PlanMatchPattern.output;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.assertions.PlanMatchPattern.project;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.assertions.PlanMatchPattern.singleGroupingSet;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.assertions.PlanMatchPattern.sort;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.assertions.PlanMatchPattern.tableScan;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.node.AggregationNode.Step.FINAL;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.node.AggregationNode.Step.PARTIAL;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.node.JoinNode.JoinType.INNER;
import static org.apache.iotdb.db.queryengine.plan.relational.sql.ast.ComparisonExpression.Operator.EQUAL;
import static org.apache.iotdb.db.queryengine.plan.relational.sql.ast.ComparisonExpression.Operator.GREATER_THAN;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class JoinTest {
  Analysis analysis;
  LogicalQueryPlan logicalQueryPlan;
  PlanNode logicalPlanNode;
  JoinNode joinNode;
  OutputNode outputNode;
  IdentitySinkNode identitySinkNode;
  MergeSortNode mergeSortNode;
  DistributedQueryPlan distributedQueryPlan;
  DeviceTableScanNode deviceTableScanNode;
  String sql;

  // ========== table1 join table1 ===============

  // no filter, no sort
  @Test
  public void innerJoinTest1() {
    // join on
    assertInnerJoinTest1(
        "SELECT t1.time, t1.tag1, t1.tag2, t1.attr2, t1.s1, t1.s2,"
            + "t2.tag1, t2.tag3, t2.attr2, t2.s1, t2.s3 "
            + "FROM table1 t1 JOIN table1 t2 ON t1.time = t2.time OFFSET 3 LIMIT 6");

    // implicit join
    assertInnerJoinTest1(
        "SELECT t1.time, t1.tag1, t1.tag2, t1.attr2, t1.s1, t1.s2,"
            + "t2.tag1, t2.tag3, t2.attr2, t2.s1, t2.s3 "
            + "FROM table1 t1, table1 t2 WHERE t1.time = t2.time OFFSET 3 LIMIT 6");

    // join using
    assertInnerJoinTest1(
        "SELECT time, t1.tag1, t1.tag2, t1.attr2, t1.s1, t1.s2,"
            + "t2.tag1, t2.tag3, t2.attr2, t2.s1, t2.s3 "
            + "FROM table1 t1 JOIN table1 t2 USING(time) OFFSET 3 LIMIT 6");
  }

  /*
   * IdentitySinkNode-210
   *   └──OutputNode-12
   *       └──OffsetNode-8
   *           └──LimitNode-9
   *               └──JoinNode-7
   *                   ├──ExchangeNode-197: [SourceAddress:192.0.11.1/test_query.2.0/205]
   *                   └──ExchangeNode-201: [SourceAddress:192.0.11.1/test_query.6.0/209]
   *
   * IdentitySinkNode-205
   *   └──MergeSortNode-149
   *       ├──ExchangeNode-194: [SourceAddress:192.0.12.1/test_query.3.0/202]
   *       ├──ExchangeNode-195: [SourceAddress:192.0.11.1/test_query.4.0/203]
   *       └──ExchangeNode-196: [SourceAddress:192.0.10.1/test_query.5.0/204]
   *
   * IdentitySinkNode-209
   *   └──MergeSortNode-156
   *       ├──ExchangeNode-198: [SourceAddress:192.0.12.1/test_query.7.0/206]
   *       ├──ExchangeNode-199: [SourceAddress:192.0.11.1/test_query.8.0/207]
   *       └──ExchangeNode-200: [SourceAddress:192.0.10.1/test_query.9.0/208]
   *
   * IdentitySinkNode-204
   *   └──SortNode-152
   *       └──DeviceTableScanNode-147
   * ...
   */
  private void assertInnerJoinTest1(String sql) {
    analysis = analyzeSQL(sql, TEST_MATADATA, QUERY_CONTEXT);
    SymbolAllocator symbolAllocator = new SymbolAllocator();
    logicalQueryPlan =
        new TableLogicalPlanner(
                QUERY_CONTEXT, TEST_MATADATA, SESSION_INFO, symbolAllocator, DEFAULT_WARNING)
            .plan(analysis);

    // LogicalPlan: `Output-Offset-Limit-Join-(Left + Right)-Sort-TableScan`
    logicalPlanNode = logicalQueryPlan.getRootNode();
    assertNodeMatches(
        logicalPlanNode, OutputNode.class, OffsetNode.class, LimitNode.class, JoinNode.class);

    joinNode = (JoinNode) getChildrenNode(logicalPlanNode, 3);
    List<JoinNode.EquiJoinClause> joinCriteria =
        Collections.singletonList(
            new JoinNode.EquiJoinClause(Symbol.of("time"), Symbol.of("time_0")));
    assertJoinNodeEquals(
        joinNode,
        INNER,
        joinCriteria,
        buildSymbols("time", "tag1", "tag2", "attr2", "s1", "s2"),
        buildSymbols("tag1_1", "tag3_3", "attr2_5", "s1_6", "s3_8"));
    assertTrue(joinNode.getLeftChild() instanceof SortNode);
    assertTrue(joinNode.getRightChild() instanceof SortNode);
    SortNode leftSortNode = (SortNode) joinNode.getLeftChild();
    assertTrue(getChildrenNode(leftSortNode, 1) instanceof DeviceTableScanNode);
    DeviceTableScanNode leftDeviceTableScanNode =
        (DeviceTableScanNode) getChildrenNode(leftSortNode, 1);
    assertTableScan(leftDeviceTableScanNode, ALL_DEVICE_ENTRIES, Ordering.ASC, 0, 0, true, "");
    SortNode rightSortNode = (SortNode) joinNode.getRightChild();
    assertTrue(getChildrenNode(rightSortNode, 1) instanceof DeviceTableScanNode);
    DeviceTableScanNode rightDeviceTableScanNode =
        (DeviceTableScanNode) getChildrenNode(rightSortNode, 1);
    assertTableScan(rightDeviceTableScanNode, ALL_DEVICE_ENTRIES, Ordering.ASC, 0, 0, true, "");

    // Before ExchangeNode logic optimize
    /*
     * IdentitySinkNode-178
     *   └──OutputNode-12
     *       └──OffsetNode-8
     *           └──LimitNode-9
     *               └──JoinNode-
     *                   ├──MergeSortNode-115
     *                   │   ├──ExchangeNode-172: [SourceAddress:192.0.12.1/test_query.2.0/176]
     *                   │   ├──SortNode-117
     *                   │   │   └──DeviceTableScanNode-113
     *                   │   └──ExchangeNode-173: [SourceAddress:192.0.10.1/test_query.3.0/177]
     *                   └──MergeSortNode-128
     *                       ├──ExchangeNode-174: [SourceAddress:192.0.12.1/test_query.2.0/176]
     *                       ├──SortNode-130
     *                       │   └──DeviceTableScanNode-126
     *                       └──ExchangeNode-175: [SourceAddress:192.0.10.1/test_query.3.0/177]
     *
     * IdentitySinkNode-201
     *   └──SortNode-141
     *       └──DeviceTableScanNode-137
     *
     * IdentitySinkNode-201
     *   └──SortNode-141
     *       └──DeviceTableScanNode-137
     *
     * IdentitySinkNode-203
     *   └──SortNode-154
     *       └──DeviceTableScanNode-150
     *
     * IdentitySinkNode-203
     *   └──SortNode-154
     *       └──DeviceTableScanNode-150
     */

    // After ExchangeNode logic optimize
    /*
     * IdentitySinkNode-210
     *   └──OutputNode-12
     *       └──OffsetNode-8
     *           └──LimitNode-9
     *               └──JoinNode-7
     *                   ├──ExchangeNode-197: [SourceAddress:192.0.11.1/test_query.2.0/205]
     *                   └──ExchangeNode-201: [SourceAddress:192.0.11.1/test_query.6.0/209]
     *
     * IdentitySinkNode-205
     *   └──MergeSortNode-149
     *       ├──ExchangeNode-194: [SourceAddress:192.0.12.1/test_query.3.0/202]
     *       ├──ExchangeNode-195: [SourceAddress:192.0.11.1/test_query.4.0/203]
     *       └──ExchangeNode-196: [SourceAddress:192.0.10.1/test_query.5.0/204]
     *
     * IdentitySinkNode-209
     *   └──MergeSortNode-156
     *       ├──ExchangeNode-198: [SourceAddress:192.0.12.1/test_query.7.0/206]
     *       ├──ExchangeNode-199: [SourceAddress:192.0.11.1/test_query.8.0/207]
     *       └──ExchangeNode-200: [SourceAddress:192.0.10.1/test_query.9.0/208]
     *
     * IdentitySinkNode-204
     *   └──SortNode-152
     *       └──DeviceTableScanNode-147
     * ...
     */

    distributedQueryPlan =
        new TableDistributedPlanner(
                analysis, symbolAllocator, logicalQueryPlan, TEST_MATADATA, null)
            .plan();
    assertEquals(9, distributedQueryPlan.getFragments().size());
    identitySinkNode =
        (IdentitySinkNode) distributedQueryPlan.getFragments().get(0).getPlanNodeTree();
    outputNode = (OutputNode) getChildrenNode(identitySinkNode, 1);
    assertTrue(getChildrenNode(outputNode, 3) instanceof JoinNode);
    joinNode = (JoinNode) getChildrenNode(outputNode, 3);
    assertTrue(joinNode.getLeftChild() instanceof ExchangeNode);
    assertTrue(joinNode.getRightChild() instanceof ExchangeNode);

    identitySinkNode =
        (IdentitySinkNode) distributedQueryPlan.getFragments().get(1).getPlanNodeTree();
    assertTrue(getChildrenNode(identitySinkNode, 1) instanceof MergeSortNode);
    assertTrue(getChildrenNode(identitySinkNode, 2) instanceof ExchangeNode);

    identitySinkNode =
        (IdentitySinkNode) distributedQueryPlan.getFragments().get(6).getPlanNodeTree();
    assertTrue(getChildrenNode(identitySinkNode, 1) instanceof SortNode);
    assertTrue(getChildrenNode(identitySinkNode, 2) instanceof DeviceTableScanNode);
    deviceTableScanNode = (DeviceTableScanNode) getChildrenNode(identitySinkNode, 2);
    assertTableScan(deviceTableScanNode, SHENZHEN_DEVICE_ENTRIES, Ordering.ASC, 0, 0, true, "");
  }

  // has filter which can be push down, filter can in subquery or outer query
  @Test
  public void innerJoinTest2() {
    // join on
    assertInnerJoinTest2(
        "SELECT t1.time, t1.tag1, t1.tag2, t1.attr2, t1.s1+1 as add_s1, t1.s2,"
            + "t2.tag1, t2.tag3, t2.attr2, t2.s1, t2.s3 "
            + "FROM (SELECT * FROM table1 WHERE tag1='beijing' AND tag2='A1' AND s1>1 AND time>11) t1 JOIN (SELECT * FROM table1 WHERE time>22 AND tag1='shenzhen' AND s2>1) t2 "
            + "ON t1.time = t2.time ORDER BY t1.tag1 OFFSET 3 LIMIT 6",
        false);

    assertInnerJoinTest2(
        "SELECT t1.time, t1.tag1, t1.tag2, t1.attr2, t1.s1+1 as add_s1, t1.s2,"
            + "t2.tag1, t2.tag3, t2.attr2, t2.s1, t2.s3 "
            + "FROM (SELECT * FROM table1) t1 JOIN (SELECT * FROM table1) t2 "
            + "ON t1.time = t2.time WHERE t1.tag1='beijing' AND t1.tag2='A1' AND t1.s1>1 AND t2.tag1='shenzhen' AND t2.s2>1 ORDER BY t1.tag1 OFFSET 3 LIMIT 6",
        false);

    assertInnerJoinTest2(
        "SELECT t1.time, t1.tag1, t1.tag2, t1.attr2, t1.s1+1 as add_s1, t1.s2,"
            + "t2.tag1, t2.tag3, t2.attr2, t2.s1, t2.s3 "
            + "FROM (SELECT * FROM table1 WHERE tag2='A1') t1 JOIN (SELECT * FROM table1 WHERE s2>1) t2 "
            + "ON t1.time = t2.time WHERE t1.tag1='beijing' AND t1.s1>1 AND t2.tag1='shenzhen' ORDER BY t1.tag1 OFFSET 3 LIMIT 6",
        false);

    // implicit join
    assertInnerJoinTest2(
        "SELECT t1.time, t1.tag1, t1.tag2, t1.attr2, t1.s1+1 as add_s1, t1.s2,"
            + "t2.tag1, t2.tag3, t2.attr2, t2.s1, t2.s3 "
            + "FROM (SELECT * FROM table1 WHERE tag2='A1') t1, (SELECT * FROM table1 WHERE s2>1) t2 "
            + "WHERE t1.time = t2.time AND t1.tag1='beijing' AND t1.s1>1 AND t2.tag1='shenzhen' ORDER BY t1.tag1 OFFSET 3 LIMIT 6",
        false);

    // join using
    assertInnerJoinTest2(
        "SELECT time, t1.tag1, t1.tag2, t1.attr2, t1.s1+1 as add_s1, t1.s2,"
            + "t2.tag1, t2.tag3, t2.attr2, t2.s1, t2.s3 "
            + "FROM (SELECT * FROM table1 WHERE tag1='beijing' AND tag2='A1' AND s1>1 AND time>11) t1 JOIN (SELECT * FROM table1 WHERE time>22 AND tag1='shenzhen' AND s2>1) t2 "
            + "USING(time) ORDER BY t1.tag1 OFFSET 3 LIMIT 6",
        true);
  }

  private void assertInnerJoinTest2(String sql, boolean joinUsing) {
    analysis = analyzeSQL(sql, TEST_MATADATA, QUERY_CONTEXT);
    SymbolAllocator symbolAllocator = new SymbolAllocator();
    logicalQueryPlan =
        new TableLogicalPlanner(
                QUERY_CONTEXT, TEST_MATADATA, SESSION_INFO, symbolAllocator, DEFAULT_WARNING)
            .plan(analysis);

    // LogicalPlan: `Output-Offset-TopK-Join-(Left + Right)-Sort-(Project)-TableScan`
    logicalPlanNode = logicalQueryPlan.getRootNode();
    if (joinUsing) {
      assertNodeMatches(
          logicalPlanNode,
          OutputNode.class,
          OffsetNode.class,
          TopKNode.class,
          ProjectNode.class,
          JoinNode.class);
    } else {
      assertNodeMatches(
          logicalPlanNode,
          OutputNode.class,
          OffsetNode.class,
          TopKNode.class,
          ProjectNode.class,
          JoinNode.class);
    }
    joinNode = (JoinNode) getChildrenNode(logicalPlanNode, 4);
    List<JoinNode.EquiJoinClause> joinCriteria =
        Collections.singletonList(
            new JoinNode.EquiJoinClause(Symbol.of("time"), Symbol.of("time_0")));
    assertJoinNodeEquals(
        joinNode,
        INNER,
        joinCriteria,
        buildSymbols("time", "tag1", "tag2", "attr2", "s1", "s2"),
        buildSymbols("tag1_1", "tag3_3", "attr2_5", "s1_6", "s3_8"));
    assertTrue(joinNode.getLeftChild() instanceof SortNode);
    assertTrue(joinNode.getRightChild() instanceof SortNode);
    SortNode leftSortNode = (SortNode) joinNode.getLeftChild();
    assertEquals(DeviceTableScanNode.class, getChildrenNode(leftSortNode, 1).getClass());
    DeviceTableScanNode leftDeviceTableScanNode =
        (DeviceTableScanNode) getChildrenNode(leftSortNode, 1);
    assertTableScan(leftDeviceTableScanNode, BEIJING_A1_DEVICE_ENTRY, Ordering.ASC, 0, 0, true, "");
    SortNode rightSortNode = (SortNode) joinNode.getRightChild();
    assertTrue(getChildrenNode(rightSortNode, 1) instanceof DeviceTableScanNode);
    DeviceTableScanNode rightDeviceTableScanNode =
        (DeviceTableScanNode) getChildrenNode(rightSortNode, 1);
    assertTableScan(
        rightDeviceTableScanNode, SHENZHEN_DEVICE_ENTRIES, Ordering.ASC, 0, 0, true, "");

    // Before ExchangeNode optimize
    /*
     * IdentitySinkNode-197
     *   └──OutputNode-21
     *       └──OffsetNode-17
     *           └──TopKNode-18
     *               └──ProjectNode-14
     *                   └──JoinNode-13
     *                       ├──ExchangeNode-193: [SourceAddress:192.0.10.1/test_query.2.0/195]
     *                       └──MergeSortNode-165
     *                           ├──SortNode-166
     *                           │   └──DeviceTableScanNode-163
     *                           └──ExchangeNode-194: [SourceAddress:192.0.11.1/test_query.3.0/196]
     *
     *  IdentitySinkNode-195
     *   └──DeviceTableScanNode-158
     *
     *  IdentitySinkNode-196
     *   └──SortNode-167
     *       └──DeviceTableScanNode-164
     */
    distributedQueryPlan =
        new TableDistributedPlanner(
                analysis, symbolAllocator, logicalQueryPlan, TEST_MATADATA, null)
            .plan();
    assertEquals(5, distributedQueryPlan.getFragments().size());
    identitySinkNode =
        (IdentitySinkNode) distributedQueryPlan.getFragments().get(0).getPlanNodeTree();
    assertTrue(getChildrenNode(identitySinkNode, 5) instanceof JoinNode);
    joinNode = (JoinNode) getChildrenNode(identitySinkNode, 5);
    assertTrue(joinNode.getLeftChild() instanceof ExchangeNode);
    assertTrue(joinNode.getRightChild() instanceof ExchangeNode);

    mergeSortNode =
        (MergeSortNode)
            distributedQueryPlan.getFragments().get(2).getPlanNodeTree().getChildren().get(0);
    assertNodeMatches(mergeSortNode, MergeSortNode.class, ExchangeNode.class);

    identitySinkNode =
        (IdentitySinkNode) distributedQueryPlan.getFragments().get(1).getPlanNodeTree();
    deviceTableScanNode = (DeviceTableScanNode) getChildrenNode(identitySinkNode, 1);
    assertTableScan(deviceTableScanNode, BEIJING_A1_DEVICE_ENTRY, Ordering.ASC, 0, 0, true, "");
  }

  // TableScan join AggTableScan (whose cardinality is at most one)
  @Test
  public void innerJoinTest3() {
    PlanTester planTester = new PlanTester();
    Expression filterPredicate =
        new ComparisonExpression(EQUAL, new SymbolReference("s1"), new SymbolReference("max"));

    PlanMatchPattern tableScan =
        tableScan("testdb.table1", ImmutableList.of("s1"), ImmutableSet.of("s1"));

    // Verify full LogicalPlan
    /*
     *   └──OutputNode
     *           └──ProjectNode
     *             └──FilterNode
     *               └──JoinNode
     *                   |──TableScanNode
     *                   ├──AggregationNode
     *                   │   └──AggregationTableScanNode
     */
    assertPlan(
        planTester.createPlan(
            "SELECT s1 FROM table1 t1 JOIN (select max(s1) as agg from table1) t2 ON t1.s1=t2.agg"),
        output(
            project(
                filter(
                    filterPredicate,
                    join(
                        JoinNode.JoinType.INNER,
                        builder ->
                            builder
                                .left(tableScan)
                                .right(
                                    aggregation(
                                        singleGroupingSet(),
                                        ImmutableMap.of(
                                            Optional.of("max"),
                                            aggregationFunction("max", ImmutableList.of("max_9"))),
                                        Collections.emptyList(),
                                        Optional.empty(),
                                        FINAL,
                                        aggregationTableScan(
                                            singleGroupingSet(),
                                            Collections.emptyList(),
                                            Optional.empty(),
                                            PARTIAL,
                                            "testdb.table1",
                                            ImmutableList.of("max_9"),
                                            ImmutableSet.of("s1_6")))))))));

    filterPredicate =
        new ComparisonExpression(EQUAL, new SymbolReference("s1"), new SymbolReference("sum"));
    assertPlan(
        planTester.createPlan(
            "SELECT s1 FROM table1 t1 JOIN (select sum(s1) as agg from table1) t2 ON t1.s1=t2.agg"),
        output(
            project(
                filter(
                    filterPredicate,
                    join(
                        JoinNode.JoinType.INNER,
                        builder ->
                            builder
                                .left(tableScan)
                                .right(
                                    aggregation(
                                        singleGroupingSet(),
                                        ImmutableMap.of(
                                            Optional.of("sum"),
                                            aggregationFunction("sum", ImmutableList.of("sum_9"))),
                                        Collections.emptyList(),
                                        Optional.empty(),
                                        FINAL,
                                        aggregationTableScan(
                                            singleGroupingSet(),
                                            Collections.emptyList(),
                                            Optional.empty(),
                                            PARTIAL,
                                            "testdb.table1",
                                            ImmutableList.of("sum_9"),
                                            ImmutableSet.of("s1_6")))))))));
  }

  @Test
  public void innerJoinTest4() {
    PlanTester planTester = new PlanTester();

    Expression filterPredicate =
        new ComparisonExpression(
            GREATER_THAN, new SymbolReference("s1"), new SymbolReference("s1_6"));

    // equiClause with non equiClause
    sql =
        "SELECT t1.s1 FROM table1 t1 JOIN table1 t2 ON t1.tag1=t2.tag1 AND t1.time=t2.time AND t1.s1>t2.s1";
    logicalQueryPlan = planTester.createPlan(sql);
    PlanMatchPattern tableScan1 =
        tableScan(
            "testdb.table1",
            ImmutableList.of("time", "tag1", "s1"),
            ImmutableSet.of("time", "tag1", "s1"));
    PlanMatchPattern tableScan2 =
        tableScan(
            "testdb.table1", ImmutableMap.of("time_0", "time", "tag1_1", "tag1", "s1_6", "s1"));
    // Verify full LogicalPlan
    /*
     *       └──OutputNode
     *           └──Project
     *              └──Filter (t1.s1>t2.s1)
     *                └──JoinNode  (t1.tag1=t2.tag1 AND t1.time=t2.time)
     *                   |──SortNode
     *                   │   └──TableScanNode
     *                   ├──SortNode
     *                   │   └──TableScanNode
     */
    assertPlan(
        logicalQueryPlan,
        output(
            project(
                filter(
                    filterPredicate,
                    join(
                        JoinNode.JoinType.INNER,
                        builder ->
                            builder
                                .left(sort(tableScan1))
                                .right(sort(tableScan2))
                                .ignoreEquiCriteria())))));

    sql = "SELECT t1.s1 FROM table1 t1 JOIN table1 t2 ON t1.s1>t2.s1";
    logicalQueryPlan = planTester.createPlan(sql);
    // Verify full LogicalPlan
    /*
     *       └──OutputNode
     *          └──Project
     *             └──Filter
     *               └──JoinNode
     *                   │   └──TableScanNode
     *                   │   └──TableScanNode
     */
    assertPlan(
        logicalQueryPlan,
        output(
            project(
                filter(
                    join(
                        JoinNode.JoinType.INNER,
                        builder ->
                            builder
                                .left(
                                    tableScan(
                                        "testdb.table1",
                                        ImmutableList.of("s1"),
                                        ImmutableSet.of("s1")))
                                .right(tableScan("testdb.table1", ImmutableMap.of("s1_6", "s1")))
                                .ignoreEquiCriteria())))));
  }

  @Test
  public void fullJoinTest() {
    PlanTester planTester = new PlanTester();
    sql =
        "SELECT t1.time FROM table1 t1 FULL JOIN table1 t2 ON t1.tag1=t2.tag1 AND t1.time=t2.time";
    logicalQueryPlan = planTester.createPlan(sql);
    PlanMatchPattern tableScan1 =
        tableScan(
            "testdb.table1", ImmutableList.of("time", "tag1"), ImmutableSet.of("time", "tag1"));
    PlanMatchPattern tableScan2 =
        tableScan("testdb.table1", ImmutableMap.of("time_0", "time", "tag1_1", "tag1"));
    // Verify full LogicalPlan
    /*
     *       └──OutputNode
     *                └──JoinNode  (t1.tag1=t2.tag1 AND t1.time=t2.time)
     *                   |──SortNode
     *                   │   └──TableScanNode
     *                   ├──SortNode
     *                   │   └──TableScanNode
     */
    assertPlan(
        logicalQueryPlan,
        output(
            join(
                JoinNode.JoinType.FULL,
                builder ->
                    builder.left(sort(tableScan1)).right(sort(tableScan2)).ignoreEquiCriteria())));
  }

  @Test
  public void aggregationTableScanWithJoinTest() {
    PlanTester planTester = new PlanTester();
    sql =
        "select * from ("
            + "select date_bin(1ms,time) as date,count(*)from table1 where tag1='Beijing' and tag2='A1' group by date_bin(1ms,time)) t0 "
            + "join ("
            + "select date_bin(1ms,time) as date,count(*)from table1 where tag1='Beijing' and tag2='A1' group by date_bin(1ms,time)) t1 "
            + "on t0.date = t1.date";
    logicalQueryPlan = planTester.createPlan(sql);
    // the sort node has been eliminated
    assertPlan(planTester.getFragmentPlan(1), aggregationTableScan());
    // the sort node has been eliminated
    assertPlan(planTester.getFragmentPlan(2), aggregationTableScan());
  }

  @Ignore
  @Test
  public void otherInnerJoinTests() {
    assertInnerJoinTest2(
        "SELECT t1.time, t1.tag1, t1.tag2, t1.attr2, t1.s1+1 as add_s1, t1.s2,"
            + "t2.tag1, t2.tag3, t2.attr2, t2.s1, t2.s3 "
            + "FROM (SELECT * FROM table1 t1 WHERE tag1='beijing' AND tag2='A1' AND s1>1 ORDER BY tag1 LIMIT 111) t1 JOIN (SELECT * FROM table1 WHERE tag1='shenzhen' AND s2>1 LIMIT 222) t2 "
            + "ON t1.time = t2.time ORDER BY t1.tag1 OFFSET 3 LIMIT 6",
        false);

    // 1. has logical or in subquery filter, outer query filter

    // has filter which can be push down, inner limit and sort, test if inner limit can be pushed
    // down

    // has filter which can be push down, inner limit, test if inner limit can be pushed down
    assertInnerJoinTest2(
        "SELECT t1.time, t1.tag1, t1.tag2, t1.attr2, t1.s1+1 as add_s1, t1.s2,"
            + "t2.tag1, t2.tag3, t2.attr2, t2.s1, t2.s3 "
            + "FROM (SELECT * FROM table1 t1 WHERE tag1='beijing' AND tag2='A1' AND s1>1 LIMIT 111) t1 JOIN (SELECT * FROM table1 WHERE tag1='shenzhen' AND s2>1 LIMIT 222) t2 "
            + "ON t1.time = t2.time ORDER BY t1.tag1 OFFSET 3 LIMIT 6",
        false);
  }

  @Test
  public void testJoinSortProperties() {
    // FULL JOIN
    PlanTester planTester = new PlanTester();
    sql =
        "select * from table1 t1 "
            + "full join table1 t2 using (time, s1)"
            + "full join table1 t3 using (time, s1)";
    logicalQueryPlan = planTester.createPlan(sql);
    assertPlan(
        logicalQueryPlan.getRootNode(),
        output(
            project(
                join(
                    sort(
                        project(
                            join(
                                sort(tableScan("testdb.table1")),
                                sort(tableScan("testdb.table1"))))),
                    sort(tableScan("testdb.table1"))))));

    assertPlan(planTester.getFragmentPlan(0), output(project(join(exchange(), exchange()))));

    // the sort node above JoinNode has been eliminated
    assertPlan(planTester.getFragmentPlan(1), project(join(exchange(), exchange())));

    assertPlan(planTester.getFragmentPlan(2), mergeSort(exchange(), exchange(), exchange()));

    assertPlan(planTester.getFragmentPlan(3), sort(tableScan("testdb.table1")));

    assertPlan(planTester.getFragmentPlan(4), sort(tableScan("testdb.table1")));

    assertPlan(planTester.getFragmentPlan(5), sort(tableScan("testdb.table1")));

    assertPlan(planTester.getFragmentPlan(6), mergeSort(exchange(), exchange(), exchange()));

    assertPlan(planTester.getFragmentPlan(7), sort(tableScan("testdb.table1")));

    assertPlan(planTester.getFragmentPlan(8), sort(tableScan("testdb.table1")));

    assertPlan(planTester.getFragmentPlan(9), sort(tableScan("testdb.table1")));

    assertPlan(planTester.getFragmentPlan(10), mergeSort(exchange(), exchange(), exchange()));

    // LEFT
    sql =
        "select * from table1 t1 "
            + "left join table1 t2 using (time, s1)"
            + "left join table1 t3 using (time, s1)";
    assertLeftOrInner(planTester);

    // INNER JOIN
    sql =
        "select * from table1 t1 "
            + "inner join table1 t2 using (time, s1)"
            + "inner join table1 t3 using (time, s1)";
    assertLeftOrInner(planTester);

    // RIGHT JOIN
    sql =
        "select * from table1 t1 "
            + "right join table1 t2 using (time, s1)"
            + "right join table1 t3 using (time, s1)";
    logicalQueryPlan = planTester.createPlan(sql);
    assertPlan(
        logicalQueryPlan.getRootNode(),
        output(
            join(
                sort(tableScan("testdb.table1")),
                sort(join(sort(tableScan("testdb.table1")), sort(tableScan("testdb.table1")))))));

    assertPlan(planTester.getFragmentPlan(0), output(join(exchange(), exchange())));

    assertPlan(planTester.getFragmentPlan(1), mergeSort(exchange(), exchange(), exchange()));

    assertPlan(planTester.getFragmentPlan(2), sort(tableScan("testdb.table1")));

    assertPlan(planTester.getFragmentPlan(3), sort(tableScan("testdb.table1")));

    assertPlan(planTester.getFragmentPlan(4), sort(tableScan("testdb.table1")));

    // the sort node above JoinNode has been eliminated
    assertPlan(planTester.getFragmentPlan(5), join(exchange(), exchange()));

    assertPlan(planTester.getFragmentPlan(6), mergeSort(exchange(), exchange(), exchange()));

    assertPlan(planTester.getFragmentPlan(10), mergeSort(exchange(), exchange(), exchange()));
  }

  private void assertLeftOrInner(PlanTester planTester) {
    logicalQueryPlan = planTester.createPlan(sql);
    assertPlan(
        logicalQueryPlan.getRootNode(),
        output(
            join(
                sort(join(sort(tableScan("testdb.table1")), sort(tableScan("testdb.table1")))),
                sort(tableScan("testdb.table1")))));

    assertPlan(planTester.getFragmentPlan(0), output(join(exchange(), exchange())));

    // the sort node above JoinNode has been eliminated
    assertPlan(planTester.getFragmentPlan(1), join(exchange(), exchange()));

    assertPlan(planTester.getFragmentPlan(2), mergeSort(exchange(), exchange(), exchange()));

    assertPlan(planTester.getFragmentPlan(3), sort(tableScan("testdb.table1")));

    assertPlan(planTester.getFragmentPlan(4), sort(tableScan("testdb.table1")));

    assertPlan(planTester.getFragmentPlan(5), sort(tableScan("testdb.table1")));

    assertPlan(planTester.getFragmentPlan(6), mergeSort(exchange(), exchange(), exchange()));

    assertPlan(planTester.getFragmentPlan(7), sort(tableScan("testdb.table1")));

    assertPlan(planTester.getFragmentPlan(8), sort(tableScan("testdb.table1")));

    assertPlan(planTester.getFragmentPlan(9), sort(tableScan("testdb.table1")));

    assertPlan(planTester.getFragmentPlan(10), mergeSort(exchange(), exchange(), exchange()));
  }

  @Test
  public void joinSortEliminationTest() {
    PlanTester planTester = new PlanTester();
    sql = "select * from table1 t1 " + "left join table1 t2 using (tag1, tag2, tag3, time)";
    logicalQueryPlan = planTester.createPlan(sql);
    assertPlan(
        logicalQueryPlan.getRootNode(),
        output(join(sort(tableScan("testdb.table1")), sort(tableScan("testdb.table1")))));

    assertPlan(planTester.getFragmentPlan(0), output(join(exchange(), exchange())));

    assertPlan(planTester.getFragmentPlan(1), mergeSort(exchange(), exchange(), exchange()));

    assertPlan(planTester.getFragmentPlan(2), tableScan("testdb.table1"));

    assertPlan(planTester.getFragmentPlan(3), tableScan("testdb.table1"));

    assertPlan(planTester.getFragmentPlan(4), tableScan("testdb.table1"));

    assertPlan(planTester.getFragmentPlan(5), mergeSort(exchange(), exchange(), exchange()));

    assertPlan(planTester.getFragmentPlan(6), tableScan("testdb.table1"));

    assertPlan(planTester.getFragmentPlan(7), tableScan("testdb.table1"));

    assertPlan(planTester.getFragmentPlan(8), tableScan("testdb.table1"));

    sql = "select * from table1 t1 " + "full join table1 t2 using (tag1, tag2, tag3, time)";
    logicalQueryPlan = planTester.createPlan(sql);
    assertPlan(
        logicalQueryPlan.getRootNode(),
        output(project(join(sort(tableScan("testdb.table1")), sort(tableScan("testdb.table1"))))));

    assertPlan(planTester.getFragmentPlan(0), output(project(join(exchange(), exchange()))));

    assertPlan(planTester.getFragmentPlan(1), mergeSort(exchange(), exchange(), exchange()));

    assertPlan(planTester.getFragmentPlan(2), tableScan("testdb.table1"));

    assertPlan(planTester.getFragmentPlan(3), tableScan("testdb.table1"));

    assertPlan(planTester.getFragmentPlan(4), tableScan("testdb.table1"));

    assertPlan(planTester.getFragmentPlan(5), mergeSort(exchange(), exchange(), exchange()));

    assertPlan(planTester.getFragmentPlan(6), tableScan("testdb.table1"));

    assertPlan(planTester.getFragmentPlan(7), tableScan("testdb.table1"));

    assertPlan(planTester.getFragmentPlan(8), tableScan("testdb.table1"));
  }
}
