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

import org.apache.iotdb.db.queryengine.plan.planner.plan.DistributedQueryPlan;
import org.apache.iotdb.db.queryengine.plan.planner.plan.LogicalQueryPlan;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.ExchangeNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.sink.IdentitySinkNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.Symbol;
import org.apache.iotdb.db.queryengine.plan.relational.planner.TableLogicalPlanner;
import org.apache.iotdb.db.queryengine.plan.relational.planner.distribute.TableDistributedPlanner;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.JoinNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.LimitNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.MergeSortNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.OffsetNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.OutputNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.ProjectNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.SortNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.TableScanNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.TopKNode;
import org.apache.iotdb.db.queryengine.plan.statement.component.Ordering;

import org.junit.Ignore;
import org.junit.Test;

import java.util.Collections;
import java.util.List;

import static org.apache.iotdb.db.queryengine.plan.relational.analyzer.AnalyzerTest.analyzeSQL;
import static org.apache.iotdb.db.queryengine.plan.relational.analyzer.StatementAnalyzer.ONLY_SUPPORT_TIME_COLUMN_EQUI_JOIN;
import static org.apache.iotdb.db.queryengine.plan.relational.analyzer.StatementAnalyzer.ONLY_SUPPORT_TIME_COLUMN_IN_USING_CLAUSE;
import static org.apache.iotdb.db.queryengine.plan.relational.analyzer.TestUtils.ALL_DEVICE_ENTRIES;
import static org.apache.iotdb.db.queryengine.plan.relational.analyzer.TestUtils.BEIJING_A1_DEVICE_ENTRY;
import static org.apache.iotdb.db.queryengine.plan.relational.analyzer.TestUtils.DEFAULT_WARNING;
import static org.apache.iotdb.db.queryengine.plan.relational.analyzer.TestUtils.QUERY_CONTEXT;
import static org.apache.iotdb.db.queryengine.plan.relational.analyzer.TestUtils.SESSION_INFO;
import static org.apache.iotdb.db.queryengine.plan.relational.analyzer.TestUtils.SHANGHAI_SHENZHEN_DEVICE_ENTRIES;
import static org.apache.iotdb.db.queryengine.plan.relational.analyzer.TestUtils.SHENZHEN_DEVICE_ENTRIES;
import static org.apache.iotdb.db.queryengine.plan.relational.analyzer.TestUtils.TEST_MATADATA;
import static org.apache.iotdb.db.queryengine.plan.relational.analyzer.TestUtils.assertAnalyzeSemanticException;
import static org.apache.iotdb.db.queryengine.plan.relational.analyzer.TestUtils.assertJoinNodeEquals;
import static org.apache.iotdb.db.queryengine.plan.relational.analyzer.TestUtils.assertMergeSortNode;
import static org.apache.iotdb.db.queryengine.plan.relational.analyzer.TestUtils.assertNodeMatches;
import static org.apache.iotdb.db.queryengine.plan.relational.analyzer.TestUtils.assertTableScan;
import static org.apache.iotdb.db.queryengine.plan.relational.analyzer.TestUtils.buildSymbols;
import static org.apache.iotdb.db.queryengine.plan.relational.analyzer.TestUtils.getChildrenNode;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.node.JoinNode.JoinType.INNER;
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
  TableScanNode tableScanNode;

  // ========== table1 join table1 ===============

  // no filter, no sort
  @Test
  public void innerJoinTest1() {
    // join on
    //        assertInnerJoinTest1(
    //            "SELECT t1.time, t1.tag1, t1.tag2, t1.attr2, t1.s1, t1.s2,"
    //                + "t2.tag1, t2.tag3, t2.attr2, t2.s1, t2.s3 "
    //                + "FROM table1 t1 JOIN table1 t2 ON t1.time = t2.time OFFSET 3 LIMIT 6",
    //            false);
    //
    //        // implicit join
    //        assertInnerJoinTest1(
    //            "SELECT t1.time, t1.tag1, t1.tag2, t1.attr2, t1.s1, t1.s2,"
    //                + "t2.tag1, t2.tag3, t2.attr2, t2.s1, t2.s3 "
    //                + "FROM table1 t1, table1 t2 WHERE t1.time = t2.time OFFSET 3 LIMIT 6",
    //            false);

    // join using
    assertInnerJoinTest1(
        "SELECT time, t1.tag1, t1.tag2, t1.attr2, t1.s1, t1.s2,"
            + "t2.tag1, t2.tag3, t2.attr2, t2.s1, t2.s3 "
            + "FROM table1 t1 JOIN table1 t2 USING(time) OFFSET 3 LIMIT 6",
        true);
  }

  private void assertInnerJoinTest1(String sql, boolean joinUsing) {
    analysis = analyzeSQL(sql, TEST_MATADATA, QUERY_CONTEXT);
    logicalQueryPlan =
        new TableLogicalPlanner(QUERY_CONTEXT, TEST_MATADATA, SESSION_INFO, DEFAULT_WARNING)
            .plan(analysis);

    // LogicalPlan: `Output-Offset-Limit-Join-(Left + Right)-Sort-(Project)-TableScan`
    logicalPlanNode = logicalQueryPlan.getRootNode();
    if (joinUsing) {
      assertNodeMatches(
          logicalPlanNode,
          OutputNode.class,
          OffsetNode.class,
          ProjectNode.class,
          LimitNode.class,
          JoinNode.class);
    } else {
      assertNodeMatches(
          logicalPlanNode, OutputNode.class, OffsetNode.class, LimitNode.class, JoinNode.class);
    }

    joinNode =
        joinUsing
            ? (JoinNode) getChildrenNode(logicalPlanNode, 4)
            : (JoinNode) getChildrenNode(logicalPlanNode, 3);
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
    assertTrue(getChildrenNode(leftSortNode, 1) instanceof TableScanNode);
    TableScanNode leftTableScanNode = (TableScanNode) getChildrenNode(leftSortNode, 1);
    assertTableScan(leftTableScanNode, ALL_DEVICE_ENTRIES, Ordering.ASC, 0, 0, true, "");
    SortNode rightSortNode = (SortNode) joinNode.getRightChild();
    assertTrue(getChildrenNode(rightSortNode, 1) instanceof ProjectNode);
    assertTrue(getChildrenNode(rightSortNode, 2) instanceof TableScanNode);
    TableScanNode rightTableScanNode = (TableScanNode) getChildrenNode(rightSortNode, 2);
    assertTableScan(rightTableScanNode, ALL_DEVICE_ENTRIES, Ordering.ASC, 0, 0, true, "");

    /*
     * IdentitySinkNode-178
     *   └──OutputNode-12
     *       └──OffsetNode-8
     *           └──LimitNode-9
     *               └──JoinNode-
     *                   ├──MergeSortNode-115
     *                   │   ├──ExchangeNode-172: [SourceAddress:192.0.12.1/test_query.2.0/176]
     *                   │   ├──SortNode-117
     *                   │   │   └──TableScanNode-113
     *                   │   └──ExchangeNode-173: [SourceAddress:192.0.10.1/test_query.3.0/177]
     *                   └──MergeSortNode-128
     *                       ├──ExchangeNode-174: [SourceAddress:192.0.12.1/test_query.2.0/176]
     *                       ├──SortNode-130
     *                       │   └──ProjectNode-126
     *                       │       └──TableScanNode-123
     *                       └──ExchangeNode-175: [SourceAddress:192.0.10.1/test_query.3.0/177]
     *
     * IdentitySinkNode-176
     *   ├──SortNode-116
     *   │   └──TableScanNode-112
     *   └──SortNode-129
     *       └──ProjectNode-125
     *           └──TableScanNode-122
     *
     * IdentitySinkNode-177
     *   ├──SortNode-118
     *   │   └──TableScanNode-114
     *   └──SortNode-131
     *       └──ProjectNode-127
     *           └──TableScanNode-124
     */
    distributedQueryPlan = new TableDistributedPlanner(analysis, logicalQueryPlan).plan();
    assertEquals(3, distributedQueryPlan.getFragments().size());
    IdentitySinkNode identitySinkNode =
        (IdentitySinkNode) distributedQueryPlan.getFragments().get(0).getPlanNodeTree();
    outputNode = (OutputNode) getChildrenNode(identitySinkNode, 1);
    assertTrue(getChildrenNode(outputNode, joinUsing ? 4 : 3) instanceof JoinNode);
    joinNode = (JoinNode) getChildrenNode(outputNode, joinUsing ? 4 : 3);
    assertTrue(joinNode.getLeftChild() instanceof MergeSortNode);
    MergeSortNode mergeSortNode = (MergeSortNode) joinNode.getLeftChild();
    assertMergeSortNode(mergeSortNode);
    leftSortNode = (SortNode) mergeSortNode.getChildren().get(1);
    tableScanNode = (TableScanNode) getChildrenNode(leftSortNode, 1);
    assertTableScan(tableScanNode, SHANGHAI_SHENZHEN_DEVICE_ENTRIES, Ordering.ASC, 0, 0, true, "");

    identitySinkNode =
        (IdentitySinkNode) distributedQueryPlan.getFragments().get(1).getPlanNodeTree();
    tableScanNode = (TableScanNode) getChildrenNode(identitySinkNode.getChildren().get(1), 2);
    assertTableScan(tableScanNode, SHENZHEN_DEVICE_ENTRIES, Ordering.ASC, 0, 0, true, "");
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
    logicalQueryPlan =
        new TableLogicalPlanner(QUERY_CONTEXT, TEST_MATADATA, SESSION_INFO, DEFAULT_WARNING)
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
    assertEquals(TableScanNode.class, getChildrenNode(leftSortNode, 1).getClass());
    TableScanNode leftTableScanNode = (TableScanNode) getChildrenNode(leftSortNode, 1);
    assertTableScan(leftTableScanNode, BEIJING_A1_DEVICE_ENTRY, Ordering.ASC, 0, 0, true, "");
    SortNode rightSortNode = (SortNode) joinNode.getRightChild();
    assertTrue(getChildrenNode(rightSortNode, 1) instanceof ProjectNode);
    assertTrue(getChildrenNode(rightSortNode, 2) instanceof TableScanNode);
    TableScanNode rightTableScanNode = (TableScanNode) getChildrenNode(rightSortNode, 2);
    assertTableScan(rightTableScanNode, SHENZHEN_DEVICE_ENTRIES, Ordering.ASC, 0, 0, true, "");

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
     *                           │   └──ProjectNode-163
     *                           │       └──TableScanNode-161
     *                           └──ExchangeNode-194: [SourceAddress:192.0.11.1/test_query.3.0/196]
     *
     *  IdentitySinkNode-195
     *   └──TableScanNode-158
     *
     *  IdentitySinkNode-196
     *   └──SortNode-167
     *       └──ProjectNode-164
     *           └──TableScanNode-162
     */
    distributedQueryPlan = new TableDistributedPlanner(analysis, logicalQueryPlan).plan();
    assertEquals(3, distributedQueryPlan.getFragments().size());
    identitySinkNode =
        (IdentitySinkNode) distributedQueryPlan.getFragments().get(0).getPlanNodeTree();
    assertTrue(getChildrenNode(identitySinkNode, 5) instanceof JoinNode);
    joinNode = (JoinNode) getChildrenNode(identitySinkNode, 5);
    assertTrue(joinNode.getLeftChild() instanceof ExchangeNode);
    assertTrue(joinNode.getRightChild() instanceof MergeSortNode);
    mergeSortNode = (MergeSortNode) joinNode.getRightChild();
    assertNodeMatches(
        mergeSortNode, MergeSortNode.class, SortNode.class, ProjectNode.class, TableScanNode.class);
    tableScanNode = (TableScanNode) getChildrenNode(mergeSortNode, 3);
    assertTableScan(tableScanNode, SHENZHEN_DEVICE_ENTRIES, Ordering.ASC, 0, 0, true, "");

    identitySinkNode =
        (IdentitySinkNode) distributedQueryPlan.getFragments().get(1).getPlanNodeTree();
    tableScanNode = (TableScanNode) getChildrenNode(identitySinkNode, 1);
    assertTableScan(tableScanNode, BEIJING_A1_DEVICE_ENTRY, Ordering.ASC, 0, 0, true, "");
  }

  // has filter which can be push down, inner limit, test if inner limit can be pushed down
  @Ignore
  @Test
  public void innerJoinTest3() {
    assertInnerJoinTest2(
        "SELECT t1.time, t1.tag1, t1.tag2, t1.attr2, t1.s1+1 as add_s1, t1.s2,"
            + "t2.tag1, t2.tag3, t2.attr2, t2.s1, t2.s3 "
            + "FROM (SELECT * FROM table1 t1 WHERE tag1='beijing' AND tag2='A1' AND s1>1 LIMIT 111) t1 JOIN (SELECT * FROM table1 WHERE tag1='shenzhen' AND s2>1 LIMIT 222) t2 "
            + "ON t1.time = t2.time ORDER BY t1.tag1 OFFSET 3 LIMIT 6",
        false);
  }

  // has filter which can be push down, inner limit and sort, test if inner limit can be pushed down
  @Ignore
  @Test
  public void innerJoinTest4() {
    assertInnerJoinTest2(
        "SELECT t1.time, t1.tag1, t1.tag2, t1.attr2, t1.s1+1 as add_s1, t1.s2,"
            + "t2.tag1, t2.tag3, t2.attr2, t2.s1, t2.s3 "
            + "FROM (SELECT * FROM table1 t1 WHERE tag1='beijing' AND tag2='A1' AND s1>1 ORDER BY tag1 LIMIT 111) t1 JOIN (SELECT * FROM table1 WHERE tag1='shenzhen' AND s2>1 LIMIT 222) t2 "
            + "ON t1.time = t2.time ORDER BY t1.tag1 OFFSET 3 LIMIT 6",
        false);
  }

  @Ignore
  @Test
  public void innerJoinTest5() {
    // 1. has logical or in subquery filter, outer query filter

    // 2. where t1.value1 > t2.value2
  }

  // ========== unsupported test ===============
  @Test
  public void unsupportedJoinTest() {
    assertAnalyzeSemanticException(
        "SELECT * FROM table1 t1 INNER JOIN table1 t2 ON t1.time>t2.time",
        ONLY_SUPPORT_TIME_COLUMN_EQUI_JOIN);

    assertAnalyzeSemanticException(
        "SELECT * FROM table1 t1 INNER JOIN table1 t2 ON t1.tag1=t2.tag2",
        ONLY_SUPPORT_TIME_COLUMN_EQUI_JOIN);

    assertAnalyzeSemanticException(
        "SELECT * FROM table1 t1 INNER JOIN table1 t2 ON t1.time>t2.time AND t1.tag1=t2.tag2",
        ONLY_SUPPORT_TIME_COLUMN_EQUI_JOIN);

    assertAnalyzeSemanticException(
        "SELECT * FROM table1 t1 INNER JOIN table1 t2 ON t1.time>t2.time OR t1.tag1=t2.tag2",
        ONLY_SUPPORT_TIME_COLUMN_EQUI_JOIN);

    assertAnalyzeSemanticException(
        "SELECT * FROM table1 t1 INNER JOIN table1 t2 USING(tag1)",
        ONLY_SUPPORT_TIME_COLUMN_IN_USING_CLAUSE);

    assertAnalyzeSemanticException(
        "SELECT * FROM table1 t1 INNER JOIN table1 t2 USING(tag1, time)",
        ONLY_SUPPORT_TIME_COLUMN_IN_USING_CLAUSE);

    // FULL, LEFT, RIGHT JOIN
    assertAnalyzeSemanticException(
        "SELECT * FROM table1 t1 FULL JOIN table1 t2 ON t1.time=t2.time",
        "FULL JOIN is not supported, only support INNER JOIN in current version");

    assertAnalyzeSemanticException(
        "SELECT * FROM table1 t1 FULL JOIN table1 t2 ON t1.time=t2.time WHERE t1.time>1",
        "FULL JOIN is not supported, only support INNER JOIN in current version");

    assertAnalyzeSemanticException(
        "SELECT * FROM table1 t1 LEFT JOIN table1 t2 ON t1.time=t2.time",
        "LEFT JOIN is not supported, only support INNER JOIN in current version");

    assertAnalyzeSemanticException(
        "SELECT * FROM table1 t1 RIGHT JOIN table1 t2 ON t1.time=t2.time",
        "RIGHT JOIN is not supported, only support INNER JOIN in current version");

    assertAnalyzeSemanticException(
        "SELECT * FROM table1 t1 CROSS JOIN table1 t2",
        "CROSS JOIN is not supported, only support INNER JOIN in current version");

    // TODO(beyyes) has non time equal join criteria;
  }
}
