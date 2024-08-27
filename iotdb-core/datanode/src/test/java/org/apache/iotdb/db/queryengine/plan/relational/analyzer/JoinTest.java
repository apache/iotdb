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

import org.apache.iotdb.db.protocol.session.IClientSession;
import org.apache.iotdb.db.queryengine.common.MPPQueryContext;
import org.apache.iotdb.db.queryengine.common.SessionInfo;
import org.apache.iotdb.db.queryengine.plan.planner.plan.DistributedQueryPlan;
import org.apache.iotdb.db.queryengine.plan.planner.plan.LogicalQueryPlan;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.sink.IdentitySinkNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.LogicalPlanner;
import org.apache.iotdb.db.queryengine.plan.relational.planner.Symbol;
import org.apache.iotdb.db.queryengine.plan.relational.planner.distribute.TableDistributedPlanner;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.JoinNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.LimitNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.MergeSortNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.OffsetNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.OutputNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.ProjectNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.SortNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.StreamSortNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.TableScanNode;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Statement;
import org.apache.iotdb.db.queryengine.plan.relational.sql.parser.SqlParser;
import org.apache.iotdb.db.queryengine.plan.statement.component.Ordering;

import org.junit.Assert;
import org.junit.Test;

import java.time.ZoneId;
import java.util.Collections;

import static org.apache.iotdb.db.queryengine.plan.relational.analyzer.AnalyzerTest.analyzeSQL;
import static org.apache.iotdb.db.queryengine.plan.relational.analyzer.AnalyzerTest.analyzeStatementWithException;
import static org.apache.iotdb.db.queryengine.plan.relational.analyzer.LimitOffsetPushDownTest.getChildrenNode;
import static org.apache.iotdb.db.queryengine.plan.relational.analyzer.TestUtils.ALL_DEVICE_ENTRIES;
import static org.apache.iotdb.db.queryengine.plan.relational.analyzer.TestUtils.DEFAULT_WARNING;
import static org.apache.iotdb.db.queryengine.plan.relational.analyzer.TestUtils.ORIGINAL_DEVICE_ENTRIES_1;
import static org.apache.iotdb.db.queryengine.plan.relational.analyzer.TestUtils.ORIGINAL_DEVICE_ENTRIES_2;
import static org.apache.iotdb.db.queryengine.plan.relational.analyzer.TestUtils.QUERY_CONTEXT;
import static org.apache.iotdb.db.queryengine.plan.relational.analyzer.TestUtils.QUERY_ID;
import static org.apache.iotdb.db.queryengine.plan.relational.analyzer.TestUtils.SESSION_INFO;
import static org.apache.iotdb.db.queryengine.plan.relational.analyzer.TestUtils.TEST_MATADATA;
import static org.apache.iotdb.db.queryengine.plan.relational.analyzer.TestUtils.assertJoinNodeEquals;
import static org.apache.iotdb.db.queryengine.plan.relational.analyzer.TestUtils.assertMergeSortNode;
import static org.apache.iotdb.db.queryengine.plan.relational.analyzer.TestUtils.assertTableScan;
import static org.apache.iotdb.db.queryengine.plan.relational.analyzer.TestUtils.buildSymbols;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.node.JoinNode.JoinType.INNER;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class JoinTest {
  Analysis analysis;
  MPPQueryContext context;

  LogicalQueryPlan logicalQueryPlan;
  PlanNode logicalPlanNode;
  JoinNode joinNode;
  OutputNode outputNode;
  ProjectNode projectNode;
  StreamSortNode streamSortNode;
  TableDistributedPlanner distributionPlanner;
  DistributedQueryPlan distributedQueryPlan;
  TableScanNode tableScanNode;

  // ========== table1 join table1 ===============

  // no filter, no sort
  @Test
  public void innerJoinTest1() {
    assertInnerJoinTest1(
        "SELECT t1.time, t1.tag1, t1.tag2, t1.attr2, t1.s1, t1.s2,"
            + "t2.tag1, t2.tag3, t2.attr2, t2.s1, t2.s3 "
            + "FROM table1 t1 JOIN table1 t2 ON t1.time = t2.time OFFSET 3 LIMIT 6");

    assertInnerJoinTest1(
        "SELECT t1.time, t1.tag1, t1.tag2, t1.attr2, t1.s1, t1.s2,"
            + "t2.tag1, t2.tag3, t2.attr2, t2.s1, t2.s3 "
            + "FROM table1 t1, table1 t2 WHERE t1.time = t2.time OFFSET 3 LIMIT 6");
  }

  private void assertInnerJoinTest1(String sql) {
    analysis = analyzeSQL(sql, TEST_MATADATA, QUERY_CONTEXT);
    logicalQueryPlan =
        new LogicalPlanner(QUERY_CONTEXT, TEST_MATADATA, SESSION_INFO, DEFAULT_WARNING)
            .plan(analysis);

    // LogicalPlan: `Output-Offset-Limit-Join-(Left + Right)-Sort-(Project)-TableScan`
    logicalPlanNode = logicalQueryPlan.getRootNode();
    assertTrue(logicalPlanNode instanceof OutputNode);
    assertTrue(getChildrenNode(logicalPlanNode, 1) instanceof OffsetNode);
    assertTrue(getChildrenNode(logicalPlanNode, 2) instanceof LimitNode);
    assertTrue(getChildrenNode(logicalPlanNode, 3) instanceof JoinNode);
    joinNode = (JoinNode) getChildrenNode(logicalPlanNode, 3);
    assertJoinNodeEquals(
        joinNode,
        INNER,
        Collections.singletonList(
            new JoinNode.EquiJoinClause(Symbol.of("time"), Symbol.of("time_0"))),
        buildSymbols("time", "tag1", "tag2", "attr2", "s1", "s2"),
        buildSymbols("tag1_1", "tag3_3", "attr2_5", "s1_6", "s3_8"));
    assertTrue(joinNode.getLeftChild() instanceof SortNode);
    assertTrue(joinNode.getRightChild() instanceof SortNode);
    SortNode leftSortNode = (SortNode) joinNode.getLeftChild();
    assertTrue(getChildrenNode(leftSortNode, 1) instanceof TableScanNode);
    TableScanNode leftTableScanNode = (TableScanNode) getChildrenNode(leftSortNode, 1);
    assertTableScan(leftTableScanNode, ALL_DEVICE_ENTRIES, Ordering.ASC, 0, 0, true);
    SortNode rightSortNode = (SortNode) joinNode.getRightChild();
    assertTrue(getChildrenNode(rightSortNode, 1) instanceof ProjectNode);
    assertTrue(getChildrenNode(rightSortNode, 2) instanceof TableScanNode);
    TableScanNode rightTableScanNode = (TableScanNode) getChildrenNode(rightSortNode, 2);
    assertTableScan(rightTableScanNode, ALL_DEVICE_ENTRIES, Ordering.ASC, 0, 0, true);

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
    assertTrue(getChildrenNode(outputNode, 3) instanceof JoinNode);
    joinNode = (JoinNode) getChildrenNode(outputNode, 3);
    assertTrue(joinNode.getLeftChild() instanceof MergeSortNode);
    MergeSortNode mergeSortNode = (MergeSortNode) joinNode.getLeftChild();
    assertMergeSortNode(mergeSortNode);
    leftSortNode = (SortNode) mergeSortNode.getChildren().get(1);
    tableScanNode = (TableScanNode) getChildrenNode(leftSortNode, 1);
    assertTableScan(tableScanNode, ORIGINAL_DEVICE_ENTRIES_1, Ordering.ASC, 0, 0, true);

    identitySinkNode =
        (IdentitySinkNode) distributedQueryPlan.getFragments().get(1).getPlanNodeTree();
    tableScanNode = (TableScanNode) getChildrenNode(identitySinkNode.getChildren().get(1), 2);
    assertTableScan(tableScanNode, ORIGINAL_DEVICE_ENTRIES_2, Ordering.ASC, 0, 0, true);
  }

  // no filter, no sort
  @Test
  public void innerJoinTest2() {
    assertInnerJoinTest1(
        "SELECT t1.time, t1.tag1, t1.tag2, t1.attr2, t1.s1, t1.s2,"
            + "t2.tag1, t2.tag3, t2.attr2, t2.s1, t2.s3 "
            + "FROM table1 t1 JOIN table1 t2 ON t1.time = t2.time OFFSET 3 LIMIT 6");

    assertInnerJoinTest1(
        "SELECT t1.time, t1.tag1, t1.tag2, t1.attr2, t1.s1, t1.s2,"
            + "t2.tag1, t2.tag3, t2.attr2, t2.s1, t2.s3 "
            + "FROM table1 t1, table1 t2 WHERE t1.time = t2.time OFFSET 3 LIMIT 6");
  }

  private void assertInnerJoinTest2(String sql) {
    analysis = analyzeSQL(sql, TEST_MATADATA, QUERY_CONTEXT);
    logicalQueryPlan =
        new LogicalPlanner(QUERY_CONTEXT, TEST_MATADATA, SESSION_INFO, DEFAULT_WARNING)
            .plan(analysis);

    // LogicalPlan: `Output-Offset-Limit-Join-(Left + Right)-Sort-(Project)-TableScan`
    logicalPlanNode = logicalQueryPlan.getRootNode();
    assertTrue(logicalPlanNode instanceof OutputNode);
    assertTrue(getChildrenNode(logicalPlanNode, 1) instanceof OffsetNode);
    assertTrue(getChildrenNode(logicalPlanNode, 2) instanceof LimitNode);
    assertTrue(getChildrenNode(logicalPlanNode, 3) instanceof JoinNode);
    joinNode = (JoinNode) getChildrenNode(logicalPlanNode, 3);
    assertJoinNodeEquals(
        joinNode,
        INNER,
        Collections.singletonList(
            new JoinNode.EquiJoinClause(Symbol.of("time"), Symbol.of("time_0"))),
        buildSymbols("time", "tag1", "tag2", "attr2", "s1", "s2"),
        buildSymbols("tag1_1", "tag3_3", "attr2_5", "s1_6", "s3_8"));
    assertTrue(joinNode.getLeftChild() instanceof SortNode);
    assertTrue(joinNode.getRightChild() instanceof SortNode);
    SortNode leftSortNode = (SortNode) joinNode.getLeftChild();
    assertTrue(getChildrenNode(leftSortNode, 1) instanceof TableScanNode);
    TableScanNode leftTableScanNode = (TableScanNode) getChildrenNode(leftSortNode, 1);
    assertTableScan(leftTableScanNode, ALL_DEVICE_ENTRIES, Ordering.ASC, 0, 0, true);
    SortNode rightSortNode = (SortNode) joinNode.getRightChild();
    assertTrue(getChildrenNode(rightSortNode, 1) instanceof ProjectNode);
    assertTrue(getChildrenNode(rightSortNode, 2) instanceof TableScanNode);
    TableScanNode rightTableScanNode = (TableScanNode) getChildrenNode(rightSortNode, 2);
    assertTableScan(rightTableScanNode, ALL_DEVICE_ENTRIES, Ordering.ASC, 0, 0, true);

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
    distributionPlanner = new TableDistributedPlanner(analysis, logicalQueryPlan);
    distributedQueryPlan = distributionPlanner.plan();
    assertEquals(3, distributedQueryPlan.getFragments().size());
    IdentitySinkNode identitySinkNode =
        (IdentitySinkNode) distributedQueryPlan.getFragments().get(0).getPlanNodeTree();
    outputNode = (OutputNode) getChildrenNode(identitySinkNode, 1);
    assertTrue(getChildrenNode(outputNode, 3) instanceof JoinNode);
    joinNode = (JoinNode) getChildrenNode(outputNode, 3);
    assertTrue(joinNode.getLeftChild() instanceof MergeSortNode);
    MergeSortNode mergeSortNode = (MergeSortNode) joinNode.getLeftChild();
    assertMergeSortNode(mergeSortNode);
    leftSortNode = (SortNode) mergeSortNode.getChildren().get(1);
    tableScanNode = (TableScanNode) getChildrenNode(leftSortNode, 1);
    assertTableScan(tableScanNode, ORIGINAL_DEVICE_ENTRIES_1, Ordering.ASC, 0, 0, true);

    identitySinkNode =
        (IdentitySinkNode) distributedQueryPlan.getFragments().get(1).getPlanNodeTree();
    tableScanNode = (TableScanNode) getChildrenNode(identitySinkNode.getChildren().get(1), 2);
    assertTableScan(tableScanNode, ORIGINAL_DEVICE_ENTRIES_2, Ordering.ASC, 0, 0, true);
  }

  @Test
  public void subQueryWithJoinTest() {
    // TODO
  }

  @Test
  public void sortWithJoinTest() {
    // TODO
  }

  // ========== unsupported test ===============

  @Test
  public void unsupportedJoinTest() {
    assertAnalyzeSemanticException(
        "SELECT * FROM table1 t1 INNER JOIN table1 t2 USING(time)",
        "Join Using clause is not supported in current version");

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

  private void assertAnalyzeSemanticException(String sql, String message) {
    try {
      context = new MPPQueryContext(sql, QUERY_ID, SESSION_INFO, null, null);
      SqlParser sqlParser = new SqlParser();
      Statement statement = sqlParser.createStatement(sql, ZoneId.systemDefault());
      SessionInfo session =
          new SessionInfo(
              0, "test", ZoneId.systemDefault(), "testdb", IClientSession.SqlDialect.TABLE);
      analyzeStatementWithException(statement, TEST_MATADATA, context, sqlParser, session);
      fail("Fail test sql: " + sql);
    } catch (Exception e) {
      Assert.assertTrue(e.getMessage(), e.getMessage().contains(message));
    }
  }
}
