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

import org.apache.iotdb.db.queryengine.plan.planner.plan.LogicalQueryPlan;
import org.apache.iotdb.db.queryengine.plan.relational.planner.assertions.PlanMatchPattern;
import org.apache.iotdb.db.queryengine.plan.relational.planner.ir.PredicateWithUncorrelatedScalarSubqueryReconstructor;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.JoinNode;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.ComparisonExpression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Expression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.LongLiteral;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.NotExpression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.SymbolReference;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.Collections;
import java.util.Optional;

import static org.apache.iotdb.db.queryengine.plan.relational.planner.assertions.PlanAssert.assertPlan;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.assertions.PlanMatchPattern.aggregation;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.assertions.PlanMatchPattern.aggregationFunction;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.assertions.PlanMatchPattern.anyTree;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.assertions.PlanMatchPattern.collect;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.assertions.PlanMatchPattern.exchange;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.assertions.PlanMatchPattern.filter;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.assertions.PlanMatchPattern.join;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.assertions.PlanMatchPattern.mergeSort;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.assertions.PlanMatchPattern.output;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.assertions.PlanMatchPattern.project;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.assertions.PlanMatchPattern.semiJoin;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.assertions.PlanMatchPattern.singleGroupingSet;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.assertions.PlanMatchPattern.sort;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.assertions.PlanMatchPattern.tableScan;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.node.AggregationNode.Step.FINAL;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.node.AggregationNode.Step.PARTIAL;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.node.AggregationNode.Step.SINGLE;
import static org.apache.iotdb.db.queryengine.plan.relational.sql.ast.ComparisonExpression.Operator.EQUAL;
import static org.apache.iotdb.db.queryengine.plan.relational.sql.ast.ComparisonExpression.Operator.GREATER_THAN;
import static org.apache.iotdb.db.queryengine.plan.relational.sql.ast.ComparisonExpression.Operator.LESS_THAN_OR_EQUAL;

public class UncorrelatedSubqueryTest {
  private PlanTester planTester;
  private PredicateWithUncorrelatedScalarSubqueryReconstructor
      predicateWithUncorrelatedScalarSubquery;

  @Before
  public void setUp() throws Exception {
    planTester = new PlanTester();
    mockPredicateWithUncorrelatedScalarSubquery();
  }

  private void mockPredicateWithUncorrelatedScalarSubquery() {
    predicateWithUncorrelatedScalarSubquery =
        Mockito.spy(new PredicateWithUncorrelatedScalarSubqueryReconstructor());
    Mockito.when(
            predicateWithUncorrelatedScalarSubquery.fetchUncorrelatedSubqueryResultForPredicate(
                Mockito.any(), Mockito.any(), Mockito.any(), Mockito.any()))
        .thenReturn(Optional.of(new LongLiteral("1")));
  }

  @Test
  public void testUncorrelatedScalarSubqueryInWhereClause() {
    String sql = "SELECT s1 FROM table1 where s1 = (select max(s1) from table1)";

    LogicalQueryPlan logicalQueryPlan =
        planTester.createPlan(sql, predicateWithUncorrelatedScalarSubquery);

    PlanMatchPattern tableScan =
        tableScan(
            "testdb.table1",
            ImmutableList.of("s1"),
            ImmutableSet.of("s1"),
            new ComparisonExpression(EQUAL, new SymbolReference("s1"), new LongLiteral("1")));

    // Verify full LogicalPlan
    /*
     *   └──OutputNode
     *           └──DeviceTableScanNode
     */
    assertPlan(logicalQueryPlan, output(tableScan));

    // Verify DistributionPlan
    assertPlan(planTester.getFragmentPlan(0), output(collect(exchange(), exchange(), exchange())));

    assertPlan(planTester.getFragmentPlan(1), tableScan);
    assertPlan(planTester.getFragmentPlan(2), tableScan);
    assertPlan(planTester.getFragmentPlan(3), tableScan);
  }

  @Test
  public void testUncorrelatedScalarSubqueryInWhereClauseWithEnforceSingleRowNode() {
    String sql = "SELECT s1 FROM table1 where s1 = (select s2 from table1)";

    LogicalQueryPlan logicalQueryPlan =
        planTester.createPlan(sql, predicateWithUncorrelatedScalarSubquery);

    PlanMatchPattern tableScan =
        tableScan(
            "testdb.table1",
            ImmutableList.of("s1"),
            ImmutableSet.of("s1"),
            new ComparisonExpression(EQUAL, new SymbolReference("s1"), new LongLiteral("1")));

    // Verify LogicalPlan
    /*
     *   └──OutputNode
     *           └──DeviceTableScanNode
     */
    assertPlan(logicalQueryPlan, output(tableScan));
  }

  @Test
  public void testUncorrelatedInPredicateSubquery() {
    String sql = "SELECT s1 FROM table1 where s1 in (select s1 from table1)";

    LogicalQueryPlan logicalQueryPlan =
        planTester.createPlan(sql, predicateWithUncorrelatedScalarSubquery);

    Expression filterPredicate = new SymbolReference("expr");

    PlanMatchPattern tableScan1 =
        tableScan("testdb.table1", ImmutableList.of("s1"), ImmutableSet.of("s1"));

    PlanMatchPattern tableScan2 = tableScan("testdb.table1", ImmutableMap.of("s1_6", "s1"));

    // Verify full LogicalPlan
    /*
    *   └──OutputNode
    *           └──ProjectNode
    *             └──FilterNode
    *               └──SemiJoinNode
    *                   |──SortNode
    *                   |   └──TableScanNode
    *                   ├──SortNode
    *                   │   └──TableScanNode

    */
    assertPlan(
        logicalQueryPlan,
        output(
            project(
                filter(
                    filterPredicate,
                    semiJoin("s1", "s1_6", "expr", sort(tableScan1), sort(tableScan2))))));

    // Verify DistributionPlan
    assertPlan(
        planTester.getFragmentPlan(0),
        output(
            project(
                filter(filterPredicate, semiJoin("s1", "s1_6", "expr", exchange(), exchange())))));

    assertPlan(planTester.getFragmentPlan(1), mergeSort(exchange(), exchange(), exchange()));

    assertPlan(planTester.getFragmentPlan(2), sort(tableScan1));

    assertPlan(planTester.getFragmentPlan(3), sort(tableScan2));

    assertPlan(planTester.getFragmentPlan(4), sort(tableScan2));

    assertPlan(planTester.getFragmentPlan(5), mergeSort(exchange(), exchange(), exchange()));
  }

  @Test
  public void testUncorrelatedNotInPredicateSubquery() {
    String sql = "SELECT s1 FROM table1 where s1 not in (select s1 from table1)";

    LogicalQueryPlan logicalQueryPlan =
        planTester.createPlan(sql, predicateWithUncorrelatedScalarSubquery);

    Expression filterPredicate = new NotExpression(new SymbolReference("expr"));

    PlanMatchPattern tableScan1 =
        tableScan("testdb.table1", ImmutableList.of("s1"), ImmutableSet.of("s1"));

    PlanMatchPattern tableScan2 = tableScan("testdb.table1", ImmutableMap.of("s1_6", "s1"));

    // Verify full LogicalPlan
    /*
    *   └──OutputNode
    *           └──ProjectNode
    *             └──FilterNode
    *               └──SemiJoinNode
    *                   |──SortNode
    *                   |   └──TableScanNode
    *                   ├──SortNode
    *                   │   └──TableScanNode

    */
    assertPlan(
        logicalQueryPlan,
        output(
            project(
                filter(
                    filterPredicate,
                    semiJoin("s1", "s1_6", "expr", sort(tableScan1), sort(tableScan2))))));
  }

  @Test
  public void testUncorrelatedAnyComparisonSubquery() {
    String sql = "SELECT s1 FROM table1 where s1 > any (select s1 from table1)";

    LogicalQueryPlan logicalQueryPlan =
        planTester.createPlan(sql, predicateWithUncorrelatedScalarSubquery);

    PlanMatchPattern tableScan1 =
        tableScan("testdb.table1", ImmutableList.of("s1"), ImmutableSet.of("s1"));

    PlanMatchPattern tableScan2 = tableScan("testdb.table1", ImmutableMap.of("s1_7", "s1"));

    PlanMatchPattern tableScan3 = tableScan("testdb.table1", ImmutableMap.of("s1_6", "s1"));

    // Verify full LogicalPlan
    /*
    *   └──OutputNode
    *           └──ProjectNode
    *             └──FilterNode
    *                └──JoinNode
    *                   |──TableScanNode
    *                   ├──AggregationNode
    *                      └──TableScanNode

    */
    assertPlan(
        logicalQueryPlan,
        output(
            project(
                anyTree(
                    join(
                        JoinNode.JoinType.INNER,
                        builder ->
                            builder
                                .left(tableScan1)
                                .right(
                                    aggregation(
                                        singleGroupingSet(),
                                        ImmutableMap.of(
                                            Optional.of("min"),
                                            aggregationFunction("min", ImmutableList.of("s1_7")),
                                            Optional.of("count_all"),
                                            aggregationFunction(
                                                "count_all", ImmutableList.of("s1_7")),
                                            Optional.of("count_non_null"),
                                            aggregationFunction("count", ImmutableList.of("s1_7"))),
                                        Collections.emptyList(),
                                        Optional.empty(),
                                        SINGLE,
                                        tableScan2)))))));

    // Verify DistributionPlan
    assertPlan(
        planTester.getFragmentPlan(0),
        output(
            project(
                filter(
                    join(
                        JoinNode.JoinType.INNER,
                        builder -> builder.left(exchange()).right(exchange()))))));

    assertPlan(planTester.getFragmentPlan(1), collect(exchange(), exchange(), exchange()));
    assertPlan(planTester.getFragmentPlan(2), tableScan1);
    assertPlan(planTester.getFragmentPlan(3), tableScan1);
    assertPlan(planTester.getFragmentPlan(4), tableScan1);

    assertPlan(
        planTester.getFragmentPlan(5),
        aggregation(
            singleGroupingSet(),
            ImmutableMap.of(
                Optional.of("min"),
                aggregationFunction("min", ImmutableList.of("min_9")),
                Optional.of("count_all"),
                aggregationFunction("count_all", ImmutableList.of("count_all_10")),
                Optional.of("count_non_null"),
                aggregationFunction("count", ImmutableList.of("count"))),
            Collections.emptyList(),
            Optional.empty(),
            FINAL,
            collect(exchange(), exchange(), exchange())));
    assertPlan(
        planTester.getFragmentPlan(6),
        aggregation(
            singleGroupingSet(),
            ImmutableMap.of(
                Optional.of("min_9"),
                aggregationFunction("min", ImmutableList.of("s1_6")),
                Optional.of("count_all_10"),
                aggregationFunction("count_all", ImmutableList.of("s1_6")),
                Optional.of("count"),
                aggregationFunction("count", ImmutableList.of("s1_6"))),
            Collections.emptyList(),
            Optional.empty(),
            PARTIAL,
            tableScan3));
    assertPlan(
        planTester.getFragmentPlan(7),
        aggregation(
            singleGroupingSet(),
            ImmutableMap.of(
                Optional.of("min_9"),
                aggregationFunction("min", ImmutableList.of("s1_6")),
                Optional.of("count_all_10"),
                aggregationFunction("count_all", ImmutableList.of("s1_6")),
                Optional.of("count"),
                aggregationFunction("count", ImmutableList.of("s1_6"))),
            Collections.emptyList(),
            Optional.empty(),
            PARTIAL,
            tableScan3));
    assertPlan(
        planTester.getFragmentPlan(8),
        aggregation(
            singleGroupingSet(),
            ImmutableMap.of(
                Optional.of("min_9"),
                aggregationFunction("min", ImmutableList.of("s1_6")),
                Optional.of("count_all_10"),
                aggregationFunction("count_all", ImmutableList.of("s1_6")),
                Optional.of("count"),
                aggregationFunction("count", ImmutableList.of("s1_6"))),
            Collections.emptyList(),
            Optional.empty(),
            PARTIAL,
            tableScan3));
  }

  @Test
  public void testUncorrelatedEqualsSomeComparisonSubquery() {
    String sql = "SELECT s1 FROM table1 where s1 = some (select s1 from table1)";

    LogicalQueryPlan logicalQueryPlan =
        planTester.createPlan(sql, predicateWithUncorrelatedScalarSubquery);

    Expression filterPredicate = new SymbolReference("expr");

    PlanMatchPattern tableScan1 =
        tableScan("testdb.table1", ImmutableList.of("s1"), ImmutableSet.of("s1"));

    PlanMatchPattern tableScan2 = tableScan("testdb.table1", ImmutableMap.of("s1_6", "s1"));

    // Verify full LogicalPlan
    /*
    *   └──OutputNode
    *           └──ProjectNode
    *             └──FilterNode
    *               └──SemiJoinNode
    *                   |──SortNode
    *                   |   └──TableScanNode
    *                   ├──SortNode
    *                   │   └──TableScanNode

    */
    assertPlan(
        logicalQueryPlan,
        output(
            project(
                filter(
                    filterPredicate,
                    semiJoin("s1", "s1_6", "expr", sort(tableScan1), sort(tableScan2))))));
  }

  @Test
  public void testUncorrelatedAllComparisonSubquery() {
    String sql = "SELECT s1 FROM table1 where s1 != all (select s1 from table1)";

    LogicalQueryPlan logicalQueryPlan =
        planTester.createPlan(sql, predicateWithUncorrelatedScalarSubquery);

    PlanMatchPattern tableScan1 =
        tableScan("testdb.table1", ImmutableList.of("s1"), ImmutableSet.of("s1"));

    PlanMatchPattern tableScan2 = tableScan("testdb.table1", ImmutableMap.of("s1_6", "s1"));

    // Verify full LogicalPlan
    /*
    *   └──OutputNode
    *           └──ProjectNode
    *             └──FilterNode
    *                └──SemiJoinNode
    *                    |──SortNode
    *                    |   └──TableScanNode
    *                    ├──SortNode
    *                    │   └──TableScanNode

    */
    assertPlan(
        logicalQueryPlan,
        output(
            project(anyTree(semiJoin("s1", "s1_6", "expr", sort(tableScan1), sort(tableScan2))))));
  }

  @Test
  public void testUncorrelatedExistsSubquery() {
    String sql = "SELECT s1 FROM table1 where exists(select s2 from table2)";

    LogicalQueryPlan logicalQueryPlan =
        planTester.createPlan(sql, predicateWithUncorrelatedScalarSubquery);

    PlanMatchPattern tableScan1 =
        tableScan("testdb.table1", ImmutableList.of("s1"), ImmutableSet.of("s1"));

    PlanMatchPattern tableScan2 = tableScan("testdb.table2", ImmutableList.of(), ImmutableSet.of());

    Expression filterPredicate =
        new ComparisonExpression(GREATER_THAN, new SymbolReference("count"), new LongLiteral("0"));
    // Verify full LogicalPlan
    /*
    *   └──OutputNode
    *      └──JoinNode
    *         |──TableScanNode
    *         |
    *         ├──ProjectNode
    *         │   └──FilterNode
    *         |      └──TableScanNode

    */
    assertPlan(
        logicalQueryPlan,
        output(
            join(
                JoinNode.JoinType.INNER,
                builder ->
                    builder
                        .left(tableScan1)
                        .right(
                            project(
                                filter(
                                    filterPredicate,
                                    aggregation(
                                        singleGroupingSet(),
                                        ImmutableMap.of(
                                            Optional.of("count"),
                                            aggregationFunction("count", ImmutableList.of())),
                                        Collections.emptyList(),
                                        Optional.empty(),
                                        SINGLE,
                                        tableScan2)))))));
  }

  @Test
  public void testUncorrelatedNotExistsSubquery() {
    String sql = "SELECT s1 FROM table1 where not exists(select s2 from table2)";

    LogicalQueryPlan logicalQueryPlan =
        planTester.createPlan(sql, predicateWithUncorrelatedScalarSubquery);

    PlanMatchPattern tableScan1 =
        tableScan("testdb.table1", ImmutableList.of("s1"), ImmutableSet.of("s1"));

    PlanMatchPattern tableScan2 = tableScan("testdb.table2", ImmutableList.of(), ImmutableSet.of());

    Expression filterPredicate =
        new ComparisonExpression(
            LESS_THAN_OR_EQUAL, new SymbolReference("count"), new LongLiteral("0"));
    // Verify full LogicalPlan
    /*
    *   └──OutputNode
    *      └──JoinNode
    *         |──TableScanNode
    *         |
    *         ├──ProjectNode
    *         │   └──FilterNode
    *         │      └──Aggregation
    *         |          └──TableScanNode

    */
    assertPlan(
        logicalQueryPlan,
        output(
            join(
                JoinNode.JoinType.INNER,
                builder ->
                    builder
                        .left(tableScan1)
                        .right(
                            project(
                                filter(
                                    filterPredicate,
                                    aggregation(
                                        singleGroupingSet(),
                                        ImmutableMap.of(
                                            Optional.of("count"),
                                            aggregationFunction("count", ImmutableList.of())),
                                        Collections.emptyList(),
                                        Optional.empty(),
                                        SINGLE,
                                        tableScan2)))))));
  }

  @Test
  public void testUncorrelatedHavingSubquery() {
    String sql =
        "SELECT min(time) as min FROM table1 group by s1 having min(time) > (select max(time) from table2)";
    LogicalQueryPlan logicalQueryPlan =
        planTester.createPlan(sql, predicateWithUncorrelatedScalarSubquery);

    PlanMatchPattern tableScan =
        tableScan("testdb.table1", ImmutableList.of("time", "s1"), ImmutableSet.of("time", "s1"));
    PlanMatchPattern agg =
        aggregation(
            singleGroupingSet("s1"),
            ImmutableMap.of(
                Optional.of("min"), aggregationFunction("min", ImmutableList.of("time"))),
            ImmutableList.of(),
            Optional.empty(),
            SINGLE,
            tableScan);

    Expression filterPredicate =
        new ComparisonExpression(GREATER_THAN, new SymbolReference("min"), new LongLiteral("1"));

    // Verify full LogicalPlan
    /*
     *   └──OutputNode
     *      └──FilterNode
     *         ├──ProjectNode
     *            └──Aggregation
     *               └──TableScanNode
     */

    assertPlan(logicalQueryPlan, output(filter(filterPredicate, project(agg))));
  }
}
