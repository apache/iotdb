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

import org.apache.iotdb.db.queryengine.plan.relational.planner.PlanTester;
import org.apache.iotdb.db.queryengine.plan.relational.planner.assertions.PlanMatchPattern;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.JoinNode;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.ArithmeticBinaryExpression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.ComparisonExpression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.LongLiteral;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.SymbolReference;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.junit.Test;

import static org.apache.iotdb.db.queryengine.plan.relational.analyzer.TestUtils.assertAnalyzeSemanticException;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.assertions.PlanAssert.assertPlan;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.assertions.PlanMatchPattern.equiJoinClause;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.assertions.PlanMatchPattern.exchange;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.assertions.PlanMatchPattern.expression;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.assertions.PlanMatchPattern.filter;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.assertions.PlanMatchPattern.join;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.assertions.PlanMatchPattern.mergeSort;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.assertions.PlanMatchPattern.output;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.assertions.PlanMatchPattern.project;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.assertions.PlanMatchPattern.sort;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.assertions.PlanMatchPattern.tableScan;
import static org.apache.iotdb.db.queryengine.plan.relational.sql.ast.SortItem.NullOrdering.LAST;
import static org.apache.iotdb.db.queryengine.plan.relational.sql.ast.SortItem.Ordering.ASCENDING;
import static org.apache.iotdb.db.queryengine.plan.relational.sql.ast.SortItem.Ordering.DESCENDING;

public class AsofJoinTest {
  @Test
  public void simpleTest() {
    PlanTester planTester = new PlanTester();

    assertPlan(
        planTester.createPlan(
            "select table1.time,table1.s1,table2.time,table2.s1 from table1 asof join table2 on table1.time < table2.time"),
        output(
            join(
                JoinNode.JoinType.INNER,
                builder ->
                    builder
                        .asofCriteria(ComparisonExpression.Operator.LESS_THAN, "time", "time_0")
                        .left(
                            sort(
                                ImmutableList.of(sort("time", ASCENDING, LAST)),
                                tableScan(
                                    "testdb.table1",
                                    ImmutableList.of("time", "s1"),
                                    ImmutableSet.of("time", "s1"))))
                        .right(
                            sort(
                                ImmutableList.of(sort("time_0", ASCENDING, LAST)),
                                tableScan(
                                    "testdb.table2",
                                    ImmutableMap.of("time_0", "time", "s1_6", "s1")))))));

    // Asof operator is '>', appended ordering is DESC
    assertPlan(
        planTester.createPlan(
            "select table1.time,table1.s1,table2.time,table2.s1 from table1 asof join table2 on table1.time > table2.time"),
        output(
            join(
                JoinNode.JoinType.INNER,
                builder ->
                    builder
                        .asofCriteria(ComparisonExpression.Operator.GREATER_THAN, "time", "time_0")
                        .left(
                            sort(
                                ImmutableList.of(sort("time", DESCENDING, LAST)),
                                tableScan(
                                    "testdb.table1",
                                    ImmutableList.of("time", "s1"),
                                    ImmutableSet.of("time", "s1"))))
                        .right(
                            sort(
                                ImmutableList.of(sort("time_0", DESCENDING, LAST)),
                                tableScan(
                                    "testdb.table2",
                                    ImmutableMap.of("time_0", "time", "s1_6", "s1")))))));
  }

  @Test
  public void toleranceTest() {
    PlanTester planTester = new PlanTester();

    // tolerance will be converted to Filter after Join, predicate is 'table1.time <= table2.time +
    // 1'
    assertPlan(
        planTester.createPlan(
            "select table1.time,table1.s1,table2.time,table2.s1 from table1 asof (tolerance 1ms) join table2 on table1.time > table2.time"),
        output(
            project(
                filter(
                    join(
                        JoinNode.JoinType.INNER,
                        builder ->
                            builder
                                .asofCriteria(
                                    ComparisonExpression.Operator.GREATER_THAN, "time", "time_0")
                                .left(
                                    sort(
                                        ImmutableList.of(sort("time", DESCENDING, LAST)),
                                        tableScan(
                                            "testdb.table1",
                                            ImmutableList.of("time", "s1"),
                                            ImmutableSet.of("time", "s1"))))
                                .right(
                                    sort(
                                        ImmutableList.of(sort("time_0", DESCENDING, LAST)),
                                        project(
                                            tableScan(
                                                "testdb.table2",
                                                ImmutableMap.of(
                                                    "time_0", "time", "s1_6", "s1"))))))))));
  }

  @Test
  public void projectInAsofCriteriaTest() {
    PlanTester planTester = new PlanTester();

    // 'table1.tag1 = table2.tag1 and table1.time + 1 > table2.time'
    // => left: order by tag1, expr; right: order by tag1_1, time_0
    assertPlan(
        planTester.createPlan(
            "select table1.time,table1.s1,table2.time,table2.s1 from table1 asof (tolerance 1ms) join table2 on table1.tag1=table2.tag1 and table1.time+1 > table2.time"),
        output(
            project(
                filter(
                    join(
                        JoinNode.JoinType.INNER,
                        builder ->
                            builder
                                .asofCriteria(
                                    ComparisonExpression.Operator.GREATER_THAN, "expr", "time_0")
                                .equiCriteria("tag1", "tag1_1")
                                .left(
                                    sort(
                                        ImmutableList.of(
                                            sort("tag1", ASCENDING, LAST),
                                            sort("expr", DESCENDING, LAST)),
                                        project(
                                            ImmutableMap.of(
                                                "expr",
                                                expression(
                                                    new ArithmeticBinaryExpression(
                                                        ArithmeticBinaryExpression.Operator.ADD,
                                                        new SymbolReference("time"),
                                                        new LongLiteral("1")))),
                                            tableScan(
                                                "testdb.table1",
                                                ImmutableList.of("time", "tag1", "s1"),
                                                ImmutableSet.of("time", "tag1", "s1")))))
                                .right(
                                    sort(
                                        project(
                                            tableScan(
                                                "testdb.table2",
                                                ImmutableMap.of(
                                                    "time_0", "time", "tag1_1", "tag1", "s1_6",
                                                    "s1"))))))))));
  }

  @Test
  // could make use of sort properties of TableScanNode
  public void sortEliminateTest() {
    PlanTester planTester = new PlanTester();

    PlanMatchPattern table1 =
        tableScan(
            "testdb.table1",
            ImmutableList.of("time", "tag1", "tag2", "tag3", "s1"),
            ImmutableSet.of("time", "tag1", "tag2", "tag3", "s1"));
    PlanMatchPattern table2 =
        tableScan(
            "testdb.table2",
            ImmutableMap.of(
                "time_0", "time", "tag1_1", "tag1", "tag2_2", "tag2", "tag3_3", "tag3", "s1_6",
                "s1"));

    assertPlan(
        planTester.createPlan(
            "select table1.time,table1.s1,table2.time,table2.s1 from table1 asof join table2 on table1.tag1 = table2.tag1 and table1.tag2 = table2.tag2 and table1.tag3 = table2.tag3 and table1.time > table2.time"),
        output(
            join(
                JoinNode.JoinType.INNER,
                builder ->
                    builder
                        .asofCriteria(ComparisonExpression.Operator.GREATER_THAN, "time", "time_0")
                        .equiCriteria(
                            ImmutableList.of(
                                equiJoinClause("tag1", "tag1_1"),
                                equiJoinClause("tag2", "tag2_2"),
                                equiJoinClause("tag3", "tag3_3")))
                        .left(sort(table1))
                        .right(sort(table2)))));

    // DistributionPlan
    assertPlan(
        planTester.getFragmentPlan(0),
        output(
            join(
                JoinNode.JoinType.INNER,
                builder ->
                    builder
                        .asofCriteria(ComparisonExpression.Operator.GREATER_THAN, "time", "time_0")
                        .equiCriteria(
                            ImmutableList.of(
                                equiJoinClause("tag1", "tag1_1"),
                                equiJoinClause("tag2", "tag2_2"),
                                equiJoinClause("tag3", "tag3_3")))
                        .left(exchange())
                        .right(exchange()))));

    assertPlan(planTester.getFragmentPlan(1), mergeSort(exchange(), exchange(), exchange()));

    // SortNode has been eliminated
    assertPlan(planTester.getFragmentPlan(2), table1);
  }

  @Test
  public void exceptionTest() {
    String errMsg = "The join expression of ASOF should be single Comparison or Logical 'AND'";
    // Wrong ASOF main join expression type
    assertAnalyzeSemanticException(
        "select * from table1 asof join table2 on table1.time is not null", errMsg);
    // Wrong concat Logical type
    assertAnalyzeSemanticException(
        "select * from table1 asof join table2 on table1.tag1 = table2.tag1 or table1.time > table2.time",
        errMsg);

    errMsg =
        "The main join expression of ASOF should only be Comparison '>, >=, <, <=', actual is (table1.time = table2.time)";
    // Wrong ASOF main join expression operator
    assertAnalyzeSemanticException(
        "select * from table1 asof join table2 on table1.time = table2.time", errMsg);
    assertAnalyzeSemanticException(
        "select * from table1 asof (tolerance 1ms) join table2 on table1.time = table2.time",
        errMsg);
    assertAnalyzeSemanticException(
        "select * from table1 asof join table2 on table1.tag1 = table2.tag1 and table1.time = table2.time",
        errMsg);
    assertAnalyzeSemanticException(
        "select * from table1 asof (tolerance 1ms) join table2 on table1.tag1 = table2.tag1 and table1.time = table2.time",
        errMsg);

    // Wrong left or right Type of ASOF main join expression
    assertAnalyzeSemanticException(
        "select * from table1 asof (tolerance 1ms) join table2 on table1.tag1 = table2.tag1 and table1.s1 > table2.s1",
        "left child type of ASOF main JOIN expression must be TIMESTAMP: actual type INT64");
    assertAnalyzeSemanticException(
        "select * from table1 asof (tolerance 1ms) join table2 on table1.tag1 = table2.tag1 and table1.s1 > table2.time",
        "left child type of ASOF main JOIN expression must be TIMESTAMP: actual type INT64");
    assertAnalyzeSemanticException(
        "select * from table1 asof (tolerance 1ms) join table2 on table1.tag1 = table2.tag1 and table1.time > table2.s1",
        "right child type of ASOF main JOIN expression must be TIMESTAMP: actual type INT64");
  }
}
