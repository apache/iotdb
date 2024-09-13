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
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.ArithmeticBinaryExpression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Cast;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.ComparisonExpression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.DoubleLiteral;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Expression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.LogicalExpression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.LongLiteral;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.SymbolReference;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.junit.Test;

import static org.apache.iotdb.db.queryengine.plan.relational.planner.assertions.PlanAssert.assertPlan;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.assertions.PlanMatchPattern.any;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.assertions.PlanMatchPattern.anyTree;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.assertions.PlanMatchPattern.dataType;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.assertions.PlanMatchPattern.exchange;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.assertions.PlanMatchPattern.expression;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.assertions.PlanMatchPattern.filter;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.assertions.PlanMatchPattern.mergeSort;
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

public class ExampleTest {
  @Test
  public void exampleTest() {
    PlanTester planTester = new PlanTester();

    String sql =
        "SELECT time, tag3, substring(tag1, 1), cast(s2 as double), s2+s3, attr1 FROM table1 "
            + "where s1>1 and s1+s3>0 and cast(s1 as double)>1.0 order by time desc, s1+s2 asc, tag2 asc, tag1 desc offset 5";

    LogicalQueryPlan logicalQueryPlan = planTester.createPlan(sql);

    // (("s1" + "s3") > 0) AND (CAST("s1" AS double) > 1E0)
    Expression filterPredicate =
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
                    new DoubleLiteral("1.0"))));

    PlanMatchPattern tableScan =
        tableScan(
            "testdb.table1",
            ImmutableList.of("time", "tag1", "tag2", "tag3", "attr1", "s1", "s2", "s3"),
            ImmutableSet.of("time", "tag1", "tag2", "tag3", "attr1", "s1", "s2", "s3"));

    // Verify full LogicalPlan
    // Output - Offset -Project - MergeSort - Sort - Project - Filter - TableScan
    assertPlan(
        logicalQueryPlan,
        output(
            offset(
                5,
                project(
                    sort(
                        ImmutableList.of(
                            sort("time", DESCENDING, LAST),
                            sort("expr_1", ASCENDING, LAST),
                            sort("tag2", ASCENDING, LAST),
                            sort("tag1", DESCENDING, LAST)),
                        project( // We need to indicate alias of expr_1 for parent
                            ImmutableMap.of(
                                "expr_1",
                                expression(
                                    new ArithmeticBinaryExpression(
                                        ADD,
                                        new SymbolReference("s1"),
                                        new SymbolReference("s2")))),
                            filter(filterPredicate, tableScan)))))));

    // You can use anyTree() to match any partial(at least one Node) of Plan
    assertPlan(logicalQueryPlan, output(anyTree(project(filter(filterPredicate, tableScan)))));

    // Verify DistributionPlan

    /*
     *   └──OutputNode-8
     *           └──OffsetNode-6
     *             └──ProjectNode
     *               └──MergeSortNode-25
     *                   ├──ExchangeNode-29: [SourceAddress:192.0.12.1/test_query.2.0/31]
     *                   ├──SortNode-27
     *                   │   └──ProjectNode-23
     *                   │           └──FilterNode-17
     *                   │               └──TableScanNode-14
     *                   └──ExchangeNode-30: [SourceAddress:192.0.10.1/test_query.3.0/32]
     */
    assertPlan(
        planTester.getFragmentPlan(0),
        anyTree(
            project(
                mergeSort(
                    exchange(), sort(project(filter(filterPredicate, tableScan))), exchange()))));

    /*
     *    SortNode-26
     *       └──ProjectNode-22
     *               └──FilterNode-16
     *                   └──TableScanNode-13
     */
    assertPlan(
        planTester.getFragmentPlan(1),
        any( // use any() to match any one node
            project(filter(filterPredicate, tableScan))));

    /*
     *    SortNode-26
     *           └──ProjectNode-19
     *               └──FilterNode-16
     *                   └──TableScanNode-13
     */
    assertPlan(planTester.getFragmentPlan(2), any(project(filter(filterPredicate, tableScan))));
  }
}
