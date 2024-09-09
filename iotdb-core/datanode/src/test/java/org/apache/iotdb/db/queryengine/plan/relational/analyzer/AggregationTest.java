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

import org.apache.iotdb.db.queryengine.plan.planner.plan.LogicalQueryPlan;
import org.apache.iotdb.db.queryengine.plan.relational.planner.PlanTester;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.ArithmeticBinaryExpression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.ComparisonExpression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.FunctionCall;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.LongLiteral;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.QualifiedName;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.SymbolReference;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.junit.Test;

import java.util.Optional;

import static org.apache.iotdb.db.queryengine.plan.relational.planner.assertions.PlanAssert.assertPlan;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.assertions.PlanMatchPattern.aggregation;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.assertions.PlanMatchPattern.aggregationFunction;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.assertions.PlanMatchPattern.aggregationTableScan;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.assertions.PlanMatchPattern.collect;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.assertions.PlanMatchPattern.exchange;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.assertions.PlanMatchPattern.expression;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.assertions.PlanMatchPattern.filter;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.assertions.PlanMatchPattern.mergeSort;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.assertions.PlanMatchPattern.output;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.assertions.PlanMatchPattern.project;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.assertions.PlanMatchPattern.singleGroupingSet;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.assertions.PlanMatchPattern.tableScan;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.node.AggregationNode.Step.FINAL;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.node.AggregationNode.Step.PARTIAL;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.node.AggregationNode.Step.SINGLE;
import static org.apache.iotdb.db.queryengine.plan.relational.sql.ast.ArithmeticBinaryExpression.Operator.ADD;
import static org.apache.iotdb.db.queryengine.plan.relational.sql.ast.ArithmeticBinaryExpression.Operator.MULTIPLY;
import static org.apache.iotdb.db.queryengine.plan.relational.sql.ast.ComparisonExpression.Operator.LESS_THAN;

// This test covers the remaining DistributionPlan cases that TSBSTest doesn't cover
public class AggregationTest {
  @Test
  public void noPushDownTest() {
    PlanTester planTester = new PlanTester();

    // Measurement column appears in groupingKeys
    // Output - Project - Aggregation - TableScan
    LogicalQueryPlan logicalQueryPlan =
        planTester.createPlan("SELECT count(s2) FROM table1 group by s1");
    assertPlan(
        logicalQueryPlan,
        output(
            project(
                aggregation(
                    singleGroupingSet("s1"),
                    ImmutableMap.of(
                        Optional.empty(), aggregationFunction("count", ImmutableList.of("s2"))),
                    ImmutableList.of(), // UnStreamable
                    Optional.empty(),
                    SINGLE,
                    tableScan(
                        "testdb.table1",
                        ImmutableList.of("s1", "s2"),
                        ImmutableSet.of("s1", "s2"))))));

    //                               - Exchange
    // Output - Project - Agg(FINAL) - Collect - Agg(PARTIAL) - TableScan
    //                               - Exchange
    assertPlan(
        planTester.getFragmentPlan(0),
        output(
            project(
                aggregation(
                    singleGroupingSet("s1"),
                    ImmutableMap.of(
                        Optional.empty(),
                        aggregationFunction("count", ImmutableList.of("count_0"))),
                    ImmutableList.of(), // UnStreamable
                    Optional.empty(),
                    FINAL,
                    collect(
                        exchange(),
                        aggregation(
                            singleGroupingSet("s1"),
                            ImmutableMap.of(
                                Optional.of("count_0"),
                                aggregationFunction("count", ImmutableList.of("s2"))),
                            ImmutableList.of(), // UnStreamable
                            Optional.empty(),
                            PARTIAL,
                            tableScan(
                                "testdb.table1",
                                ImmutableList.of("s1", "s2"),
                                ImmutableSet.of("s1", "s2"))),
                        exchange())))));
    // Agg(PARTIAL) - TableScan
    assertPlan(
        planTester.getFragmentPlan(1),
        aggregation(
            singleGroupingSet("s1"),
            ImmutableMap.of(
                Optional.of("count_0"), aggregationFunction("count", ImmutableList.of("s2"))),
            ImmutableList.of(), // UnStreamable
            Optional.empty(),
            PARTIAL,
            tableScan("testdb.table1", ImmutableList.of("s1", "s2"), ImmutableSet.of("s1", "s2"))));
    // Agg(PARTIAL) - TableScan
    assertPlan(
        planTester.getFragmentPlan(2),
        aggregation(
            singleGroupingSet("s1"),
            ImmutableMap.of(
                Optional.of("count_0"), aggregationFunction("count", ImmutableList.of("s2"))),
            ImmutableList.of(), // UnStreamable
            Optional.empty(),
            PARTIAL,
            tableScan("testdb.table1", ImmutableList.of("s1", "s2"), ImmutableSet.of("s1", "s2"))));

    // Output - Project - Aggregation - TableScan
    assertPlan(
        planTester.createPlan("SELECT count(s2) FROM table1 group by s1, tag1, tag2, tag3, time"),
        output(
            project(
                aggregation(
                    singleGroupingSet("s1", "tag1", "tag2", "tag3", "time"),
                    ImmutableMap.of(
                        Optional.empty(), aggregationFunction("count", ImmutableList.of("s2"))),
                    ImmutableList.of("tag1", "tag2", "tag3"), // Streamable
                    Optional.empty(),
                    SINGLE,
                    tableScan(
                        "testdb.table1",
                        ImmutableList.of("time", "tag1", "tag2", "tag3", "s1", "s2"),
                        ImmutableSet.of("time", "tag1", "tag2", "tag3", "s1", "s2"))))));

    //                               - Exchange
    // Output - Project - Agg(FINAL) - Collect - Agg(PARTIAL) - TableScan
    //                               - Exchange
    assertPlan(
        planTester.getFragmentPlan(0),
        output(
            project(
                aggregation(
                    singleGroupingSet("s1", "tag1", "tag2", "tag3", "time"),
                    ImmutableMap.of(
                        Optional.empty(),
                        aggregationFunction("count", ImmutableList.of("count_0"))),
                    ImmutableList.of("tag1", "tag2", "tag3"), // Streamable
                    Optional.empty(),
                    FINAL,
                    mergeSort(
                        exchange(),
                        aggregation(
                            singleGroupingSet("s1", "tag1", "tag2", "tag3", "time"),
                            ImmutableMap.of(
                                Optional.of("count_0"),
                                aggregationFunction("count", ImmutableList.of("s2"))),
                            ImmutableList.of("tag1", "tag2", "tag3"), // Streamable
                            Optional.empty(),
                            PARTIAL,
                            tableScan(
                                "testdb.table1",
                                ImmutableList.of("time", "tag1", "tag2", "tag3", "s1", "s2"),
                                ImmutableSet.of("time", "tag1", "tag2", "tag3", "s1", "s2"))),
                        exchange())))));
    assertPlan(
        planTester.getFragmentPlan(1),
        aggregation(
            singleGroupingSet("s1", "tag1", "tag2", "tag3", "time"),
            ImmutableMap.of(
                Optional.of("count_0"), aggregationFunction("count", ImmutableList.of("s2"))),
            ImmutableList.of("tag1", "tag2", "tag3"), // Streamable
            Optional.empty(),
            PARTIAL,
            tableScan(
                "testdb.table1",
                ImmutableList.of("time", "tag1", "tag2", "tag3", "s1", "s2"),
                ImmutableSet.of("time", "tag1", "tag2", "tag3", "s1", "s2"))));
    assertPlan(
        planTester.getFragmentPlan(2),
        aggregation(
            singleGroupingSet("s1", "tag1", "tag2", "tag3", "time"),
            ImmutableMap.of(
                Optional.of("count_0"), aggregationFunction("count", ImmutableList.of("s2"))),
            ImmutableList.of("tag1", "tag2", "tag3"), // Streamable
            Optional.empty(),
            PARTIAL,
            tableScan(
                "testdb.table1",
                ImmutableList.of("time", "tag1", "tag2", "tag3", "s1", "s2"),
                ImmutableSet.of("time", "tag1", "tag2", "tag3", "s1", "s2"))));
    // TODO Add more tests about streamable when develop subquery, especially when there is SortNode

    // Expr appears in groupingKeys, and it is not date_bin(time)
    // Output - Project - Aggregation - Project - TableScan
    logicalQueryPlan =
        planTester.createPlan("SELECT count(s2) FROM table1 group by substring(tag1,1)");
    assertPlan(
        logicalQueryPlan,
        output(
            project(
                aggregation(
                    singleGroupingSet("substring"),
                    ImmutableMap.of(
                        Optional.empty(), aggregationFunction("count", ImmutableList.of("s2"))),
                    ImmutableList.of(), // UnStreamable
                    Optional.empty(),
                    SINGLE,
                    project(
                        ImmutableMap.of(
                            "substring",
                            expression(
                                new FunctionCall(
                                    QualifiedName.of("substring"),
                                    ImmutableList.of(
                                        new SymbolReference("tag1"), new LongLiteral("1"))))),
                        tableScan(
                            "testdb.table1",
                            ImmutableList.of("tag1", "s2"),
                            ImmutableSet.of("tag1", "s2")))))));

    // Value filter exists and cannot be push-down
    // Output - Project - Aggregation - Project - TableScan (The value filter has been push-down)
    logicalQueryPlan =
        planTester.createPlan("SELECT count(s2) FROM table1 where s1*s2 < 1 group by tag1");
    assertPlan(
        logicalQueryPlan,
        output(
            project(
                aggregation(
                    singleGroupingSet("tag1"),
                    ImmutableMap.of(
                        Optional.empty(), aggregationFunction("count", ImmutableList.of("s2"))),
                    ImmutableList.of("tag1"), // Streamable
                    Optional.empty(),
                    SINGLE,
                    project(
                        filter(
                            new ComparisonExpression(
                                LESS_THAN,
                                new ArithmeticBinaryExpression(
                                    MULTIPLY, new SymbolReference("s1"), new SymbolReference("s2")),
                                new LongLiteral("1")),
                            tableScan(
                                "testdb.table1",
                                ImmutableList.of("tag1", "s1", "s2"),
                                ImmutableSet.of("tag1", "s2", "s1"))))))));

    // AggFunction cannot be push down
    // Output - Project - Aggregation - TableScan
    logicalQueryPlan = planTester.createPlan("SELECT mode(s2) FROM table1 group by tag1");
    assertPlan(
        logicalQueryPlan,
        output(
            project(
                aggregation(
                    singleGroupingSet("tag1"),
                    ImmutableMap.of(
                        Optional.empty(), aggregationFunction("mode", ImmutableList.of("s2"))),
                    ImmutableList.of("tag1"), // Streamable
                    Optional.empty(),
                    SINGLE,
                    tableScan(
                        "testdb.table1",
                        ImmutableList.of("tag1", "s2"),
                        ImmutableSet.of("tag1", "s2"))))));

    // Expr appears in arguments of AggFunction
    // Output - Project - Aggregation - Project - TableScan
    logicalQueryPlan = planTester.createPlan("SELECT count(s2+1) FROM table1 group by tag1");
    assertPlan(
        logicalQueryPlan,
        output(
            project(
                aggregation(
                    singleGroupingSet("tag1"),
                    ImmutableMap.of(
                        Optional.empty(), aggregationFunction("count", ImmutableList.of("expr_1"))),
                    ImmutableList.of("tag1"), // Streamable
                    Optional.empty(),
                    SINGLE,
                    project(
                        ImmutableMap.of(
                            "expr_1",
                            expression(
                                new ArithmeticBinaryExpression(
                                    ADD, new SymbolReference("s2"), new LongLiteral("1")))),
                        tableScan(
                            "testdb.table1",
                            ImmutableList.of("tag1", "s2"),
                            ImmutableSet.of("tag1", "s2")))))));
  }

  @Test
  public void partialPushDownTest() {
    PlanTester planTester = new PlanTester();

    // Only partial ID columns appear in GroupingKeys
    // Output - Project - Aggregation - AggTableScan
    LogicalQueryPlan logicalQueryPlan =
        planTester.createPlan("SELECT count(s2) FROM table1 group by tag1");
    assertPlan(
        logicalQueryPlan,
        output(
            project(
                aggregation(
                    singleGroupingSet("tag1"),
                    ImmutableMap.of(
                        Optional.empty(),
                        aggregationFunction("count", ImmutableList.of("count_0"))),
                    ImmutableList.of("tag1"), // Streamable
                    Optional.empty(),
                    FINAL,
                    aggregationTableScan(
                        singleGroupingSet("tag1"),
                        ImmutableList.of("tag1"), // Streamable
                        Optional.empty(),
                        PARTIAL,
                        "testdb.table1",
                        ImmutableList.of("tag1", "count_0"),
                        ImmutableSet.of("tag1", "s2"))))));

    // Only Attribute columns appear in GroupingKeys
    // Output - Project - Aggregation - AggTableScan
    logicalQueryPlan = planTester.createPlan("SELECT count(s2) FROM table1 group by attr1");
    assertPlan(
        logicalQueryPlan,
        output(
            project(
                aggregation(
                    singleGroupingSet("attr1"),
                    ImmutableMap.of(
                        Optional.empty(),
                        aggregationFunction("count", ImmutableList.of("count_0"))),
                    ImmutableList.of("attr1"), // Streamable
                    Optional.empty(),
                    FINAL,
                    aggregationTableScan(
                        singleGroupingSet("attr1"),
                        ImmutableList.of("attr1"), // Streamable
                        Optional.empty(),
                        PARTIAL,
                        "testdb.table1",
                        ImmutableList.of("attr1", "count_0"),
                        ImmutableSet.of("attr1", "s2"))))));

    // Only partial ID columns, Attribute columns, time or date_bin(time) appear in GroupingKeys
    // Output - Project - Aggregation - AggTableScan
    logicalQueryPlan =
        planTester.createPlan(
            "SELECT count(s2) FROM table1 group by attr1, tag1, date_bin(1h, time)");
    assertPlan(
        logicalQueryPlan,
        output(
            project(
                aggregation(
                    singleGroupingSet("attr1", "tag1", "date_bin$gid"),
                    ImmutableMap.of(
                        Optional.empty(),
                        aggregationFunction("count", ImmutableList.of("count_0"))),
                    ImmutableList.of("attr1", "tag1"), // Streamable
                    Optional.empty(),
                    FINAL,
                    aggregationTableScan(
                        singleGroupingSet("attr1", "tag1", "date_bin$gid"),
                        ImmutableList.of("attr1", "tag1"), // Streamable
                        Optional.empty(),
                        PARTIAL,
                        "testdb.table1",
                        ImmutableList.of("attr1", "tag1", "date_bin$gid", "count_0"),
                        ImmutableSet.of("attr1", "tag1", "time", "s2"))))));
  }

  @Test
  public void completePushDownTest() {
    PlanTester planTester = new PlanTester();

    // All ID columns appear in GroupingKeys
    // Output - Project - AggTableScan
    LogicalQueryPlan logicalQueryPlan =
        planTester.createPlan("SELECT count(s2) FROM table1 group by tag1, tag2, tag3");
    assertPlan(
        logicalQueryPlan,
        output(
            project(
                aggregationTableScan(
                    singleGroupingSet("tag1", "tag2", "tag3"),
                    ImmutableList.of("tag1", "tag2", "tag3"), // Streamable
                    Optional.empty(),
                    SINGLE,
                    "testdb.table1",
                    ImmutableList.of("tag1", "tag2", "tag3", "count"),
                    ImmutableSet.of("tag1", "tag2", "tag3", "s2")))));

    // Output - MergeSort - Project - AggTableScan
    assertPlan(
        planTester.getFragmentPlan(0),
        output(
            mergeSort(
                exchange(),
                project(
                    aggregationTableScan(
                        singleGroupingSet("tag1", "tag2", "tag3"),
                        ImmutableList.of("tag1", "tag2", "tag3"), // Streamable
                        Optional.empty(),
                        SINGLE,
                        "testdb.table1",
                        ImmutableList.of("tag1", "tag2", "tag3", "count"),
                        ImmutableSet.of("tag1", "tag2", "tag3", "s2"))),
                exchange())));

    // Project - AggTableScan
    assertPlan(
        planTester.getFragmentPlan(1),
        project(
            aggregationTableScan(
                singleGroupingSet("tag1", "tag2", "tag3"),
                ImmutableList.of("tag1", "tag2", "tag3"), // Streamable
                Optional.empty(),
                SINGLE,
                "testdb.table1",
                ImmutableList.of("tag1", "tag2", "tag3", "count"),
                ImmutableSet.of("tag1", "tag2", "tag3", "s2"))));

    // Project - AggTableScan
    assertPlan(
        planTester.getFragmentPlan(2),
        project(
            aggregationTableScan(
                singleGroupingSet("tag1", "tag2", "tag3"),
                ImmutableList.of("tag1", "tag2", "tag3"), // Streamable
                Optional.empty(),
                SINGLE,
                "testdb.table1",
                ImmutableList.of("tag1", "tag2", "tag3", "count"),
                ImmutableSet.of("tag1", "tag2", "tag3", "s2"))));

    // All ID columns appear in GroupingKeys, and Attribute columns , time or date_bin(time) also
    // appear
    logicalQueryPlan =
        planTester.createPlan(
            "SELECT count(s2) FROM table1 group by tag1, tag2, tag3, attr1, date_bin(1h, time)");
    assertPlan(
        logicalQueryPlan,
        output(
            project(
                aggregationTableScan(
                    singleGroupingSet("tag1", "tag2", "tag3", "attr1", "date_bin$gid"),
                    ImmutableList.of("tag1", "tag2", "tag3", "attr1"), // Streamable
                    Optional.empty(),
                    SINGLE,
                    "testdb.table1",
                    ImmutableList.of("tag1", "tag2", "tag3", "attr1", "date_bin$gid", "count"),
                    ImmutableSet.of("tag1", "tag2", "tag3", "attr1", "time", "s2")))));
    // Output - Project - Agg(FINAL) - MergeSort - AggTableScan
    assertPlan(
        planTester.getFragmentPlan(0),
        output(
            project(
                aggregation(
                    singleGroupingSet("tag1", "tag2", "tag3", "attr1", "date_bin$gid"),
                    ImmutableMap.of(
                        Optional.empty(),
                        aggregationFunction("count", ImmutableList.of("count_0"))),
                    ImmutableList.of("tag1", "tag2", "tag3", "attr1"), // Streamable
                    Optional.empty(),
                    FINAL,
                    mergeSort(
                        exchange(),
                        aggregationTableScan(
                            singleGroupingSet("tag1", "tag2", "tag3", "attr1", "date_bin$gid"),
                            ImmutableList.of("tag1", "tag2", "tag3", "attr1"), // Streamable
                            Optional.empty(),
                            PARTIAL,
                            "testdb.table1",
                            ImmutableList.of(
                                "tag1", "tag2", "tag3", "attr1", "date_bin$gid", "count_0"),
                            ImmutableSet.of("tag1", "tag2", "tag3", "attr1", "time", "s2")),
                        exchange())))));

    assertPlan(
        planTester.getFragmentPlan(1),
        aggregationTableScan(
            singleGroupingSet("tag1", "tag2", "tag3", "attr1", "date_bin$gid"),
            ImmutableList.of("tag1", "tag2", "tag3", "attr1"), // Streamable
            Optional.empty(),
            PARTIAL,
            "testdb.table1",
            ImmutableList.of("tag1", "tag2", "tag3", "attr1", "date_bin$gid", "count_0"),
            ImmutableSet.of("tag1", "tag2", "tag3", "attr1", "time", "s2")));

    assertPlan(
        planTester.getFragmentPlan(2),
        aggregationTableScan(
            singleGroupingSet("tag1", "tag2", "tag3", "attr1", "date_bin$gid"),
            ImmutableList.of("tag1", "tag2", "tag3", "attr1"), // Streamable
            Optional.empty(),
            PARTIAL,
            "testdb.table1",
            ImmutableList.of("tag1", "tag2", "tag3", "attr1", "date_bin$gid", "count_0"),
            ImmutableSet.of("tag1", "tag2", "tag3", "attr1", "time", "s2")));

    logicalQueryPlan =
        planTester.createPlan(
            "SELECT count(s2) FROM table1 group by tag1, tag2, tag3, attr1, time");
    assertPlan(
        logicalQueryPlan,
        output(
            project(
                aggregationTableScan(
                    singleGroupingSet("tag1", "tag2", "tag3", "attr1", "time"),
                    ImmutableList.of("tag1", "tag2", "tag3", "attr1"), // Streamable
                    Optional.empty(),
                    SINGLE,
                    "testdb.table1",
                    ImmutableList.of("tag1", "tag2", "tag3", "attr1", "time", "count"),
                    ImmutableSet.of("tag1", "tag2", "tag3", "attr1", "time", "s2")))));

    // GlobalAggregation
    assertPlan(
        planTester.createPlan("SELECT count(s2) FROM table1"),
        output(
            aggregationTableScan(
                singleGroupingSet(),
                ImmutableList.of(), // UnStreamable
                Optional.empty(),
                SINGLE,
                "testdb.table1",
                ImmutableList.of("count"),
                ImmutableSet.of("s2"))));
    assertPlan(
        planTester.getFragmentPlan(0),
        output(
            collect(
                exchange(),
                aggregationTableScan(
                    singleGroupingSet(),
                    ImmutableList.of(), // UnStreamable
                    Optional.empty(),
                    SINGLE,
                    "testdb.table1",
                    ImmutableList.of("count"),
                    ImmutableSet.of("s2")),
                exchange())));
    assertPlan(
        planTester.getFragmentPlan(1),
        aggregationTableScan(
            singleGroupingSet(),
            ImmutableList.of(), // UnStreamable
            Optional.empty(),
            SINGLE,
            "testdb.table1",
            ImmutableList.of("count"),
            ImmutableSet.of("s2")));
    assertPlan(
        planTester.getFragmentPlan(2),
        aggregationTableScan(
            singleGroupingSet(),
            ImmutableList.of(), // UnStreamable
            Optional.empty(),
            SINGLE,
            "testdb.table1",
            ImmutableList.of("count"),
            ImmutableSet.of("s2")));
  }
}
