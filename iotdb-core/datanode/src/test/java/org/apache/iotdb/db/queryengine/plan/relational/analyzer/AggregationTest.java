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
import static org.apache.iotdb.db.queryengine.plan.relational.planner.assertions.PlanMatchPattern.expression;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.assertions.PlanMatchPattern.output;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.assertions.PlanMatchPattern.project;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.assertions.PlanMatchPattern.singleGroupingSet;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.assertions.PlanMatchPattern.tableScan;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.node.AggregationNode.Step.FINAL;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.node.AggregationNode.Step.PARTIAL;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.node.AggregationNode.Step.SINGLE;
import static org.apache.iotdb.db.queryengine.plan.relational.sql.ast.ArithmeticBinaryExpression.Operator.ADD;

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
                    Optional.empty(),
                    SINGLE,
                    tableScan(
                        "testdb.table1",
                        ImmutableList.of("s1", "s2"),
                        ImmutableSet.of("s1", "s2"))))));

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

    // Value filter exists
    // Output - Project - Aggregation - Project - TableScan (The value filter has been push-down)
    logicalQueryPlan =
        planTester.createPlan("SELECT count(s2) FROM table1 where s1 < 1 group by tag1");
    assertPlan(
        logicalQueryPlan,
        output(
            project(
                aggregation(
                    singleGroupingSet("tag1"),
                    ImmutableMap.of(
                        Optional.empty(), aggregationFunction("count", ImmutableList.of("s2"))),
                    Optional.empty(),
                    SINGLE,
                    tableScan(
                        "testdb.table1",
                        ImmutableList.of("tag1", "s2"),
                        ImmutableSet.of(
                            "tag1", "s2",
                            "s1")))))); // We need to fetch data of column s1 to filter value

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
                    Optional.empty(),
                    FINAL,
                    aggregationTableScan(
                        singleGroupingSet("tag1"),
                        Optional.empty(),
                        PARTIAL,
                        "testdb.table1",
                        ImmutableList.of("tag1", "count_0"),
                        ImmutableSet.of("tag1", "count_0"))))));

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
                    Optional.empty(),
                    FINAL,
                    aggregationTableScan(
                        singleGroupingSet("attr1"),
                        Optional.empty(),
                        PARTIAL,
                        "testdb.table1",
                        ImmutableList.of("attr1", "count_0"),
                        ImmutableSet.of("attr1", "count_0"))))));

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
                    singleGroupingSet("attr1", "tag1", "date_bin"),
                    ImmutableMap.of(
                        Optional.empty(),
                        aggregationFunction("count", ImmutableList.of("count_0"))),
                    Optional.empty(),
                    FINAL,
                    aggregationTableScan(
                        singleGroupingSet("attr1", "tag1", "date_bin"),
                        Optional.empty(),
                        PARTIAL,
                        "testdb.table1",
                        ImmutableList.of("attr1", "tag1", "date_bin", "count_0"),
                        ImmutableSet.of("attr1", "tag1", "date_bin", "count_0"))))));
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
                    Optional.empty(),
                    SINGLE,
                    "testdb.table1",
                    ImmutableList.of("tag1", "tag2", "tag3", "count_0"),
                    ImmutableSet.of("tag1", "tag2", "tag3", "count_0")))));

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
                    singleGroupingSet("tag1", "tag2", "tag3", "attr1", "date_bin"),
                    Optional.empty(),
                    SINGLE,
                    "testdb.table1",
                    ImmutableList.of("tag1", "tag2", "tag3", "attr1", "date_bin", "count_0"),
                    ImmutableSet.of("tag1", "tag2", "tag3", "attr1", "date_bin", "count_0")))));
    logicalQueryPlan =
        planTester.createPlan(
            "SELECT count(s2) FROM table1 group by tag1, tag2, tag3, attr1, time");
    assertPlan(
        logicalQueryPlan,
        output(
            project(
                aggregationTableScan(
                    singleGroupingSet("tag1", "tag2", "tag3", "attr1", "time"),
                    Optional.empty(),
                    SINGLE,
                    "testdb.table1",
                    ImmutableList.of("tag1", "tag2", "tag3", "attr1", "time", "count_0"),
                    ImmutableSet.of("tag1", "tag2", "tag3", "attr1", "time", "count_0")))));

    // GlobalAggregation
    assertPlan(
        planTester.createPlan("SELECT count(s2) FROM table1"),
        output(
            aggregationTableScan(
                singleGroupingSet(),
                Optional.empty(),
                SINGLE,
                "testdb.table1",
                ImmutableList.of("count_0"),
                ImmutableSet.of("count_0"))));
  }
}
