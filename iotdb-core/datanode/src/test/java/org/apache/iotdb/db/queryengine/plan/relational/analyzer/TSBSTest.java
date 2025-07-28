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
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.PlanTester;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.ArithmeticBinaryExpression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.ComparisonExpression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.DoubleLiteral;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.FunctionCall;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.LongLiteral;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.QualifiedName;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.SymbolReference;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.junit.Ignore;
import org.junit.Test;

import java.util.Optional;

import static org.apache.iotdb.commons.udf.builtin.relational.TableBuiltinScalarFunction.DATE_BIN;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.assertions.PlanAssert.assertPlan;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.assertions.PlanMatchPattern.aggregation;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.assertions.PlanMatchPattern.aggregationFunction;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.assertions.PlanMatchPattern.aggregationTableScan;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.assertions.PlanMatchPattern.exchange;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.assertions.PlanMatchPattern.expression;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.assertions.PlanMatchPattern.filter;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.assertions.PlanMatchPattern.mergeSort;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.assertions.PlanMatchPattern.output;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.assertions.PlanMatchPattern.project;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.assertions.PlanMatchPattern.singleGroupingSet;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.node.AggregationNode.Step.FINAL;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.node.AggregationNode.Step.INTERMEDIATE;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.node.AggregationNode.Step.PARTIAL;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.node.AggregationNode.Step.SINGLE;
import static org.apache.iotdb.db.queryengine.plan.relational.sql.ast.ArithmeticBinaryExpression.Operator.DIVIDE;
import static org.apache.iotdb.db.queryengine.plan.relational.sql.ast.ArithmeticBinaryExpression.Operator.MULTIPLY;
import static org.apache.iotdb.db.queryengine.plan.relational.sql.ast.ComparisonExpression.Operator.GREATER_THAN;
import static org.apache.iotdb.db.queryengine.plan.relational.sql.ast.ComparisonExpression.Operator.GREATER_THAN_OR_EQUAL;
import static org.apache.iotdb.db.queryengine.plan.relational.sql.ast.ComparisonExpression.Operator.LESS_THAN;

@Ignore // TODO
public class TSBSTest {
  private static final PlanTester planTester = new PlanTester(new TSBSMetadata());

  @Test
  public void r01Test() {
    // Output - Aggregation - AggregationTableScan
    LogicalQueryPlan logicalQueryPlan =
        planTester.createPlan(
            "SELECT name, driver, max_time(latitude), last_value(latitude), last_value(longitude) \n"
                + "  FROM readings \n"
                + "  WHERE fleet = 'South' and name IS NOT NULL\n"
                + "  GROUP BY name, driver");
    assertPlan(
        logicalQueryPlan,
        output(
            aggregation(
                singleGroupingSet("name", "driver"),
                ImmutableMap.of(
                    Optional.of("last_value_final_0"),
                    aggregationFunction("last_value", ImmutableList.of("last_value_2")),
                    Optional.of("last_value_final_1"),
                    aggregationFunction("last_value", ImmutableList.of("last_value_3")),
                    Optional.of("max_time_final"),
                    aggregationFunction("max_time", ImmutableList.of("max_time_1"))),
                ImmutableList.of("name", "driver"),
                Optional.empty(),
                FINAL,
                aggregationTableScan(
                    singleGroupingSet("name", "driver"),
                    ImmutableList.of("name", "driver"),
                    Optional.empty(),
                    PARTIAL,
                    "tsbs.readings",
                    ImmutableList.of(
                        "name", "driver", "max_time_1", "last_value_3", "last_value_2"),
                    ImmutableSet.of("driver", "latitude", "name", "longitude")))));

    // Output - Agg(FINAL) - MergeSortNode - Agg(INTERMEDIATE) - AggTableScan
    //                                                         - Exchange
    PlanNode f0 = planTester.getFragmentPlan(0);
    assertPlan(
        f0,
        output(
            aggregation(
                singleGroupingSet("name", "driver"),
                ImmutableMap.of(
                    Optional.of("last_value_final_0"),
                    aggregationFunction(
                        "last_value", ImmutableList.of("last_value_intermediate_0")),
                    Optional.of("last_value_final_1"),
                    aggregationFunction(
                        "last_value", ImmutableList.of("last_value_intermediate_1")),
                    Optional.of("max_time_final"),
                    aggregationFunction("max_time", ImmutableList.of("max_time_intermediate"))),
                ImmutableList.of("name", "driver"), // Streamable
                Optional.empty(),
                FINAL,
                mergeSort(
                    aggregation(
                        singleGroupingSet("name", "driver"),
                        ImmutableMap.of(
                            Optional.of("last_value_intermediate_0"),
                            aggregationFunction("last_value", ImmutableList.of("last_value_2")),
                            Optional.of("last_value_intermediate_1"),
                            aggregationFunction("last_value", ImmutableList.of("last_value_3")),
                            Optional.of("max_time_intermediate"),
                            aggregationFunction("max_time", ImmutableList.of("max_time_1"))),
                        ImmutableList.of("name", "driver"), // Streamable
                        Optional.empty(),
                        INTERMEDIATE,
                        aggregationTableScan(
                            singleGroupingSet("name", "driver"),
                            ImmutableList.of("name", "driver"), // Streamable
                            Optional.empty(),
                            PARTIAL,
                            "tsbs.readings",
                            ImmutableList.of(
                                "name", "driver", "max_time_1", "last_value_3", "last_value_2"),
                            ImmutableSet.of("driver", "latitude", "name", "longitude"))),
                    exchange()))));

    // Aggregation(INTERMEDIATE) - AggregationTableScan
    PlanNode f1 = planTester.getFragmentPlan(1);
    assertPlan(
        f1,
        aggregation(
            singleGroupingSet("name", "driver"),
            ImmutableMap.of(
                Optional.of("last_value_intermediate_0"),
                aggregationFunction("last_value", ImmutableList.of("last_value_2")),
                Optional.of("last_value_intermediate_1"),
                aggregationFunction("last_value", ImmutableList.of("last_value_3")),
                Optional.of("max_time_intermediate"),
                aggregationFunction("max_time", ImmutableList.of("max_time_1"))),
            ImmutableList.of("name", "driver"),
            Optional.empty(),
            INTERMEDIATE,
            aggregationTableScan(
                singleGroupingSet("name", "driver"),
                ImmutableList.of("name", "driver"),
                Optional.empty(),
                PARTIAL,
                "tsbs.readings",
                ImmutableList.of("name", "driver", "max_time_1", "last_value_3", "last_value_2"),
                ImmutableSet.of("driver", "latitude", "name", "longitude"))));
  }

  @Test
  public void r02Test() {
    // Output - Aggregation - AggregationTableScan
    LogicalQueryPlan logicalQueryPlan =
        planTester.createPlan(
            "SELECT name, driver, max_time(fuel_state), last_value(fuel_state)\n"
                + "  FROM diagnostics\n"
                + "  WHERE fleet='South' and fuel_state <= 0.1 and name IS NOT NULL \n"
                + "  GROUP BY name, driver");
    assertPlan(
        logicalQueryPlan,
        output(
            aggregation(
                singleGroupingSet("name", "driver"),
                ImmutableMap.of(
                    Optional.of("last_value"),
                    aggregationFunction("last_value", ImmutableList.of("last_value_1")),
                    Optional.of("max_time"),
                    aggregationFunction("max_time", ImmutableList.of("max_time_0"))),
                ImmutableList.of("name", "driver"), // Streamable
                Optional.empty(),
                FINAL,
                aggregationTableScan(
                    singleGroupingSet("name", "driver"),
                    ImmutableList.of("name", "driver"),
                    Optional.empty(),
                    PARTIAL,
                    "tsbs.diagnostics",
                    ImmutableList.of("name", "driver", "max_time_0", "last_value_1"),
                    ImmutableSet.of("driver", "name", "fuel_state")))));

    // Output - Aggregation(FINAL) -MergeSortNode - Aggregation(INTERMEDIATE) - AggregationTableScan
    //                                            - Exchange
    PlanNode f0 = planTester.getFragmentPlan(0);
    assertPlan(
        f0,
        output(
            aggregation(
                singleGroupingSet("name", "driver"),
                ImmutableMap.of(
                    Optional.of("last_value_final"),
                    aggregationFunction(
                        "last_value", ImmutableList.of("last_value_intermediate_0")),
                    Optional.of("max_time_final"),
                    aggregationFunction("max_time", ImmutableList.of("max_time_intermediate"))),
                ImmutableList.of("name", "driver"), // Streamable
                Optional.empty(),
                FINAL,
                mergeSort(
                    aggregation(
                        singleGroupingSet("name", "driver"),
                        ImmutableMap.of(
                            Optional.of("last_value_intermediate_0"),
                            aggregationFunction("last_value", ImmutableList.of("last_value_1")),
                            Optional.of("max_time_intermediate"),
                            aggregationFunction("max_time", ImmutableList.of("max_time_0"))),
                        ImmutableList.of("name", "driver"), // Streamable
                        Optional.empty(),
                        INTERMEDIATE,
                        aggregationTableScan(
                            singleGroupingSet("name", "driver"),
                            ImmutableList.of("name", "driver"), // Streamable
                            Optional.empty(),
                            PARTIAL,
                            "tsbs.diagnostics",
                            ImmutableList.of("name", "driver", "max_time_0", "last_value_1"),
                            ImmutableSet.of("driver", "name", "fuel_state"))),
                    exchange()))));

    // Aggregation(INTERMEDIATE) - AggregationTableScan
    PlanNode f1 = planTester.getFragmentPlan(1);
    assertPlan(
        f1,
        aggregation(
            singleGroupingSet("name", "driver"),
            ImmutableMap.of(
                Optional.of("last_value_intermediate_0"),
                aggregationFunction("last_value", ImmutableList.of("last_value_1")),
                Optional.of("max_time_intermediate"),
                aggregationFunction("max_time", ImmutableList.of("max_time_0"))),
            ImmutableList.of("name", "driver"), // Streamable
            Optional.empty(),
            INTERMEDIATE,
            aggregationTableScan(
                singleGroupingSet("name", "driver"),
                ImmutableList.of("name", "driver"), // Streamable
                Optional.empty(),
                PARTIAL,
                "tsbs.diagnostics",
                ImmutableList.of("name", "driver", "max_time_0", "last_value_1"),
                ImmutableSet.of("driver", "name", "fuel_state"))));
  }

  @Test
  public void r03Test() {
    LogicalQueryPlan logicalQueryPlan =
        planTester.createPlan(
            "SELECT ts, name, driver, current_load, load_capacity\n"
                + "  FROM (\n"
                + "         SELECT name, driver, max_time(current_load) as ts, last_value(current_load) as current_load, load_capacity \n"
                + "           FROM diagnostics \n"
                + "           WHERE fleet = 'South' \n"
                + "           GROUP BY name, driver, load_capacity\n"
                + "       )\n"
                + "  WHERE current_load >= (0.9 * load_capacity)");
    // Output - Filter - Aggregation - AggregationTableScan
    assertPlan(
        logicalQueryPlan,
        output(
            filter(
                new ComparisonExpression(
                    GREATER_THAN_OR_EQUAL,
                    new SymbolReference("last_value"),
                    new ArithmeticBinaryExpression(
                        MULTIPLY, new SymbolReference("load_capacity"), new DoubleLiteral("0.9"))),
                aggregation(
                    singleGroupingSet("name", "driver", "load_capacity"),
                    ImmutableMap.of(
                        Optional.of("last_value"),
                        aggregationFunction("last_value", ImmutableList.of("last_value_1")),
                        Optional.of("max_time"),
                        aggregationFunction("max_time", ImmutableList.of("max_time_0"))),
                    ImmutableList.of("name", "driver", "load_capacity"), // Streamable
                    Optional.empty(),
                    FINAL,
                    aggregationTableScan(
                        singleGroupingSet("name", "driver", "load_capacity"),
                        ImmutableList.of("name", "driver", "load_capacity"), // Streamable
                        Optional.empty(),
                        PARTIAL,
                        "tsbs.diagnostics",
                        ImmutableList.of(
                            "name", "driver", "load_capacity", "max_time_0", "last_value_1"),
                        ImmutableSet.of("driver", "name", "current_load", "load_capacity"))))));

    // Output - Filter - Aggregation(FINAL) - MergeSort - Aggregation(INTERMEDIATE) - AggTableScan
    //                                                                         - Exchange
    PlanNode f0 = planTester.getFragmentPlan(0);
    assertPlan(
        f0,
        output(
            filter(
                new ComparisonExpression(
                    GREATER_THAN_OR_EQUAL,
                    new SymbolReference("last_value_final"),
                    new ArithmeticBinaryExpression(
                        MULTIPLY, new SymbolReference("load_capacity"), new DoubleLiteral("0.9"))),
                aggregation(
                    singleGroupingSet("name", "driver", "load_capacity"),
                    ImmutableMap.of(
                        Optional.of("last_value_final"),
                        aggregationFunction(
                            "last_value", ImmutableList.of("last_value_intermediate")),
                        Optional.of("max_time_final"),
                        aggregationFunction("max_time", ImmutableList.of("max_time_intermediate"))),
                    ImmutableList.of("name", "driver", "load_capacity"), // Streamable
                    Optional.empty(),
                    FINAL,
                    mergeSort(
                        aggregation(
                            singleGroupingSet("name", "driver", "load_capacity"),
                            ImmutableMap.of(
                                Optional.of("last_value_intermediate"),
                                aggregationFunction("last_value", ImmutableList.of("last_value_1")),
                                Optional.of("max_time_intermediate"),
                                aggregationFunction("max_time", ImmutableList.of("max_time_0"))),
                            ImmutableList.of("name", "driver", "load_capacity"), // Streamable
                            Optional.empty(),
                            INTERMEDIATE,
                            aggregationTableScan(
                                singleGroupingSet("name", "driver", "load_capacity"),
                                ImmutableList.of("name", "driver", "load_capacity"), // Streamable
                                Optional.empty(),
                                PARTIAL,
                                "tsbs.diagnostics",
                                ImmutableList.of(
                                    "name",
                                    "driver",
                                    "load_capacity",
                                    "max_time_0",
                                    "last_value_1"),
                                ImmutableSet.of(
                                    "driver", "name", "current_load", "load_capacity"))),
                        exchange())))));

    // Aggregation(INTERMEDIATE) - AggregationTableScan
    PlanNode f1 = planTester.getFragmentPlan(1);
    assertPlan(
        f1,
        aggregation(
            singleGroupingSet("name", "driver", "load_capacity"),
            ImmutableMap.of(
                Optional.of("last_value_intermediate"),
                aggregationFunction("last_value", ImmutableList.of("last_value_1")),
                Optional.of("max_time_intermediate"),
                aggregationFunction("max_time", ImmutableList.of("max_time_0"))),
            ImmutableList.of("name", "driver", "load_capacity"), // Streamable
            Optional.empty(),
            INTERMEDIATE,
            aggregationTableScan(
                singleGroupingSet("name", "driver", "load_capacity"),
                ImmutableList.of("name", "driver", "load_capacity"), // Streamable
                Optional.empty(),
                PARTIAL,
                "tsbs.diagnostics",
                ImmutableList.of("name", "driver", "load_capacity", "max_time_0", "last_value_1"),
                ImmutableSet.of("driver", "name", "current_load", "load_capacity"))));
  }

  @Test
  public void r04Test() {
    LogicalQueryPlan plan =
        planTester.createPlan(
            "SELECT name, driver\n"
                + "  FROM readings\n"
                + "  WHERE time > 0 AND time <= 1 AND fleet = 'West' \n"
                + "  GROUP BY name, driver\n"
                + "  HAVING avg(velocity) < 1");
    // Output - P - F - Aggregation(FINAL) - AggTableScan
    assertPlan(
        plan,
        output(
            project(
                filter(
                    new ComparisonExpression(
                        LESS_THAN, new SymbolReference("avg"), new LongLiteral("1")),
                    aggregation(
                        singleGroupingSet("name", "driver"),
                        ImmutableMap.of(
                            Optional.of("avg"),
                            aggregationFunction("avg", ImmutableList.of("avg_0"))),
                        ImmutableList.of("name", "driver"), // Streamable
                        Optional.empty(),
                        FINAL,
                        aggregationTableScan(
                            singleGroupingSet("name", "driver"),
                            ImmutableList.of("name", "driver"), // Streamable
                            Optional.empty(),
                            PARTIAL,
                            "tsbs.readings",
                            ImmutableList.of("name", "driver", "avg_0"),
                            ImmutableSet.of("name", "driver", "velocity", "time")))))));

    //                                                                        - Exchange
    // Output - P - F - Aggregation(FINAL) - MergeSort - Aggregation(INTERMEDIATE) - AggTableScan
    //                                                                        - Exchange
    PlanNode f0 = planTester.getFragmentPlan(0);
    assertPlan(
        f0,
        output(
            project(
                filter(
                    new ComparisonExpression(
                        LESS_THAN, new SymbolReference("avg"), new LongLiteral("1")),
                    aggregation(
                        singleGroupingSet("name", "driver"),
                        ImmutableMap.of(
                            Optional.of("avg"),
                            aggregationFunction("avg", ImmutableList.of("avg_intermediate"))),
                        ImmutableList.of("name", "driver"), // Streamable
                        Optional.empty(),
                        FINAL,
                        mergeSort(
                            exchange(),
                            aggregation(
                                singleGroupingSet("name", "driver"),
                                ImmutableMap.of(
                                    Optional.of("avg_intermediate"),
                                    aggregationFunction("avg", ImmutableList.of("avg_0"))),
                                ImmutableList.of("name", "driver"), // Streamable
                                Optional.empty(),
                                INTERMEDIATE,
                                aggregationTableScan(
                                    singleGroupingSet("name", "driver"),
                                    ImmutableList.of("name", "driver"), // Streamable
                                    Optional.empty(),
                                    PARTIAL,
                                    "tsbs.readings",
                                    ImmutableList.of("name", "driver", "avg_0"),
                                    ImmutableSet.of("name", "driver", "velocity", "time"))),
                            exchange()))))));

    // Agg(INTERMEDIATE) - AggTableScan
    PlanNode f1 = planTester.getFragmentPlan(1);
    assertPlan(
        f1,
        aggregation(
            singleGroupingSet("name", "driver"),
            ImmutableMap.of(
                Optional.of("avg_intermediate"),
                aggregationFunction("avg", ImmutableList.of("avg_0"))),
            ImmutableList.of("name", "driver"), // Streamable
            Optional.empty(),
            INTERMEDIATE,
            aggregationTableScan(
                singleGroupingSet("name", "driver"),
                ImmutableList.of("name", "driver"), // Streamable
                Optional.empty(),
                PARTIAL,
                "tsbs.readings",
                ImmutableList.of("name", "driver", "avg_0"),
                ImmutableSet.of("name", "driver", "velocity", "time"))));

    // Agg(INTERMEDIATE) - AggTableScan
    PlanNode f2 = planTester.getFragmentPlan(2);
    assertPlan(
        f2,
        aggregation(
            singleGroupingSet("name", "driver"),
            ImmutableMap.of(
                Optional.of("avg_intermediate"),
                aggregationFunction("avg", ImmutableList.of("avg_0"))),
            ImmutableList.of("name", "driver"), // Streamable
            Optional.empty(),
            INTERMEDIATE,
            aggregationTableScan(
                singleGroupingSet("name", "driver"),
                ImmutableList.of("name", "driver"), // Streamable
                Optional.empty(),
                PARTIAL,
                "tsbs.readings",
                ImmutableList.of("name", "driver", "avg_0"),
                ImmutableSet.of("name", "driver", "velocity", "time"))));
  }

  @Test
  public void r05_r06Test() {
    LogicalQueryPlan plan =
        planTester.createPlan(
            "SELECT name, driver \n"
                + "  FROM ( \n"
                + "         SELECT name, driver, avg(velocity) as mean_velocity \n"
                + "           FROM readings \n"
                + "           WHERE fleet = 'West'\n"
                + "           GROUP BY name, driver, date_bin(10m, time)\n"
                + "        ) \n"
                + "  WHERE mean_velocity > 1 \n"
                + "  GROUP BY name, driver \n"
                + "  HAVING count(*) > 22");

    // Output - P - F - Agg(SINGLE) - P - F - P - Agg(FINAL) - AggTableScan
    assertPlan(
        plan,
        output(
            project(
                filter(
                    new ComparisonExpression(
                        GREATER_THAN, new SymbolReference("count"), new LongLiteral("22")),
                    aggregation(
                        singleGroupingSet("name", "driver"),
                        ImmutableMap.of(
                            Optional.of("count"), aggregationFunction("count", ImmutableList.of())),
                        ImmutableList.of(), // UnStreamable
                        Optional.empty(),
                        SINGLE,
                        project(
                            filter(
                                new ComparisonExpression(
                                    GREATER_THAN, new SymbolReference("avg"), new LongLiteral("1")),
                                project(
                                    aggregation(
                                        singleGroupingSet("name", "driver", "date_bin$gid"),
                                        ImmutableMap.of(
                                            Optional.of("avg"),
                                            aggregationFunction("avg", ImmutableList.of("avg_0"))),
                                        ImmutableList.of("name", "driver"), // Streamable
                                        Optional.empty(),
                                        FINAL,
                                        aggregationTableScan(
                                            singleGroupingSet("name", "driver", "date_bin$gid"),
                                            ImmutableList.of("name", "driver"), // Streamable
                                            Optional.empty(),
                                            PARTIAL,
                                            "tsbs.readings",
                                            ImmutableList.of(
                                                "name", "driver", "date_bin$gid", "avg_0"),
                                            ImmutableSet.of(
                                                "name", "driver", "velocity", "time")))))))))));

    //                                                                  - Exchange
    // Output - P - F - Agg(SINGLE) - P - F - P - Agg(FINAL) -MergeSort - Agg(INTERMEDIATE) -
    // AggTableScan
    //                                                                  - Exchange
    PlanNode f0 = planTester.getFragmentPlan(0);
    assertPlan(
        f0,
        output(
            project(
                filter(
                    new ComparisonExpression(
                        GREATER_THAN, new SymbolReference("count"), new LongLiteral("22")),
                    aggregation(
                        singleGroupingSet("name", "driver"),
                        ImmutableMap.of(
                            Optional.of("count"), aggregationFunction("count", ImmutableList.of())),
                        ImmutableList.of(), // UnStreamable
                        Optional.empty(),
                        SINGLE,
                        project(
                            filter(
                                new ComparisonExpression(
                                    GREATER_THAN, new SymbolReference("avg"), new LongLiteral("1")),
                                project(
                                    aggregation(
                                        singleGroupingSet("name", "driver", "date_bin$gid"),
                                        ImmutableMap.of(
                                            Optional.of("avg"),
                                            aggregationFunction(
                                                "avg", ImmutableList.of("avg_intermediate"))),
                                        ImmutableList.of("name", "driver"), // Streamable
                                        Optional.empty(),
                                        FINAL,
                                        mergeSort(
                                            exchange(),
                                            aggregation(
                                                singleGroupingSet("name", "driver", "date_bin$gid"),
                                                ImmutableMap.of(
                                                    Optional.of("avg_intermediate"),
                                                    aggregationFunction(
                                                        "avg", ImmutableList.of("avg_0"))),
                                                ImmutableList.of("name", "driver"), // Streamable
                                                Optional.empty(),
                                                INTERMEDIATE,
                                                aggregationTableScan(
                                                    singleGroupingSet(
                                                        "name", "driver", "date_bin$gid"),
                                                    ImmutableList.of(
                                                        "name", "driver"), // Streamable
                                                    Optional.empty(),
                                                    PARTIAL,
                                                    "tsbs.readings",
                                                    ImmutableList.of(
                                                        "name", "driver", "date_bin$gid", "avg_0"),
                                                    ImmutableSet.of(
                                                        "name", "driver", "velocity", "time"))),
                                            exchange()))))))))));

    // Agg(INTERMEDIATE) - AggTableScan
    PlanNode f1 = planTester.getFragmentPlan(1);
    assertPlan(
        f1,
        aggregation(
            singleGroupingSet("name", "driver", "date_bin$gid"),
            ImmutableMap.of(
                Optional.of("avg_intermediate"),
                aggregationFunction("avg", ImmutableList.of("avg_0"))),
            ImmutableList.of("name", "driver"), // Streamable
            Optional.empty(),
            INTERMEDIATE,
            aggregationTableScan(
                singleGroupingSet("name", "driver", "date_bin$gid"),
                ImmutableList.of("name", "driver"), // Streamable
                Optional.empty(),
                PARTIAL,
                "tsbs.readings",
                ImmutableList.of("name", "driver", "date_bin$gid", "avg_0"),
                ImmutableSet.of("name", "driver", "velocity", "time"))));

    // Agg(INTERMEDIATE) - AggTableScan
    PlanNode f2 = planTester.getFragmentPlan(2);
    assertPlan(
        f2,
        aggregation(
            singleGroupingSet("name", "driver", "date_bin$gid"),
            ImmutableMap.of(
                Optional.of("avg_intermediate"),
                aggregationFunction("avg", ImmutableList.of("avg_0"))),
            ImmutableList.of("name", "driver"), // Streamable
            Optional.empty(),
            INTERMEDIATE,
            aggregationTableScan(
                singleGroupingSet("name", "driver", "date_bin$gid"),
                ImmutableList.of("name", "driver"), // Streamable
                Optional.empty(),
                PARTIAL,
                "tsbs.readings",
                ImmutableList.of("name", "driver", "date_bin$gid", "avg_0"),
                ImmutableSet.of("name", "driver", "velocity", "time"))));
  }

  @Test
  public void r07Test() {
    LogicalQueryPlan plan =
        planTester.createPlan(
            "SELECT avg(fuel_consumption) as avg_fuel_consumption, avg(nominal_fuel_consumption) as nominal_fuel_consumption \n"
                + "  FROM readings \n"
                + "  GROUP BY fleet");
    // Output - P - Agg(PARTIAL) - AggTableScan
    assertPlan(
        plan,
        output(
            project(
                aggregation(
                    singleGroupingSet("fleet"),
                    ImmutableMap.of(
                        Optional.of("avg"),
                        aggregationFunction("avg", ImmutableList.of("avg_1")),
                        Optional.of("avg_0"),
                        aggregationFunction("avg", ImmutableList.of("avg_2"))),
                    ImmutableList.of("fleet"), // Streamable
                    Optional.empty(),
                    FINAL,
                    aggregationTableScan(
                        singleGroupingSet("fleet"),
                        ImmutableList.of("fleet"), // Streamable
                        Optional.empty(),
                        PARTIAL,
                        "tsbs.readings",
                        ImmutableList.of("fleet", "avg_1", "avg_2"),
                        ImmutableSet.of(
                            "fleet", "fuel_consumption", "nominal_fuel_consumption"))))));

    //                         - Exchange
    // Output - P - Agg(FINAL) - MergeSort - Agg(INTERMEDIATE) - AggTableScan
    //                         - Exchange
    PlanNode f0 = planTester.getFragmentPlan(0);
    assertPlan(
        f0,
        output(
            project(
                aggregation(
                    singleGroupingSet("fleet"),
                    ImmutableMap.of(
                        Optional.of("avg"),
                        aggregationFunction("avg", ImmutableList.of("avg_intermediate")),
                        Optional.of("avg_0"),
                        aggregationFunction("avg", ImmutableList.of("avg_intermediate0"))),
                    ImmutableList.of("fleet"), // Streamable
                    Optional.empty(),
                    FINAL,
                    mergeSort(
                        exchange(),
                        aggregation(
                            singleGroupingSet("fleet"),
                            ImmutableMap.of(
                                Optional.of("avg_intermediate"),
                                aggregationFunction("avg", ImmutableList.of("avg_1")),
                                Optional.of("avg_intermediate0"),
                                aggregationFunction("avg", ImmutableList.of("avg_2"))),
                            ImmutableList.of("fleet"), // Streamable
                            Optional.empty(),
                            INTERMEDIATE,
                            aggregationTableScan(
                                singleGroupingSet("fleet"),
                                ImmutableList.of("fleet"), // Streamable
                                Optional.empty(),
                                PARTIAL,
                                "tsbs.readings",
                                ImmutableList.of("fleet", "avg_1", "avg_2"),
                                ImmutableSet.of(
                                    "fleet", "fuel_consumption", "nominal_fuel_consumption"))),
                        exchange())))));

    // Agg(INTERMEDIATE) - AggTableScan
    PlanNode f1 = planTester.getFragmentPlan(1);
    assertPlan(
        f1,
        aggregation(
            singleGroupingSet("fleet"),
            ImmutableMap.of(
                Optional.of("avg_intermediate"),
                aggregationFunction("avg", ImmutableList.of("avg_1")),
                Optional.of("avg_intermediate0"),
                aggregationFunction("avg", ImmutableList.of("avg_2"))),
            ImmutableList.of("fleet"), // Streamable
            Optional.empty(),
            INTERMEDIATE,
            aggregationTableScan(
                singleGroupingSet("fleet"),
                ImmutableList.of("fleet"), // Streamable
                Optional.empty(),
                PARTIAL,
                "tsbs.readings",
                ImmutableList.of("fleet", "avg_1", "avg_2"),
                ImmutableSet.of("fleet", "fuel_consumption", "nominal_fuel_consumption"))));

    // Agg(INTERMEDIATE) - AggTableScan
    PlanNode f2 = planTester.getFragmentPlan(2);
    assertPlan(
        f2,
        aggregation(
            singleGroupingSet("fleet"),
            ImmutableMap.of(
                Optional.of("avg_intermediate"),
                aggregationFunction("avg", ImmutableList.of("avg_1")),
                Optional.of("avg_intermediate0"),
                aggregationFunction("avg", ImmutableList.of("avg_2"))),
            ImmutableList.of("fleet"), // Streamable
            Optional.empty(),
            INTERMEDIATE,
            aggregationTableScan(
                singleGroupingSet("fleet"),
                ImmutableList.of("fleet"), // Streamable
                Optional.empty(),
                PARTIAL,
                "tsbs.readings",
                ImmutableList.of("fleet", "avg_1", "avg_2"),
                ImmutableSet.of("fleet", "fuel_consumption", "nominal_fuel_consumption"))));
  }

  @Test
  public void r08Test() {
    LogicalQueryPlan plan =
        planTester.createPlan(
            "SELECT fleet, name, driver, avg(hours_driven) as avg_daily_hours\n"
                + "  FROM (\n"
                + "          SELECT date_bin(1d, time) as time, fleet, name, driver, count(avg_velocity) / 6 as hours_driven \n"
                + "          FROM ( \n"
                + "                SELECT date_bin(10m, time) as time, fleet, name, driver, avg(velocity) as avg_velocity \n"
                + "                  FROM readings\n"
                + "                  GROUP BY 1, fleet, name, driver\n"
                + "                  having avg(velocity) > 1\n"
                + "               ) \n"
                + "          GROUP BY 1, fleet, name, driver\n"
                + "       )\n"
                + "   GROUP BY fleet, name, driver");
    // Output - P - Agg(SINGLE) - Agg(SINGLE) - P - F - Agg(FINAL) -AggTableScan
    assertPlan(
        plan,
        output(
            aggregation(
                singleGroupingSet("fleet", "name", "driver"),
                ImmutableMap.of(
                    Optional.of("avg_2"), aggregationFunction("avg", ImmutableList.of("expr"))),
                ImmutableList.of(), // UnStreamable
                Optional.empty(),
                SINGLE,
                project(
                    ImmutableMap.of(
                        "expr",
                        expression(
                            new ArithmeticBinaryExpression(
                                DIVIDE, new SymbolReference("count"), new LongLiteral("6")))),
                    aggregation(
                        singleGroupingSet("fleet", "name", "driver", "date_bin$gid_1"),
                        ImmutableMap.of(
                            Optional.of("count"),
                            aggregationFunction("count", ImmutableList.of("avg"))),
                        ImmutableList.of(),
                        Optional.empty(),
                        SINGLE,
                        project(
                            ImmutableMap.of(
                                "date_bin$gid_1",
                                expression(
                                    new FunctionCall(
                                        QualifiedName.of(DATE_BIN.getFunctionName()),
                                        ImmutableList.of(
                                            new LongLiteral("0"),
                                            new LongLiteral("86400000"),
                                            new SymbolReference("date_bin$gid"),
                                            new LongLiteral("0"))))),
                            filter(
                                new ComparisonExpression(
                                    GREATER_THAN, new SymbolReference("avg"), new LongLiteral("1")),
                                aggregation(
                                    singleGroupingSet("fleet", "name", "driver", "date_bin$gid"),
                                    ImmutableMap.of(
                                        Optional.of("avg"),
                                        aggregationFunction("avg", ImmutableList.of("avg_3"))),
                                    ImmutableList.of("fleet", "name", "driver"),
                                    Optional.empty(),
                                    FINAL,
                                    aggregationTableScan(
                                        singleGroupingSet(
                                            "fleet", "name", "driver", "date_bin$gid"),
                                        ImmutableList.of("fleet", "name", "driver"), // Streamable
                                        Optional.empty(),
                                        PARTIAL,
                                        "tsbs.readings",
                                        ImmutableList.of(
                                            "fleet", "name", "driver", "date_bin$gid", "avg_3"),
                                        ImmutableSet.of(
                                            "fleet", "name", "driver", "time", "velocity"))))))))));

    PlanNode f0 = planTester.getFragmentPlan(0);
    //                                                       -Exchange
    // Output -P -Agg(SINGLE) -Agg(SINGLE) -P -F -Agg(FINAL) -MergeSort -Agg(INTERMEDIATE)
    // -AggTableScan
    //                                                       -Exchange
    assertPlan(
        f0,
        output(
            aggregation(
                singleGroupingSet("fleet", "name", "driver"),
                ImmutableMap.of(
                    Optional.of("avg_2"), aggregationFunction("avg", ImmutableList.of("expr"))),
                ImmutableList.of(), // UnStreamable
                Optional.empty(),
                SINGLE,
                project(
                    ImmutableMap.of(
                        "expr",
                        expression(
                            new ArithmeticBinaryExpression(
                                DIVIDE, new SymbolReference("count"), new LongLiteral("6")))),
                    aggregation(
                        singleGroupingSet("fleet", "name", "driver", "date_bin$gid_1"),
                        ImmutableMap.of(
                            Optional.of("count"),
                            aggregationFunction("count", ImmutableList.of("avg"))),
                        ImmutableList.of(),
                        Optional.empty(),
                        SINGLE,
                        project(
                            ImmutableMap.of(
                                "date_bin$gid_1",
                                expression(
                                    new FunctionCall(
                                        QualifiedName.of(DATE_BIN.getFunctionName()),
                                        ImmutableList.of(
                                            new LongLiteral("0"),
                                            new LongLiteral("86400000"),
                                            new SymbolReference("date_bin$gid"),
                                            new LongLiteral("0"))))),
                            filter(
                                new ComparisonExpression(
                                    GREATER_THAN, new SymbolReference("avg"), new LongLiteral("1")),
                                aggregation(
                                    singleGroupingSet("fleet", "name", "driver", "date_bin$gid"),
                                    ImmutableMap.of(
                                        Optional.of("avg"),
                                        aggregationFunction(
                                            "avg", ImmutableList.of("avg_intermediate"))),
                                    ImmutableList.of("fleet", "name", "driver"),
                                    Optional.empty(),
                                    FINAL,
                                    mergeSort(
                                        exchange(),
                                        aggregation(
                                            singleGroupingSet(
                                                "fleet", "name", "driver", "date_bin$gid"),
                                            ImmutableMap.of(
                                                Optional.of("avg_intermediate"),
                                                aggregationFunction(
                                                    "avg", ImmutableList.of("avg_3"))),
                                            ImmutableList.of("fleet", "name", "driver"),
                                            Optional.empty(),
                                            INTERMEDIATE,
                                            aggregationTableScan(
                                                singleGroupingSet(
                                                    "fleet", "name", "driver", "date_bin$gid"),
                                                ImmutableList.of(
                                                    "fleet", "name", "driver"), // Streamable
                                                Optional.empty(),
                                                PARTIAL,
                                                "tsbs.readings",
                                                ImmutableList.of(
                                                    "fleet",
                                                    "name",
                                                    "driver",
                                                    "date_bin$gid",
                                                    "avg_3"),
                                                ImmutableSet.of(
                                                    "fleet",
                                                    "name",
                                                    "driver",
                                                    "time",
                                                    "velocity"))),
                                        exchange())))))))));

    // Agg(INTERMEDIATE) - AggTableScan
    PlanNode f1 = planTester.getFragmentPlan(1);
    assertPlan(
        f1,
        aggregation(
            singleGroupingSet("fleet", "name", "driver", "date_bin$gid"),
            ImmutableMap.of(
                Optional.of("avg_intermediate"),
                aggregationFunction("avg", ImmutableList.of("avg_3"))),
            ImmutableList.of("fleet", "name", "driver"),
            Optional.empty(),
            INTERMEDIATE,
            aggregationTableScan(
                singleGroupingSet("fleet", "name", "driver", "date_bin$gid"),
                ImmutableList.of("fleet", "name", "driver"), // Streamable
                Optional.empty(),
                PARTIAL,
                "tsbs.readings",
                ImmutableList.of("fleet", "name", "driver", "date_bin$gid", "avg_3"),
                ImmutableSet.of("fleet", "name", "driver", "time", "velocity"))));

    // Agg(INTERMEDIATE) - AggTableScan
    PlanNode f2 = planTester.getFragmentPlan(2);
    assertPlan(
        f2,
        aggregation(
            singleGroupingSet("fleet", "name", "driver", "date_bin$gid"),
            ImmutableMap.of(
                Optional.of("avg_intermediate"),
                aggregationFunction("avg", ImmutableList.of("avg_3"))),
            ImmutableList.of("fleet", "name", "driver"),
            Optional.empty(),
            INTERMEDIATE,
            aggregationTableScan(
                singleGroupingSet("fleet", "name", "driver", "date_bin$gid"),
                ImmutableList.of("fleet", "name", "driver"), // Streamable
                Optional.empty(),
                PARTIAL,
                "tsbs.readings",
                ImmutableList.of("fleet", "name", "driver", "date_bin$gid", "avg_3"),
                ImmutableSet.of("fleet", "name", "driver", "time", "velocity"))));
  }

  // TODO After TableFunction supported
  @Ignore
  @Test
  public void r09Test() {}

  @Test
  public void r10Test() {
    LogicalQueryPlan plan =
        planTester.createPlan(
            "SELECT fleet, model, load_capacity, avg(ml / load_capacity) \n"
                + "  FROM ( \n"
                + "        SELECT fleet, model, load_capacity, avg(current_load) AS ml \n"
                + "          FROM diagnostics \n"
                + "          WHERE name IS NOT NULL \n"
                + "          GROUP BY fleet, model, name, load_capacity\n"
                + "        )\n"
                + "  GROUP BY fleet, model, load_capacity");
    // Output - P - Aggregation(FINAL) - MergeSort - Aggregation(PARTIAL) - AggTableScan
    assertPlan(
        plan,
        output(
            aggregation(
                singleGroupingSet("fleet", "model", "load_capacity"),
                ImmutableMap.of(
                    Optional.of("avg_0"), aggregationFunction("avg", ImmutableList.of("expr"))),
                ImmutableList.of(), // UnStreamable
                Optional.empty(),
                SINGLE,
                project( // the column "load_capacity" is referenced twice, so these two ProjectNode
                    // cannot be inlined
                    ImmutableMap.of(
                        "expr",
                        expression(
                            new ArithmeticBinaryExpression(
                                DIVIDE,
                                new SymbolReference("avg"),
                                new SymbolReference("load_capacity")))),
                    project(
                        aggregation(
                            singleGroupingSet("fleet", "model", "name", "load_capacity"),
                            ImmutableMap.of(
                                Optional.of("avg"),
                                aggregationFunction("avg", ImmutableList.of("avg_1"))),
                            ImmutableList.of(
                                "fleet", "model", "name", "load_capacity"), // Streamable
                            Optional.empty(),
                            FINAL,
                            aggregationTableScan(
                                singleGroupingSet("fleet", "model", "name", "load_capacity"),
                                ImmutableList.of(
                                    "fleet", "model", "name", "load_capacity"), // Streamable
                                Optional.empty(),
                                PARTIAL,
                                "tsbs.diagnostics",
                                ImmutableList.of(
                                    "fleet", "model", "name", "load_capacity", "avg_1"),
                                ImmutableSet.of(
                                    "fleet",
                                    "model",
                                    "name",
                                    "load_capacity",
                                    "current_load"))))))));

    //                                                                     - Exchange
    // Output -Agg(SINGLE) -P -P -Agg(FINAL) -MergeSort -Agg(INTERMEDIATE) -AggTableScan
    //                                                                     - Exchange
    PlanNode f0 = planTester.getFragmentPlan(0);
    assertPlan(
        f0,
        output(
            aggregation(
                singleGroupingSet("fleet", "model", "load_capacity"),
                ImmutableMap.of(
                    Optional.of("avg_0"), aggregationFunction("avg", ImmutableList.of("expr"))),
                ImmutableList.of(), // UnStreamable
                Optional.empty(),
                SINGLE,
                project( // the column "load_capacity" is referenced twice, so these two ProjectNode
                    // cannot be inlined
                    ImmutableMap.of(
                        "expr",
                        expression(
                            new ArithmeticBinaryExpression(
                                DIVIDE,
                                new SymbolReference("avg"),
                                new SymbolReference("load_capacity")))),
                    project(
                        aggregation(
                            singleGroupingSet("fleet", "model", "name", "load_capacity"),
                            ImmutableMap.of(
                                Optional.of("avg"),
                                aggregationFunction("avg", ImmutableList.of("avg_intermediate"))),
                            ImmutableList.of(
                                "fleet", "model", "name", "load_capacity"), // Streamable
                            Optional.empty(),
                            FINAL,
                            mergeSort(
                                exchange(),
                                aggregation(
                                    singleGroupingSet("fleet", "model", "name", "load_capacity"),
                                    ImmutableMap.of(
                                        Optional.of("avg_intermediate"),
                                        aggregationFunction("avg", ImmutableList.of("avg_1"))),
                                    ImmutableList.of(
                                        "fleet", "model", "name", "load_capacity"), // Streamable
                                    Optional.empty(),
                                    INTERMEDIATE,
                                    aggregationTableScan(
                                        singleGroupingSet(
                                            "fleet", "model", "name", "load_capacity"),
                                        ImmutableList.of(
                                            "fleet",
                                            "model",
                                            "name",
                                            "load_capacity"), // Streamable
                                        Optional.empty(),
                                        PARTIAL,
                                        "tsbs.diagnostics",
                                        ImmutableList.of(
                                            "fleet", "model", "name", "load_capacity", "avg_1"),
                                        ImmutableSet.of(
                                            "fleet",
                                            "model",
                                            "name",
                                            "load_capacity",
                                            "current_load"))),
                                exchange())))))));
    // Agg(INTERMEDIATE) - AggTableScan
    PlanNode f1 = planTester.getFragmentPlan(1);
    assertPlan(
        f1,
        aggregation(
            singleGroupingSet("fleet", "model", "name", "load_capacity"),
            ImmutableMap.of(
                Optional.of("avg_intermediate"),
                aggregationFunction("avg", ImmutableList.of("avg_1"))),
            ImmutableList.of("fleet", "model", "name", "load_capacity"), // Streamable
            Optional.empty(),
            INTERMEDIATE,
            aggregationTableScan(
                singleGroupingSet("fleet", "model", "name", "load_capacity"),
                ImmutableList.of("fleet", "model", "name", "load_capacity"), // Streamable
                Optional.empty(),
                PARTIAL,
                "tsbs.diagnostics",
                ImmutableList.of("fleet", "model", "name", "load_capacity", "avg_1"),
                ImmutableSet.of("fleet", "model", "name", "load_capacity", "current_load"))));

    // Agg(INTERMEDIATE) - AggTableScan
    PlanNode f2 = planTester.getFragmentPlan(2);
    assertPlan(
        f2,
        aggregation(
            singleGroupingSet("fleet", "model", "name", "load_capacity"),
            ImmutableMap.of(
                Optional.of("avg_intermediate"),
                aggregationFunction("avg", ImmutableList.of("avg_1"))),
            ImmutableList.of("fleet", "model", "name", "load_capacity"), // Streamable
            Optional.empty(),
            INTERMEDIATE,
            aggregationTableScan(
                singleGroupingSet("fleet", "model", "name", "load_capacity"),
                ImmutableList.of("fleet", "model", "name", "load_capacity"), // Streamable
                Optional.empty(),
                PARTIAL,
                "tsbs.diagnostics",
                ImmutableList.of("fleet", "model", "name", "load_capacity", "avg_1"),
                ImmutableSet.of("fleet", "model", "name", "load_capacity", "current_load"))));
  }

  @Test
  public void r11Test() {
    LogicalQueryPlan plan =
        planTester.createPlan(
            "SELECT date_bin(1d, ten_minutes), model, fleet, sum(ten_mins_per_day) / 144 \n"
                + "  FROM ( \n"
                + "        SELECT date_bin(10m, time) as ten_minutes, model, fleet, count(*) AS ten_mins_per_day\n"
                + "          FROM diagnostics \n"
                + "          GROUP BY 1, model, fleet, name\n"
                + "          HAVING avg(STATUS) < 1\n"
                + "        ) \n"
                + "  GROUP BY 1, model, fleet");
    // Output -P -Agg(SINGLE) -P -P -F -P -Agg(FINAL) -AggTableScan
    assertPlan(
        plan,
        output(
            project(
                aggregation(
                    singleGroupingSet("model", "fleet", "date_bin$gid_1"),
                    ImmutableMap.of(
                        Optional.of("sum"), aggregationFunction("sum", ImmutableList.of("count"))),
                    ImmutableList.of(),
                    Optional.empty(),
                    SINGLE,
                    project(
                        ImmutableMap.of(
                            "date_bin$gid_1",
                            expression(
                                new FunctionCall(
                                    QualifiedName.of(DATE_BIN.getFunctionName()),
                                    ImmutableList.of(
                                        new LongLiteral("0"),
                                        new LongLiteral("86400000"),
                                        new SymbolReference("date_bin$gid"),
                                        new LongLiteral("0"))))),
                        project(
                            filter(
                                new ComparisonExpression(
                                    LESS_THAN, new SymbolReference("avg"), new LongLiteral("1")),
                                project(
                                    aggregation(
                                        singleGroupingSet("model", "fleet", "name", "date_bin$gid"),
                                        ImmutableMap.of(
                                            Optional.of("avg"),
                                            aggregationFunction("avg", ImmutableList.of("avg_3")),
                                            Optional.of("count"),
                                            aggregationFunction(
                                                "count", ImmutableList.of("count_2"))),
                                        ImmutableList.of("model", "fleet", "name"),
                                        Optional.empty(),
                                        FINAL,
                                        aggregationTableScan(
                                            singleGroupingSet(
                                                "model", "fleet", "name", "date_bin$gid"),
                                            ImmutableList.of(
                                                "model", "fleet", "name"), // Streamable
                                            Optional.empty(),
                                            PARTIAL,
                                            "tsbs.diagnostics",
                                            ImmutableList.of(
                                                "model",
                                                "fleet",
                                                "name",
                                                "date_bin$gid",
                                                "count_2",
                                                "avg_3"),
                                            ImmutableSet.of(
                                                "model", "fleet", "name", "time",
                                                "status")))))))))));

    //                                                                              - Exchange
    // Output -P -Agg(SINGLE) -P -P -F -P -Agg(FINAL) -MergeSort -Agg(INTERMEDIATE) -AggTableScan
    //                                                                              - Exchange
    PlanNode f0 = planTester.getFragmentPlan(0);
    assertPlan(
        f0,
        output(
            project(
                aggregation(
                    singleGroupingSet("model", "fleet", "date_bin$gid_1"),
                    ImmutableMap.of(
                        Optional.of("sum"), aggregationFunction("sum", ImmutableList.of("count"))),
                    ImmutableList.of(),
                    Optional.empty(),
                    SINGLE,
                    project(
                        ImmutableMap.of(
                            "date_bin$gid_1",
                            expression(
                                new FunctionCall(
                                    QualifiedName.of(DATE_BIN.getFunctionName()),
                                    ImmutableList.of(
                                        new LongLiteral("0"),
                                        new LongLiteral("86400000"),
                                        new SymbolReference("date_bin$gid"),
                                        new LongLiteral("0"))))),
                        project(
                            filter(
                                new ComparisonExpression(
                                    LESS_THAN, new SymbolReference("avg"), new LongLiteral("1")),
                                project(
                                    aggregation(
                                        singleGroupingSet("model", "fleet", "name", "date_bin$gid"),
                                        ImmutableMap.of(
                                            Optional.of("avg"),
                                            aggregationFunction(
                                                "avg", ImmutableList.of("avg_intermediate")),
                                            Optional.of("count"),
                                            aggregationFunction(
                                                "count", ImmutableList.of("count_intermediate"))),
                                        ImmutableList.of("model", "fleet", "name"),
                                        Optional.empty(),
                                        FINAL,
                                        mergeSort(
                                            exchange(),
                                            aggregation(
                                                singleGroupingSet(
                                                    "model", "fleet", "name", "date_bin$gid"),
                                                ImmutableMap.of(
                                                    Optional.of("avg_intermediate"),
                                                    aggregationFunction(
                                                        "avg", ImmutableList.of("avg_3")),
                                                    Optional.of("count_intermediate"),
                                                    aggregationFunction(
                                                        "count", ImmutableList.of("count_2"))),
                                                ImmutableList.of("model", "fleet", "name"),
                                                Optional.empty(),
                                                INTERMEDIATE,
                                                aggregationTableScan(
                                                    singleGroupingSet(
                                                        "model", "fleet", "name", "date_bin$gid"),
                                                    ImmutableList.of(
                                                        "model", "fleet", "name"), // Streamable
                                                    Optional.empty(),
                                                    PARTIAL,
                                                    "tsbs.diagnostics",
                                                    ImmutableList.of(
                                                        "model",
                                                        "fleet",
                                                        "name",
                                                        "date_bin$gid",
                                                        "count_2",
                                                        "avg_3"),
                                                    ImmutableSet.of(
                                                        "model", "fleet", "name", "time",
                                                        "status"))),
                                            exchange()))))))))));
    // Agg(INTERMEDIATE) - AggTableScan
    PlanNode f1 = planTester.getFragmentPlan(1);
    assertPlan(
        f1,
        aggregation(
            singleGroupingSet("model", "fleet", "name", "date_bin$gid"),
            ImmutableMap.of(
                Optional.of("avg_intermediate"),
                aggregationFunction("avg", ImmutableList.of("avg_3")),
                Optional.of("count_intermediate"),
                aggregationFunction("count", ImmutableList.of("count_2"))),
            ImmutableList.of("model", "fleet", "name"),
            Optional.empty(),
            INTERMEDIATE,
            aggregationTableScan(
                singleGroupingSet("model", "fleet", "name", "date_bin$gid"),
                ImmutableList.of("model", "fleet", "name"), // Streamable
                Optional.empty(),
                PARTIAL,
                "tsbs.diagnostics",
                ImmutableList.of("model", "fleet", "name", "date_bin$gid", "count_2", "avg_3"),
                ImmutableSet.of("model", "fleet", "name", "time", "status"))));

    // Agg(INTERMEDIATE) - AggTableScan
    PlanNode f2 = planTester.getFragmentPlan(2);
    assertPlan(
        f2,
        aggregation(
            singleGroupingSet("model", "fleet", "name", "date_bin$gid"),
            ImmutableMap.of(
                Optional.of("avg_intermediate"),
                aggregationFunction("avg", ImmutableList.of("avg_3")),
                Optional.of("count_intermediate"),
                aggregationFunction("count", ImmutableList.of("count_2"))),
            ImmutableList.of("model", "fleet", "name"),
            Optional.empty(),
            INTERMEDIATE,
            aggregationTableScan(
                singleGroupingSet("model", "fleet", "name", "date_bin$gid"),
                ImmutableList.of("model", "fleet", "name"), // Streamable
                Optional.empty(),
                PARTIAL,
                "tsbs.diagnostics",
                ImmutableList.of("model", "fleet", "name", "date_bin$gid", "count_2", "avg_3"),
                ImmutableSet.of("model", "fleet", "name", "time", "status"))));
  }

  // TODO After TableFunction supported
  @Ignore
  @Test
  public void r12Test() {}
}
