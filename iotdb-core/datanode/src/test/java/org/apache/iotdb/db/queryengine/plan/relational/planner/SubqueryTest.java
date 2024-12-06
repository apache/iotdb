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
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.JoinNode;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.ComparisonExpression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Expression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.SymbolReference;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.junit.Test;

import java.util.Collections;
import java.util.Optional;

import static org.apache.iotdb.db.queryengine.plan.relational.planner.assertions.PlanAssert.assertPlan;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.assertions.PlanMatchPattern.aggregation;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.assertions.PlanMatchPattern.aggregationFunction;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.assertions.PlanMatchPattern.aggregationTableScan;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.assertions.PlanMatchPattern.any;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.assertions.PlanMatchPattern.anyTree;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.assertions.PlanMatchPattern.collect;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.assertions.PlanMatchPattern.enforceSingleRow;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.assertions.PlanMatchPattern.exchange;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.assertions.PlanMatchPattern.filter;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.assertions.PlanMatchPattern.join;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.assertions.PlanMatchPattern.output;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.assertions.PlanMatchPattern.project;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.assertions.PlanMatchPattern.singleGroupingSet;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.assertions.PlanMatchPattern.tableScan;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.node.AggregationNode.Step.FINAL;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.node.AggregationNode.Step.INTERMEDIATE;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.node.AggregationNode.Step.PARTIAL;
import static org.apache.iotdb.db.queryengine.plan.relational.sql.ast.ComparisonExpression.Operator.EQUAL;

public class SubqueryTest {
  @Test
  public void testUncorrelatedScalarSubqueryInWhereClause() {
    PlanTester planTester = new PlanTester();

    String sql = "SELECT s1 FROM table1 where s1 = (select max(s1) from table1)";

    LogicalQueryPlan logicalQueryPlan = planTester.createPlan(sql);

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
        logicalQueryPlan,
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

    // Verify DistributionPlan
    assertPlan(
        planTester.getFragmentPlan(0),
        output(
            project(
                filter(
                    filterPredicate,
                    join(
                        JoinNode.JoinType.INNER,
                        builder ->
                            builder
                                .left(collect(exchange(), tableScan, exchange()))
                                .right(
                                    aggregation(
                                        singleGroupingSet(),
                                        ImmutableMap.of(
                                            Optional.of("max"),
                                            aggregationFunction("max", ImmutableList.of("max_10"))),
                                        Collections.emptyList(),
                                        Optional.empty(),
                                        FINAL,
                                        collect(
                                            exchange(),
                                            aggregation(
                                                singleGroupingSet(),
                                                ImmutableMap.of(
                                                    Optional.of("max_10"),
                                                    aggregationFunction(
                                                        "max", ImmutableList.of("max_9"))),
                                                Collections.emptyList(),
                                                Optional.empty(),
                                                INTERMEDIATE,
                                                aggregationTableScan(
                                                    singleGroupingSet(),
                                                    Collections.emptyList(),
                                                    Optional.empty(),
                                                    PARTIAL,
                                                    "testdb.table1",
                                                    ImmutableList.of("max_9"),
                                                    ImmutableSet.of("s1_6"))),
                                            exchange()))))))));

    assertPlan(planTester.getFragmentPlan(1), tableScan);

    assertPlan(planTester.getFragmentPlan(2), tableScan);

    assertPlan(
        planTester.getFragmentPlan(3),
        aggregation(
            singleGroupingSet(),
            ImmutableMap.of(
                Optional.of("max_10"), aggregationFunction("max", ImmutableList.of("max_9"))),
            Collections.emptyList(),
            Optional.empty(),
            INTERMEDIATE,
            aggregationTableScan(
                singleGroupingSet(),
                Collections.emptyList(),
                Optional.empty(),
                PARTIAL,
                "testdb.table1",
                ImmutableList.of("max_9"),
                ImmutableSet.of("s1_6"))));

    assertPlan(
        planTester.getFragmentPlan(4),
        aggregation(
            singleGroupingSet(),
            ImmutableMap.of(
                Optional.of("max_10"), aggregationFunction("max", ImmutableList.of("max_9"))),
            Collections.emptyList(),
            Optional.empty(),
            INTERMEDIATE,
            aggregationTableScan(
                singleGroupingSet(),
                Collections.emptyList(),
                Optional.empty(),
                PARTIAL,
                "testdb.table1",
                ImmutableList.of("max_9"),
                ImmutableSet.of("s1_6"))));
  }

  @Test
  public void testUncorrelatedScalarSubqueryInWhereClauseWithEnforceSingleRowNode() {
    PlanTester planTester = new PlanTester();

    String sql = "SELECT s1 FROM table1 where s1 = (select s2 from table1)";

    LogicalQueryPlan logicalQueryPlan = planTester.createPlan(sql);

    PlanMatchPattern tableScan1 =
        tableScan("testdb.table1", ImmutableList.of("s1"), ImmutableSet.of("s1"));

    // Verify LogicalPlan
    /*
    *   └──OutputNode
    *           └──ProjectNode
    *             └──FilterNode
    *               └──JoinNode
    *                   |──TableScanNode
    *                   ├──EnforceSingleRowNode
    *                   │   └──TableScanNode

    */
    assertPlan(
        logicalQueryPlan,
        output(
            project(
                anyTree(
                    join(
                        JoinNode.JoinType.INNER,
                        builder -> builder.left(tableScan1).right(enforceSingleRow(any())))))));
  }
}
