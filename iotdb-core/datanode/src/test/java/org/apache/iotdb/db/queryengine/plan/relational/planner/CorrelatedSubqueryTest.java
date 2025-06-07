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
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.BooleanLiteral;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.CoalesceExpression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Expression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.NotExpression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.SymbolReference;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.junit.Test;

import java.util.Collections;
import java.util.Optional;

import static org.apache.iotdb.db.queryengine.plan.relational.planner.assertions.PlanAssert.assertPlan;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.assertions.PlanMatchPattern.aggregation;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.assertions.PlanMatchPattern.expression;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.assertions.PlanMatchPattern.filter;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.assertions.PlanMatchPattern.join;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.assertions.PlanMatchPattern.output;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.assertions.PlanMatchPattern.project;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.assertions.PlanMatchPattern.singleGroupingSet;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.assertions.PlanMatchPattern.sort;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.assertions.PlanMatchPattern.tableScan;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.node.AggregationNode.Step.SINGLE;

public class CorrelatedSubqueryTest {
  @Test
  public void testCorrelatedExistsSubquery() {
    PlanTester planTester = new PlanTester();

    String sql =
        "SELECT s1 FROM table1 t1 where exists(select s1 from table2 t2 where t1.s1 = t2.s2)";

    LogicalQueryPlan logicalQueryPlan = planTester.createPlan(sql);

    PlanMatchPattern tableScan1 =
        tableScan("testdb.table1", ImmutableList.of("s1"), ImmutableSet.of("s1"));

    PlanMatchPattern tableScan2 = tableScan("testdb.table2", ImmutableMap.of("s2_7", "s2"));

    Expression filterPredicate =
        new CoalesceExpression(new BooleanLiteral("true"), new BooleanLiteral("false"));

    // Verify full LogicalPlan
    /*
    *   └──OutputNode
    *      └──JoinNode
    *         |──SortNode
    *         |  └──TableScanNode
    *         ├──SortNode
    *         │   └──AggregationNode
    *         |      └──TableScanNode

    */
    assertPlan(
        logicalQueryPlan,
        output(
            join(
                JoinNode.JoinType.INNER,
                builder ->
                    builder
                        .equiCriteria("s1", "s2_7")
                        .left(sort(tableScan1))
                        .right(
                            sort(
                                aggregation(
                                    singleGroupingSet("s2_7"),
                                    ImmutableMap.of(),
                                    Collections.emptyList(),
                                    Optional.empty(),
                                    SINGLE,
                                    tableScan2))))));

    // not exists with other filter
    sql =
        "SELECT s1 FROM table1 t1 where tag1 = 'd01' and not exists(select s1 from table2 t2 where t1.s1 = t2.s2)";

    logicalQueryPlan = planTester.createPlan(sql);

    PlanMatchPattern tableScan3 = tableScan("testdb.table2", ImmutableMap.of("s2_3", "s2"));

    Expression filterPredicate1 =
        new NotExpression(
            new CoalesceExpression(
                new SymbolReference("subqueryTrue"), new BooleanLiteral("false")));

    // Verify full LogicalPlan
    /*
    *   └──OutputNode
    *      └──ProjectNode
    *         └──FilterNode
    *            └──JoinNode
    *               |──SortNode
    *               |  └──TableScanNode
    *               |──SortNode
    *               |  └──ProjectNode
    *               │     └──AggregationNode
    *               |        └──TableScanNode

    */
    assertPlan(
        logicalQueryPlan,
        output(
            project(
                filter(
                    filterPredicate1,
                    join(
                        JoinNode.JoinType.LEFT,
                        builder ->
                            builder
                                .equiCriteria("s1", "s2_3")
                                .left(sort(tableScan1))
                                .right(
                                    sort(
                                        project(
                                            ImmutableMap.of(
                                                "subqueryTrue",
                                                expression(new BooleanLiteral("true"))),
                                            aggregation(
                                                singleGroupingSet("s2_3"),
                                                ImmutableMap.of(),
                                                Collections.emptyList(),
                                                Optional.empty(),
                                                SINGLE,
                                                tableScan3)))))))));
  }
}
