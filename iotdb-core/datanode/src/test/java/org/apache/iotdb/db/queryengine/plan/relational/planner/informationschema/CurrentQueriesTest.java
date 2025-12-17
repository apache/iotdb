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
package org.apache.iotdb.db.queryengine.plan.relational.planner.informationschema;

import org.apache.iotdb.db.queryengine.plan.planner.plan.LogicalQueryPlan;
import org.apache.iotdb.db.queryengine.plan.relational.planner.PlanTester;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.ComparisonExpression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.StringLiteral;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.SymbolReference;

import com.google.common.collect.ImmutableList;
import org.junit.Test;

import java.util.Optional;

import static org.apache.iotdb.commons.schema.column.ColumnHeaderConstant.BIN;
import static org.apache.iotdb.commons.schema.column.ColumnHeaderConstant.CLIENT_IP;
import static org.apache.iotdb.commons.schema.column.ColumnHeaderConstant.COST_TIME;
import static org.apache.iotdb.commons.schema.column.ColumnHeaderConstant.DATA_NODE_ID_TABLE_MODEL;
import static org.apache.iotdb.commons.schema.column.ColumnHeaderConstant.END_TIME_TABLE_MODEL;
import static org.apache.iotdb.commons.schema.column.ColumnHeaderConstant.NUMS;
import static org.apache.iotdb.commons.schema.column.ColumnHeaderConstant.QUERY_ID_TABLE_MODEL;
import static org.apache.iotdb.commons.schema.column.ColumnHeaderConstant.START_TIME_TABLE_MODEL;
import static org.apache.iotdb.commons.schema.column.ColumnHeaderConstant.STATEMENT_TABLE_MODEL;
import static org.apache.iotdb.commons.schema.column.ColumnHeaderConstant.STATE_TABLE_MODEL;
import static org.apache.iotdb.commons.schema.column.ColumnHeaderConstant.USER_TABLE_MODEL;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.assertions.PlanAssert.assertPlan;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.assertions.PlanMatchPattern.collect;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.assertions.PlanMatchPattern.exchange;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.assertions.PlanMatchPattern.filter;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.assertions.PlanMatchPattern.infoSchemaTableScan;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.assertions.PlanMatchPattern.output;
import static org.apache.iotdb.db.queryengine.plan.relational.sql.ast.ComparisonExpression.Operator.EQUAL;

public class CurrentQueriesTest {
  private final PlanTester planTester = new PlanTester();

  @Test
  public void testCurrentQueries() {
    LogicalQueryPlan logicalQueryPlan =
        planTester.createPlan("select * from information_schema.current_queries");
    assertPlan(
        logicalQueryPlan,
        output(
            infoSchemaTableScan(
                "information_schema.current_queries",
                Optional.empty(),
                ImmutableList.of(
                    QUERY_ID_TABLE_MODEL,
                    STATE_TABLE_MODEL,
                    START_TIME_TABLE_MODEL,
                    END_TIME_TABLE_MODEL,
                    DATA_NODE_ID_TABLE_MODEL,
                    COST_TIME,
                    STATEMENT_TABLE_MODEL,
                    USER_TABLE_MODEL,
                    CLIENT_IP))));

    //                  - Exchange
    // Output - Collect - Exchange
    assertPlan(planTester.getFragmentPlan(0), output(collect(exchange(), exchange())));
    // TableScan
    assertPlan(
        planTester.getFragmentPlan(1),
        infoSchemaTableScan("information_schema.current_queries", Optional.of(1)));
    // TableScan
    assertPlan(
        planTester.getFragmentPlan(2),
        infoSchemaTableScan("information_schema.current_queries", Optional.of(2)));
  }

  @Test
  public void testCurrentQueriesFilterPushDown() {
    // Normal case
    LogicalQueryPlan logicalQueryPlan =
        planTester.createPlan(
            "select * from information_schema.current_queries where state='RUNNING'");
    assertPlan(
        logicalQueryPlan,
        output(
            infoSchemaTableScan(
                "information_schema.current_queries",
                Optional.empty(),
                ImmutableList.of(
                    QUERY_ID_TABLE_MODEL,
                    STATE_TABLE_MODEL,
                    START_TIME_TABLE_MODEL,
                    END_TIME_TABLE_MODEL,
                    DATA_NODE_ID_TABLE_MODEL,
                    COST_TIME,
                    STATEMENT_TABLE_MODEL,
                    USER_TABLE_MODEL,
                    CLIENT_IP))));

    // mixed push down and cannot push down term
    logicalQueryPlan =
        planTester.createPlan(
            "select * from information_schema.current_queries where state='RUNNING' and query_id='1'");
    assertPlan(
        logicalQueryPlan,
        output(
            filter(
                new ComparisonExpression(
                    EQUAL, new SymbolReference(QUERY_ID_TABLE_MODEL), new StringLiteral("1")),
                infoSchemaTableScan(
                    "information_schema.current_queries",
                    Optional.empty(),
                    ImmutableList.of(
                        QUERY_ID_TABLE_MODEL,
                        STATE_TABLE_MODEL,
                        START_TIME_TABLE_MODEL,
                        END_TIME_TABLE_MODEL,
                        DATA_NODE_ID_TABLE_MODEL,
                        COST_TIME,
                        STATEMENT_TABLE_MODEL,
                        USER_TABLE_MODEL,
                        CLIENT_IP)))));

    // More than one state='xxx' terms
    logicalQueryPlan =
        planTester.createPlan(
            "select * from information_schema.current_queries where state='RUNNING' and state='xx'");
    assertPlan(
        logicalQueryPlan,
        output(
            filter(
                new ComparisonExpression(
                    EQUAL, new SymbolReference(STATE_TABLE_MODEL), new StringLiteral("xx")),
                infoSchemaTableScan(
                    "information_schema.current_queries",
                    Optional.empty(),
                    ImmutableList.of(
                        QUERY_ID_TABLE_MODEL,
                        STATE_TABLE_MODEL,
                        START_TIME_TABLE_MODEL,
                        END_TIME_TABLE_MODEL,
                        DATA_NODE_ID_TABLE_MODEL,
                        COST_TIME,
                        STATEMENT_TABLE_MODEL,
                        USER_TABLE_MODEL,
                        CLIENT_IP)))));
  }

  @Test
  public void testQueriesCostsHistogram() {
    LogicalQueryPlan logicalQueryPlan =
        planTester.createPlan("select * from information_schema.queries_costs_histogram");
    assertPlan(
        logicalQueryPlan,
        output(
            infoSchemaTableScan(
                "information_schema.queries_costs_histogram",
                Optional.empty(),
                ImmutableList.of(BIN, NUMS, DATA_NODE_ID_TABLE_MODEL))));

    //                  - Exchange
    // Output - Collect - Exchange
    assertPlan(planTester.getFragmentPlan(0), output(collect(exchange(), exchange())));
    // TableScan
    assertPlan(
        planTester.getFragmentPlan(1),
        infoSchemaTableScan("information_schema.queries_costs_histogram", Optional.of(1)));
    // TableScan
    assertPlan(
        planTester.getFragmentPlan(2),
        infoSchemaTableScan("information_schema.queries_costs_histogram", Optional.of(2)));
  }
}
