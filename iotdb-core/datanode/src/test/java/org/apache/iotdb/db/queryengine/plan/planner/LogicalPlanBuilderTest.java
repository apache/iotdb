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
package org.apache.iotdb.db.queryengine.plan.planner;

import org.apache.iotdb.db.queryengine.common.MPPQueryContext;
import org.apache.iotdb.db.queryengine.common.QueryId;
import org.apache.iotdb.db.queryengine.plan.analyze.Analysis;
import org.apache.iotdb.db.queryengine.plan.parser.StatementGenerator;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.SortNode;
import org.apache.iotdb.db.queryengine.plan.statement.component.OrderByKey;
import org.apache.iotdb.db.queryengine.plan.statement.component.Ordering;
import org.apache.iotdb.db.queryengine.plan.statement.component.SortItem;
import org.apache.iotdb.db.queryengine.plan.statement.crud.QueryStatement;

import org.junit.Test;

import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class LogicalPlanBuilderTest {

  @Test
  public void testPlanDeviceViewDoesNotAppendDefaultSortItemsToStatement() {
    QueryStatement queryStatement =
        (QueryStatement)
            StatementGenerator.createStatement(
                "select s1 from root.sg.* order by time desc align by device",
                ZonedDateTime.now().getOffset());
    int originalSortItemCount = queryStatement.getSortItemList().size();

    planDeviceView(queryStatement, "first_plan");
    int sortItemCountAfterFirstPlan = queryStatement.getSortItemList().size();
    planDeviceView(queryStatement, "retry_plan");

    assertEquals(
        Arrays.asList(originalSortItemCount, originalSortItemCount),
        Arrays.asList(sortItemCountAfterFirstPlan, queryStatement.getSortItemList().size()));
  }

  @Test
  public void testPlanOrderByUsesDeviceViewMergeKeysWithoutMutatingStatement() {
    QueryStatement queryStatement =
        (QueryStatement)
            StatementGenerator.createStatement(
                "select s1 from root.sg.* order by s1 desc align by device",
                ZonedDateTime.now().getOffset());
    List<SortItem> originalSortItems = new ArrayList<>(queryStatement.getSortItemList());
    String originalOrderByClause = queryStatement.getOrderByComponent().toSQLString();
    List<SortItem> expectedSortItems = new ArrayList<>(originalSortItems);
    expectedSortItems.add(new SortItem(OrderByKey.DEVICE, Ordering.ASC));
    expectedSortItems.add(new SortItem(OrderByKey.TIME, Ordering.ASC));

    Analysis analysis = new Analysis();
    analysis.setOrderByExpressions(Collections.emptySet());
    analysis.setSelectExpressions(Collections.emptySet());
    LogicalPlanBuilder builder =
        new LogicalPlanBuilder(analysis, new MPPQueryContext(new QueryId("two_stage_plan")))
            .planDeviceView(
                Collections.emptyMap(),
                Collections.emptySet(),
                Collections.emptyMap(),
                Collections.emptySet(),
                queryStatement,
                analysis)
            .planOrderBy(queryStatement, analysis);

    assertTrue(builder.getRoot() instanceof SortNode);
    assertTrue(
        ((SortNode) builder.getRoot())
            .getOrderByParameter()
            .getSortItemList()
            .get(0)
            .isExpression());
    assertEquals(
        expectedSortItems, ((SortNode) builder.getRoot()).getOrderByParameter().getSortItemList());
    assertEquals(originalSortItems, queryStatement.getSortItemList());
    assertEquals(originalOrderByClause, queryStatement.getOrderByComponent().toSQLString());
  }

  private static void planDeviceView(QueryStatement queryStatement, String queryId) {
    Analysis analysis = new Analysis();
    new LogicalPlanBuilder(analysis, new MPPQueryContext(new QueryId(queryId)))
        .planDeviceView(
            Collections.emptyMap(),
            Collections.emptySet(),
            Collections.emptyMap(),
            Collections.emptySet(),
            queryStatement,
            analysis);
  }
}
