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

package org.apache.iotdb.db.queryengine.plan.relational.planner.ir;

import org.apache.iotdb.commons.queryengine.common.SessionInfo;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.Symbol;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.BetweenPredicate;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.ComparisonExpression;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.Expression;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.LogicalExpression;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.LongLiteral;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.SymbolReference;
import org.apache.iotdb.commons.queryengine.plan.relational.type.InternalTypeManager;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.TableMetadataImpl;
import org.apache.iotdb.db.queryengine.plan.relational.planner.PlannerContext;

import org.junit.Test;

import java.time.ZoneId;
import java.util.Arrays;

import static org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.ComparisonExpression.Operator.GREATER_THAN;
import static org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.ComparisonExpression.Operator.GREATER_THAN_OR_EQUAL;
import static org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.ComparisonExpression.Operator.LESS_THAN;
import static org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.ComparisonExpression.Operator.LESS_THAN_OR_EQUAL;
import static org.junit.Assert.assertArrayEquals;

public class GapFillStartAndEndTimeExtractVisitorTest {

  private static final Symbol TIME = new Symbol("time");
  private static final PlannerContext PLANNER_CONTEXT =
      new PlannerContext(new TableMetadataImpl(), new InternalTypeManager());
  private static final SessionInfo SESSION = new SessionInfo(0, "root", ZoneId.of("UTC"));

  @Test
  public void testTighterUpperBoundIsUsed() {
    assertTimeRange(
        1,
        5,
        time(GREATER_THAN_OR_EQUAL, 1),
        time(LESS_THAN_OR_EQUAL, 10),
        time(LESS_THAN_OR_EQUAL, 5));
    assertTimeRange(
        1,
        5,
        time(GREATER_THAN_OR_EQUAL, 1),
        time(LESS_THAN_OR_EQUAL, 5),
        time(LESS_THAN_OR_EQUAL, 10));
    assertTimeRange(1, 5, timeBetween(1, 10), time(LESS_THAN_OR_EQUAL, 5));
    assertTimeRange(1, 5, time(GREATER_THAN_OR_EQUAL, 1), time(LESS_THAN, 11), time(LESS_THAN, 6));
    assertTimeRange(
        1, 5, time(GREATER_THAN_OR_EQUAL, 1), time(LESS_THAN_OR_EQUAL, 10), time(LESS_THAN, 6));
    assertTimeRange(
        1, 5, time(GREATER_THAN_OR_EQUAL, 1), time(LESS_THAN, 11), time(LESS_THAN_OR_EQUAL, 5));
  }

  @Test
  public void testTighterLowerBoundIsUsed() {
    assertTimeRange(
        5,
        10,
        time(GREATER_THAN_OR_EQUAL, 1),
        time(GREATER_THAN_OR_EQUAL, 5),
        time(LESS_THAN_OR_EQUAL, 10));
    assertTimeRange(
        5,
        10,
        time(GREATER_THAN_OR_EQUAL, 5),
        time(GREATER_THAN_OR_EQUAL, 1),
        time(LESS_THAN_OR_EQUAL, 10));
    assertTimeRange(5, 10, timeBetween(1, 10), time(GREATER_THAN_OR_EQUAL, 5));
    assertTimeRange(
        5, 10, time(GREATER_THAN, 0), time(GREATER_THAN, 4), time(LESS_THAN_OR_EQUAL, 10));
  }

  /** Checks the time range inferred from the conjunction of the given terms. */
  private static void assertTimeRange(long startTime, long endTime, Expression... terms) {
    GapFillStartAndEndTimeExtractVisitor.Context context =
        new GapFillStartAndEndTimeExtractVisitor.Context();
    new LogicalExpression(LogicalExpression.Operator.AND, Arrays.asList(terms))
        .accept(new GapFillStartAndEndTimeExtractVisitor(TIME, PLANNER_CONTEXT, SESSION), context);
    // with a 1 ms interval every time is the start of its own window
    assertArrayEquals(
        new long[] {startTime, endTime}, context.getTimeRange(0, 0, 1, ZoneId.of("UTC")));
  }

  private static Expression time(ComparisonExpression.Operator operator, long value) {
    return new ComparisonExpression(operator, new SymbolReference("time"), literal(value));
  }

  private static Expression timeBetween(long min, long max) {
    return new BetweenPredicate(new SymbolReference("time"), literal(min), literal(max));
  }

  private static LongLiteral literal(long value) {
    return new LongLiteral(String.valueOf(value));
  }
}
