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

import org.apache.iotdb.commons.exception.SemanticException;
import org.apache.iotdb.db.queryengine.plan.relational.planner.ir.GapFillStartAndEndTimeExtractVisitor.Context;

import org.junit.Test;

import java.time.ZoneOffset;

import static org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.ComparisonExpression.Operator.GREATER_THAN;
import static org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.ComparisonExpression.Operator.GREATER_THAN_OR_EQUAL;
import static org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.ComparisonExpression.Operator.LESS_THAN;
import static org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.ComparisonExpression.Operator.LESS_THAN_OR_EQUAL;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertThrows;

public class GapFillStartAndEndTimeExtractVisitorTest {

  @Test
  public void testExclusiveLongMaxStartIsRejected() {
    Context context = new Context();
    context.updateStartTime(Long.MAX_VALUE, GREATER_THAN);
    context.updateEndTime(Long.MAX_VALUE, LESS_THAN_OR_EQUAL);
    assertThrows(SemanticException.class, () -> context.getTimeRange(0, 0, 1, ZoneOffset.UTC));
  }

  @Test
  public void testExclusiveLongMinEndIsRejected() {
    Context context = new Context();
    context.updateStartTime(Long.MIN_VALUE, GREATER_THAN_OR_EQUAL);
    context.updateEndTime(Long.MIN_VALUE, LESS_THAN);
    assertThrows(SemanticException.class, () -> context.getTimeRange(0, 0, 1, ZoneOffset.UTC));
  }

  @Test
  public void testInclusiveLongBoundariesArePreserved() {
    Context context = new Context();
    context.updateStartTime(Long.MIN_VALUE, GREATER_THAN_OR_EQUAL);
    context.updateEndTime(Long.MAX_VALUE, LESS_THAN_OR_EQUAL);
    assertArrayEquals(
        new long[] {Long.MIN_VALUE, Long.MAX_VALUE}, context.getTimeRange(0, 0, 1, ZoneOffset.UTC));
  }
}
