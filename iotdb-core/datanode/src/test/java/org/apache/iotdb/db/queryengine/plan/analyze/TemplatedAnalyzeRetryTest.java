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
package org.apache.iotdb.db.queryengine.plan.analyze;

import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.queryengine.common.SessionInfo;
import org.apache.iotdb.commons.schema.template.Template;
import org.apache.iotdb.db.queryengine.common.MPPQueryContext;
import org.apache.iotdb.db.queryengine.common.QueryId;
import org.apache.iotdb.db.queryengine.plan.expression.multi.FunctionExpression;
import org.apache.iotdb.db.queryengine.plan.parser.StatementGenerator;
import org.apache.iotdb.db.queryengine.plan.statement.crud.QueryStatement;

import org.junit.Test;

import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Arrays;
import java.util.Collections;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertTrue;

public class TemplatedAnalyzeRetryTest {

  @Test
  public void testCountTimeTemplateAnalysisDoesNotMutateStatementForRetry() throws Exception {
    QueryStatement queryStatement =
        (QueryStatement)
            StatementGenerator.createStatement(
                "select count_time(*) from root.sg.* align by device",
                ZonedDateTime.now().getOffset());
    queryStatement.semanticCheck();

    Analysis analysis = new Analysis();
    boolean canBuildPlan =
        TemplatedAggregationAnalyze.canBuildAggregationPlanUseTemplate(
            analysis,
            queryStatement,
            null,
            null,
            createQueryContext(),
            createAlignedTemplate(),
            Collections.emptyList());

    assertTrue(canBuildPlan);
    FunctionExpression countTimeExpression =
        (FunctionExpression)
            queryStatement.getSelectComponent().getResultColumns().get(0).getExpression();
    assertEquals("*", countTimeExpression.getExpressions().get(0).getOutputSymbol());
    FunctionExpression analyzedCountTimeExpression =
        (FunctionExpression) analysis.getAggregationExpressions().iterator().next();
    assertNotSame(countTimeExpression, analyzedCountTimeExpression);
    assertEquals("count_time(*)", analyzedCountTimeExpression.getExpressionString());
    assertEquals("count_time(Time)", analyzedCountTimeExpression.getOutputSymbol());

    // QueryExecution analyzes the same statement again when retrying a dispatch failure.
    queryStatement.semanticCheck();
  }

  @Test
  public void testFailedTemplateAttemptDoesNotConsumeLimitOffset() throws Exception {
    QueryStatement queryStatement =
        (QueryStatement)
            StatementGenerator.createStatement(
                "select __endTime, count(s1) from root.sg.* "
                    + "group by ([0, 100), 10ms) limit 5 offset 12 align by device",
                ZonedDateTime.now().getOffset());

    boolean canBuildPlan =
        TemplatedAggregationAnalyze.canBuildAggregationPlanUseTemplate(
            new Analysis(),
            queryStatement,
            null,
            null,
            createQueryContext(),
            createAlignedTemplate(),
            Arrays.asList(new PartialPath("root.sg.d1"), new PartialPath("root.sg.d2")));

    assertFalse(canBuildPlan);
    assertEquals(
        Arrays.asList(5L, 12L, 0L, 100L),
        Arrays.asList(
            queryStatement.getRowLimit(),
            queryStatement.getRowOffset(),
            queryStatement.getGroupByTimeComponent().getStartTime(),
            queryStatement.getGroupByTimeComponent().getEndTime()));
  }

  private static MPPQueryContext createQueryContext() {
    return new MPPQueryContext(
        "",
        new QueryId("test_query"),
        new SessionInfo(0, "test", ZoneId.systemDefault()),
        new TEndPoint(),
        new TEndPoint());
  }

  private static Template createAlignedTemplate() throws Exception {
    return new Template(
        "template",
        Collections.emptyList(),
        Collections.emptyList(),
        Collections.emptyList(),
        Collections.emptyList(),
        true);
  }
}
