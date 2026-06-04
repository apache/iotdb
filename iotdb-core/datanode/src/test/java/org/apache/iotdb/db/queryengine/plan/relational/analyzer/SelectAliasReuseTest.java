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

import org.apache.iotdb.commons.queryengine.common.SessionInfo;
import org.apache.iotdb.commons.queryengine.common.SqlDialect;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.Expression;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.FunctionCall;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.Identifier;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.Query;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.QuerySpecification;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.Statement;
import org.apache.iotdb.db.queryengine.plan.relational.planner.PlanTester;
import org.apache.iotdb.db.queryengine.plan.relational.sql.parser.SqlParser;

import org.junit.Test;

import java.time.ZoneId;
import java.util.List;

import static org.apache.iotdb.db.queryengine.plan.relational.analyzer.AnalyzerTest.analyzeStatement;
import static org.apache.iotdb.db.queryengine.plan.relational.analyzer.TestUtils.QUERY_CONTEXT;
import static org.apache.iotdb.db.queryengine.plan.relational.analyzer.TestUtils.TEST_MATADATA;
import static org.apache.iotdb.db.queryengine.plan.relational.analyzer.TestUtils.assertAnalyzeSemanticException;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class SelectAliasReuseTest {

  @Test
  public void groupByAndOrderByAliasUseRewrittenExpression() {
    String sql =
        "SELECT date_bin(1h, time) AS hour_time, AVG(s1) AS avg_s1 "
            + "FROM table1 GROUP BY hour_time ORDER BY hour_time";

    AnalyzedQuery analyzedQuery = analyze(sql);
    assertDateBin(
        analyzedQuery.analysis.getGroupingSets(analyzedQuery.query).getOriginalExpressions());
    assertDateBin(analyzedQuery.analysis.getOrderByExpressions(analyzedQuery.query));

    new PlanTester().createPlan(sql);
  }

  @Test
  public void groupByInputColumnTakesPrecedenceOverAlias() {
    String sql = "SELECT x + 1 AS x, COUNT(s1) FROM table_with_x GROUP BY x";

    AnalyzedQuery analyzedQuery = analyze(sql);
    assertIdentifier(
        analyzedQuery.analysis.getGroupingSets(analyzedQuery.query).getOriginalExpressions().get(0),
        "x");

    new PlanTester().createPlan(sql);
  }

  @Test
  public void orderByOutputAliasTakesPrecedenceOverInputColumn() {
    String sql = "SELECT s1 AS x FROM table_with_x ORDER BY x";

    AnalyzedQuery analyzedQuery = analyze(sql);
    assertIdentifier(
        analyzedQuery.analysis.getOrderByExpressions(analyzedQuery.query).get(0), "s1");

    new PlanTester().createPlan(sql);
  }

  @Test
  public void orderByAliasWithoutInputColumn() {
    String sql = "SELECT s1 AS x FROM table1 ORDER BY x";

    AnalyzedQuery analyzedQuery = analyze(sql);
    assertIdentifier(
        analyzedQuery.analysis.getOrderByExpressions(analyzedQuery.query).get(0), "s1");

    new PlanTester().createPlan(sql);
  }

  @Test
  public void duplicateAliasesAreAmbiguous() {
    assertAnalyzeSemanticException(
        "SELECT s1 AS x, s2 AS x FROM table1 ORDER BY x", "Column alias 'x' is ambiguous");

    assertAnalyzeSemanticException(
        "SELECT s1 AS x, s2 AS x, COUNT(*) FROM table1 GROUP BY x",
        "Column alias 'x' is ambiguous");
  }

  @Test
  public void invalidAliasReferencesStillFail() {
    assertAnalyzeSemanticException(
        "SELECT AVG(s1) AS avg_s1 FROM table1 GROUP BY avg_s1",
        "GROUP BY clause cannot contain aggregations");

    assertAnalyzeSemanticException(
        "SELECT s1 AS x, table1.x + 1 FROM table1", "Column 'table1.x' cannot be resolved");

    assertAnalyzeSemanticException(
        "SELECT s1 AS x FROM table1 ORDER BY table1.x", "Column 'table1.x' cannot be resolved");

    assertAnalyzeSemanticException(
        "SELECT s1 AS x, COUNT(*) FROM table1 GROUP BY table1.x",
        "Column 'table1.x' cannot be resolved");

    assertAnalyzeSemanticException(
        "SELECT s1 AS x FROM table1 WHERE x > 1", "Column 'x' cannot be resolved");

    assertAnalyzeSemanticException(
        "SELECT s1 AS x FROM table1 ORDER BY x + 1", "Column 'x' cannot be resolved");

    assertAnalyzeSemanticException(
        "SELECT AVG(s1) AS avg_s1 FROM table1 HAVING avg_s1 > 1",
        "Column 'avg_s1' cannot be resolved");

    assertAnalyzeSemanticException(
        "SELECT s1 + 1 AS x, x * 2 AS y FROM table1", "Column 'x' cannot be resolved");
  }

  @Test
  public void selectAliasDoesNotLeakIntoSubquery() {
    assertAnalyzeSemanticException(
        "SELECT s1 AS x FROM table1 ORDER BY (SELECT x FROM table1)",
        "Column 'x' cannot be resolved");
  }

  @Test
  public void ordinalAndFullExpressionsStillWork() {
    new PlanTester()
        .createPlan("SELECT date_bin(1h, time), AVG(s1) FROM table1 GROUP BY 1 ORDER BY 1");

    new PlanTester()
        .createPlan(
            "SELECT date_bin(1h, time), AVG(s1) FROM table1 "
                + "GROUP BY date_bin(1h, time) ORDER BY AVG(s1)");
  }

  @Test
  public void dateBinGapFillAliasUsesRewrittenGroupingKey() {
    String sql =
        "SELECT date_bin_gapfill(1h, time) AS hour_time, AVG(s1) "
            + "FROM table1 GROUP BY hour_time ORDER BY hour_time";

    AnalyzedQuery analyzedQuery = analyze(sql);
    assertNotNull(analyzedQuery.analysis.getGapFill(analyzedQuery.query));
    assertDateBin(
        analyzedQuery.analysis.getGroupingSets(analyzedQuery.query).getOriginalExpressions());
    assertDateBin(analyzedQuery.analysis.getOrderByExpressions(analyzedQuery.query));
  }

  private static AnalyzedQuery analyze(String sql) {
    SqlParser sqlParser = new SqlParser();
    Statement statement = sqlParser.createStatement(sql, ZoneId.systemDefault(), null);
    SessionInfo session =
        new SessionInfo(0, "test", ZoneId.systemDefault(), "testdb", SqlDialect.TABLE);
    Analysis analysis =
        analyzeStatement(statement, TEST_MATADATA, QUERY_CONTEXT, sqlParser, session);
    Query query = (Query) statement;
    return new AnalyzedQuery(analysis, (QuerySpecification) query.getQueryBody());
  }

  private static void assertDateBin(List<Expression> expressions) {
    assertEquals(1, expressions.size());
    assertTrue(expressions.get(0) instanceof FunctionCall);
    assertEquals("date_bin", ((FunctionCall) expressions.get(0)).getName().getSuffix());
  }

  private static void assertIdentifier(Expression expression, String name) {
    assertTrue(expression instanceof Identifier);
    assertEquals(name, ((Identifier) expression).getValue());
  }

  private static class AnalyzedQuery {
    private final Analysis analysis;
    private final QuerySpecification query;

    private AnalyzedQuery(Analysis analysis, QuerySpecification query) {
      this.analysis = analysis;
      this.query = query;
    }
  }
}
