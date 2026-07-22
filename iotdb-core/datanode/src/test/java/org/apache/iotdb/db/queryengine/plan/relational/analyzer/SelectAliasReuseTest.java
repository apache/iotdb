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
import org.apache.iotdb.commons.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.Symbol;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.node.ProjectNode;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.ArithmeticBinaryExpression;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.Cast;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.ExistsPredicate;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.Expression;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.FieldReference;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.FrameBound;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.FunctionCall;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.GenericDataType;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.Identifier;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.OrderBy;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.Query;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.QuerySpecification;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.Statement;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.SubqueryExpression;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.SymbolReference;
import org.apache.iotdb.db.queryengine.plan.planner.plan.LogicalQueryPlan;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanGraphJsonPrinter;
import org.apache.iotdb.db.queryengine.plan.relational.planner.PlanTester;
import org.apache.iotdb.db.queryengine.plan.relational.sql.parser.SqlParser;

import org.junit.Test;

import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.apache.iotdb.db.queryengine.plan.relational.analyzer.AnalyzerTest.analyzeStatement;
import static org.apache.iotdb.db.queryengine.plan.relational.analyzer.TestUtils.QUERY_CONTEXT;
import static org.apache.iotdb.db.queryengine.plan.relational.analyzer.TestUtils.TEST_MATADATA;
import static org.apache.iotdb.db.queryengine.plan.relational.analyzer.TestUtils.assertAnalyzeSemanticException;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class SelectAliasReuseTest {

  @Test
  public void groupByAliasUsesExpressionAndOrderByAliasUsesOutputField() {
    String sql =
        "SELECT date_bin(1h, time) AS hour_time, AVG(s1) AS avg_s1 "
            + "FROM table1 GROUP BY hour_time ORDER BY hour_time";

    AnalyzedQuery analyzedQuery = analyze(sql);
    assertDateBin(
        analyzedQuery.analysis.getGroupingSets(analyzedQuery.query).getOriginalExpressions());
    assertFieldReference(
        analyzedQuery.analysis.getOrderByExpressions(analyzedQuery.query).get(0), 0);

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
  public void groupByAliasIsNotBlockedByOuterScopeColumn() {
    String sql =
        "SELECT x FROM table_with_x WHERE EXISTS ("
            + "SELECT s1 AS x, COUNT(*) FROM table1 "
            + "WHERE table_with_x.s1 = table1.s1 GROUP BY x)";

    AnalyzedQuery analyzedQuery = analyze(sql);
    QuerySpecification innerQuery = getExistsSubquery(analyzedQuery.query);
    assertIdentifier(
        analyzedQuery.analysis.getGroupingSets(innerQuery).getOriginalExpressions().get(0), "s1");

    new PlanTester().createPlan(sql);
  }

  @Test
  public void groupingSetsAliasesUseResolvedExpressions() {
    AnalyzedQuery rollup = analyze("SELECT s1 AS x, COUNT(*) FROM table1 GROUP BY ROLLUP(x)");
    Analysis.GroupingSetAnalysis rollupAnalysis = rollup.analysis.getGroupingSets(rollup.query);
    assertSingleOriginalIdentifier(rollupAnalysis, "s1");
    assertEquals(1, rollupAnalysis.getRollups().size());

    AnalyzedQuery cube = analyze("SELECT s1 AS x, COUNT(*) FROM table1 GROUP BY CUBE(x)");
    Analysis.GroupingSetAnalysis cubeAnalysis = cube.analysis.getGroupingSets(cube.query);
    assertSingleOriginalIdentifier(cubeAnalysis, "s1");
    assertEquals(1, cubeAnalysis.getCubes().size());

    AnalyzedQuery explicit =
        analyze("SELECT s1 AS x, COUNT(*) FROM table1 GROUP BY GROUPING SETS ((x))");
    Analysis.GroupingSetAnalysis explicitAnalysis =
        explicit.analysis.getGroupingSets(explicit.query);
    assertSingleOriginalIdentifier(explicitAnalysis, "s1");
    assertEquals(1, explicitAnalysis.getOrdinarySets().size());
  }

  @Test
  public void groupingSetsInputColumnTakesPrecedenceOverAlias() {
    String sql = "SELECT x + 1 AS x, COUNT(s1) FROM table_with_x GROUP BY ROLLUP(x)";

    AnalyzedQuery analyzedQuery = analyze(sql);
    assertSingleOriginalIdentifier(
        analyzedQuery.analysis.getGroupingSets(analyzedQuery.query), "x");
  }

  @Test
  public void groupingSetsAliasIsNotBlockedByOuterScopeColumn() {
    String sql =
        "SELECT x FROM table_with_x WHERE EXISTS ("
            + "SELECT s1 AS x, COUNT(*) FROM table1 "
            + "WHERE table_with_x.s1 = table1.s1 GROUP BY GROUPING SETS ((x)))";

    AnalyzedQuery analyzedQuery = analyze(sql);
    QuerySpecification innerQuery = getExistsSubquery(analyzedQuery.query);
    assertSingleOriginalIdentifier(analyzedQuery.analysis.getGroupingSets(innerQuery), "s1");
  }

  @Test
  public void orderByOutputAliasTakesPrecedenceOverInputColumn() {
    String sql = "SELECT s1 AS x FROM table_with_x ORDER BY x";

    AnalyzedQuery analyzedQuery = analyze(sql);
    assertFieldReference(
        analyzedQuery.analysis.getOrderByExpressions(analyzedQuery.query).get(0), 0);

    new PlanTester().createPlan(sql);
  }

  @Test
  public void orderByAliasWithoutInputColumn() {
    String sql = "SELECT s1 AS x FROM table1 ORDER BY x";

    AnalyzedQuery analyzedQuery = analyze(sql);
    assertFieldReference(
        analyzedQuery.analysis.getOrderByExpressions(analyzedQuery.query).get(0), 0);

    new PlanTester().createPlan(sql);
  }

  @Test
  public void selectDistinctOrderByAliasUsesOutputField() {
    String sql = "SELECT DISTINCT s1 AS x FROM table1 ORDER BY x";

    AnalyzedQuery analyzedQuery = analyze(sql);
    assertFieldReference(
        analyzedQuery.analysis.getOrderByExpressions(analyzedQuery.query).get(0), 0);

    new PlanTester().createPlan(sql);
  }

  @Test
  public void orderByExpressionUsesOrderByScopeWithoutAliasRewrite() {
    String sql = "SELECT s1 AS x FROM table1 ORDER BY x + 1";

    AnalyzedQuery analyzedQuery = analyze(sql);
    Expression orderByExpression =
        analyzedQuery.analysis.getOrderByExpressions(analyzedQuery.query).get(0);
    assertTrue(orderByExpression instanceof ArithmeticBinaryExpression);

    new PlanTester().createPlan(sql);
  }

  @Test
  public void orderByWindowFunctionAliasReusesSelectOutputField() {
    String sql = "SELECT row_number() OVER (ORDER BY s1) AS rn FROM table1 ORDER BY rn";

    AnalyzedQuery analyzedQuery = analyze(sql);
    OrderBy orderBy = analyzedQuery.query.getOrderBy().get();
    List<FunctionCall> selectWindowFunctions =
        analyzedQuery.analysis.getWindowFunctions(analyzedQuery.query);

    assertEquals(1, selectWindowFunctions.size());
    assertEquals("row_number", selectWindowFunctions.get(0).getName().getSuffix());
    assertTrue(analyzedQuery.analysis.getOrderByWindowFunctions(orderBy).isEmpty());
    assertFieldReference(
        analyzedQuery.analysis.getOrderByExpressions(analyzedQuery.query).get(0), 0);

    new PlanTester().createPlan(sql);
  }

  @Test
  public void orderByWindowFunctionAliasExpressionUsesOrderByScope() {
    String sql = "SELECT row_number() OVER (ORDER BY s1) AS rn FROM table1 ORDER BY rn + 1";

    AnalyzedQuery analyzedQuery = analyze(sql);
    OrderBy orderBy = analyzedQuery.query.getOrderBy().get();
    List<FunctionCall> selectWindowFunctions =
        analyzedQuery.analysis.getWindowFunctions(analyzedQuery.query);
    Expression orderByExpression =
        analyzedQuery.analysis.getOrderByExpressions(analyzedQuery.query).get(0);

    assertEquals(1, selectWindowFunctions.size());
    assertEquals("row_number", selectWindowFunctions.get(0).getName().getSuffix());
    assertTrue(analyzedQuery.analysis.getOrderByWindowFunctions(orderBy).isEmpty());
    assertTrue(orderByExpression instanceof ArithmeticBinaryExpression);

    new PlanTester().createPlan(sql);
  }

  @Test
  public void lateralColumnAliasInSelectList() {
    String sql = "SELECT s1 AS x, x + 1 AS y FROM table1";

    AnalyzedQuery analyzedQuery = analyze(sql);
    assertIdentifier(getSelectExpression(analyzedQuery, 0), "s1");
    assertArithmeticIdentifiers(getSelectExpression(analyzedQuery, 1), "s1", null);

    new PlanTester().createPlan(sql);
  }

  @Test
  public void lateralColumnAliasCanChainAndRepeat() {
    String chained = "SELECT s1 AS x, x + 1 AS y, y * 2 AS z FROM table1";
    AnalyzedQuery analyzedChained = analyze(chained);
    assertArithmeticIdentifiers(getSelectExpression(analyzedChained, 1), "s1", null);
    assertTrue(
        ((ArithmeticBinaryExpression) getSelectExpression(analyzedChained, 2)).getLeft()
            instanceof ArithmeticBinaryExpression);
    new PlanTester().createPlan(chained);

    String repeated = "SELECT s1 AS x, x + x AS y FROM table1";
    AnalyzedQuery analyzedRepeated = analyze(repeated);
    assertArithmeticIdentifiers(getSelectExpression(analyzedRepeated, 1), "s1", "s1");
    new PlanTester().createPlan(repeated);
  }

  @Test
  public void lateralColumnAliasSupportsWideSelectListChain() {
    int aliasCount = 64;
    String sql = wideLateralColumnAliasSql(aliasCount);

    AnalyzedQuery analyzedQuery = analyze(sql);
    assertEquals(
        aliasCount + 1, analyzedQuery.analysis.getSelectExpressions(analyzedQuery.query).size());
    assertTrue(
        getSelectExpression(analyzedQuery, aliasCount) instanceof ArithmeticBinaryExpression);

    new PlanTester().createPlan(sql);
  }

  @Test
  public void lateralColumnAliasDoesNotSupportForwardOrSelfReference() {
    assertAnalyzeSemanticException(
        "SELECT y + 1 AS x, s1 AS y FROM table1", "Column 'y' cannot be resolved");

    assertAnalyzeSemanticException(
        "SELECT x + 1 AS x FROM table1", "Column 'x' cannot be resolved");
  }

  @Test
  public void inputColumnTakesPrecedenceOverLateralColumnAlias() {
    String sql = "SELECT s1 AS x, x + 1 AS y FROM table_with_x";

    AnalyzedQuery analyzedQuery = analyze(sql);
    assertArithmeticIdentifiers(getSelectExpression(analyzedQuery, 1), "x", null);

    new PlanTester().createPlan(sql);
  }

  @Test
  public void lateralColumnAliasMatchesDelimitedIdentifiersByCanonicalName() {
    String exactMatch = "SELECT s1 AS \"x\", \"x\" + 1 AS y FROM table1";
    AnalyzedQuery analyzedQuery = analyze(exactMatch);
    assertArithmeticIdentifiers(getSelectExpression(analyzedQuery, 1), "s1", null);
    new PlanTester().createPlan(exactMatch);

    String uppercaseExactMatch = "SELECT s1 AS \"X\", \"X\" + 1 AS y FROM table1";
    AnalyzedQuery analyzedUppercaseQuery = analyze(uppercaseExactMatch);
    assertArithmeticIdentifiers(getSelectExpression(analyzedUppercaseQuery, 1), "s1", null);
    new PlanTester().createPlan(uppercaseExactMatch);

    String caseDifferingDelimitedMatch = "SELECT s1 AS \"X\", \"x\" + 1 AS y FROM table1";
    AnalyzedQuery analyzedCaseDifferingDelimitedQuery = analyze(caseDifferingDelimitedMatch);
    assertArithmeticIdentifiers(
        getSelectExpression(analyzedCaseDifferingDelimitedQuery, 1), "s1", null);
    new PlanTester().createPlan(caseDifferingDelimitedMatch);

    String unquotedReferenceToDelimitedAlias = "SELECT s1 AS \"x\", x + 1 AS y FROM table1";
    AnalyzedQuery analyzedUnquotedReferenceQuery = analyze(unquotedReferenceToDelimitedAlias);
    assertArithmeticIdentifiers(getSelectExpression(analyzedUnquotedReferenceQuery, 1), "s1", null);
    new PlanTester().createPlan(unquotedReferenceToDelimitedAlias);

    String delimitedReferenceToUnquotedAlias = "SELECT s1 AS x, \"x\" + 1 AS y FROM table1";
    AnalyzedQuery analyzedDelimitedReferenceQuery = analyze(delimitedReferenceToUnquotedAlias);
    assertArithmeticIdentifiers(
        getSelectExpression(analyzedDelimitedReferenceQuery, 1), "s1", null);
    new PlanTester().createPlan(delimitedReferenceToUnquotedAlias);
  }

  @Test
  public void duplicateLateralColumnAliasesAreAmbiguousUnlessInputColumnMatches() {
    assertAnalyzeSemanticException(
        "SELECT s1 AS x, s2 AS x, x + 1 AS y FROM table1", "Column alias 'x' is ambiguous");

    assertAnalyzeSemanticException(
        "SELECT s1 AS \"x\", s2 AS X, x + 1 AS y FROM table1", "Column alias 'x' is ambiguous");

    String sql = "SELECT s1 AS x, s2 AS x, x + 1 AS y FROM table_with_x";
    AnalyzedQuery analyzedQuery = analyze(sql);
    assertArithmeticIdentifiers(getSelectExpression(analyzedQuery, 2), "x", null);
    new PlanTester().createPlan(sql);
  }

  @Test
  public void lateralColumnAliasWorksWithAggregates() {
    String sql = "SELECT avg(s1) AS a, a + 1 AS b FROM table1";

    AnalyzedQuery analyzedQuery = analyze(sql);
    assertTrue(
        ((ArithmeticBinaryExpression) getSelectExpression(analyzedQuery, 1)).getLeft()
            instanceof FunctionCall);

    new PlanTester().createPlan(sql);
  }

  @Test
  public void lateralColumnAliasCopiesAllRowsAggregate() {
    String sql = "SELECT count(*) AS c, c + 1 AS c2 FROM table1";

    AnalyzedQuery analyzedQuery = analyze(sql);
    assertTrue(
        ((ArithmeticBinaryExpression) getSelectExpression(analyzedQuery, 1)).getLeft()
            instanceof FunctionCall);

    new PlanTester().createPlan(sql);
  }

  @Test
  public void lateralColumnAliasRewritesBeforeAggregationValidation() {
    assertAnalyzeSemanticException(
        "SELECT s1 AS x, avg(s2) + x AS y FROM table1",
        "must be an aggregate expression or appear in GROUP BY clause");
  }

  @Test
  public void groupByAliasUsesLateralColumnAliasRewrittenExpression() {
    String sql = "SELECT s1 AS x, x + 1 AS y, COUNT(*) FROM table1 GROUP BY y, x";

    AnalyzedQuery analyzedQuery = analyze(sql);
    List<Expression> groupByExpressions =
        analyzedQuery.analysis.getGroupingSets(analyzedQuery.query).getOriginalExpressions();
    assertTrue(groupByExpressions.get(0) instanceof ArithmeticBinaryExpression);
    assertArithmeticIdentifiers(groupByExpressions.get(0), "s1", null);
    assertIdentifier(groupByExpressions.get(1), "s1");

    new PlanTester().createPlan(sql);
  }

  @Test
  public void orderByCanReuseLateralColumnAliasOutput() {
    String sql = "SELECT s1 AS x, x + 1 AS y FROM table1 ORDER BY y";

    AnalyzedQuery analyzedQuery = analyze(sql);
    assertFieldReference(
        analyzedQuery.analysis.getOrderByExpressions(analyzedQuery.query).get(0), 1);

    new PlanTester().createPlan(sql);
  }

  @Test
  public void selectDistinctSupportsLateralColumnAliasWithoutOrderBy() {
    String sql = "SELECT DISTINCT s1 AS x, x + 1 AS y FROM table1";

    AnalyzedQuery analyzedQuery = analyze(sql);
    assertArithmeticIdentifiers(getSelectExpression(analyzedQuery, 1), "s1", null);

    new PlanTester().createPlan(sql);
  }

  @Test
  public void columnsSelectItemDoesNotRegisterLateralColumnAlias() {
    assertAnalyzeSemanticException(
        "SELECT COLUMNS('s.*') AS x, x + 1 AS y FROM table1", "Column 'x' cannot be resolved");
  }

  @Test
  public void windowFunctionLateralColumnAliasIsExplicitlyRejected() {
    assertAnalyzeSemanticException(
        "SELECT row_number() OVER (ORDER BY s1) AS rn, rn + 1 AS rn2 FROM table1",
        "Lateral column alias 'rn' containing window function is not supported");
  }

  @Test
  public void lateralColumnAliasCanBeUsedInWindowSpecification() {
    String orderBySql = "SELECT s1 AS x, row_number() OVER (ORDER BY x) AS rn FROM table1";
    AnalyzedQuery analyzedOrderBy = analyze(orderBySql);
    FunctionCall rowNumber = (FunctionCall) getSelectExpression(analyzedOrderBy, 1);
    assertIdentifier(
        analyzedOrderBy
            .analysis
            .getWindow(rowNumber)
            .getOrderBy()
            .get()
            .getSortItems()
            .get(0)
            .getSortKey(),
        "s1");
    new PlanTester().createPlan(orderBySql);

    String partitionBySql = "SELECT s1 AS x, avg(s2) OVER (PARTITION BY x) AS a FROM table1";
    AnalyzedQuery analyzedPartitionBy = analyze(partitionBySql);
    FunctionCall avg = (FunctionCall) getSelectExpression(analyzedPartitionBy, 1);
    assertIdentifier(analyzedPartitionBy.analysis.getWindow(avg).getPartitionBy().get(0), "s1");
    new PlanTester().createPlan(partitionBySql);
  }

  @Test
  public void namedWindowDefinitionDoesNotSeeLateralColumnAlias() {
    assertAnalyzeSemanticException(
        "SELECT s1 AS x, row_number() OVER w AS rn FROM table1 WINDOW w AS (ORDER BY x)",
        "Column 'x' cannot be resolved");
  }

  @Test
  public void lateralColumnAliasCanBeUsedInWindowFrameBounds() {
    String sql =
        "SELECT s1 AS x, sum(s2) OVER "
            + "(ORDER BY time ROWS BETWEEN x PRECEDING AND CURRENT ROW) AS total FROM table1";

    AnalyzedQuery analyzedQuery = analyze(sql);
    FunctionCall sum = (FunctionCall) getSelectExpression(analyzedQuery, 1);
    FrameBound start = analyzedQuery.analysis.getWindow(sum).getFrame().get().getStart();
    assertIdentifier(start.getValue().get(), "s1");

    new PlanTester().createPlan(sql);
  }

  @Test
  public void lateralColumnAliasDoesNotRewriteCastTypeNames() {
    String sql = "SELECT s1 AS INT64, CAST(s2 AS INT64) AS y FROM table1";

    AnalyzedQuery analyzedQuery = analyze(sql);
    Expression expression = getSelectExpression(analyzedQuery, 1);
    assertTrue(expression instanceof Cast);
    assertTrue(((Cast) expression).getType() instanceof GenericDataType);
    assertEquals("INT64", ((GenericDataType) ((Cast) expression).getType()).getName().getValue());

    new PlanTester().createPlan(sql);
  }

  @Test
  public void lateralColumnAliasCanBeSelectedDirectly() {
    String sql = "SELECT s1 AS x, x FROM table1";

    AnalyzedQuery analyzedQuery = analyze(sql);
    assertIdentifier(getSelectExpression(analyzedQuery, 1), "s1");
    assertEquals("x", getOutputFieldName(analyzedQuery, 1));

    new PlanTester().createPlan(sql);
  }

  @Test
  public void lateralColumnAliasPreservesOriginalOutputNameWithoutExplicitAlias() {
    String sql = "SELECT s1 + 1 AS x, x FROM table1";

    AnalyzedQuery analyzedQuery = analyze(sql);
    assertTrue(getSelectExpression(analyzedQuery, 1) instanceof ArithmeticBinaryExpression);
    assertEquals("x", getOutputFieldName(analyzedQuery, 1));

    new PlanTester().createPlan(sql);
  }

  @Test
  public void lateralColumnAliasPlanReusesPreviousProjection() {
    LogicalQueryPlan plan =
        new PlanTester().createPlan("SELECT CAST(s2 AS double) AS x, x + 1.0 AS y FROM table1");
    List<ProjectNode> projects = new ArrayList<>();
    collectProjectNodes(plan.getRootNode(), projects);

    Symbol castSymbol = null;
    int castAssignments = 0;
    for (ProjectNode project : projects) {
      for (Map.Entry<Symbol, Expression> assignment : project.getAssignments().entrySet()) {
        if (assignment.getValue() instanceof Cast) {
          castSymbol = assignment.getKey();
          castAssignments++;
        }
      }
    }

    assertNotNull(castSymbol);
    assertEquals(1, castAssignments);
    assertTrue(hasArithmeticOnSymbol(projects, castSymbol));
    assertFalse(hasArithmeticOnCast(projects));
  }

  @Test
  public void lateralColumnAliasDeepChainPlanExpressionsRemainShallow() {
    LogicalQueryPlan plan = new PlanTester().createPlan(wideLateralColumnAliasSql(200));
    List<ProjectNode> projects = new ArrayList<>();
    collectProjectNodes(plan.getRootNode(), projects);

    int maxArithmeticDepth = 0;
    for (ProjectNode project : projects) {
      for (Expression expression : project.getAssignments().getExpressions()) {
        maxArithmeticDepth = Math.max(maxArithmeticDepth, arithmeticDepth(expression));
      }
    }

    assertTrue("Max arithmetic depth: " + maxArithmeticDepth, maxArithmeticDepth <= 32);
    String planJson = PlanGraphJsonPrinter.toPrettyJson(plan.getRootNode());
    assertTrue("Plan JSON length: " + planJson.length(), planJson.length() < 2_000_000);
  }

  @Test
  public void duplicateAliasesAreAmbiguous() {
    assertAnalyzeSemanticException(
        "SELECT s1 AS x, s2 AS x FROM table1 ORDER BY x", "Column alias 'x' is ambiguous");

    assertAnalyzeSemanticException(
        "SELECT s1 AS x, s2 AS x, COUNT(*) FROM table1 GROUP BY x",
        "Column alias 'x' is ambiguous");

    assertAnalyzeSemanticException(
        "SELECT s1 AS x, s2 AS x, COUNT(*) FROM table1 GROUP BY ROLLUP(x)",
        "Column alias 'x' is ambiguous");

    assertAnalyzeSemanticException(
        "SELECT s1 AS x, s2 AS x, COUNT(*) FROM table1 GROUP BY CUBE(x)",
        "Column alias 'x' is ambiguous");

    assertAnalyzeSemanticException(
        "SELECT s1 AS x, s2 AS x, COUNT(*) FROM table1 GROUP BY GROUPING SETS ((x))",
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
        "SELECT AVG(s1) AS avg_s1 FROM table1 HAVING avg_s1 > 1",
        "Column 'avg_s1' cannot be resolved");
  }

  @Test
  public void selectAliasDoesNotLeakIntoSubquery() {
    assertAnalyzeSemanticException(
        "SELECT s1 AS x, (SELECT x FROM table1) FROM table1", "Column 'x' cannot be resolved");
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
    assertFieldReference(
        analyzedQuery.analysis.getOrderByExpressions(analyzedQuery.query).get(0), 0);
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

  private static void assertSingleOriginalIdentifier(
      Analysis.GroupingSetAnalysis analysis, String name) {
    assertEquals(1, analysis.getOriginalExpressions().size());
    assertIdentifier(analysis.getOriginalExpressions().get(0), name);
  }

  private static Expression getSelectExpression(AnalyzedQuery analyzedQuery, int index) {
    return analyzedQuery
        .analysis
        .getSelectExpressions(analyzedQuery.query)
        .get(index)
        .getExpression();
  }

  private static String getOutputFieldName(AnalyzedQuery analyzedQuery, int index) {
    List<Field> fields =
        new ArrayList<>(
            analyzedQuery
                .analysis
                .getScope(analyzedQuery.query)
                .getRelationType()
                .getVisibleFields());
    return fields.get(index).getName().orElse(null);
  }

  private static String wideLateralColumnAliasSql(int aliasCount) {
    StringBuilder sql = new StringBuilder("SELECT s1 AS c0");
    for (int i = 1; i < aliasCount; i++) {
      sql.append(", c").append(i - 1).append(" + 1 AS c").append(i);
    }
    sql.append(", c0 + c").append(aliasCount - 1).append(" AS final_value FROM table1");
    return sql.toString();
  }

  private static void assertArithmeticIdentifiers(
      Expression expression, String leftName, String rightName) {
    assertTrue(expression instanceof ArithmeticBinaryExpression);
    ArithmeticBinaryExpression arithmeticExpression = (ArithmeticBinaryExpression) expression;
    assertIdentifier(arithmeticExpression.getLeft(), leftName);
    if (rightName != null) {
      assertIdentifier(arithmeticExpression.getRight(), rightName);
    }
  }

  private static void assertIdentifier(Expression expression, String name) {
    assertTrue(expression instanceof Identifier);
    assertEquals(name, ((Identifier) expression).getValue());
  }

  private static void assertFieldReference(Expression expression, int index) {
    assertTrue(expression instanceof FieldReference);
    assertEquals(index, ((FieldReference) expression).getFieldIndex());
  }

  private static void collectProjectNodes(PlanNode node, List<ProjectNode> projects) {
    if (node instanceof ProjectNode) {
      projects.add((ProjectNode) node);
    }
    for (PlanNode child : node.getChildren()) {
      collectProjectNodes(child, projects);
    }
  }

  private static boolean hasArithmeticOnSymbol(List<ProjectNode> projects, Symbol symbol) {
    for (ProjectNode project : projects) {
      for (Expression expression : project.getAssignments().getExpressions()) {
        if (expression instanceof ArithmeticBinaryExpression) {
          ArithmeticBinaryExpression arithmeticExpression = (ArithmeticBinaryExpression) expression;
          if (isSymbolReference(arithmeticExpression.getLeft(), symbol)
              || isSymbolReference(arithmeticExpression.getRight(), symbol)) {
            return true;
          }
        }
      }
    }
    return false;
  }

  private static boolean hasArithmeticOnCast(List<ProjectNode> projects) {
    for (ProjectNode project : projects) {
      for (Expression expression : project.getAssignments().getExpressions()) {
        if (expression instanceof ArithmeticBinaryExpression) {
          ArithmeticBinaryExpression arithmeticExpression = (ArithmeticBinaryExpression) expression;
          if (arithmeticExpression.getLeft() instanceof Cast
              || arithmeticExpression.getRight() instanceof Cast) {
            return true;
          }
        }
      }
    }
    return false;
  }

  private static int arithmeticDepth(Expression expression) {
    if (!(expression instanceof ArithmeticBinaryExpression)) {
      return 0;
    }
    ArithmeticBinaryExpression arithmeticExpression = (ArithmeticBinaryExpression) expression;
    return 1
        + Math.max(
            arithmeticDepth(arithmeticExpression.getLeft()),
            arithmeticDepth(arithmeticExpression.getRight()));
  }

  private static boolean isSymbolReference(Expression expression, Symbol symbol) {
    return expression instanceof SymbolReference
        && ((SymbolReference) expression).getName().equals(symbol.getName());
  }

  private static QuerySpecification getExistsSubquery(QuerySpecification query) {
    assertTrue(query.getWhere().get() instanceof ExistsPredicate);
    Expression subquery = ((ExistsPredicate) query.getWhere().get()).getSubquery();
    assertTrue(subquery instanceof SubqueryExpression);
    return (QuerySpecification) ((SubqueryExpression) subquery).getQuery().getQueryBody();
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
