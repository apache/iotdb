/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.iotdb.db.queryengine.plan.relational.analyzer;

import org.apache.iotdb.db.protocol.session.IClientSession;
import org.apache.iotdb.db.queryengine.common.MPPQueryContext;
import org.apache.iotdb.db.queryengine.common.SessionInfo;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.Metadata;
import org.apache.iotdb.db.queryengine.plan.relational.security.AccessControl;
import org.apache.iotdb.db.queryengine.plan.relational.security.AllowAllAccessControl;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Statement;
import org.apache.iotdb.db.queryengine.plan.relational.sql.parser.SqlParser;
import org.apache.iotdb.db.queryengine.plan.relational.sql.rewrite.StatementRewriteFactory;

import org.junit.Assert;
import org.junit.Test;

import java.time.ZoneId;
import java.util.Collections;

import static org.apache.iotdb.db.queryengine.execution.warnings.WarningCollector.NOOP;
import static org.apache.iotdb.db.queryengine.plan.relational.analyzer.TestUtils.QUERY_CONTEXT;
import static org.apache.iotdb.db.queryengine.plan.relational.analyzer.TestUtils.TEST_MATADATA;
import static org.junit.Assert.fail;

public class RowPatternRecognitionTest {
  private static final AccessControl nopAccessControl = new AllowAllAccessControl();

  // table1's columns: time, tag1, tag2, tag3, attr1, attr2, s1, s2, s3

  @Test
  public void testInputColumns() {
    assertTestSuccess(
        "SELECT * "
            + "FROM table1 "
            + "MATCH_RECOGNIZE ( "
            + "  ORDER BY time "
            + "  MEASURES "
            + "    A.s1 AS col1 "
            + "  PATTERN (A B+) "
            + "  DEFINE "
            + "    B AS B.s2 > 5 "
            + ") AS m");

    assertTestFail(
        "SELECT * "
            + "FROM table1 "
            + "MATCH_RECOGNIZE ( "
            + "  ORDER BY time "
            + "  MEASURES "
            + "    A.s1 AS col1 "
            + "  PATTERN (A B+) "
            + "  DEFINE "
            + "    B AS B.s4 > 5 "
            + ") AS m",
        "Column s4 prefixed with label B cannot be resolved");

    assertTestFail(
        "SELECT * "
            + "FROM table1 "
            + "MATCH_RECOGNIZE ( "
            + "  PARTITION BY s4 "
            + "  ORDER BY time "
            + "  MEASURES "
            + "    A.s1 AS col1 "
            + "  PATTERN (A B+) "
            + "  DEFINE "
            + "    B AS B.s2 > 5 "
            + ") AS m",
        "Column s4 is not present in the input relation");
  }

  @Test
  public void testSubsetClause() {
    assertTestFail(
        "SELECT * "
            + "FROM table1 "
            + "MATCH_RECOGNIZE ( "
            + "  ORDER BY time "
            + "  MEASURES "
            + "    A.s1 AS col1 "
            + "  PATTERN (A B+) "
            + "  SUBSET A = (B) "
            + "  DEFINE "
            + "    B AS B.s2 > 5 "
            + ") AS m",
        "union pattern variable name: A is a duplicate of primary pattern variable name");

    assertTestFail(
        "SELECT * "
            + "FROM table1 "
            + "MATCH_RECOGNIZE ( "
            + "  ORDER BY time "
            + "  MEASURES "
            + "    A.s1 AS col1 "
            + "  PATTERN (A B+) "
            + "  SUBSET U = (A), "
            + "         U = (B) "
            + "  DEFINE "
            + "    B AS B.s2 > 5 "
            + ") AS m",
        "union pattern variable name: U is declared twice");

    assertTestFail(
        "SELECT * "
            + "FROM table1 "
            + "MATCH_RECOGNIZE ( "
            + "  ORDER BY time "
            + "  MEASURES "
            + "    A.s1 AS col1 "
            + "  PATTERN (A B+) "
            + "  SUBSET U = (A, C) "
            + "  DEFINE "
            + "    B AS B.s2 > 5 "
            + ") AS m",
        "subset element: C is not a primary pattern variable");
  }

  @Test
  public void testDefineClause() {
    assertTestFail(
        "SELECT * "
            + "FROM table1 "
            + "MATCH_RECOGNIZE ( "
            + "  ORDER BY time "
            + "  MEASURES "
            + "    A.s1 AS col1 "
            + "  PATTERN (A B+) "
            + "  DEFINE "
            + "    B AS B.s2 > 5, "
            + "    C AS true "
            + ") AS m",
        "defined variable: C is not a primary pattern variable");

    assertTestFail(
        "SELECT * "
            + "FROM table1 "
            + "MATCH_RECOGNIZE ( "
            + "  ORDER BY time "
            + "  MEASURES "
            + "    A.s1 AS col1 "
            + "  PATTERN (A B+) "
            + "  DEFINE "
            + "    B AS B.s2 > 5, "
            + "    B AS true "
            + ") AS m",
        "pattern variable with name: B is defined twice");

    assertTestFail(
        "SELECT * "
            + "FROM table1 "
            + "MATCH_RECOGNIZE ( "
            + "  ORDER BY time "
            + "  MEASURES "
            + "    A.s1 AS col1 "
            + "  PATTERN (A B+) "
            + "  DEFINE "
            + "    B AS B.s2 "
            + ") AS m",
        "Expression defining a label must be boolean (actual type: INT64)");

    // FINAL semantics is not supported in DEFINE clause. RUNNING semantics is supported
    assertTestFail(
        "SELECT * "
            + "FROM table1 "
            + "MATCH_RECOGNIZE ( "
            + "  ORDER BY time "
            + "  MEASURES "
            + "    A.s1 AS col1 "
            + "  PATTERN (A B+) "
            + "  DEFINE "
            + "    B AS FINAL RPR_LAST(B.s2) > 5"
            + ") AS m",
        "FINAL semantics is not supported in DEFINE clause");

    assertTestSuccess(
        "SELECT * "
            + "FROM table1 "
            + "MATCH_RECOGNIZE ( "
            + "  ORDER BY time "
            + "  MEASURES "
            + "    A.s1 AS col1 "
            + "  PATTERN (A B+) "
            + "  DEFINE "
            + "    B AS RUNNING RPR_LAST(B.s2) > 5"
            + ") AS m");
  }

  @Test
  public void testPatternExclusions() {
    String sql =
        "SELECT * "
            + "FROM table1 "
            + "MATCH_RECOGNIZE ( "
            + "  ORDER BY time "
            + "  MEASURES "
            + "    A.s1 AS col1 "
            + "  %s "
            + "  PATTERN ({- A -} B+) "
            + "  DEFINE "
            + "    B AS B.s2 > 5"
            + ") AS m";

    assertTestSuccess(String.format(sql, ""));
    assertTestSuccess(String.format(sql, "ONE ROW PER MATCH"));
    assertTestSuccess(String.format(sql, "ALL ROWS PER MATCH"));
    assertTestSuccess(String.format(sql, "ALL ROWS PER MATCH SHOW EMPTY MATCHES"));
    assertTestSuccess(String.format(sql, "ALL ROWS PER MATCH OMIT EMPTY MATCHES"));

    assertTestFail(
        String.format(sql, "ALL ROWS PER MATCH WITH UNMATCHED ROWS"),
        "Pattern exclusion syntax is not allowed when ALL ROWS PER MATCH WITH UNMATCHED ROWS is specified");
  }

  @Test
  public void testPatternFunctions() {
    String sql =
        "SELECT * "
            + "FROM table1 "
            + "MATCH_RECOGNIZE ( "
            + "  ORDER BY time "
            + "  MEASURES "
            + "    %s AS col1 "
            + "  PATTERN (A B+) "
            + "  DEFINE "
            + "    B AS B.s2 > 5"
            + ") AS m";

    assertTestSuccess(String.format(sql, "RPR_LAST(A.s1)"));

    assertTestFail(
        String.format(sql, "LAST(A.s1)"),
        "Pattern recognition function names cannot be LAST or FIRST, use RPR_LAST or RPR_FIRST instead.");
    assertTestFail(
        String.format(sql, "FIRST(A.s1)"),
        "Pattern recognition function names cannot be LAST or FIRST, use RPR_LAST or RPR_FIRST instead.");
    assertTestFail(
        String.format(sql, "RPR_LAT(A.s1)"), "Unknown pattern recognition function: RPR_LAT");
    assertTestFail(
        String.format(sql, "\"RPP_LAST\"(s2)"),
        "Pattern recognition function name must not be delimited: RPP_LAST");
    assertTestFail(
        String.format(sql, "A.RPP_LAST(s2)"),
        "Pattern recognition function name must not be qualified: a.rpp_last");
  }

  @Test
  public void testPatternQuantifiers() {
    String sql =
        "SELECT * "
            + "FROM table1 "
            + "MATCH_RECOGNIZE ( "
            + "  ORDER BY time "
            + "  MEASURES "
            + "    A.s1 AS col1 "
            + "  PATTERN (A B%s) "
            + "  DEFINE "
            + "    B AS B.s2 > 5"
            + ") AS m";

    assertTestSuccess(String.format(sql, "*"));
    assertTestSuccess(String.format(sql, "*?"));
    assertTestSuccess(String.format(sql, "+"));
    assertTestSuccess(String.format(sql, "+?"));
    assertTestSuccess(String.format(sql, "?"));
    assertTestSuccess(String.format(sql, "??"));
    assertTestSuccess(String.format(sql, "{,}"));
    assertTestSuccess(String.format(sql, "{5}"));
    assertTestSuccess(String.format(sql, "{5,}"));
    assertTestSuccess(String.format(sql, "{0,}"));

    assertTestFail(
        String.format(sql, "{0}"),
        "Pattern quantifier upper bound must be greater than or equal to 1");
    assertTestFail(
        String.format(sql, "{,0}"),
        "Pattern quantifier upper bound must be greater than or equal to 1");
    assertTestFail(
        String.format(sql, "{0,0}"),
        "Pattern quantifier upper bound must be greater than or equal to 1");
    assertTestFail(
        String.format(sql, "{3000000000}"),
        "Pattern quantifier lower bound must not exceed 2147483647");
    assertTestFail(
        String.format(sql, "{5,1}"), "Pattern quantifier lower bound must not exceed upper bound");
  }

  @Test
  public void testAfterMatchSkipClause() {
    String sql =
        "SELECT * "
            + "FROM table1 "
            + "MATCH_RECOGNIZE ( "
            + "  ORDER BY time "
            + "  MEASURES "
            + "    A.s1 AS col1 "
            + "  %s "
            + "  PATTERN (A B+) "
            + "  DEFINE "
            + "    B AS B.s2 > 5"
            + ") AS m";

    assertTestSuccess(String.format(sql, ""));
    assertTestSuccess(String.format(sql, "AFTER MATCH SKIP PAST LAST ROW"));
    assertTestSuccess(String.format(sql, "AFTER MATCH SKIP TO NEXT ROW"));
    assertTestSuccess(String.format(sql, "AFTER MATCH SKIP TO FIRST B"));
    assertTestSuccess(String.format(sql, "AFTER MATCH SKIP TO LAST B"));
    assertTestSuccess(String.format(sql, "AFTER MATCH SKIP TO B"));

    assertTestFail(
        String.format(sql, "AFTER MATCH SKIP TO C"),
        "C is not a primary or union pattern variable");
  }

  @Test
  public void testRunningAndFinalSemantics() {
    String sql =
        "SELECT * "
            + "FROM table1 "
            + "MATCH_RECOGNIZE ( "
            + "  ORDER BY time "
            + "  MEASURES "
            + "    %s AS col1 "
            + "  PATTERN (A B+) "
            + "  DEFINE "
            + "    B AS B.s2 > 5 "
            + ") AS m";

    assertTestSuccess(String.format(sql, "FINAL RPR_LAST(A.s1)"));
    assertTestSuccess(String.format(sql, "FINAL RPR_FIRST(A.s1)"));

    assertTestFail(
        String.format(sql, "FINAL PREV(A.s1)"),
        "FINAL semantics is not supported with prev pattern recognition function");
    assertTestFail(
        String.format(sql, "FINAL NEXT(A.s1)"),
        "FINAL semantics is not supported with next pattern recognition function");
    assertTestFail(
        String.format(sql, "FINAL CLASSIFIER(A.s1)"),
        "FINAL semantics is not supported with classifier pattern recognition function");
    assertTestFail(
        String.format(sql, "FINAL MATCH_NUMBER(A.s1)"),
        "FINAL semantics is not supported with match_number pattern recognition function");
  }

  @Test
  public void testPatternNavigationFunctions() {
    String sql =
        "SELECT * "
            + "FROM table1 "
            + "MATCH_RECOGNIZE ( "
            + "  ORDER BY time "
            + "  MEASURES "
            + "    %s AS col1 "
            + "  PATTERN (A B+) "
            + "  DEFINE "
            + "    B AS B.s2 > 5 "
            + ") AS m";

    assertTestSuccess(String.format(sql, "PREV(RPR_LAST(A.s1, 2), 3)"));

    assertTestFail(
        String.format(sql, "PREV()"),
        "prev pattern recognition function requires 1 or 2 arguments");
    assertTestFail(
        String.format(sql, "PREV(A.s1, 'str')"),
        "prev pattern recognition navigation function requires a number as the second argument");
    assertTestFail(
        String.format(sql, "PREV(A.s1, -5)"),
        "prev pattern recognition navigation function requires a non-negative number as the second argument (actual: -5)");
    assertTestFail(
        String.format(sql, "PREV(A.s1, 3000000000)"),
        "The second argument of prev pattern recognition navigation function must not exceed 2147483647 (actual: 3000000000)");
    assertTestFail(
        String.format(sql, "RPR_LAST(NEXT(A.s1, 2))"),
        "Cannot nest next pattern navigation function inside rpr_last pattern navigation function");
    assertTestFail(
        String.format(sql, "PREV(NEXT(A.s1, 2))"),
        "Cannot nest next pattern navigation function inside prev pattern navigation function");
  }

  @Test
  public void testClassifierFunction() {
    String sql =
        "SELECT * "
            + "FROM table1 "
            + "MATCH_RECOGNIZE ( "
            + "  ORDER BY time "
            + "  MEASURES "
            + "    %s AS col1 "
            + "  PATTERN (A B+) "
            + "  SUBSET U = (A, B) "
            + "  DEFINE "
            + "    B AS B.s2 > 5 "
            + ") AS m";

    assertTestSuccess(String.format(sql, "CLASSIFIER()"));
    assertTestSuccess(String.format(sql, "CLASSIFIER(A)"));
    assertTestSuccess(String.format(sql, "CLASSIFIER(U)"));

    assertTestFail(
        String.format(sql, "CLASSIFIER(A, B)"),
        "CLASSIFIER pattern recognition function takes no arguments or 1 argument");
    assertTestFail(
        String.format(sql, "CLASSIFIER(A.s1)"),
        "CLASSIFIER function argument should be primary pattern variable or subset name. Actual: DereferenceExpression");
    assertTestFail(
        String.format(sql, "CLASSIFIER(C)"), "C is not a primary pattern variable or subset name");
  }

  @Test
  public void testMatchNumberFunction() {
    String sql =
        "SELECT * "
            + "FROM table1 "
            + "MATCH_RECOGNIZE ( "
            + "  ORDER BY time "
            + "  MEASURES "
            + "    %s AS col1 "
            + "  PATTERN (A B+) "
            + "  SUBSET U = (A, B) "
            + "  DEFINE "
            + "    B AS B.s2 > 5 "
            + ") AS m";

    assertTestSuccess(String.format(sql, "MATCH_NUMBER()"));

    assertTestFail(
        String.format(sql, "MATCH_NUMBER(A)"),
        "MATCH_NUMBER pattern recognition function takes no arguments");
  }

  private void assertTestFail(String sql, String errMsg) {
    try {
      analyzeSQL(sql, TEST_MATADATA, QUERY_CONTEXT);
      fail("No exception!");
    } catch (Exception e) {
      Assert.assertTrue(e.getMessage(), e.getMessage().contains(errMsg));
    }
  }

  private void assertTestSuccess(String sql) {
    try {
      analyzeSQL(sql, TEST_MATADATA, QUERY_CONTEXT);
    } catch (Exception e) {
      fail("Unexpected exception: " + e.getMessage());
    }
  }

  public static void analyzeSQL(String sql, Metadata metadata, final MPPQueryContext context) {
    SqlParser sqlParser = new SqlParser();
    Statement statement = sqlParser.createStatement(sql, ZoneId.systemDefault(), null);
    SessionInfo session =
        new SessionInfo(
            0, "test", ZoneId.systemDefault(), "testdb", IClientSession.SqlDialect.TABLE);
    analyzeStatement(statement, metadata, context, sqlParser, session);
  }

  public static void analyzeStatement(
      final Statement statement,
      final Metadata metadata,
      final MPPQueryContext context,
      final SqlParser sqlParser,
      final SessionInfo session) {
    final StatementAnalyzerFactory statementAnalyzerFactory =
        new StatementAnalyzerFactory(metadata, sqlParser, nopAccessControl);

    Analyzer analyzer =
        new Analyzer(
            context,
            session,
            statementAnalyzerFactory,
            Collections.emptyList(),
            Collections.emptyMap(),
            new StatementRewriteFactory().getStatementRewrite(),
            NOOP);
    analyzer.analyze(statement);
  }
}
