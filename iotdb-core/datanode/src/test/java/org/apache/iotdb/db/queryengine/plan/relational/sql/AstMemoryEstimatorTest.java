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

package org.apache.iotdb.db.queryengine.plan.relational.sql;

import org.apache.iotdb.db.protocol.session.IClientSession;
import org.apache.iotdb.db.protocol.session.InternalClientSession;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Statement;
import org.apache.iotdb.db.queryengine.plan.relational.sql.parser.SqlParser;

import org.junit.Before;
import org.junit.Test;

import java.time.ZoneId;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class AstMemoryEstimatorTest {

  private SqlParser sqlParser;
  private IClientSession clientSession;

  @Before
  public void setUp() {
    sqlParser = new SqlParser();
    clientSession = new InternalClientSession("testClient");
    clientSession.setDatabaseName("testdb");
  }

  private Statement parseSQL(String sql) {
    return sqlParser.createStatement(sql, ZoneId.systemDefault(), clientSession);
  }

  private long estimateMemorySize(Statement statement) {
    return statement == null ? 0L : statement.ramBytesUsed();
  }

  // ==================== Basic Tests ====================

  @Test
  public void testNullStatement() {
    assertEquals(0L, estimateMemorySize(null));
  }

  // ==================== Literal Types ====================

  @Test
  public void testStringLiteral() {
    Statement statement = parseSQL("SELECT 'hello world' FROM table1");
    long memorySize = estimateMemorySize(statement);
    assertTrue("Memory size should be positive", memorySize > 0);
  }

  @Test
  public void testLongLiteral() {
    Statement statement = parseSQL("SELECT 123456789 FROM table1");
    long memorySize = estimateMemorySize(statement);
    assertTrue("Memory size should be positive", memorySize > 0);
  }

  @Test
  public void testDoubleLiteral() {
    Statement statement = parseSQL("SELECT 3.14159 FROM table1");
    long memorySize = estimateMemorySize(statement);
    assertTrue("Memory size should be positive", memorySize > 0);
  }

  @Test
  public void testBooleanLiteral() {
    Statement statement = parseSQL("SELECT true, false FROM table1");
    long memorySize = estimateMemorySize(statement);
    assertTrue("Memory size should be positive", memorySize > 0);
  }

  @Test
  public void testNullLiteral() {
    Statement statement = parseSQL("SELECT NULL FROM table1");
    long memorySize = estimateMemorySize(statement);
    assertTrue("Memory size should be positive", memorySize > 0);
  }

  // ==================== Identifiers ====================

  @Test
  public void testIdentifier() {
    Statement statement = parseSQL("SELECT column_name FROM table1");
    long memorySize = estimateMemorySize(statement);
    assertTrue("Memory size should be positive", memorySize > 0);
  }

  @Test
  public void testQuotedIdentifier() {
    Statement statement = parseSQL("SELECT \"my column\" FROM \"my table\"");
    long memorySize = estimateMemorySize(statement);
    assertTrue("Memory size should be positive", memorySize > 0);
  }

  // ==================== Function Calls ====================

  @Test
  public void testAggregateFunctions() {
    Statement statement = parseSQL("SELECT count(*), sum(a), avg(b), max(c), min(d) FROM table1");
    long memorySize = estimateMemorySize(statement);
    assertTrue("Memory size should be positive", memorySize > 0);
  }

  @Test
  public void testFunctionWithDistinct() {
    Statement statement = parseSQL("SELECT count(DISTINCT a) FROM table1");
    long memorySize = estimateMemorySize(statement);
    assertTrue("Memory size should be positive", memorySize > 0);
  }

  // ==================== Comparison and Logical Expressions ====================

  @Test
  public void testComparisonOperators() {
    Statement statement =
        parseSQL("SELECT * FROM table1 WHERE a > 10 AND b < 20 AND c >= 5 AND d <= 15");
    long memorySize = estimateMemorySize(statement);
    assertTrue("Memory size should be positive", memorySize > 0);
  }

  @Test
  public void testLogicalOperators() {
    Statement statement = parseSQL("SELECT * FROM table1 WHERE a > 10 AND b < 20 OR c > 5");
    long memorySize = estimateMemorySize(statement);
    assertTrue("Memory size should be positive", memorySize > 0);
  }

  @Test
  public void testNotExpression() {
    Statement statement = parseSQL("SELECT * FROM table1 WHERE NOT (a > 10)");
    long memorySize = estimateMemorySize(statement);
    assertTrue("Memory size should be positive", memorySize > 0);
  }

  // ==================== Predicates ====================

  @Test
  public void testBetweenPredicate() {
    Statement statement = parseSQL("SELECT * FROM table1 WHERE a BETWEEN 10 AND 20");
    long memorySize = estimateMemorySize(statement);
    assertTrue("Memory size should be positive", memorySize > 0);
  }

  @Test
  public void testNotBetweenPredicate() {
    Statement statement = parseSQL("SELECT * FROM table1 WHERE a NOT BETWEEN 10 AND 20");
    long memorySize = estimateMemorySize(statement);
    assertTrue("Memory size should be positive", memorySize > 0);
  }

  @Test
  public void testInPredicate() {
    Statement statement = parseSQL("SELECT * FROM table1 WHERE a IN (1, 2, 3, 4, 5)");
    long memorySize = estimateMemorySize(statement);
    assertTrue("Memory size should be positive", memorySize > 0);
  }

  @Test
  public void testNotInPredicate() {
    Statement statement = parseSQL("SELECT * FROM table1 WHERE a NOT IN (1, 2, 3)");
    long memorySize = estimateMemorySize(statement);
    assertTrue("Memory size should be positive", memorySize > 0);
  }

  @Test
  public void testLikePredicate() {
    Statement statement = parseSQL("SELECT * FROM table1 WHERE name LIKE '%test%'");
    long memorySize = estimateMemorySize(statement);
    assertTrue("Memory size should be positive", memorySize > 0);
  }

  @Test
  public void testLikeWithEscape() {
    Statement statement = parseSQL("SELECT * FROM table1 WHERE name LIKE '%\\%%' ESCAPE '\\'");
    long memorySize = estimateMemorySize(statement);
    assertTrue("Memory size should be positive", memorySize > 0);
  }

  @Test
  public void testIsNullPredicate() {
    Statement statement = parseSQL("SELECT * FROM table1 WHERE a IS NULL");
    long memorySize = estimateMemorySize(statement);
    assertTrue("Memory size should be positive", memorySize > 0);
  }

  @Test
  public void testIsNotNullPredicate() {
    Statement statement = parseSQL("SELECT * FROM table1 WHERE a IS NOT NULL");
    long memorySize = estimateMemorySize(statement);
    assertTrue("Memory size should be positive", memorySize > 0);
  }

  // ==================== Case Expressions ====================

  @Test
  public void testSimpleCaseExpression() {
    Statement statement =
        parseSQL("SELECT CASE a WHEN 1 THEN 'one' WHEN 2 THEN 'two' ELSE 'other' END FROM table1");
    long memorySize = estimateMemorySize(statement);
    assertTrue("Memory size should be positive", memorySize > 0);
  }

  @Test
  public void testSimpleCaseWithoutElse() {
    Statement statement =
        parseSQL("SELECT CASE a WHEN 1 THEN 'one' WHEN 2 THEN 'two' END FROM table1");
    long memorySize = estimateMemorySize(statement);
    assertTrue("Memory size should be positive", memorySize > 0);
  }

  @Test
  public void testSearchedCaseExpression() {
    Statement statement =
        parseSQL(
            "SELECT CASE WHEN a > 10 THEN 'big' WHEN a > 5 THEN 'medium' ELSE 'small' END FROM table1");
    long memorySize = estimateMemorySize(statement);
    assertTrue("Memory size should be positive", memorySize > 0);
  }

  // ==================== Other Expressions ====================

  @Test
  public void testCoalesceExpression() {
    Statement statement = parseSQL("SELECT coalesce(a, b, c) FROM table1");
    long memorySize = estimateMemorySize(statement);
    assertTrue("Memory size should be positive", memorySize > 0);
  }

  @Test
  public void testDereferenceExpression() {
    Statement statement = parseSQL("SELECT t1.column_name FROM table1 AS t1");
    long memorySize = estimateMemorySize(statement);
    assertTrue("Memory size should be positive", memorySize > 0);
  }

  @Test
  public void testTrimExpression() {
    Statement statement = parseSQL("SELECT trim(name) FROM table1");
    long memorySize = estimateMemorySize(statement);
    assertTrue("Memory size should be positive", memorySize > 0);
  }

  @Test
  public void testTrimWithSpecification() {
    Statement statement = parseSQL("SELECT trim(BOTH ' ' FROM name) FROM table1");
    long memorySize = estimateMemorySize(statement);
    assertTrue("Memory size should be positive", memorySize > 0);
  }

  @Test
  public void testIfExpression() {
    Statement statement = parseSQL("SELECT if(a > 10, 'big', 'small') FROM table1");
    long memorySize = estimateMemorySize(statement);
    assertTrue("Memory size should be positive", memorySize > 0);
  }

  @Test
  public void testNullIfExpression() {
    Statement statement = parseSQL("SELECT nullif(a, 0) FROM table1");
    long memorySize = estimateMemorySize(statement);
    assertTrue("Memory size should be positive", memorySize > 0);
  }

  @Test
  public void testExtractExpression() {
    Statement statement = parseSQL("SELECT EXTRACT(YEAR FROM time) FROM table1");
    long memorySize = estimateMemorySize(statement);
    assertTrue("Memory size should be positive", memorySize > 0);
  }

  @Test
  public void testSubqueryExpression() {
    Statement statement = parseSQL("SELECT * FROM (SELECT a, b FROM table1 WHERE a > 10) AS sub");
    long memorySize = estimateMemorySize(statement);
    assertTrue("Memory size should be positive", memorySize > 0);
  }

  @Test
  public void testInSubquery() {
    Statement statement = parseSQL("SELECT * FROM table1 WHERE a IN (SELECT b FROM table2)");
    long memorySize = estimateMemorySize(statement);
    assertTrue("Memory size should be positive", memorySize > 0);
  }

  @Test
  public void testExistsPredicate() {
    Statement statement = parseSQL("SELECT * FROM table1 WHERE EXISTS (SELECT 1 FROM table2)");
    long memorySize = estimateMemorySize(statement);
    assertTrue("Memory size should be positive", memorySize > 0);
  }

  @Test
  public void testCastExpression() {
    Statement statement = parseSQL("SELECT cast(a AS STRING) FROM table1");
    long memorySize = estimateMemorySize(statement);
    assertTrue("Memory size should be positive", memorySize > 0);
  }

  @Test
  public void testTryCastExpression() {
    Statement statement = parseSQL("SELECT try_cast(a AS INT64) FROM table1");
    long memorySize = estimateMemorySize(statement);
    assertTrue("Memory size should be positive", memorySize > 0);
  }

  // ==================== Special Functions ====================

  @Test
  public void testCurrentDatabase() {
    Statement statement = parseSQL("SELECT CURRENT_DATABASE FROM table1");
    long memorySize = estimateMemorySize(statement);
    assertTrue("Memory size should be positive", memorySize > 0);
  }

  @Test
  public void testCurrentUser() {
    Statement statement = parseSQL("SELECT CURRENT_USER FROM table1");
    long memorySize = estimateMemorySize(statement);
    assertTrue("Memory size should be positive", memorySize > 0);
  }

  @Test
  public void testNowFunction() {
    Statement statement = parseSQL("SELECT now() FROM table1");
    long memorySize = estimateMemorySize(statement);
    assertTrue("Memory size should be positive", memorySize > 0);
  }

  // ==================== Row and Values ====================

  @Test
  public void testRowConstructor() {
    Statement statement = parseSQL("SELECT ROW(1, 'a', 100) FROM table1");
    long memorySize = estimateMemorySize(statement);
    assertTrue("Memory size should be positive", memorySize > 0);
  }

  @Test
  public void testValuesClause() {
    Statement statement = parseSQL("VALUES (1, 'a'), (2, 'b'), (3, 'c')");
    long memorySize = estimateMemorySize(statement);
    assertTrue("Memory size should be positive", memorySize > 0);
  }

  // ==================== Query Structure ====================

  @Test
  public void testSimpleSelectQuery() {
    Statement statement = parseSQL("SELECT * FROM table1");
    long memorySize = estimateMemorySize(statement);
    assertTrue("Memory size should be positive", memorySize > 0);
  }

  @Test
  public void testSelectWithColumns() {
    Statement statement = parseSQL("SELECT a, b, c FROM table1");
    long memorySize = estimateMemorySize(statement);
    assertTrue("Memory size should be positive", memorySize > 0);
  }

  @Test
  public void testSelectWithAlias() {
    Statement statement = parseSQL("SELECT a AS col_a, b AS col_b FROM table1 AS t1");
    long memorySize = estimateMemorySize(statement);
    assertTrue("Memory size should be positive", memorySize > 0);
  }

  @Test
  public void testSelectWithDistinct() {
    Statement statement = parseSQL("SELECT DISTINCT a, b FROM table1");
    long memorySize = estimateMemorySize(statement);
    assertTrue("Memory size should be positive", memorySize > 0);
  }

  @Test
  public void testSelectAll() {
    Statement statement = parseSQL("SELECT ALL a, b FROM table1");
    long memorySize = estimateMemorySize(statement);
    assertTrue("Memory size should be positive", memorySize > 0);
  }

  @Test
  public void testTableSubquery() {
    Statement statement = parseSQL("SELECT * FROM (SELECT a FROM table1) AS sub");
    long memorySize = estimateMemorySize(statement);
    assertTrue("Memory size should be positive", memorySize > 0);
  }

  // ==================== WITH Query ====================

  @Test
  public void testWithQuery() {
    Statement statement = parseSQL("WITH cte AS (SELECT a FROM table1) SELECT * FROM cte");
    long memorySize = estimateMemorySize(statement);
    assertTrue("Memory size should be positive", memorySize > 0);
  }

  @Test
  public void testWithQueryWithColumnNames() {
    Statement statement =
        parseSQL("WITH cte (col1, col2) AS (SELECT a, b FROM table1) SELECT * FROM cte");
    long memorySize = estimateMemorySize(statement);
    assertTrue("Memory size should be positive", memorySize > 0);
  }

  @Test
  public void testMultipleWithQueries() {
    Statement statement =
        parseSQL(
            "WITH cte1 AS (SELECT a FROM table1), cte2 AS (SELECT b FROM table2) "
                + "SELECT * FROM cte1, cte2");
    long memorySize = estimateMemorySize(statement);
    assertTrue("Memory size should be positive", memorySize > 0);
  }

  // ==================== ORDER BY, LIMIT, OFFSET ====================

  @Test
  public void testOrderBy() {
    Statement statement = parseSQL("SELECT * FROM table1 ORDER BY a");
    long memorySize = estimateMemorySize(statement);
    assertTrue("Memory size should be positive", memorySize > 0);
  }

  @Test
  public void testLimitOffset() {
    Statement statement = parseSQL("SELECT * FROM table1 LIMIT 10 OFFSET 5");
    long memorySize = estimateMemorySize(statement);
    assertTrue("Memory size should be positive", memorySize > 0);
  }

  @Test
  public void testLimitOnly() {
    Statement statement = parseSQL("SELECT * FROM table1 LIMIT 100");
    long memorySize = estimateMemorySize(statement);
    assertTrue("Memory size should be positive", memorySize > 0);
  }

  @Test
  public void testOffsetOnly() {
    Statement statement = parseSQL("SELECT * FROM table1 OFFSET 10");
    long memorySize = estimateMemorySize(statement);
    assertTrue("Memory size should be positive", memorySize > 0);
  }

  // ==================== GROUP BY and HAVING ====================

  @Test
  public void testGroupBy() {
    Statement statement = parseSQL("SELECT a, count(*) FROM table1 GROUP BY a");
    long memorySize = estimateMemorySize(statement);
    assertTrue("Memory size should be positive", memorySize > 0);
  }

  @Test
  public void testGroupByMultipleColumns() {
    Statement statement = parseSQL("SELECT a, b, count(*) FROM table1 GROUP BY a, b");
    long memorySize = estimateMemorySize(statement);
    assertTrue("Memory size should be positive", memorySize > 0);
  }

  @Test
  public void testGroupingSets() {
    Statement statement =
        parseSQL("SELECT a, b, count(*) FROM table1 GROUP BY GROUPING SETS ((a), (b), (a, b), ())");
    long memorySize = estimateMemorySize(statement);
    assertTrue("Memory size should be positive", memorySize > 0);
  }

  @Test
  public void testRollup() {
    Statement statement = parseSQL("SELECT a, b, count(*) FROM table1 GROUP BY ROLLUP (a, b)");
    long memorySize = estimateMemorySize(statement);
    assertTrue("Memory size should be positive", memorySize > 0);
  }

  @Test
  public void testCube() {
    Statement statement = parseSQL("SELECT a, b, count(*) FROM table1 GROUP BY CUBE (a, b)");
    long memorySize = estimateMemorySize(statement);
    assertTrue("Memory size should be positive", memorySize > 0);
  }

  @Test
  public void testHaving() {
    Statement statement = parseSQL("SELECT a, count(*) FROM table1 GROUP BY a HAVING count(*) > 5");
    long memorySize = estimateMemorySize(statement);
    assertTrue("Memory size should be positive", memorySize > 0);
  }

  // ==================== FILL Clause ====================

  @Test
  public void testFillLinear() {
    Statement statement = parseSQL("SELECT * FROM table1 FILL METHOD LINEAR");
    long memorySize = estimateMemorySize(statement);
    assertTrue("Memory size should be positive", memorySize > 0);
  }

  @Test
  public void testFillPrevious() {
    Statement statement = parseSQL("SELECT * FROM table1 FILL METHOD PREVIOUS");
    long memorySize = estimateMemorySize(statement);
    assertTrue("Memory size should be positive", memorySize > 0);
  }

  @Test
  public void testFillConstant() {
    Statement statement = parseSQL("SELECT * FROM table1 FILL METHOD CONSTANT 0");
    long memorySize = estimateMemorySize(statement);
    assertTrue("Memory size should be positive", memorySize > 0);
  }

  // ==================== JOIN ====================

  @Test
  public void testCrossJoin() {
    Statement statement = parseSQL("SELECT * FROM table1 CROSS JOIN table2");
    long memorySize = estimateMemorySize(statement);
    assertTrue("Memory size should be positive", memorySize > 0);
  }

  @Test
  public void testJoinUsing() {
    Statement statement = parseSQL("SELECT * FROM table1 JOIN table2 USING (id)");
    long memorySize = estimateMemorySize(statement);
    assertTrue("Memory size should be positive", memorySize > 0);
  }

  @Test
  public void testJoinOn() {
    Statement statement = parseSQL("SELECT * FROM table1 JOIN table2 ON table1.id > table2.id");
    long memorySize = estimateMemorySize(statement);
    assertTrue("Memory size should be positive", memorySize > 0);
  }

  @Test
  public void testLeftJoin() {
    Statement statement = parseSQL("SELECT * FROM table1 LEFT JOIN table2 USING (id)");
    long memorySize = estimateMemorySize(statement);
    assertTrue("Memory size should be positive", memorySize > 0);
  }

  @Test
  public void testRightJoin() {
    Statement statement = parseSQL("SELECT * FROM table1 RIGHT JOIN table2 USING (id)");
    long memorySize = estimateMemorySize(statement);
    assertTrue("Memory size should be positive", memorySize > 0);
  }

  @Test
  public void testFullJoin() {
    Statement statement = parseSQL("SELECT * FROM table1 FULL JOIN table2 USING (id)");
    long memorySize = estimateMemorySize(statement);
    assertTrue("Memory size should be positive", memorySize > 0);
  }

  // ==================== Window Functions ====================

  @Test
  public void testWindowFunctionWithOver() {
    Statement statement =
        parseSQL("SELECT a, sum(b) OVER (PARTITION BY a ORDER BY time) FROM table1");
    long memorySize = estimateMemorySize(statement);
    assertTrue("Memory size should be positive", memorySize > 0);
  }

  @Test
  public void testNamedWindow() {
    Statement statement =
        parseSQL("SELECT a, sum(b) OVER w FROM table1 WINDOW w AS (PARTITION BY a ORDER BY time)");
    long memorySize = estimateMemorySize(statement);
    assertTrue("Memory size should be positive", memorySize > 0);
  }

  @Test
  public void testMultipleNamedWindows() {
    Statement statement =
        parseSQL(
            "SELECT a, sum(b) OVER w1, avg(c) OVER w2 FROM table1 "
                + "WINDOW w1 AS (PARTITION BY a), w2 AS (PARTITION BY a ORDER BY time)");
    long memorySize = estimateMemorySize(statement);
    assertTrue("Memory size should be positive", memorySize > 0);
  }

  // ==================== MATCH_RECOGNIZE (Pattern Recognition) ====================

  @Test
  public void testMatchRecognize() {
    Statement statement =
        parseSQL(
            "SELECT * FROM table1 "
                + "MATCH_RECOGNIZE ( "
                + "  PARTITION BY a "
                + "  ORDER BY time "
                + "  MEASURES A.b AS ab "
                + "  PATTERN (A B+) "
                + "  DEFINE A AS A.c > 10, B AS B.c < 5 "
                + ")");
    long memorySize = estimateMemorySize(statement);
    assertTrue("Memory size should be positive", memorySize > 0);
  }

  @Test
  public void testMatchRecognizeWithQuantifiers() {
    Statement statement =
        parseSQL(
            "SELECT * FROM table1 "
                + "MATCH_RECOGNIZE ( "
                + "  PATTERN (A* B+ C? D{2,5}) "
                + "  DEFINE A AS A.x > 0, B AS B.x > 0, C AS C.x > 0, D AS D.x > 0 "
                + ")");
    long memorySize = estimateMemorySize(statement);
    assertTrue("Memory size should be positive", memorySize > 0);
  }

  @Test
  public void testMatchRecognizeWithAlternation() {
    Statement statement =
        parseSQL(
            "SELECT * FROM table1 "
                + "MATCH_RECOGNIZE ( "
                + "  PATTERN ((A | B) C) "
                + "  DEFINE A AS A.x > 0, B AS B.x < 0, C AS C.x > 0 "
                + ")");
    long memorySize = estimateMemorySize(statement);
    assertTrue("Memory size should be positive", memorySize > 0);
  }

  @Test
  public void testMatchRecognizeWithSubset() {
    Statement statement =
        parseSQL(
            "SELECT * FROM table1 "
                + "MATCH_RECOGNIZE ( "
                + "  PATTERN (A B C) "
                + "  SUBSET AB = (A, B) "
                + "  DEFINE A AS A.x > 0, B AS B.x > 0, C AS C.x > 0 "
                + ")");
    long memorySize = estimateMemorySize(statement);
    assertTrue("Memory size should be positive", memorySize > 0);
  }

  @Test
  public void testMatchRecognizeWithRowsPerMatch() {
    Statement statement =
        parseSQL(
            "SELECT * FROM table1 "
                + "MATCH_RECOGNIZE ( "
                + "  ONE ROW PER MATCH "
                + "  PATTERN (A B) "
                + "  DEFINE A AS A.x > 0, B AS B.x > 0 "
                + ")");
    long memorySize = estimateMemorySize(statement);
    assertTrue("Memory size should be positive", memorySize > 0);
  }

  @Test
  public void testMatchRecognizeWithSkipTo() {
    Statement statement =
        parseSQL(
            "SELECT * FROM table1 "
                + "MATCH_RECOGNIZE ( "
                + "  AFTER MATCH SKIP TO NEXT ROW "
                + "  PATTERN (A B) "
                + "  DEFINE A AS A.x > 0, B AS B.x > 0 "
                + ")");
    long memorySize = estimateMemorySize(statement);
    assertTrue("Memory size should be positive", memorySize > 0);
  }

  // ==================== Statements ====================

  @Test
  public void testExplain() {
    Statement statement = parseSQL("EXPLAIN SELECT * FROM table1");
    long memorySize = estimateMemorySize(statement);
    assertTrue("Memory size should be positive", memorySize > 0);
  }

  @Test
  public void testExplainAnalyze() {
    Statement statement = parseSQL("EXPLAIN ANALYZE SELECT * FROM table1");
    long memorySize = estimateMemorySize(statement);
    assertTrue("Memory size should be positive", memorySize > 0);
  }

  @Test
  public void testExplainAnalyzeVerbose() {
    Statement statement = parseSQL("EXPLAIN ANALYZE VERBOSE SELECT * FROM table1");
    long memorySize = estimateMemorySize(statement);
    assertTrue("Memory size should be positive", memorySize > 0);
  }

  @Test
  public void testInsert() {
    Statement statement = parseSQL("INSERT INTO table1 (a, b) SELECT a, b FROM table2");
    long memorySize = estimateMemorySize(statement);
    assertTrue("Memory size should be positive", memorySize > 0);
  }

  @Test
  public void testDelete() {
    Statement statement = parseSQL("DELETE FROM table1 WHERE a > 10");
    long memorySize = estimateMemorySize(statement);
    assertTrue("Memory size should be positive", memorySize > 0);
  }

  @Test
  public void testDeleteWithoutWhere() {
    Statement statement = parseSQL("DELETE FROM table1");
    long memorySize = estimateMemorySize(statement);
    assertTrue("Memory size should be positive", memorySize > 0);
  }

  @Test
  public void testUpdate() {
    Statement statement = parseSQL("UPDATE table1 SET a = 100 WHERE b > 50");
    long memorySize = estimateMemorySize(statement);
    assertTrue("Memory size should be positive", memorySize > 0);
  }

  @Test
  public void testUpdateMultipleColumns() {
    Statement statement = parseSQL("UPDATE table1 SET a = 100, b = 'new' WHERE c > 50");
    long memorySize = estimateMemorySize(statement);
    assertTrue("Memory size should be positive", memorySize > 0);
  }

  // ==================== Prepared Statements ====================

  @Test
  public void testPrepare() {
    Statement statement = parseSQL("PREPARE myquery FROM SELECT * FROM table1 WHERE a > 10");
    long memorySize = estimateMemorySize(statement);
    assertTrue("Memory size should be positive", memorySize > 0);
  }

  @Test
  public void testExecute() {
    Statement statement = parseSQL("EXECUTE myquery");
    long memorySize = estimateMemorySize(statement);
    assertTrue("Memory size should be positive", memorySize > 0);
  }

  @Test
  public void testExecuteWithParams() {
    Statement statement = parseSQL("EXECUTE myquery USING 10, 'test'");
    long memorySize = estimateMemorySize(statement);
    assertTrue("Memory size should be positive", memorySize > 0);
  }

  @Test
  public void testExecuteImmediate() {
    Statement statement = parseSQL("EXECUTE IMMEDIATE 'SELECT * FROM table1'");
    long memorySize = estimateMemorySize(statement);
    assertTrue("Memory size should be positive", memorySize > 0);
  }

  @Test
  public void testExecuteImmediateWithParams() {
    Statement statement = parseSQL("EXECUTE IMMEDIATE 'SELECT * FROM table1' USING 10, 'test'");
    long memorySize = estimateMemorySize(statement);
    assertTrue("Memory size should be positive", memorySize > 0);
  }

  @Test
  public void testDeallocate() {
    Statement statement = parseSQL("DEALLOCATE PREPARE myquery");
    long memorySize = estimateMemorySize(statement);
    assertTrue("Memory size should be positive", memorySize > 0);
  }

  // ==================== Complex Query Tests ====================

  @Test
  public void testMemorySizeIncreasesWithComplexity() {
    Statement simple = parseSQL("SELECT * FROM table1");
    Statement medium = parseSQL("SELECT a, b FROM table1 WHERE a > 10");
    Statement complex =
        parseSQL(
            "SELECT a, b, count(*) FROM table1 WHERE a > 10 AND b < 20 "
                + "GROUP BY a, b HAVING count(*) > 5 LIMIT 100 OFFSET 10");

    long simpleSize = estimateMemorySize(simple);
    long mediumSize = estimateMemorySize(medium);
    long complexSize = estimateMemorySize(complex);

    assertTrue("Simple query should have smaller memory than medium", simpleSize < mediumSize);
    assertTrue("Medium query should have smaller memory than complex", mediumSize < complexSize);
  }

  @Test
  public void testDeepNestedQuery() {
    String sql =
        "SELECT * FROM ("
            + "SELECT * FROM ("
            + "SELECT * FROM ("
            + "SELECT * FROM table1"
            + ") AS l3"
            + ") AS l2"
            + ") AS l1";
    Statement statement = parseSQL(sql);
    long memorySize = estimateMemorySize(statement);
    assertTrue("Memory size should be positive for nested queries", memorySize > 0);
  }

  @Test
  public void testComplexWhereClause() {
    Statement statement =
        parseSQL(
            "SELECT * FROM table1 WHERE "
                + "(a > 10 AND b < 20) OR (c BETWEEN 5 AND 15) "
                + "AND d IN (1, 2, 3) AND e LIKE '%test%' "
                + "AND f IS NOT NULL");
    long memorySize = estimateMemorySize(statement);
    assertTrue("Memory size should be positive", memorySize > 0);
  }

  @Test
  public void testComplexSelectWithManyFunctions() {
    Statement statement =
        parseSQL(
            "SELECT "
                + "count(*), sum(a), avg(b), max(c), min(d), "
                + "coalesce(e, f, g), nullif(h, 0), "
                + "CASE WHEN i > 10 THEN 'big' ELSE 'small' END, "
                + "cast(j AS STRING), trim(k) "
                + "FROM table1 "
                + "GROUP BY l");
    long memorySize = estimateMemorySize(statement);
    assertTrue("Memory size should be positive", memorySize > 0);
  }

  @Test
  public void testComplexJoinQuery() {
    Statement statement =
        parseSQL(
            "SELECT t1.a, t2.b, t3.c "
                + "FROM table1 AS t1 "
                + "LEFT JOIN table2 AS t2 USING (id) "
                + "CROSS JOIN table3 AS t3 "
                + "WHERE t1.x > 10 AND t2.y < 20");
    long memorySize = estimateMemorySize(statement);
    assertTrue("Memory size should be positive", memorySize > 0);
  }

  @Test
  public void testVeryLongInList() {
    StringBuilder sb = new StringBuilder("SELECT * FROM table1 WHERE a IN (");
    for (int i = 0; i < 100; i++) {
      if (i > 0) sb.append(", ");
      sb.append(i);
    }
    sb.append(")");
    Statement statement = parseSQL(sb.toString());
    long memorySize = estimateMemorySize(statement);
    assertTrue("Memory size should be positive", memorySize > 0);
  }

  @Test
  public void testLongStringLiteral() {
    StringBuilder sb = new StringBuilder("SELECT '");
    for (int i = 0; i < 1000; i++) {
      sb.append("x");
    }
    sb.append("' FROM table1");
    Statement statement = parseSQL(sb.toString());
    long memorySize = estimateMemorySize(statement);
    assertTrue("Memory size should be positive", memorySize > 0);
  }
}
