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
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Explain;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.ExplainAnalyze;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.ExplainOutputFormat;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Statement;
import org.apache.iotdb.db.queryengine.plan.relational.sql.parser.SqlParser;

import org.junit.Before;
import org.junit.Test;

import java.time.ZoneId;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class ExplainFormatTest {

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

  @Test
  public void testExplainDefaultFormat() {
    Statement stmt = parseSQL("EXPLAIN SELECT * FROM table1");
    assertTrue(stmt instanceof Explain);
    assertEquals(ExplainOutputFormat.GRAPHVIZ, ((Explain) stmt).getOutputFormat());
  }

  @Test
  public void testExplainGraphvizFormat() {
    Statement stmt = parseSQL("EXPLAIN (FORMAT GRAPHVIZ) SELECT * FROM table1");
    assertTrue(stmt instanceof Explain);
    assertEquals(ExplainOutputFormat.GRAPHVIZ, ((Explain) stmt).getOutputFormat());
  }

  @Test
  public void testExplainJsonFormat() {
    Statement stmt = parseSQL("EXPLAIN (FORMAT JSON) SELECT * FROM table1");
    assertTrue(stmt instanceof Explain);
    assertEquals(ExplainOutputFormat.JSON, ((Explain) stmt).getOutputFormat());
  }

  @Test
  public void testExplainJsonFormatCaseInsensitive() {
    Statement stmt = parseSQL("EXPLAIN (FORMAT json) SELECT * FROM table1");
    assertTrue(stmt instanceof Explain);
    assertEquals(ExplainOutputFormat.JSON, ((Explain) stmt).getOutputFormat());
  }

  @Test(expected = Exception.class)
  public void testExplainInvalidFormat() {
    parseSQL("EXPLAIN (FORMAT XML) SELECT * FROM table1");
  }

  @Test
  public void testExplainAnalyzeDefaultFormat() {
    Statement stmt = parseSQL("EXPLAIN ANALYZE SELECT * FROM table1");
    assertTrue(stmt instanceof ExplainAnalyze);
    assertEquals(ExplainOutputFormat.TEXT, ((ExplainAnalyze) stmt).getOutputFormat());
  }

  @Test
  public void testExplainAnalyzeTextFormat() {
    Statement stmt = parseSQL("EXPLAIN ANALYZE (FORMAT TEXT) SELECT * FROM table1");
    assertTrue(stmt instanceof ExplainAnalyze);
    assertEquals(ExplainOutputFormat.TEXT, ((ExplainAnalyze) stmt).getOutputFormat());
  }

  @Test
  public void testExplainAnalyzeJsonFormat() {
    Statement stmt = parseSQL("EXPLAIN ANALYZE (FORMAT JSON) SELECT * FROM table1");
    assertTrue(stmt instanceof ExplainAnalyze);
    assertEquals(ExplainOutputFormat.JSON, ((ExplainAnalyze) stmt).getOutputFormat());
  }

  @Test
  public void testExplainAnalyzeVerboseJsonFormat() {
    Statement stmt = parseSQL("EXPLAIN ANALYZE VERBOSE (FORMAT JSON) SELECT * FROM table1");
    assertTrue(stmt instanceof ExplainAnalyze);
    ExplainAnalyze ea = (ExplainAnalyze) stmt;
    assertEquals(ExplainOutputFormat.JSON, ea.getOutputFormat());
    assertTrue(ea.isVerbose());
  }

  @Test(expected = Exception.class)
  public void testExplainAnalyzeInvalidFormat() {
    parseSQL("EXPLAIN ANALYZE (FORMAT GRAPHVIZ) SELECT * FROM table1");
  }

  @Test(expected = Exception.class)
  public void testExplainTextFormatInvalid() {
    // TEXT is not valid for EXPLAIN (only GRAPHVIZ and JSON)
    parseSQL("EXPLAIN (FORMAT TEXT) SELECT * FROM table1");
  }
}
