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

import org.apache.iotdb.db.queryengine.common.SessionInfo;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.Metadata;
import org.apache.iotdb.db.queryengine.plan.relational.security.AccessControl;
import org.apache.iotdb.db.relational.sql.parser.SqlParser;
import org.apache.iotdb.db.relational.sql.tree.Statement;

import org.junit.Test;
import org.mockito.Mockito;

import java.time.ZoneId;
import java.util.Collections;

import static org.apache.iotdb.db.queryengine.execution.warnings.WarningCollector.NOOP;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

public class AnalyzerTest {

  private final SqlParser sqlParser = new SqlParser();

  private final NopAccessControl nopAccessControl = new NopAccessControl();

  @Test
  public void testRawDataQuery() {
    String sql = "select s1, status, s1 + 1 as t from table1 where time > 100 and s2 > 10;";
    Metadata metadata = Mockito.mock(Metadata.class);
    Mockito.when(metadata.tableExists(Mockito.any())).thenReturn(true);

    Analysis actualAnalysis = analyzeSQL(sql, metadata);
    assertNotNull(actualAnalysis);
    System.out.println(actualAnalysis.getTypes());
  }

  private Analysis analyzeSQL(String sql, Metadata metadata) {
    try {
      Statement statement = sqlParser.createStatement(sql);
      SessionInfo session = new SessionInfo(0, "test", ZoneId.systemDefault().getId(), "testdb");
      StatementAnalyzerFactory statementAnalyzerFactory =
          new StatementAnalyzerFactory(metadata, sqlParser, nopAccessControl);

      Analyzer analyzer =
          new Analyzer(
              session,
              statementAnalyzerFactory,
              Collections.emptyList(),
              Collections.emptyMap(),
              NOOP);
      return analyzer.analyze(statement);
    } catch (Exception e) {
      e.printStackTrace();
      fail(sql + ", " + e.getMessage());
    }
    fail();
    return null;
  }

  private static class NopAccessControl implements AccessControl {}
}
