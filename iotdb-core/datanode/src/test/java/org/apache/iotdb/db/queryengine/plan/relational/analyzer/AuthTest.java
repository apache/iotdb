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

import org.apache.iotdb.commons.exception.auth.AccessDeniedException;
import org.apache.iotdb.db.protocol.session.IClientSession;
import org.apache.iotdb.db.queryengine.common.MPPQueryContext;
import org.apache.iotdb.db.queryengine.common.SessionInfo;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.QualifiedObjectName;
import org.apache.iotdb.db.queryengine.plan.relational.security.AccessControlImpl;
import org.apache.iotdb.db.queryengine.plan.relational.security.ITableAuthChecker;
import org.apache.iotdb.db.queryengine.plan.relational.security.TableModelPrivilege;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Statement;
import org.apache.iotdb.db.queryengine.plan.relational.sql.parser.SqlParser;

import org.junit.Test;
import org.mockito.Mockito;

import java.time.ZoneId;
import java.util.Collections;

import static org.apache.iotdb.db.queryengine.execution.warnings.WarningCollector.NOOP;
import static org.apache.iotdb.db.queryengine.plan.relational.analyzer.TestMatadata.DB1;
import static org.apache.iotdb.db.queryengine.plan.relational.analyzer.TestMatadata.TABLE1;
import static org.apache.iotdb.db.queryengine.plan.relational.analyzer.TestUtils.QUERY_ID;
import static org.apache.iotdb.db.queryengine.plan.relational.analyzer.TestUtils.TEST_MATADATA;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;

public class AuthTest {

  private final SqlParser sqlParser = new SqlParser();

  private final ZoneId zoneId = ZoneId.systemDefault();

  private final QualifiedObjectName TESTDB_TABLE1 = new QualifiedObjectName(DB1, TABLE1);

  @Test
  public void testQueryRelatedAuth() {
    String sql = String.format("SELECT * FROM %s.%s", DB1, TABLE1);
    ITableAuthChecker authChecker = Mockito.mock(ITableAuthChecker.class);
    // user `root` always succeed
    Mockito.doNothing().when(authChecker).checkTablePrivilege(eq("root"), any(), any());
    // user `user1` don't hava testdb.table1's SELECT privilege
    String errorMsg = "user1 doesn't have SELECT privilege on TABLE testdb.table1";
    Mockito.doThrow(new AccessDeniedException(errorMsg))
        .when(authChecker)
        .checkTablePrivilege(eq("user1"), eq(TESTDB_TABLE1), eq(TableModelPrivilege.SELECT));
    // user `user2` hava testdb.table1's SELECT privilege
    Mockito.doNothing()
        .when(authChecker)
        .checkTablePrivilege(eq("user2"), eq(TESTDB_TABLE1), eq(TableModelPrivilege.SELECT));

    String userName = "root";
    try {
      analyzeSQL(sql, userName, authChecker);
    } catch (Exception e) {
      fail(e.getMessage());
    }

    userName = "user1";
    try {
      analyzeSQL(sql, userName, authChecker);
      fail("user1 should be denied");
    } catch (AccessDeniedException e) {
      assertEquals(AccessDeniedException.PREFIX + errorMsg, e.getMessage());
    } catch (Exception e) {
      fail("Unexpected exception : " + e.getMessage());
    }

    userName = "user2";
    try {
      analyzeSQL(sql, userName, authChecker, DB1);
    } catch (Exception e) {
      fail(e.getMessage());
    }
  }

  private void analyzeSQL(String sql, String userName, ITableAuthChecker authChecker) {
    analyzeSQL(sql, userName, authChecker, null);
  }

  private void analyzeSQL(
      String sql,
      String userName,
      ITableAuthChecker authChecker,
      String databaseNameInSessionInfo) {
    Statement statement = sqlParser.createStatement(sql, zoneId);
    SessionInfo session =
        new SessionInfo(
            0, userName, zoneId, databaseNameInSessionInfo, IClientSession.SqlDialect.TABLE);
    StatementAnalyzerFactory statementAnalyzerFactory =
        new StatementAnalyzerFactory(TEST_MATADATA, sqlParser, new AccessControlImpl(authChecker));
    MPPQueryContext context = new MPPQueryContext(sql, QUERY_ID, 0, session, null, null);
    Analyzer analyzer =
        new Analyzer(
            context,
            session,
            statementAnalyzerFactory,
            Collections.emptyList(),
            Collections.emptyMap(),
            NOOP);
    analyzer.analyze(statement);
  }
}
