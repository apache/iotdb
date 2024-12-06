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
import org.apache.iotdb.db.queryengine.plan.execution.config.TableConfigTaskVisitor;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.QualifiedObjectName;
import org.apache.iotdb.db.queryengine.plan.relational.security.AccessControlImpl;
import org.apache.iotdb.db.queryengine.plan.relational.security.ITableAuthChecker;
import org.apache.iotdb.db.queryengine.plan.relational.security.TableModelPrivilege;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Statement;
import org.apache.iotdb.db.queryengine.plan.relational.sql.parser.SqlParser;
import org.apache.iotdb.db.queryengine.plan.relational.sql.rewrite.StatementRewrite;

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

  private final QualifiedObjectName testdbTable1 = new QualifiedObjectName(DB1, TABLE1);

  private final String userRoot = "root";

  private final String user1 = "user1";

  private final String user2 = "user2";

  @Test
  public void testQueryRelatedAuth() {
    String sql = String.format("SELECT * FROM %s.%s", DB1, TABLE1);
    ITableAuthChecker authChecker = Mockito.mock(ITableAuthChecker.class);
    // user `root` always succeed
    Mockito.doNothing().when(authChecker).checkTablePrivilege(eq(userRoot), any(), any());
    // user `user1` don't hava testdb.table1's SELECT privilege
    String errorMsg =
        String.format(
            "%s doesn't have %s privilege on TABLE %s.%s",
            user1, TableModelPrivilege.SELECT, DB1, TABLE1);
    Mockito.doThrow(new AccessDeniedException(errorMsg))
        .when(authChecker)
        .checkTablePrivilege(eq(user1), eq(testdbTable1), eq(TableModelPrivilege.SELECT));
    // user `user2` has testdb.table1's SELECT privilege
    Mockito.doNothing()
        .when(authChecker)
        .checkTablePrivilege(eq(user2), eq(testdbTable1), eq(TableModelPrivilege.SELECT));

    String userName = userRoot;
    try {
      analyzeSQL(sql, userName, authChecker);
    } catch (Exception e) {
      fail(e.getMessage());
    }

    userName = user1;
    try {
      analyzeSQL(sql, userName, authChecker);
      fail("user1 should be denied");
    } catch (AccessDeniedException e) {
      assertEquals(AccessDeniedException.PREFIX + errorMsg, e.getMessage());
    } catch (Exception e) {
      fail("Unexpected exception : " + e.getMessage());
    }

    userName = user2;
    try {
      analyzeSQL(sql, userName, authChecker, DB1);
    } catch (Exception e) {
      fail(e.getMessage());
    }
  }

  @Test
  public void testDatabaseManagementRelatedAuth() {

    // create database
    String databaseTest1 = "test1";
    String sql = String.format("CREATE DATABASE %s", databaseTest1);
    ITableAuthChecker authChecker = Mockito.mock(ITableAuthChecker.class);
    // user `root` always succeed
    Mockito.doNothing().when(authChecker).checkDatabasePrivilege(eq(userRoot), any(), any());
    // user `user1` don't hava test1's CREATE privilege
    String errorMsg =
        String.format(
            "%s doesn't have %s privilege on DATABASE %s",
            user1, TableModelPrivilege.CREATE, databaseTest1);
    Mockito.doThrow(new AccessDeniedException(errorMsg))
        .when(authChecker)
        .checkDatabasePrivilege(eq(user1), eq(databaseTest1), eq(TableModelPrivilege.CREATE));
    // user `user2` has test1's CREATE privilege
    Mockito.doNothing()
        .when(authChecker)
        .checkDatabasePrivilege(eq(user2), eq(databaseTest1), eq(TableModelPrivilege.CREATE));

    String userName = userRoot;
    try {
      analyzeConfigTask(sql, userName, authChecker);
    } catch (Exception e) {
      fail(e.getMessage());
    }

    userName = user1;
    try {
      analyzeConfigTask(sql, userName, authChecker);
      fail("user1 should be denied");
    } catch (AccessDeniedException e) {
      assertEquals(AccessDeniedException.PREFIX + errorMsg, e.getMessage());
    } catch (Exception e) {
      fail("Unexpected exception : " + e.getMessage());
    }

    userName = user2;
    try {
      analyzeConfigTask(sql, userName, authChecker);
    } catch (Exception e) {
      fail(e.getMessage());
    }

    // use database
    String databaseTest2 = "test2";
    // user `root` always succeed
    Mockito.doNothing().when(authChecker).checkDatabaseVisibility(eq(userRoot), any());
    // user `user1` can't see DATABASE test1
    errorMsg = String.format("%s has no privileges on DATABASE %s", user1, databaseTest1);
    Mockito.doThrow(new AccessDeniedException(errorMsg))
        .when(authChecker)
        .checkDatabaseVisibility(eq(user1), eq(databaseTest1));
    // user `user1` can see DATABASE test2
    Mockito.doNothing().when(authChecker).checkDatabaseVisibility(eq(user1), eq(databaseTest2));

    sql = String.format("USE %s", databaseTest1);
    userName = userRoot;
    try {
      analyzeConfigTask(sql, userName, authChecker);
    } catch (Exception e) {
      fail(e.getMessage());
    }

    userName = user1;
    try {
      analyzeConfigTask(sql, userName, authChecker);
      fail("user1 should be denied");
    } catch (AccessDeniedException e) {
      assertEquals(AccessDeniedException.PREFIX + errorMsg, e.getMessage());
    } catch (Exception e) {
      fail("Unexpected exception : " + e.getMessage());
    }

    sql = String.format("USE %s", databaseTest2);
    try {
      analyzeConfigTask(sql, userName, authChecker);
    } catch (Exception e) {
      fail(e.getMessage());
    }

    // TODO show databases

    // TODO show databases

    // TODO drop database

    // TODO alter database

  }

  private void analyzeSQL(String sql, String userName, ITableAuthChecker authChecker) {
    analyzeSQL(sql, userName, authChecker, null);
  }

  private void analyzeSQL(
      String sql,
      String userName,
      ITableAuthChecker authChecker,
      String databaseNameInSessionInfo) {
    IClientSession clientSession = Mockito.mock(IClientSession.class);
    Mockito.when(clientSession.getDatabaseName()).thenReturn(databaseNameInSessionInfo);
    Statement statement = sqlParser.createStatement(sql, zoneId, clientSession);

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
            StatementRewrite.NOOP,
            NOOP);
    analyzer.analyze(statement);
  }

  private void analyzeConfigTask(String sql, String userName, ITableAuthChecker authChecker) {
    IClientSession clientSession = Mockito.mock(IClientSession.class);
    Mockito.when(clientSession.getDatabaseName()).thenReturn(null);
    Statement statement = sqlParser.createStatement(sql, zoneId, clientSession);

    SessionInfo session =
        new SessionInfo(0, userName, zoneId, null, IClientSession.SqlDialect.TABLE);
    MPPQueryContext context = new MPPQueryContext(sql, QUERY_ID, 0, session, null, null);

    statement.accept(
        new TableConfigTaskVisitor(
            Mockito.mock(IClientSession.class), TEST_MATADATA, new AccessControlImpl(authChecker)),
        context);
  }

  private void analyzeConfigTask(
      String sql, String userName, ITableAuthChecker authChecker, IClientSession clientSession) {
    Statement statement = sqlParser.createStatement(sql, zoneId, clientSession);
    SessionInfo session =
        new SessionInfo(0, userName, zoneId, null, IClientSession.SqlDialect.TABLE);
    MPPQueryContext context = new MPPQueryContext(sql, QUERY_ID, 0, session, null, null);

    statement.accept(
        new TableConfigTaskVisitor(
            clientSession, TEST_MATADATA, new AccessControlImpl(authChecker)),
        context);
  }
}
