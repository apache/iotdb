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

package org.apache.iotdb.db.audit;

import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.commons.audit.AuditEventType;
import org.apache.iotdb.commons.audit.AuditLogOperation;
import org.apache.iotdb.commons.audit.IAuditEntity;
import org.apache.iotdb.commons.audit.UserEntity;
import org.apache.iotdb.commons.auth.entity.PrivilegeType;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.queryengine.common.SessionInfo;
import org.apache.iotdb.commons.queryengine.common.SqlDialect;
import org.apache.iotdb.db.protocol.session.IClientSession;
import org.apache.iotdb.db.protocol.session.InternalClientSession;
import org.apache.iotdb.db.queryengine.common.MPPQueryContext;
import org.apache.iotdb.db.queryengine.common.QueryId;
import org.apache.iotdb.db.queryengine.plan.parser.StatementGenerator;
import org.apache.iotdb.db.queryengine.plan.relational.security.AccessControlImpl;
import org.apache.iotdb.db.queryengine.plan.relational.security.ITableAuthChecker;
import org.apache.iotdb.db.queryengine.plan.relational.security.TreeAccessCheckContext;
import org.apache.iotdb.db.queryengine.plan.relational.security.TreeAccessCheckVisitor;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.RelationalAuthorStatement;
import org.apache.iotdb.db.queryengine.plan.relational.sql.parser.SqlParser;
import org.apache.iotdb.db.queryengine.plan.relational.type.AuthorRType;
import org.apache.iotdb.db.queryengine.plan.statement.Statement;
import org.apache.iotdb.db.queryengine.plan.statement.crud.InsertRowStatement;
import org.apache.iotdb.db.queryengine.plan.statement.sys.AuthorStatement;

import org.apache.tsfile.common.conf.TSFileConfig;
import org.apache.tsfile.utils.Binary;
import org.junit.Test;

import java.time.ZoneId;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

public class DNAuditLoggerTest {

  @Test
  public void testAlterUserPasswordSqlIsSanitized() {
    assertEquals(
        "alter user alice set password ...",
        DNAuditLogger.sanitizeAuditSql("alter user alice set password 'secret'"));
    assertEquals(
        "ALTER  USER\talice\nSET  PASSWORD\t...;",
        DNAuditLogger.sanitizeAuditSql("ALTER  USER\talice\nSET  PASSWORD\t\"Secret@123456\";"));
    assertEquals(
        "ALTER USER alice SET PASSWORD ...;",
        DNAuditLogger.sanitizeAuditSql("ALTER USER alice SET PASSWORD 'pass''word';"));
    assertEquals(
        "ALTER USER alice SET PASSWORD ...",
        DNAuditLogger.sanitizeAuditSql("ALTER USER alice SET PASSWORD \"pass\"\"word\""));
    assertEquals(
        "ALTER USER alice SET PASSWORD ...;",
        DNAuditLogger.sanitizeAuditSql("ALTER USER alice SET PASSWORD U&'pass\\0077ord';"));
  }

  @Test
  public void testNonPasswordAlterUserSqlIsNotSanitized() {
    assertSqlIsUnchanged("ALTER USER alice RENAME TO bob;");
    assertSqlIsUnchanged("ALTER USER alice ACCOUNT UNLOCK;");
    assertSqlIsUnchanged("ALTER USER alice SET MAX_SESSION_PER_USER 10;");
    assertSqlIsUnchanged("ALTER USER alice SET MIN_SESSION_PER_USER 1;");
  }

  @Test
  public void testExistingAuditSqlSanitizationIsPreserved() {
    assertNull(DNAuditLogger.sanitizeAuditSql(null));
    assertEquals(
        "CREATE USER alice ...",
        DNAuditLogger.sanitizeAuditSql("CREATE USER alice 'Secret@123456'"));
    assertEquals(
        "INSERT INTO root.sg.d(time, s) values(...)",
        DNAuditLogger.sanitizeAuditSql("INSERT INTO root.sg.d(time, s) values(1, 'secret')"));
  }

  @Test
  public void testTreeObjectAuthenticationSqlIsSanitizedBeforePersistence() throws Exception {
    String originalSql = "ALTER USER alice SET PASSWORD 'treeSecret';";
    Statement statement = StatementGenerator.createStatement(originalSql, ZoneId.systemDefault());
    assertTrue(statement instanceof AuthorStatement);
    TreeAccessCheckContext auditEntity =
        (TreeAccessCheckContext)
            new TreeAccessCheckContext(7L, "alice", "127.0.0.1").setSqlString(originalSql);

    statement.accept(new TreeAccessCheckVisitor(), auditEntity);

    assertPersistedAuditSql(objectAuthenticationAuditEntity(auditEntity, true), "null");
  }

  @Test
  public void testTableObjectAuthenticationSqlIsRemovedAfterParsingBeforePersistence()
      throws Exception {
    assertTablePasswordSqlIsRemoved("ALTER USER alice SET PASSWORD 'Secret' -- rotate", "Secret");
    assertTablePasswordSqlIsRemoved(
        "ALTER USER alice SET PASSWORD 'Secret' /* rotate */", "Secret");
    assertTablePasswordSqlIsRemoved(
        "ALTER USER alice SET PASSWORD U&'Secret' UESCAPE '#'", "Secret");
  }

  private static void assertTablePasswordSqlIsRemoved(String originalSql, String password)
      throws Exception {
    IClientSession clientSession = new InternalClientSession("table_audit_test");
    clientSession.setDatabaseName("database");
    org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.Statement parsedStatement =
        new SqlParser().createStatement(originalSql, ZoneId.systemDefault(), clientSession);
    assertTrue(parsedStatement instanceof RelationalAuthorStatement);
    RelationalAuthorStatement authorStatement = (RelationalAuthorStatement) parsedStatement;
    assertEquals(AuthorRType.UPDATE_USER, authorStatement.getAuthorType());

    MPPQueryContext queryContext =
        new MPPQueryContext(
            originalSql,
            new QueryId("table_audit_test"),
            new SessionInfo(
                1L,
                new UserEntity(7L, "alice", "127.0.0.1"),
                ZoneId.systemDefault(),
                "database",
                SqlDialect.TABLE),
            new TEndPoint(),
            new TEndPoint());
    new AccessControlImpl(mock(ITableAuthChecker.class), new TreeAccessCheckVisitor())
        .checkUserCanRunRelationalAuthorStatement("alice", authorStatement, queryContext);

    assertEquals(originalSql, queryContext.getSql());
    assertNull(queryContext.getSqlString());
    String persistedSql = getPersistedAuditSql(objectAuthenticationAuditEntity(queryContext, true));
    assertEquals("null", persistedSql);
    assertFalse(persistedSql.contains(password));
  }

  private static void assertSqlIsUnchanged(String sql) {
    assertEquals(sql, DNAuditLogger.sanitizeAuditSql(sql));
  }

  private static IAuditEntity objectAuthenticationAuditEntity(
      IAuditEntity auditEntity, boolean result) {
    return auditEntity
        .setAuditEventType(AuditEventType.OBJECT_AUTHENTICATION)
        .setAuditLogOperation(AuditLogOperation.DDL)
        .setPrivilegeType(PrivilegeType.SECURITY)
        .setResult(result)
        .setDatabase("database");
  }

  private static void assertPersistedAuditSql(IAuditEntity auditEntity, String expectedSql)
      throws Exception {
    assertEquals(expectedSql, getPersistedAuditSql(auditEntity));
  }

  private static String getPersistedAuditSql(IAuditEntity auditEntity) throws Exception {
    InsertRowStatement statement =
        DNAuditLogger.generateInsertStatement(
            auditEntity, "alice", new PartialPath("root.__audit.log.node_0.u_7"), 1L);

    return ((Binary) statement.getValues()[8]).getStringValue(TSFileConfig.STRING_CHARSET);
  }
}
