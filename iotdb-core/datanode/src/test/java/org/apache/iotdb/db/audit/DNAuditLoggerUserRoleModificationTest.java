/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.db.audit;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.audit.AuditEventType;
import org.apache.iotdb.commons.audit.AuditLogOperation;
import org.apache.iotdb.commons.audit.IAuditEntity;
import org.apache.iotdb.commons.audit.UserEntity;
import org.apache.iotdb.commons.auth.entity.PrivilegeType;
import org.apache.iotdb.commons.exception.IoTDBException;
import org.apache.iotdb.commons.queryengine.common.SessionInfo;
import org.apache.iotdb.commons.queryengine.common.SqlDialect;
import org.apache.iotdb.db.queryengine.plan.execution.config.ConfigTaskResult;
import org.apache.iotdb.db.queryengine.plan.execution.config.IConfigTask;
import org.apache.iotdb.db.queryengine.plan.relational.security.TreeAccessCheckContext;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.RelationalAuthorStatement;
import org.apache.iotdb.db.queryengine.plan.relational.type.AuthorRType;
import org.apache.iotdb.db.queryengine.plan.statement.AuthorType;
import org.apache.iotdb.db.queryengine.plan.statement.sys.AuthorStatement;
import org.apache.iotdb.rpc.RpcUtils;
import org.apache.iotdb.rpc.TSStatusCode;

import com.google.common.util.concurrent.SettableFuture;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import java.time.ZoneId;
import java.util.function.Supplier;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

public class DNAuditLoggerUserRoleModificationTest {

  @Test
  public void testTreeGrantRoleSuccess() {
    DNAuditLogger auditLogger = mock(DNAuditLogger.class, CALLS_REAL_METHODS);
    String sql = "GRANT ROLE role1 TO user1";

    auditLogger.logUserRoleModification(
        treeStatement(AuthorType.GRANT_USER_ROLE), sessionInfo(), sql, RpcUtils.SUCCESS_STATUS);

    assertAuditLog(auditLogger, true, sql);
  }

  @Test
  public void testTreeRevokeRoleFailure() {
    DNAuditLogger auditLogger = mock(DNAuditLogger.class, CALLS_REAL_METHODS);
    String sql = "REVOKE ROLE role1 FROM user1";

    auditLogger.logUserRoleModification(
        treeStatement(AuthorType.REVOKE_USER_ROLE),
        sessionInfo(),
        sql,
        RpcUtils.getStatus(TSStatusCode.USER_NOT_HAS_ROLE));

    assertAuditLog(auditLogger, false, sql);
  }

  @Test
  public void testTableGrantRoleFailure() {
    DNAuditLogger auditLogger = mock(DNAuditLogger.class, CALLS_REAL_METHODS);
    String sql = "GRANT ROLE role1 TO user1";

    auditLogger.logUserRoleModification(
        tableStatement(AuthorRType.GRANT_USER_ROLE),
        sessionInfo(),
        sql,
        RpcUtils.getStatus(TSStatusCode.ROLE_NOT_EXIST));

    assertAuditLog(auditLogger, false, sql);
  }

  @Test
  public void testTableRevokeRoleSuccess() {
    DNAuditLogger auditLogger = mock(DNAuditLogger.class, CALLS_REAL_METHODS);
    String sql = "REVOKE ROLE role1 FROM user1";

    auditLogger.logUserRoleModification(
        tableStatement(AuthorRType.REVOKE_USER_ROLE), sessionInfo(), sql, RpcUtils.SUCCESS_STATUS);

    assertAuditLog(auditLogger, true, sql);
  }

  @Test
  public void testTreeAuthorizationFailure() {
    DNAuditLogger auditLogger = mock(DNAuditLogger.class, CALLS_REAL_METHODS);
    String sql = "GRANT ROLE role1 TO user1";

    auditLogger.logUserRoleModificationAuthorizationFailure(
        treeStatement(AuthorType.GRANT_USER_ROLE),
        treeAuditEntity(sql),
        RpcUtils.getStatus(TSStatusCode.NO_PERMISSION));

    assertAuditLog(auditLogger, false, sql);
  }

  @Test
  public void testSuccessfulAuthorizationIsLoggedAfterExecutionOnly() {
    DNAuditLogger auditLogger = mock(DNAuditLogger.class, CALLS_REAL_METHODS);

    auditLogger.logUserRoleModificationAuthorizationFailure(
        treeStatement(AuthorType.GRANT_USER_ROLE),
        treeAuditEntity("grant role"),
        RpcUtils.SUCCESS_STATUS);

    verify(auditLogger, never()).log(any(), any());
  }

  @Test
  public void testRedirectIsIgnored() {
    DNAuditLogger auditLogger = mock(DNAuditLogger.class, CALLS_REAL_METHODS);

    auditLogger.logUserRoleModification(
        treeStatement(AuthorType.GRANT_USER_ROLE),
        sessionInfo(),
        "grant role",
        RpcUtils.getStatus(TSStatusCode.REDIRECTION_RECOMMEND));

    verify(auditLogger, never()).log(any(), any());
  }

  @Test
  public void testPrivilegeGrantIsIgnored() {
    DNAuditLogger auditLogger = mock(DNAuditLogger.class, CALLS_REAL_METHODS);
    AuthorStatement statement = new AuthorStatement(AuthorType.GRANT_USER);
    statement.setUserName("user1");

    auditLogger.logUserRoleModification(statement, sessionInfo(), "grant privilege", null);

    verify(auditLogger, never()).log(any(), any());
  }

  @Test
  public void testMissingSessionIsIgnored() {
    DNAuditLogger auditLogger = mock(DNAuditLogger.class, CALLS_REAL_METHODS);

    auditLogger.logUserRoleModification(
        treeStatement(AuthorType.GRANT_USER_ROLE), null, "grant role", null);

    verify(auditLogger, never()).log(any(), any());
  }

  @Test
  public void testConcreteTreeConfigTaskRecordsSuccess() throws Exception {
    UserRoleModificationAuditContext.AuditLogWriter auditLogWriter =
        mock(UserRoleModificationAuditContext.AuditLogWriter.class);
    UserRoleModificationAuditContext context =
        UserRoleModificationAuditContext.forTreeStatement(
            treeStatement(AuthorType.GRANT_USER_ROLE), sessionInfo(), auditLogWriter);
    SettableFuture<ConfigTaskResult> future = SettableFuture.create();
    IConfigTask task = UserRoleModificationAuditTask.wrap(ignored -> future, context);

    assertSame(future, task.execute(null));
    future.set(new ConfigTaskResult(TSStatusCode.SUCCESS_STATUS));

    assertAuditStatus(auditLogWriter, TSStatusCode.SUCCESS_STATUS);
  }

  @Test
  public void testConcreteTableConfigTaskRecordsFailure() throws Exception {
    UserRoleModificationAuditContext.AuditLogWriter auditLogWriter =
        mock(UserRoleModificationAuditContext.AuditLogWriter.class);
    UserRoleModificationAuditContext context =
        UserRoleModificationAuditContext.forTableStatement(
            tableStatement(AuthorRType.REVOKE_USER_ROLE), sessionInfo(), auditLogWriter);
    SettableFuture<ConfigTaskResult> future = SettableFuture.create();
    IConfigTask task = UserRoleModificationAuditTask.wrap(ignored -> future, context);

    task.execute(null);
    future.set(new ConfigTaskResult(RpcUtils.getStatus(TSStatusCode.USER_NOT_HAS_ROLE)));

    assertAuditStatus(auditLogWriter, TSStatusCode.USER_NOT_HAS_ROLE);
  }

  @Test
  public void testConcreteConfigTaskRecordsUnexpectedFailure() throws Exception {
    UserRoleModificationAuditContext.AuditLogWriter auditLogWriter =
        mock(UserRoleModificationAuditContext.AuditLogWriter.class);
    UserRoleModificationAuditContext context =
        UserRoleModificationAuditContext.forTreeStatement(
            treeStatement(AuthorType.GRANT_USER_ROLE), sessionInfo(), auditLogWriter);
    SettableFuture<ConfigTaskResult> future = SettableFuture.create();
    IConfigTask task = UserRoleModificationAuditTask.wrap(ignored -> future, context);

    task.execute(null);
    future.setException(new RuntimeException());

    verify(auditLogWriter).log(null);
  }

  @Test
  public void testConcreteConfigTaskIgnoresRedirectResult() throws Exception {
    UserRoleModificationAuditContext.AuditLogWriter auditLogWriter =
        mock(UserRoleModificationAuditContext.AuditLogWriter.class);
    UserRoleModificationAuditContext context =
        UserRoleModificationAuditContext.forTreeStatement(
            treeStatement(AuthorType.GRANT_USER_ROLE), sessionInfo(), auditLogWriter);
    SettableFuture<ConfigTaskResult> future = SettableFuture.create();
    IConfigTask task = UserRoleModificationAuditTask.wrap(ignored -> future, context);

    task.execute(null);
    future.set(new ConfigTaskResult(TSStatusCode.REDIRECTION_RECOMMEND));

    verify(auditLogWriter, never()).log(any());
  }

  @Test
  public void testConcreteConfigTaskIgnoresRedirectException() throws Exception {
    UserRoleModificationAuditContext.AuditLogWriter auditLogWriter =
        mock(UserRoleModificationAuditContext.AuditLogWriter.class);
    UserRoleModificationAuditContext context =
        UserRoleModificationAuditContext.forTreeStatement(
            treeStatement(AuthorType.GRANT_USER_ROLE), sessionInfo(), auditLogWriter);
    SettableFuture<ConfigTaskResult> future = SettableFuture.create();
    IConfigTask task = UserRoleModificationAuditTask.wrap(ignored -> future, context);

    task.execute(null);
    future.setException(new IoTDBException(RpcUtils.getStatus(TSStatusCode.REDIRECTION_RECOMMEND)));

    verify(auditLogWriter, never()).log(any());
  }

  @Test
  public void testUserRoleAuditContextRecordsOnlyOnce() {
    UserRoleModificationAuditContext.AuditLogWriter auditLogWriter =
        mock(UserRoleModificationAuditContext.AuditLogWriter.class);
    UserRoleModificationAuditContext context =
        UserRoleModificationAuditContext.forTreeStatement(
            treeStatement(AuthorType.GRANT_USER_ROLE), sessionInfo(), auditLogWriter);

    context.log(RpcUtils.SUCCESS_STATUS);
    context.log(RpcUtils.getStatus(TSStatusCode.EXECUTE_STATEMENT_ERROR));

    assertAuditStatus(auditLogWriter, TSStatusCode.SUCCESS_STATUS);
  }

  @Test
  public void testPrivilegeGrantDoesNotWrapConcreteTask() {
    UserRoleModificationAuditContext.AuditLogWriter auditLogWriter =
        mock(UserRoleModificationAuditContext.AuditLogWriter.class);
    UserRoleModificationAuditContext context =
        UserRoleModificationAuditContext.forTreeStatement(
            treeStatement(AuthorType.GRANT_USER), sessionInfo(), auditLogWriter);
    IConfigTask delegate = ignored -> SettableFuture.create();

    assertSame(delegate, UserRoleModificationAuditTask.wrap(delegate, context));
    context.log(RpcUtils.SUCCESS_STATUS);

    verify(auditLogWriter, never()).log(any());
  }

  private static AuthorStatement treeStatement(AuthorType type) {
    AuthorStatement statement = new AuthorStatement(type);
    statement.setUserName("user1");
    statement.setRoleName("role1");
    return statement;
  }

  private static RelationalAuthorStatement tableStatement(AuthorRType type) {
    RelationalAuthorStatement statement = new RelationalAuthorStatement(type);
    statement.setUserName("user1");
    statement.setRoleName("role1");
    return statement;
  }

  private static IAuditEntity treeAuditEntity(String sql) {
    return new TreeAccessCheckContext(7L, "operator", "127.0.0.1")
        .setDatabase("database")
        .setSqlString(sql);
  }

  private static SessionInfo sessionInfo() {
    return new SessionInfo(
        1L,
        new UserEntity(7L, "operator", "127.0.0.1"),
        ZoneId.systemDefault(),
        "database",
        SqlDialect.TABLE);
  }

  private static void assertAuditStatus(
      UserRoleModificationAuditContext.AuditLogWriter auditLogWriter,
      TSStatusCode expectedStatusCode) {
    ArgumentCaptor<TSStatus> statusCaptor = ArgumentCaptor.forClass(TSStatus.class);
    verify(auditLogWriter).log(statusCaptor.capture());
    assertEquals(expectedStatusCode.getStatusCode(), statusCaptor.getValue().getCode());
  }

  @SuppressWarnings("unchecked")
  private static void assertAuditLog(DNAuditLogger auditLogger, boolean result, String sql) {
    ArgumentCaptor<IAuditEntity> entityCaptor = ArgumentCaptor.forClass(IAuditEntity.class);
    ArgumentCaptor<Supplier<String>> logCaptor = ArgumentCaptor.forClass(Supplier.class);
    verify(auditLogger).log(entityCaptor.capture(), logCaptor.capture());

    IAuditEntity entity = entityCaptor.getValue();
    assertEquals(7L, entity.getUserId());
    assertEquals("operator", entity.getUsername());
    assertEquals("127.0.0.1", entity.getCliHostname());
    assertEquals(AuditEventType.MODIFY_ROLE_MEMBERSHIP, entity.getAuditEventType());
    assertEquals(AuditLogOperation.CONTROL, entity.getAuditLogOperation());
    assertEquals(PrivilegeType.SECURITY, entity.getPrivilegeTypes().get(0));
    if (result) {
      assertTrue(entity.getResult());
    } else {
      assertFalse(entity.getResult());
    }
    assertEquals("database", entity.getDatabase());
    assertEquals(sql, entity.getSqlString());
    assertEquals("user: user1, role: role1", logCaptor.getValue().get());
  }
}
