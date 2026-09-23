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

import org.apache.iotdb.commons.audit.AuditEventType;
import org.apache.iotdb.commons.audit.AuditLogOperation;
import org.apache.iotdb.commons.audit.IAuditEntity;
import org.apache.iotdb.commons.audit.UserEntity;
import org.apache.iotdb.commons.auth.entity.PrivilegeType;
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
import java.util.Collections;
import java.util.function.Supplier;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

public class PasswordChangeAuditContextTest {

  @Test
  public void testTreePasswordChangeSuccess() {
    PasswordChangeAuditContext.AuditLogWriter auditLogWriter =
        mock(PasswordChangeAuditContext.AuditLogWriter.class);
    AuthorStatement statement = treePasswordChangeStatement();
    PasswordChangeAuditContext context =
        PasswordChangeAuditContext.forTreeStatement(statement, sessionInfo(), auditLogWriter);

    context.log(RpcUtils.SUCCESS_STATUS);

    assertAuditLog(auditLogWriter, true);
  }

  @Test
  public void testTablePasswordChangeFailure() {
    PasswordChangeAuditContext.AuditLogWriter auditLogWriter =
        mock(PasswordChangeAuditContext.AuditLogWriter.class);
    RelationalAuthorStatement statement =
        new RelationalAuthorStatement(
            AuthorRType.UPDATE_USER,
            "target_user",
            null,
            null,
            null,
            Collections.emptySet(),
            false,
            null);
    PasswordChangeAuditContext context =
        PasswordChangeAuditContext.forTableStatement(
            statement, sessionInfo(SqlDialect.TABLE), auditLogWriter);

    context.log(RpcUtils.getStatus(TSStatusCode.EXECUTE_STATEMENT_ERROR));

    assertAuditLog(auditLogWriter, false);
  }

  @Test
  public void testOtherAuthorStatementIsIgnored() {
    PasswordChangeAuditContext.AuditLogWriter auditLogWriter =
        mock(PasswordChangeAuditContext.AuditLogWriter.class);
    PasswordChangeAuditContext context =
        PasswordChangeAuditContext.forTreeStatement(
            new AuthorStatement(AuthorType.CREATE_USER), sessionInfo(), auditLogWriter);

    context.log(RpcUtils.SUCCESS_STATUS);

    verify(auditLogWriter, never()).log(any(), any());
  }

  @Test
  public void testRenameUserStatementIsIgnored() {
    PasswordChangeAuditContext.AuditLogWriter auditLogWriter =
        mock(PasswordChangeAuditContext.AuditLogWriter.class);
    PasswordChangeAuditContext context =
        PasswordChangeAuditContext.forTreeAuthorization(
            new AuthorStatement(AuthorType.RENAME_USER),
            new TreeAccessCheckContext(7L, "operator", "127.0.0.1"),
            auditLogWriter);

    context.log(null);

    verify(auditLogWriter, never()).log(any(), any());
  }

  @Test
  public void testDeniedTreePasswordChangeUsesAuthorizationIdentity() {
    PasswordChangeAuditContext.AuditLogWriter auditLogWriter =
        mock(PasswordChangeAuditContext.AuditLogWriter.class);
    AuthorStatement statement = treePasswordChangeStatement();
    TreeAccessCheckContext authorizationContext =
        new TreeAccessCheckContext(7L, "operator", "127.0.0.1");
    authorizationContext.setDatabase("database");
    PasswordChangeAuditContext context =
        PasswordChangeAuditContext.forTreeAuthorization(
            statement, authorizationContext, auditLogWriter);

    context.log(null);

    assertAuditLog(auditLogWriter, false);
  }

  @Test
  public void testPasswordChangeAuditIsRecordedOnlyOnce() {
    PasswordChangeAuditContext.AuditLogWriter auditLogWriter =
        mock(PasswordChangeAuditContext.AuditLogWriter.class);
    PasswordChangeAuditContext context =
        PasswordChangeAuditContext.forTreeStatement(
            treePasswordChangeStatement(), sessionInfo(), auditLogWriter);

    context.log(RpcUtils.SUCCESS_STATUS);
    context.log(RpcUtils.getStatus(TSStatusCode.EXECUTE_STATEMENT_ERROR));

    assertAuditLog(auditLogWriter, true);
  }

  @Test
  public void testPasswordChangeAuditTaskRecordsSuccess() throws Exception {
    PasswordChangeAuditContext.AuditLogWriter auditLogWriter =
        mock(PasswordChangeAuditContext.AuditLogWriter.class);
    PasswordChangeAuditContext context =
        PasswordChangeAuditContext.forTreeStatement(
            treePasswordChangeStatement(), sessionInfo(), auditLogWriter);
    SettableFuture<ConfigTaskResult> future = SettableFuture.create();
    IConfigTask delegate = ignored -> future;
    IConfigTask task = PasswordChangeAuditTask.wrap(delegate, context);

    assertSame(future, task.execute(null));
    future.set(new ConfigTaskResult(TSStatusCode.SUCCESS_STATUS));

    assertAuditLog(auditLogWriter, true);
  }

  @Test
  public void testPasswordChangeAuditTaskRecordsAsynchronousFailure() throws Exception {
    PasswordChangeAuditContext.AuditLogWriter auditLogWriter =
        mock(PasswordChangeAuditContext.AuditLogWriter.class);
    PasswordChangeAuditContext context =
        PasswordChangeAuditContext.forTableStatement(
            tablePasswordChangeStatement(), sessionInfo(SqlDialect.TABLE), auditLogWriter);
    SettableFuture<ConfigTaskResult> future = SettableFuture.create();
    IConfigTask task = PasswordChangeAuditTask.wrap(ignored -> future, context);

    task.execute(null);
    future.setException(new RuntimeException());

    assertAuditLog(auditLogWriter, false);
  }

  @Test
  public void testPasswordChangeAuditTaskRecordsFailedResult() throws Exception {
    PasswordChangeAuditContext.AuditLogWriter auditLogWriter =
        mock(PasswordChangeAuditContext.AuditLogWriter.class);
    PasswordChangeAuditContext context =
        PasswordChangeAuditContext.forTreeStatement(
            treePasswordChangeStatement(), sessionInfo(), auditLogWriter);
    SettableFuture<ConfigTaskResult> future = SettableFuture.create();
    IConfigTask task = PasswordChangeAuditTask.wrap(ignored -> future, context);

    task.execute(null);
    future.set(new ConfigTaskResult(RpcUtils.getStatus(TSStatusCode.EXECUTE_STATEMENT_ERROR)));

    assertAuditLog(auditLogWriter, false);
  }

  @Test
  public void testPasswordChangeAuditTaskTreatsRedirectionAsSuccess() throws Exception {
    PasswordChangeAuditContext.AuditLogWriter auditLogWriter =
        mock(PasswordChangeAuditContext.AuditLogWriter.class);
    PasswordChangeAuditContext context =
        PasswordChangeAuditContext.forTreeStatement(
            treePasswordChangeStatement(), sessionInfo(), auditLogWriter);
    SettableFuture<ConfigTaskResult> future = SettableFuture.create();
    IConfigTask task = PasswordChangeAuditTask.wrap(ignored -> future, context);

    task.execute(null);
    future.set(new ConfigTaskResult(TSStatusCode.REDIRECTION_RECOMMEND));

    assertAuditLog(auditLogWriter, true);
  }

  @Test
  public void testPasswordChangeAuditTaskRecordsSynchronousFailure() {
    PasswordChangeAuditContext.AuditLogWriter auditLogWriter =
        mock(PasswordChangeAuditContext.AuditLogWriter.class);
    PasswordChangeAuditContext context =
        PasswordChangeAuditContext.forTreeStatement(
            treePasswordChangeStatement(), sessionInfo(), auditLogWriter);
    IConfigTask task =
        PasswordChangeAuditTask.wrap(
            ignored -> {
              throw new IllegalStateException();
            },
            context);

    assertThrows(IllegalStateException.class, () -> task.execute(null));

    assertAuditLog(auditLogWriter, false);
  }

  @Test
  public void testPasswordChangeAuditTaskRecordsInterruption() {
    PasswordChangeAuditContext.AuditLogWriter auditLogWriter =
        mock(PasswordChangeAuditContext.AuditLogWriter.class);
    PasswordChangeAuditContext context =
        PasswordChangeAuditContext.forTreeStatement(
            treePasswordChangeStatement(), sessionInfo(), auditLogWriter);
    IConfigTask task =
        PasswordChangeAuditTask.wrap(
            ignored -> {
              throw new InterruptedException();
            },
            context);

    assertThrows(InterruptedException.class, () -> task.execute(null));

    assertAuditLog(auditLogWriter, false);
  }

  @Test
  public void testPasswordChangeAuditTaskDoesNotWrapOtherStatements() {
    PasswordChangeAuditContext.AuditLogWriter auditLogWriter =
        mock(PasswordChangeAuditContext.AuditLogWriter.class);
    PasswordChangeAuditContext context =
        PasswordChangeAuditContext.forTreeStatement(
            new AuthorStatement(AuthorType.RENAME_USER), sessionInfo(), auditLogWriter);
    IConfigTask delegate = ignored -> SettableFuture.create();

    assertSame(delegate, PasswordChangeAuditTask.wrap(delegate, context));
  }

  private static SessionInfo sessionInfo() {
    return sessionInfo(SqlDialect.TREE);
  }

  private static SessionInfo sessionInfo(SqlDialect sqlDialect) {
    return new SessionInfo(
        1L,
        new UserEntity(7L, "operator", "127.0.0.1"),
        ZoneId.systemDefault(),
        "database",
        sqlDialect);
  }

  private static AuthorStatement treePasswordChangeStatement() {
    return new AuthorStatement(AuthorType.UPDATE_USER) {
      @Override
      public String getUserName() {
        return "target_user";
      }
    };
  }

  private static RelationalAuthorStatement tablePasswordChangeStatement() {
    return new RelationalAuthorStatement(
        AuthorRType.UPDATE_USER,
        "target_user",
        null,
        null,
        null,
        Collections.emptySet(),
        false,
        null);
  }

  @SuppressWarnings("unchecked")
  private static void assertAuditLog(
      PasswordChangeAuditContext.AuditLogWriter auditLogWriter, boolean expectedResult) {
    ArgumentCaptor<IAuditEntity> entityCaptor = ArgumentCaptor.forClass(IAuditEntity.class);
    ArgumentCaptor<Supplier<String>> logCaptor = ArgumentCaptor.forClass(Supplier.class);
    verify(auditLogWriter).log(entityCaptor.capture(), logCaptor.capture());

    IAuditEntity entity = entityCaptor.getValue();
    assertEquals(7L, entity.getUserId());
    assertEquals("operator", entity.getUsername());
    assertEquals("127.0.0.1", entity.getCliHostname());
    assertEquals(AuditEventType.MODIFY_PASSWD, entity.getAuditEventType());
    assertEquals(AuditLogOperation.CONTROL, entity.getAuditLogOperation());
    assertEquals(PrivilegeType.SECURITY, entity.getPrivilegeTypes().get(0));
    if (expectedResult) {
      assertTrue(entity.getResult());
    } else {
      assertFalse(entity.getResult());
    }
    assertEquals("database", entity.getDatabase());
    assertNull(entity.getSqlString());
    assertEquals("target_user", logCaptor.getValue().get());
  }
}
