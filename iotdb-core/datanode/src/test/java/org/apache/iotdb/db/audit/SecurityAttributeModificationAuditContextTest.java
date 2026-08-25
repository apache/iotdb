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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

public class SecurityAttributeModificationAuditContextTest {

  @Test
  public void testTreeUserRoleModification() {
    SecurityAttributeModificationAuditContext.AuditLogWriter writer =
        mock(SecurityAttributeModificationAuditContext.AuditLogWriter.class);
    SecurityAttributeModificationAuditContext context =
        SecurityAttributeModificationAuditContext.forTreeStatement(
            treeStatement(AuthorType.GRANT_USER_ROLE, "user1", "role1"),
            sessionInfo(),
            "GRANT ROLE role1 TO user1",
            writer);

    context.record(RpcUtils.SUCCESS_STATUS);

    assertAuditLog(writer, true, "GRANT ROLE role1 TO user1", "user: user1, role: role1");
  }

  @Test
  public void testTreeUserPrivilegeModification() {
    SecurityAttributeModificationAuditContext.AuditLogWriter writer =
        mock(SecurityAttributeModificationAuditContext.AuditLogWriter.class);
    SecurityAttributeModificationAuditContext context =
        SecurityAttributeModificationAuditContext.forTreeStatement(
            treeStatement(AuthorType.REVOKE_USER, "user1", null),
            sessionInfo(),
            "REVOKE READ_DATA ON root.** FROM user1",
            writer);

    context.record(RpcUtils.SUCCESS_STATUS);

    assertAuditLog(writer, true, "REVOKE READ_DATA ON root.** FROM user1", "user1");
  }

  @Test
  public void testTableRolePrivilegeModificationFailure() {
    SecurityAttributeModificationAuditContext.AuditLogWriter writer =
        mock(SecurityAttributeModificationAuditContext.AuditLogWriter.class);
    SecurityAttributeModificationAuditContext context =
        SecurityAttributeModificationAuditContext.forTableStatement(
            tableStatement(AuthorRType.GRANT_ROLE_DB, null, "role1"),
            sessionInfo(),
            "GRANT SELECT ON DATABASE db TO ROLE role1",
            writer);

    context.record(RpcUtils.getStatus(TSStatusCode.EXECUTE_STATEMENT_ERROR));

    assertAuditLog(writer, false, "GRANT SELECT ON DATABASE db TO ROLE role1", "role1");
  }

  @Test
  public void testRoleCreationIsSecurityAttributeModification() {
    SecurityAttributeModificationAuditContext.AuditLogWriter writer =
        mock(SecurityAttributeModificationAuditContext.AuditLogWriter.class);
    SecurityAttributeModificationAuditContext context =
        SecurityAttributeModificationAuditContext.forTreeStatement(
            treeStatement(AuthorType.CREATE_ROLE, null, "role1"),
            sessionInfo(),
            "CREATE ROLE role1",
            writer);

    context.record(RpcUtils.SUCCESS_STATUS);

    assertAuditLog(writer, true, "CREATE ROLE role1", "role1");
  }

  @Test
  public void testNonSecurityAttributeModificationIsIgnored() {
    SecurityAttributeModificationAuditContext.AuditLogWriter writer =
        mock(SecurityAttributeModificationAuditContext.AuditLogWriter.class);
    SecurityAttributeModificationAuditContext context =
        SecurityAttributeModificationAuditContext.forTreeStatement(
            treeStatement(AuthorType.CREATE_USER, "user1", null),
            sessionInfo(),
            "CREATE USER user1 'password'",
            writer);

    context.record(RpcUtils.SUCCESS_STATUS);

    verify(writer, never()).log(any(), any());
  }

  @Test
  public void testAuthorizationFailureIsRecorded() {
    SecurityAttributeModificationAuditContext.AuditLogWriter writer =
        mock(SecurityAttributeModificationAuditContext.AuditLogWriter.class);
    SecurityAttributeModificationAuditContext context =
        SecurityAttributeModificationAuditContext.forTreeAuthorization(
            treeStatement(AuthorType.GRANT_ROLE, null, "role1"),
            treeAuditEntity("GRANT READ_DATA ON root.** TO ROLE role1"),
            writer);

    context.recordAuthorizationFailure(RpcUtils.getStatus(TSStatusCode.NO_PERMISSION));

    assertAuditLog(writer, false, "GRANT READ_DATA ON root.** TO ROLE role1", "role1");
  }

  @Test
  public void testSuccessfulAuthorizationIsRecordedAfterExecutionOnly() {
    SecurityAttributeModificationAuditContext.AuditLogWriter writer =
        mock(SecurityAttributeModificationAuditContext.AuditLogWriter.class);
    SecurityAttributeModificationAuditContext context =
        SecurityAttributeModificationAuditContext.forTreeAuthorization(
            treeStatement(AuthorType.GRANT_ROLE, null, "role1"),
            treeAuditEntity("grant privilege"),
            writer);

    context.recordAuthorizationFailure(RpcUtils.SUCCESS_STATUS);

    verify(writer, never()).log(any(), any());
  }

  @Test
  public void testRedirectedModificationIsIgnored() {
    SecurityAttributeModificationAuditContext.AuditLogWriter writer =
        mock(SecurityAttributeModificationAuditContext.AuditLogWriter.class);
    SecurityAttributeModificationAuditContext context =
        SecurityAttributeModificationAuditContext.forTreeStatement(
            treeStatement(AuthorType.REVOKE_ROLE, null, "role1"),
            sessionInfo(),
            "revoke privilege",
            writer);

    context.record(RpcUtils.getStatus(TSStatusCode.REDIRECTION_RECOMMEND));

    verify(writer, never()).log(any(), any());
  }

  @Test
  public void testTracksAsynchronousExecutionResult() {
    SecurityAttributeModificationAuditContext.AuditLogWriter writer =
        mock(SecurityAttributeModificationAuditContext.AuditLogWriter.class);
    SecurityAttributeModificationAuditContext context =
        SecurityAttributeModificationAuditContext.forTableStatement(
            tableStatement(AuthorRType.REVOKE_USER_SYS, "user1", null),
            sessionInfo(),
            "REVOKE MAINTAIN FROM user1",
            writer);
    SettableFuture<ConfigTaskResult> future = SettableFuture.create();

    assertSame(future, context.track(future));
    future.set(new ConfigTaskResult(TSStatusCode.SUCCESS_STATUS));

    assertAuditLog(writer, true, "REVOKE MAINTAIN FROM user1", "user1");
  }

  @Test
  public void testTracksAsynchronousExecutionFailureOnce() {
    SecurityAttributeModificationAuditContext.AuditLogWriter writer =
        mock(SecurityAttributeModificationAuditContext.AuditLogWriter.class);
    SecurityAttributeModificationAuditContext context =
        SecurityAttributeModificationAuditContext.forTreeStatement(
            treeStatement(AuthorType.DROP_ROLE, null, "role1"),
            sessionInfo(),
            "DROP ROLE role1",
            writer);
    SettableFuture<ConfigTaskResult> future = SettableFuture.create();

    context.track(future);
    context.record(null);
    future.setException(new RuntimeException());

    assertAuditLog(writer, false, "DROP ROLE role1", "role1");
  }

  private static AuthorStatement treeStatement(
      AuthorType type, String targetUsername, String targetRoleName) {
    return new AuthorStatement(type) {
      @Override
      public String getUserName() {
        return targetUsername;
      }

      @Override
      public String getRoleName() {
        return targetRoleName;
      }
    };
  }

  private static RelationalAuthorStatement tableStatement(
      AuthorRType type, String targetUsername, String targetRoleName) {
    return new RelationalAuthorStatement(type, targetUsername, targetRoleName, false);
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

  @SuppressWarnings("unchecked")
  private static void assertAuditLog(
      SecurityAttributeModificationAuditContext.AuditLogWriter writer,
      boolean expectedResult,
      String expectedSql,
      String expectedLog) {
    ArgumentCaptor<IAuditEntity> entityCaptor = ArgumentCaptor.forClass(IAuditEntity.class);
    ArgumentCaptor<Supplier<String>> logCaptor = ArgumentCaptor.forClass(Supplier.class);
    verify(writer).log(entityCaptor.capture(), logCaptor.capture());

    IAuditEntity entity = entityCaptor.getValue();
    assertEquals(7L, entity.getUserId());
    assertEquals("operator", entity.getUsername());
    assertEquals("127.0.0.1", entity.getCliHostname());
    assertEquals(AuditEventType.MODIFY_SECURITY_ATTRIBUTE, entity.getAuditEventType());
    assertEquals(AuditLogOperation.CONTROL, entity.getAuditLogOperation());
    assertEquals(PrivilegeType.SECURITY, entity.getPrivilegeTypes().get(0));
    if (expectedResult) {
      assertTrue(entity.getResult());
    } else {
      assertFalse(entity.getResult());
    }
    assertEquals("database", entity.getDatabase());
    assertEquals(expectedSql, entity.getSqlString());
    assertEquals(expectedLog, logCaptor.getValue().get());
  }
}
