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
import org.apache.iotdb.commons.auth.entity.PrivilegeType;
import org.apache.iotdb.commons.queryengine.common.SessionInfo;
import org.apache.iotdb.db.i18n.DataNodeMiscMessages;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.RelationalAuthorStatement;
import org.apache.iotdb.db.queryengine.plan.relational.type.AuthorRType;
import org.apache.iotdb.db.queryengine.plan.statement.AuthorType;
import org.apache.iotdb.db.queryengine.plan.statement.sys.AuthorStatement;
import org.apache.iotdb.rpc.RpcUtils;
import org.apache.iotdb.rpc.TSStatusCode;

import org.junit.Test;
import org.mockito.ArgumentCaptor;

import java.util.List;
import java.util.Optional;
import java.util.function.Supplier;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class UserRoleModificationAuditContextTest {

  @Test
  public void testTreeGrantRoleSuccess() {
    DNAuditLogger auditLogger = mock(DNAuditLogger.class);
    UserRoleModificationAuditContext context =
        new UserRoleModificationAuditContext(auditLogger, "GRANT ROLE role1 TO user1");
    context.setSessionInfo(mockSessionInfo());
    AuthorStatement statement = new AuthorStatement(AuthorType.GRANT_USER_ROLE);
    statement.setUserName("user1");
    statement.setRoleName("role1");

    context.track(statement);
    context.log(RpcUtils.SUCCESS_STATUS);

    assertAuditLogs(auditLogger, "GRANT ROLE role1 TO user1");
  }

  @Test
  public void testTableRevokeRoleSuccess() {
    DNAuditLogger auditLogger = mock(DNAuditLogger.class);
    UserRoleModificationAuditContext context =
        new UserRoleModificationAuditContext(auditLogger, "REVOKE ROLE role1 FROM user1");
    context.setSessionInfo(mockSessionInfo());
    RelationalAuthorStatement statement =
        new RelationalAuthorStatement(AuthorRType.REVOKE_USER_ROLE);
    statement.setUserName("user1");
    statement.setRoleName("role1");

    context.track(statement);
    context.log(RpcUtils.SUCCESS_STATUS);

    assertAuditLogs(auditLogger, "REVOKE ROLE role1 FROM user1");
  }

  @Test
  public void testTreeRevokeRoleSuccess() {
    DNAuditLogger auditLogger = mock(DNAuditLogger.class);
    UserRoleModificationAuditContext context =
        new UserRoleModificationAuditContext(auditLogger, "REVOKE ROLE role1 FROM user1");
    context.setSessionInfo(mockSessionInfo());
    AuthorStatement statement = new AuthorStatement(AuthorType.REVOKE_USER_ROLE);
    statement.setUserName("user1");
    statement.setRoleName("role1");

    context.track(statement);
    context.log(RpcUtils.SUCCESS_STATUS);

    assertAuditLogs(auditLogger, "REVOKE ROLE role1 FROM user1");
  }

  @Test
  public void testTableGrantRoleSuccess() {
    DNAuditLogger auditLogger = mock(DNAuditLogger.class);
    UserRoleModificationAuditContext context =
        new UserRoleModificationAuditContext(auditLogger, "GRANT ROLE role1 TO user1");
    context.setSessionInfo(mockSessionInfo());
    RelationalAuthorStatement statement =
        new RelationalAuthorStatement(AuthorRType.GRANT_USER_ROLE);
    statement.setUserName("user1");
    statement.setRoleName("role1");

    context.track(statement);
    context.log(RpcUtils.SUCCESS_STATUS);

    assertAuditLogs(auditLogger, "GRANT ROLE role1 TO user1");
  }

  @Test
  public void testFailedModificationIsIgnored() {
    DNAuditLogger auditLogger = mock(DNAuditLogger.class);
    UserRoleModificationAuditContext context =
        new UserRoleModificationAuditContext(auditLogger, "grant role");
    context.setSessionInfo(mockSessionInfo());
    AuthorStatement statement = new AuthorStatement(AuthorType.GRANT_USER_ROLE);
    statement.setUserName("user1");
    statement.setRoleName("role1");
    context.track(statement);

    context.log(RpcUtils.getStatus(TSStatusCode.NO_PERMISSION));

    verify(auditLogger, never()).log(any(), any());
  }

  @Test
  public void testRedirectedModificationIsIgnored() {
    DNAuditLogger auditLogger = mock(DNAuditLogger.class);
    UserRoleModificationAuditContext context =
        new UserRoleModificationAuditContext(auditLogger, "grant role");
    context.setSessionInfo(mockSessionInfo());
    AuthorStatement statement = new AuthorStatement(AuthorType.GRANT_USER_ROLE);
    statement.setUserName("user1");
    statement.setRoleName("role1");
    context.track(statement);

    context.log(RpcUtils.getStatus(TSStatusCode.REDIRECTION_RECOMMEND));

    verify(auditLogger, never()).log(any(), any());
  }

  @Test
  public void testPrivilegeGrantIsIgnored() {
    DNAuditLogger auditLogger = mock(DNAuditLogger.class);
    UserRoleModificationAuditContext context =
        new UserRoleModificationAuditContext(auditLogger, "grant privilege");
    context.setSessionInfo(mockSessionInfo());
    context.track(new AuthorStatement(AuthorType.GRANT_USER));

    context.log(RpcUtils.SUCCESS_STATUS);

    verify(auditLogger, never()).log(any(), any());
  }

  private static SessionInfo mockSessionInfo() {
    SessionInfo session = mock(SessionInfo.class);
    when(session.getUserId()).thenReturn(7L);
    when(session.getUserName()).thenReturn("operator");
    when(session.getCliHostname()).thenReturn("127.0.0.1");
    when(session.getDatabaseName()).thenReturn(Optional.of("database"));
    return session;
  }

  @SuppressWarnings("unchecked")
  private static void assertAuditLogs(DNAuditLogger auditLogger, String sql) {
    ArgumentCaptor<IAuditEntity> entityCaptor = ArgumentCaptor.forClass(IAuditEntity.class);
    ArgumentCaptor<Supplier<String>> logCaptor = ArgumentCaptor.forClass(Supplier.class);
    verify(auditLogger, times(2)).log(entityCaptor.capture(), logCaptor.capture());

    List<IAuditEntity> entities = entityCaptor.getAllValues();
    List<Supplier<String>> logs = logCaptor.getAllValues();
    assertAuditLog(entities.get(0), sql, AuditEventType.MODIFY_SECURITY_ATTRIBUTE);
    assertAuditLog(entities.get(1), sql, AuditEventType.MODIFY_ROLE_MEMBERSHIP);
    assertEquals(
        String.format(
            DataNodeMiscMessages
                .LOG_SECURITY_ATTRIBUTE_USER_ROLE_MEMBERSHIP_USER_ARG_ROLE_ARG_D6DC8233,
            "user1",
            "role1"),
        logs.get(0).get());
    assertEquals(
        String.format(DataNodeMiscMessages.LOG_USER_ARG_ROLE_ARG_422D48D3, "user1", "role1"),
        logs.get(1).get());
  }

  private static void assertAuditLog(IAuditEntity entity, String sql, AuditEventType eventType) {
    assertEquals(7L, entity.getUserId());
    assertEquals("operator", entity.getUsername());
    assertEquals("127.0.0.1", entity.getCliHostname());
    assertEquals(eventType, entity.getAuditEventType());
    assertEquals(AuditLogOperation.CONTROL, entity.getAuditLogOperation());
    assertEquals(PrivilegeType.SECURITY, entity.getPrivilegeTypes().get(0));
    assertTrue(entity.getResult());
    assertEquals("database", entity.getDatabase());
    assertEquals(sql, entity.getSqlString());
  }
}
