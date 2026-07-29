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
import org.apache.iotdb.db.protocol.session.IClientSession;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.RelationalAuthorStatement;
import org.apache.iotdb.db.queryengine.plan.relational.type.AuthorRType;
import org.apache.iotdb.db.queryengine.plan.statement.AuthorType;
import org.apache.iotdb.db.queryengine.plan.statement.sys.AuthorStatement;
import org.apache.iotdb.rpc.RpcUtils;
import org.apache.iotdb.rpc.TSStatusCode;

import org.junit.Test;
import org.mockito.ArgumentCaptor;

import java.util.function.Supplier;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class UserRoleModificationAuditContextTest {

  @Test
  public void testTreeGrantRoleSuccess() {
    DNAuditLogger auditLogger = mock(DNAuditLogger.class);
    UserRoleModificationAuditContext context =
        new UserRoleModificationAuditContext(auditLogger, "GRANT ROLE role1 TO user1");
    context.setClientSession(mockSession());
    AuthorStatement statement = new AuthorStatement(AuthorType.GRANT_USER_ROLE);
    statement.setUserName("user1");
    statement.setRoleName("role1");

    context.track(statement);
    context.log(RpcUtils.SUCCESS_STATUS);

    assertAuditLog(auditLogger, "GRANT ROLE role1 TO user1");
  }

  @Test
  public void testTableRevokeRoleSuccess() {
    DNAuditLogger auditLogger = mock(DNAuditLogger.class);
    UserRoleModificationAuditContext context =
        new UserRoleModificationAuditContext(auditLogger, "REVOKE ROLE role1 FROM user1");
    context.setClientSession(mockSession());
    RelationalAuthorStatement statement =
        new RelationalAuthorStatement(AuthorRType.REVOKE_USER_ROLE);
    statement.setUserName("user1");
    statement.setRoleName("role1");

    context.track(statement);
    context.log(RpcUtils.SUCCESS_STATUS);

    assertAuditLog(auditLogger, "REVOKE ROLE role1 FROM user1");
  }

  @Test
  public void testFailedModificationIsIgnored() {
    DNAuditLogger auditLogger = mock(DNAuditLogger.class);
    UserRoleModificationAuditContext context =
        new UserRoleModificationAuditContext(auditLogger, "grant role");
    context.setClientSession(mockSession());
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
    context.setClientSession(mockSession());
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
    context.setClientSession(mockSession());
    context.track(new AuthorStatement(AuthorType.GRANT_USER));

    context.log(RpcUtils.SUCCESS_STATUS);

    verify(auditLogger, never()).log(any(), any());
  }

  private static IClientSession mockSession() {
    IClientSession session = mock(IClientSession.class);
    when(session.getUserId()).thenReturn(7L);
    when(session.getUsername()).thenReturn("operator");
    when(session.getClientAddress()).thenReturn("127.0.0.1");
    when(session.getDatabaseName()).thenReturn("database");
    return session;
  }

  @SuppressWarnings("unchecked")
  private static void assertAuditLog(DNAuditLogger auditLogger, String sql) {
    ArgumentCaptor<IAuditEntity> entityCaptor = ArgumentCaptor.forClass(IAuditEntity.class);
    ArgumentCaptor<Supplier<String>> logCaptor = ArgumentCaptor.forClass(Supplier.class);
    verify(auditLogger).log(entityCaptor.capture(), logCaptor.capture());

    IAuditEntity entity = entityCaptor.getValue();
    assertEquals(7L, entity.getUserId());
    assertEquals("operator", entity.getUsername());
    assertEquals("127.0.0.1", entity.getCliHostname());
    assertEquals(AuditEventType.MODIFY_USER_ROLE, entity.getAuditEventType());
    assertEquals(AuditLogOperation.CONTROL, entity.getAuditLogOperation());
    assertEquals(PrivilegeType.SECURITY, entity.getPrivilegeTypes().get(0));
    assertTrue(entity.getResult());
    assertEquals("database", entity.getDatabase());
    assertEquals(sql, entity.getSqlString());
    assertEquals("user: user1, role: role1", logCaptor.getValue().get());
  }
}
