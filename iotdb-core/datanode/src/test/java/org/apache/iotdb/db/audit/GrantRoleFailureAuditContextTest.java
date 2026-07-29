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
import static org.junit.Assert.assertFalse;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class GrantRoleFailureAuditContextTest {

  @Test
  public void testTreeGrantRoleFailure() {
    DNAuditLogger auditLogger = mock(DNAuditLogger.class);
    GrantRoleFailureAuditContext context = newContext(auditLogger);
    AuthorStatement statement = new AuthorStatement(AuthorType.GRANT_USER_ROLE);
    statement.setUserName("user1");

    context.track(statement);
    context.log(RpcUtils.getStatus(TSStatusCode.NO_PERMISSION));

    assertAuditLog(auditLogger);
  }

  @Test
  public void testTableGrantRoleFailure() {
    DNAuditLogger auditLogger = mock(DNAuditLogger.class);
    GrantRoleFailureAuditContext context = newContext(auditLogger);
    RelationalAuthorStatement statement =
        new RelationalAuthorStatement(AuthorRType.GRANT_USER_ROLE);
    statement.setUserName("user1");

    context.track(statement);
    context.log(RpcUtils.getStatus(TSStatusCode.ROLE_NOT_EXIST));

    assertAuditLog(auditLogger);
  }

  @Test
  public void testSuccessfulGrantIsIgnored() {
    DNAuditLogger auditLogger = mock(DNAuditLogger.class);
    GrantRoleFailureAuditContext context = newContext(auditLogger);
    context.track(treeGrantRoleStatement());

    context.log(RpcUtils.SUCCESS_STATUS);

    verify(auditLogger, never()).log(any(), any());
  }

  @Test
  public void testRedirectedGrantIsIgnored() {
    DNAuditLogger auditLogger = mock(DNAuditLogger.class);
    GrantRoleFailureAuditContext context = newContext(auditLogger);
    context.track(treeGrantRoleStatement());

    context.log(RpcUtils.getStatus(TSStatusCode.REDIRECTION_RECOMMEND));

    verify(auditLogger, never()).log(any(), any());
  }

  @Test
  public void testPrivilegeGrantIsIgnored() {
    DNAuditLogger auditLogger = mock(DNAuditLogger.class);
    GrantRoleFailureAuditContext context = newContext(auditLogger);
    context.track(new AuthorStatement(AuthorType.GRANT_USER));

    context.log(RpcUtils.getStatus(TSStatusCode.NO_PERMISSION));

    verify(auditLogger, never()).log(any(), any());
  }

  private static GrantRoleFailureAuditContext newContext(DNAuditLogger auditLogger) {
    GrantRoleFailureAuditContext context =
        new GrantRoleFailureAuditContext(auditLogger, "GRANT ROLE role1 TO user1");
    context.setClientSession(mockSession());
    return context;
  }

  private static AuthorStatement treeGrantRoleStatement() {
    AuthorStatement statement = new AuthorStatement(AuthorType.GRANT_USER_ROLE);
    statement.setUserName("user1");
    return statement;
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
  private static void assertAuditLog(DNAuditLogger auditLogger) {
    ArgumentCaptor<IAuditEntity> entityCaptor = ArgumentCaptor.forClass(IAuditEntity.class);
    ArgumentCaptor<Supplier<String>> logCaptor = ArgumentCaptor.forClass(Supplier.class);
    verify(auditLogger).log(entityCaptor.capture(), logCaptor.capture());

    IAuditEntity entity = entityCaptor.getValue();
    assertEquals(7L, entity.getUserId());
    assertEquals("operator", entity.getUsername());
    assertEquals("127.0.0.1", entity.getCliHostname());
    assertEquals(AuditEventType.GRANT_ROLE_FAILED, entity.getAuditEventType());
    assertEquals(AuditLogOperation.CONTROL, entity.getAuditLogOperation());
    assertEquals(PrivilegeType.SECURITY, entity.getPrivilegeTypes().get(0));
    assertFalse(entity.getResult());
    assertEquals("database", entity.getDatabase());
    assertEquals("GRANT ROLE role1 TO user1", entity.getSqlString());
    assertEquals("user1", logCaptor.getValue().get());
  }
}
