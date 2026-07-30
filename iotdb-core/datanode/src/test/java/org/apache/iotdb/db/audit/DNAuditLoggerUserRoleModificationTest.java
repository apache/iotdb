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

import org.apache.iotdb.commons.audit.AuditEventType;
import org.apache.iotdb.commons.audit.AuditLogOperation;
import org.apache.iotdb.commons.audit.IAuditEntity;
import org.apache.iotdb.commons.audit.UserEntity;
import org.apache.iotdb.commons.auth.entity.PrivilegeType;
import org.apache.iotdb.commons.queryengine.common.SessionInfo;
import org.apache.iotdb.commons.queryengine.common.SqlDialect;
import org.apache.iotdb.db.queryengine.plan.relational.security.TreeAccessCheckContext;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.RelationalAuthorStatement;
import org.apache.iotdb.db.queryengine.plan.relational.type.AuthorRType;
import org.apache.iotdb.db.queryengine.plan.statement.AuthorType;
import org.apache.iotdb.db.queryengine.plan.statement.sys.AuthorStatement;
import org.apache.iotdb.rpc.RpcUtils;
import org.apache.iotdb.rpc.TSStatusCode;

import org.junit.Test;
import org.mockito.ArgumentCaptor;

import java.time.ZoneId;
import java.util.function.Supplier;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
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
    AuthorStatement statement = new AuthorStatement(AuthorType.GRANT_USER_ROLE);
    statement.setUserName("user1");
    statement.setRoleName("role1");

    auditLogger.logUserRoleModification(statement, sessionInfo(), sql, RpcUtils.SUCCESS_STATUS);

    assertAuditLog(auditLogger, true, sql);
  }

  @Test
  public void testTableRevokeRoleFailure() {
    DNAuditLogger auditLogger = mock(DNAuditLogger.class, CALLS_REAL_METHODS);
    String sql = "REVOKE ROLE role1 FROM user1";
    RelationalAuthorStatement statement =
        new RelationalAuthorStatement(AuthorRType.REVOKE_USER_ROLE);
    statement.setUserName("user1");
    statement.setRoleName("role1");

    auditLogger.logUserRoleModification(
        statement, sessionInfo(), sql, RpcUtils.getStatus(TSStatusCode.EXECUTE_STATEMENT_ERROR));

    assertAuditLog(auditLogger, false, sql);
  }

  @Test
  public void testTreeAuthorizationFailure() {
    DNAuditLogger auditLogger = mock(DNAuditLogger.class, CALLS_REAL_METHODS);
    String sql = "GRANT ROLE role1 TO user1";
    AuthorStatement statement = new AuthorStatement(AuthorType.GRANT_USER_ROLE);
    statement.setUserName("user1");
    statement.setRoleName("role1");

    auditLogger.logUserRoleModificationAuthorizationFailure(
        statement, treeAuditEntity(sql), RpcUtils.getStatus(TSStatusCode.NO_PERMISSION));

    assertAuditLog(auditLogger, false, sql);
  }

  @Test
  public void testSuccessfulAuthorizationIsLoggedAfterExecutionOnly() {
    DNAuditLogger auditLogger = mock(DNAuditLogger.class, CALLS_REAL_METHODS);
    AuthorStatement statement = new AuthorStatement(AuthorType.GRANT_USER_ROLE);
    statement.setUserName("user1");
    statement.setRoleName("role1");

    auditLogger.logUserRoleModificationAuthorizationFailure(
        statement, treeAuditEntity("grant role"), RpcUtils.SUCCESS_STATUS);

    verify(auditLogger, never()).log(any(), any());
  }

  @Test
  public void testRedirectIsSuccessfulModification() {
    DNAuditLogger auditLogger = mock(DNAuditLogger.class, CALLS_REAL_METHODS);
    AuthorStatement statement = new AuthorStatement(AuthorType.REVOKE_USER_ROLE);
    statement.setUserName("user1");
    statement.setRoleName("role1");

    auditLogger.logUserRoleModification(
        statement,
        sessionInfo(),
        "revoke role",
        RpcUtils.getStatus(TSStatusCode.REDIRECTION_RECOMMEND));

    assertAuditLog(auditLogger, true, "revoke role");
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
    AuthorStatement statement = new AuthorStatement(AuthorType.GRANT_USER_ROLE);
    statement.setUserName("user1");
    statement.setRoleName("role1");

    auditLogger.logUserRoleModification(statement, null, "grant role", null);

    verify(auditLogger, never()).log(any(), any());
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
  private static void assertAuditLog(DNAuditLogger auditLogger, boolean result, String sql) {
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
