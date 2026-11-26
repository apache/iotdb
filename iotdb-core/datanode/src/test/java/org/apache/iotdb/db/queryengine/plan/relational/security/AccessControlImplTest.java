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

package org.apache.iotdb.db.queryengine.plan.relational.security;

import org.apache.iotdb.commons.audit.AuditEventType;
import org.apache.iotdb.commons.audit.AuditLogOperation;
import org.apache.iotdb.commons.auth.entity.PrivilegeType;
import org.apache.iotdb.commons.auth.entity.User;
import org.apache.iotdb.commons.exception.auth.AccessDeniedException;
import org.apache.iotdb.db.auth.AuthorityChecker;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyCollection;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

public class AccessControlImplTest {

  private static final List<PrivilegeType> AUDIT_PRIVILEGE =
      Collections.singletonList(PrivilegeType.AUDIT);
  private static final long USER_ID = User.INTERNAL_USER_END_ID + 1;
  private static final String USER_NAME = "user";

  @Before
  public void setUp() {
    AuthorityChecker.getAuthorityFetcher().getAuthorCache().invalidAllCache();
  }

  @After
  public void tearDown() {
    AuthorityChecker.getAuthorityFetcher().getAuthorCache().invalidAllCache();
  }

  @Test
  public void testSuperUserMissingPrivilegeCheckIsAudited() {
    ITableAuthChecker authChecker = mock(ITableAuthChecker.class);
    AccessControlImpl accessControl =
        new AccessControlImpl(authChecker, new TreeAccessCheckVisitor());
    TreeAccessCheckContext auditContext =
        new TreeAccessCheckContext(AuthorityChecker.SUPER_USER_ID, AuthorityChecker.SUPER_USER, "");

    accessControl.checkMissingPrivileges(
        AuthorityChecker.SUPER_USER, AUDIT_PRIVILEGE, auditContext);

    assertAuditContext(auditContext, true);
    verify(authChecker, never()).checkGlobalPrivileges(anyString(), anyCollection(), any());
  }

  @Test
  public void testUserWithAuditPrivilegeCheckIsAudited() {
    User user = new User(USER_NAME, "password", USER_ID);
    user.grantSysPrivilege(PrivilegeType.AUDIT, false);
    AuthorityChecker.getAuthorityFetcher().getAuthorCache().putUserCache(USER_NAME, user);
    AccessControlImpl accessControl =
        new AccessControlImpl(new ITableAuthCheckerImpl(), new TreeAccessCheckVisitor());
    TreeAccessCheckContext auditContext = new TreeAccessCheckContext(USER_ID, USER_NAME, "");

    accessControl.checkMissingPrivileges(USER_NAME, AUDIT_PRIVILEGE, auditContext);

    assertAuditContext(auditContext, true);
  }

  @Test
  public void testMissingPrivilegeFailureIsAudited() {
    User user = new User(USER_NAME, "password", USER_ID);
    AuthorityChecker.getAuthorityFetcher().getAuthorCache().putUserCache(USER_NAME, user);
    AccessControlImpl accessControl =
        new AccessControlImpl(new ITableAuthCheckerImpl(), new TreeAccessCheckVisitor());
    TreeAccessCheckContext auditContext = new TreeAccessCheckContext(USER_ID, USER_NAME, "");

    assertThrows(
        AccessDeniedException.class,
        () -> accessControl.checkMissingPrivileges(USER_NAME, AUDIT_PRIVILEGE, auditContext));

    assertAuditContext(auditContext, false);
  }

  private static void assertAuditContext(
      TreeAccessCheckContext auditContext, boolean expectedResult) {
    assertEquals(AuditEventType.OBJECT_AUTHENTICATION, auditContext.getAuditEventType());
    assertEquals(AuditLogOperation.CONTROL, auditContext.getAuditLogOperation());
    assertEquals(AUDIT_PRIVILEGE, auditContext.getPrivilegeTypes());
    if (expectedResult) {
      assertTrue(auditContext.getResult());
    } else {
      assertFalse(auditContext.getResult());
    }
  }
}
