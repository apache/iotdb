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

package org.apache.iotdb.db.queryengine.plan.relational.sql.parser;

import org.apache.iotdb.commons.auth.entity.PrivilegeModelType;
import org.apache.iotdb.commons.auth.entity.PrivilegeType;
import org.apache.iotdb.db.protocol.session.IClientSession;
import org.apache.iotdb.db.protocol.session.InternalClientSession;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.RelationalAuthorStatement;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Statement;
import org.apache.iotdb.db.queryengine.plan.relational.type.AuthorRType;

import org.junit.Test;

import java.time.ZonedDateTime;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

public class AuthorStatementTest {
  private SqlParser parser = new SqlParser();
  IClientSession clientSession = new InternalClientSession("internal");
  private final String databaseName = "test";
  private final String idName = "iotdb_user";

  public AuthorStatementTest() {
    clientSession.setDatabaseName(databaseName);
  }

  private RelationalAuthorStatement createAuthorStatement(String sql) {
    Statement stmt = parser.createStatement(sql, ZonedDateTime.now().getOffset(), clientSession);
    return (RelationalAuthorStatement) stmt;
  }

  private void checkNullStatementFields(RelationalAuthorStatement authorStatement) {
    assertNull(authorStatement.getDatabase());
    assertNull(authorStatement.getTableName());
    assertNull(authorStatement.getPrivilegeTypes());
    assertFalse(authorStatement.isGrantOption());
  }

  @Test
  public void testAuthorUserAndRole() {
    RelationalAuthorStatement stmt = createAuthorStatement("create user user1 'passw'");
    assertEquals(AuthorRType.CREATE_USER, stmt.getAuthorType());
    assertEquals("user1", stmt.getUserName());
    assertEquals("passw", stmt.getPassword());
    checkNullStatementFields(stmt);
    stmt = createAuthorStatement("create role role1");
    assertEquals("role1", stmt.getRoleName());
    assertEquals(AuthorRType.CREATE_ROLE, stmt.getAuthorType());
    assertNull(stmt.getUserName());
    checkNullStatementFields(stmt);
    stmt = createAuthorStatement("GRANT ROLE role1 to user1");
    assertEquals("role1", stmt.getRoleName());
    assertEquals("user1", stmt.getUserName());
    assertEquals(AuthorRType.GRANT_USER_ROLE, stmt.getAuthorType());
    checkNullStatementFields(stmt);
    stmt = createAuthorStatement("REVOKE ROLE role1 from user1");
    assertEquals("role1", stmt.getRoleName());
    assertEquals("user1", stmt.getUserName());
    assertEquals(AuthorRType.REVOKE_USER_ROLE, stmt.getAuthorType());
    checkNullStatementFields(stmt);

    stmt = createAuthorStatement("DROP USER user1");
    assertEquals("user1", stmt.getUserName());
    assertEquals(AuthorRType.DROP_USER, stmt.getAuthorType());
    checkNullStatementFields(stmt);

    stmt = createAuthorStatement("DROP ROLE role1");
    assertEquals("role1", stmt.getRoleName());
    assertEquals(AuthorRType.DROP_ROLE, stmt.getAuthorType());
    checkNullStatementFields(stmt);
  }

  private void checkRevokePrivileges(
      boolean isUser,
      List<PrivilegeType> privilegeTypeList,
      String scope,
      boolean isGrantOption,
      boolean checkCatch) {

    StringBuilder privileges = new StringBuilder();
    for (int i = 0; i < privilegeTypeList.size(); i++) {
      privileges.append(privilegeTypeList.get(i).toString());
      if (i != privilegeTypeList.size() - 1) {
        privileges.append(",");
      }
    }
    String sql =
        String.format(
            "REVOKE %s %s %s FROM %s %s",
            isGrantOption ? "GRANT OPTION FOR" : "",
            privileges,
            scope.isEmpty() ? "" : " on " + scope,
            isUser ? "USER" : "ROLE",
            idName);
    if (checkCatch) {
      assertThrows(ParsingException.class, () -> createAuthorStatement(sql));
      return;
    }
    RelationalAuthorStatement stmt = createAuthorStatement(sql);
    PrivilegeModelType modelType = PrivilegeModelType.INVALID;
    int ind = 0;
    for (PrivilegeType privilegeType : stmt.getPrivilegeTypes()) {
      if (ind == 0) {
        modelType = privilegeType.getModelType();
      }
      assertEquals(modelType, privilegeType.getModelType());
      ind++;
    }

    AuthorRType authorRType = AuthorRType.CREATE_ROLE;
    if (scope.toLowerCase().contains("database")) {
      authorRType = isUser ? AuthorRType.REVOKE_USER_DB : AuthorRType.REVOKE_ROLE_DB;
      assertTrue(scope.contains(stmt.getDatabase()));
      assertEquals("", stmt.getTableName());
    } else if (scope.toLowerCase().contains("table")) {
      authorRType = isUser ? AuthorRType.REVOKE_USER_TB : AuthorRType.REVOKE_ROLE_TB;
      assertTrue(scope.contains(stmt.getTableName()));
      assertEquals(databaseName, stmt.getDatabase());
    } else if (scope.toLowerCase().contains(".")) {
      authorRType = isUser ? AuthorRType.REVOKE_USER_TB : AuthorRType.REVOKE_ROLE_TB;
      assertTrue(scope.toLowerCase().contains(stmt.getTableName()));
      assertTrue(scope.toLowerCase().contains(stmt.getDatabase()));
    } else if (scope.toLowerCase().contains("any")) {
      authorRType = isUser ? AuthorRType.REVOKE_USER_ANY : AuthorRType.REVOKE_ROLE_ANY;
    }
    if (scope.isEmpty()) {
      authorRType = isUser ? AuthorRType.REVOKE_USER_SYS : AuthorRType.REVOKE_ROLE_SYS;
    }

    assertEquals(authorRType, stmt.getAuthorType());
    assertEquals(isUser ? idName : "", stmt.getUserName());
    assertEquals(isUser ? "" : idName, stmt.getRoleName());
    Set<PrivilegeType> privilegeTypes = stmt.getPrivilegeTypes();
    Set<PrivilegeType> privilegeTypeSet = new HashSet<>(privilegeTypeList);
    assertEquals(privilegeTypes, privilegeTypeSet);
    assertNull(stmt.getPassword());
    assertEquals(isGrantOption, stmt.isGrantOption());
  }

  private void checkGrantPrivileges(
      boolean isUser,
      List<PrivilegeType> privilegeTypeList,
      String scope,
      boolean isGrantOption,
      boolean checkCatch) {
    StringBuilder privileges = new StringBuilder();
    for (int i = 0; i < privilegeTypeList.size(); i++) {
      privileges.append(privilegeTypeList.get(i).toString());
      if (i != privilegeTypeList.size() - 1) {
        privileges.append(",");
      }
    }
    String sql =
        String.format(
            "GRANT %s %s TO %s %s %s",
            privileges,
            scope.isEmpty() ? "" : " on " + scope,
            isUser ? "USER" : "ROLE",
            idName,
            isGrantOption ? "WITH GRANT OPTION" : "");
    if (checkCatch) {
      assertThrows(ParsingException.class, () -> createAuthorStatement(sql));
      return;
    }
    RelationalAuthorStatement stmt = createAuthorStatement(sql);
    PrivilegeModelType modelType = PrivilegeModelType.INVALID;
    // 1. All privileges are same model.
    int ind = 0;
    for (PrivilegeType privilegeType : stmt.getPrivilegeTypes()) {
      if (ind == 0) {
        modelType = privilegeType.getModelType();
      }
      assertEquals(modelType, privilegeType.getModelType());
      ind++;
    }
    AuthorRType authType = AuthorRType.CREATE_ROLE;
    if (scope.toLowerCase().contains("database")) {
      authType = isUser ? AuthorRType.GRANT_USER_DB : AuthorRType.GRANT_ROLE_DB;
      assertTrue(scope.contains(stmt.getDatabase()));
      assertEquals("", stmt.getTableName());
    } else if (scope.toLowerCase().contains("table")) {
      authType = isUser ? AuthorRType.GRANT_USER_TB : AuthorRType.GRANT_ROLE_TB;
      assertTrue(scope.contains(stmt.getTableName()));
      assertEquals(databaseName, stmt.getDatabase());
    } else if (scope.toLowerCase().contains(".")) {
      authType = isUser ? AuthorRType.GRANT_USER_TB : AuthorRType.GRANT_ROLE_TB;
      assertTrue(scope.toLowerCase().contains(stmt.getTableName()));
      assertTrue(scope.toLowerCase().contains(stmt.getDatabase()));
    } else if (scope.toLowerCase().contains("any")) {
      authType = isUser ? AuthorRType.GRANT_USER_ANY : AuthorRType.GRANT_ROLE_ANY;
    }
    if (scope.isEmpty()) {
      authType = isUser ? AuthorRType.GRANT_USER_SYS : AuthorRType.GRANT_ROLE_SYS;
    }
    assertEquals(authType, stmt.getAuthorType());
    assertEquals(isUser ? idName : "", stmt.getUserName());
    assertEquals(isUser ? "" : idName, stmt.getRoleName());
    Set<PrivilegeType> privilegeTypes = stmt.getPrivilegeTypes();
    Set<PrivilegeType> privilegeTypeSet = new HashSet<>(privilegeTypeList);
    assertEquals(privilegeTypeSet, privilegeTypes);
    assertNull(stmt.getPassword());
    assertEquals(isGrantOption, stmt.isGrantOption());
  }

  @Test
  public void testGrantStatement() {
    // System privileges
    checkGrantPrivileges(true, Collections.singletonList(PrivilegeType.SECURITY), "", true, false);
    checkGrantPrivileges(true, Collections.singletonList(PrivilegeType.SECURITY), "", false, false);
    checkGrantPrivileges(
        true, Arrays.asList(PrivilegeType.SYSTEM, PrivilegeType.SECURITY), "", true, false);
    checkGrantPrivileges(
        true, Collections.singletonList(PrivilegeType.SECURITY), "ANY", true, true);

    // Illegal privilege
    checkGrantPrivileges(
        true, Collections.singletonList(PrivilegeType.MANAGE_DATABASE), "", true, true);

    // Relational privileges
    checkGrantPrivileges(
        true, Collections.singletonList(PrivilegeType.CREATE), "Database testdb", true, false);
    checkGrantPrivileges(
        false, Collections.singletonList(PrivilegeType.SELECT), "Table testtb", true, false);
    checkGrantPrivileges(
        true, Collections.singletonList(PrivilegeType.INSERT), "testdb.testtb", false, false);
    checkGrantPrivileges(true, Collections.singletonList(PrivilegeType.INSERT), "ANY", true, false);

    checkGrantPrivileges(
        true,
        Arrays.asList(PrivilegeType.CREATE, PrivilegeType.INSERT),
        "Database testdb",
        true,
        false);
    checkGrantPrivileges(
        false,
        Arrays.asList(PrivilegeType.CREATE, PrivilegeType.INSERT),
        "Table testtb",
        true,
        false);
    checkGrantPrivileges(
        true,
        Arrays.asList(PrivilegeType.CREATE, PrivilegeType.INSERT),
        "testdb.testtb",
        false,
        false);

    // Illegal privileges combination.
    checkGrantPrivileges(
        true,
        Arrays.asList(PrivilegeType.CREATE, PrivilegeType.MANAGE_ROLE),
        "testdb.testtb",
        false,
        true);

    // test all privileges

    RelationalAuthorStatement stmt = createAuthorStatement("GRANT ALL TO USER test");
    assertEquals(AuthorRType.GRANT_USER_ALL, stmt.getAuthorType());

    stmt = createAuthorStatement("GRANT ALL TO ROLE test with grant option");
    assertEquals(AuthorRType.GRANT_ROLE_ALL, stmt.getAuthorType());
    assertTrue(stmt.isGrantOption());
  }

  @Test
  public void testRevokeStatement() {
    // System privileges
    checkRevokePrivileges(true, Collections.singletonList(PrivilegeType.SECURITY), "", true, false);
    checkRevokePrivileges(
        true, Collections.singletonList(PrivilegeType.SECURITY), "", false, false);
    checkRevokePrivileges(
        true, Arrays.asList(PrivilegeType.SYSTEM, PrivilegeType.SECURITY), "", false, false);
    checkRevokePrivileges(
        true, Arrays.asList(PrivilegeType.SYSTEM, PrivilegeType.SECURITY), "", true, false);

    // Illegal privileges combination
    checkRevokePrivileges(
        true, Arrays.asList(PrivilegeType.SECURITY, PrivilegeType.ALTER), "", false, true);

    // Relational privileges
    checkRevokePrivileges(
        true, Collections.singletonList(PrivilegeType.CREATE), "Database testdb", true, false);

    checkRevokePrivileges(
        true, Collections.singletonList(PrivilegeType.CREATE), "table testtb", false, false);

    checkRevokePrivileges(
        true, Collections.singletonList(PrivilegeType.CREATE), "testdb.testtb", false, false);

    checkRevokePrivileges(
        true,
        Arrays.asList(PrivilegeType.SELECT, PrivilegeType.INSERT),
        "testdb.testtb",
        false,
        false);

    checkRevokePrivileges(
        true,
        Arrays.asList(PrivilegeType.SELECT, PrivilegeType.SECURITY),
        "testdb.testtb",
        false,
        true);

    RelationalAuthorStatement stmt = createAuthorStatement("REVOKE ALL FROM USER test");
    assertEquals(AuthorRType.REVOKE_USER_ALL, stmt.getAuthorType());

    stmt = createAuthorStatement("REVOKE GRANT OPTION FOR ALL FROM ROLE test");
    assertEquals(AuthorRType.REVOKE_ROLE_ALL, stmt.getAuthorType());
    assertTrue(stmt.isGrantOption());
  }
}
