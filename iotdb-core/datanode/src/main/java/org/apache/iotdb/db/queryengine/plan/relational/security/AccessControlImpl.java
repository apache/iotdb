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

import org.apache.iotdb.commons.auth.entity.PrivilegeType;
import org.apache.iotdb.commons.exception.auth.AccessDeniedException;
import org.apache.iotdb.commons.schema.table.InformationSchema;
import org.apache.iotdb.db.auth.AuthorityChecker;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.QualifiedObjectName;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.RelationalAuthorStatement;
import org.apache.iotdb.db.queryengine.plan.relational.type.AuthorRType;

import java.util.Objects;

import static org.apache.iotdb.db.auth.AuthorityChecker.ONLY_ADMIN_ALLOWED;

public class AccessControlImpl implements AccessControl {

  private final ITableAuthChecker authChecker;

  public AccessControlImpl(ITableAuthChecker authChecker) {
    this.authChecker = authChecker;
  }

  @Override
  public void checkCanCreateDatabase(String userName, String databaseName) {
    authChecker.checkDatabasePrivilege(userName, databaseName, TableModelPrivilege.CREATE);
  }

  @Override
  public void checkCanDropDatabase(String userName, String databaseName) {
    authChecker.checkDatabasePrivilege(userName, databaseName, TableModelPrivilege.DROP);
  }

  @Override
  public void checkCanAlterDatabase(String userName, String databaseName) {
    authChecker.checkDatabasePrivilege(userName, databaseName, TableModelPrivilege.ALTER);
  }

  @Override
  public void checkCanShowOrUseDatabase(String userName, String databaseName) {
    authChecker.checkDatabaseVisibility(userName, databaseName);
  }

  @Override
  public void checkCanCreateTable(String userName, QualifiedObjectName tableName) {
    authChecker.checkTablePrivilege(userName, tableName, TableModelPrivilege.CREATE);
  }

  @Override
  public void checkCanDropTable(String userName, QualifiedObjectName tableName) {
    authChecker.checkTablePrivilege(userName, tableName, TableModelPrivilege.DROP);
  }

  @Override
  public void checkCanAlterTable(String userName, QualifiedObjectName tableName) {
    authChecker.checkTablePrivilege(userName, tableName, TableModelPrivilege.ALTER);
  }

  @Override
  public void checkCanInsertIntoTable(String userName, QualifiedObjectName tableName) {
    authChecker.checkTablePrivilege(userName, tableName, TableModelPrivilege.INSERT);
  }

  @Override
  public void checkCanSelectFromTable(String userName, QualifiedObjectName tableName) {
    if (tableName.getDatabaseName().equals(InformationSchema.INFORMATION_DATABASE)) {
      // Currently only root user can select from information schema
      checkUserIsAdmin(userName);
      return;
    }
    authChecker.checkTablePrivilege(userName, tableName, TableModelPrivilege.SELECT);
  }

  @Override
  public void checkCanSelectFromDatabase4Pipe(final String userName, final String databaseName) {
    if (Objects.isNull(userName)) {
      throw new AccessDeniedException("User not exists");
    }
    authChecker.checkDatabasePrivilege(userName, databaseName, TableModelPrivilege.SELECT);
  }

  @Override
  public boolean checkCanSelectFromTable4Pipe(
      final String userName, final QualifiedObjectName tableName) {
    return Objects.nonNull(userName) && authChecker.checkTablePrivilege4Pipe(userName, tableName);
  }

  @Override
  public void checkCanDeleteFromTable(String userName, QualifiedObjectName tableName) {
    authChecker.checkTablePrivilege(userName, tableName, TableModelPrivilege.DELETE);
  }

  @Override
  public void checkCanShowOrDescTable(String userName, QualifiedObjectName tableName) {
    authChecker.checkTableVisibility(userName, tableName);
  }

  @Override
  public void checkUserCanRunRelationalAuthorStatement(
      String userName, RelationalAuthorStatement statement) {
    AuthorRType type = statement.getAuthorType();
    switch (type) {
      case CREATE_USER:
        // admin cannot be created.
        if (AuthorityChecker.SUPER_USER.equals(statement.getUserName())) {
          throw new AccessDeniedException("Cannot create user has same name with admin user");
        }
        if (AuthorityChecker.SUPER_USER.equals(userName)) {
          return;
        }
        authChecker.checkGlobalPrivilege(userName, TableModelPrivilege.MANAGE_USER);
        return;
      case DROP_USER:
        if (AuthorityChecker.SUPER_USER.equals(statement.getUserName())
            || statement.getUserName().equals(userName)) {
          throw new AccessDeniedException("Cannot drop admin user or yourself");
        }
        if (AuthorityChecker.SUPER_USER.equals(userName)) {
          return;
        }
        authChecker.checkGlobalPrivilege(userName, TableModelPrivilege.MANAGE_USER);
        return;
      case UPDATE_USER:
      case LIST_USER_PRIV:
        if (AuthorityChecker.SUPER_USER.equals(userName)
            || statement.getUserName().equals(userName)) {
          return;
        }
        authChecker.checkGlobalPrivilege(userName, TableModelPrivilege.MANAGE_USER);
        return;
      case LIST_USER:
        if (AuthorityChecker.SUPER_USER.equals(userName)) {
          return;
        }
        authChecker.checkGlobalPrivilege(userName, TableModelPrivilege.MANAGE_USER);
        return;
      case CREATE_ROLE:
        if (AuthorityChecker.SUPER_USER.equals(statement.getRoleName())) {
          throw new AccessDeniedException("Cannot create role has same name with admin user");
        }
        if (AuthorityChecker.SUPER_USER.equals(userName)) {
          return;
        }
        authChecker.checkGlobalPrivilege(userName, TableModelPrivilege.MANAGE_ROLE);
        return;

      case DROP_ROLE:
        if (AuthorityChecker.SUPER_USER.equals(statement.getUserName())) {
          throw new AccessDeniedException("Cannot drop role with admin name");
        }
        if (AuthorityChecker.SUPER_USER.equals(userName)) {
          return;
        }
        authChecker.checkGlobalPrivilege(userName, TableModelPrivilege.MANAGE_ROLE);
        return;

      case GRANT_USER_ROLE:
        if (AuthorityChecker.SUPER_USER.equals(statement.getUserName())) {
          throw new AccessDeniedException("Cannot grant role to admin");
        }
        if (AuthorityChecker.SUPER_USER.equals(userName)) {
          return;
        }
        authChecker.checkGlobalPrivilege(userName, TableModelPrivilege.MANAGE_ROLE);
        return;

      case REVOKE_USER_ROLE:
        if (AuthorityChecker.SUPER_USER.equals(statement.getUserName())) {
          throw new AccessDeniedException("Cannot revoke role from admin");
        }
        if (AuthorityChecker.SUPER_USER.equals(userName)) {
          return;
        }
        authChecker.checkGlobalPrivilege(userName, TableModelPrivilege.MANAGE_ROLE);
        return;
      case LIST_ROLE:
        if (AuthorityChecker.SUPER_USER.equals(userName)) {
          return;
        }

        // user can list his roles.
        if (statement.getUserName() != null && statement.getUserName().equals(userName)) {
          return;
        }
        authChecker.checkGlobalPrivilege(userName, TableModelPrivilege.MANAGE_ROLE);
        return;
      case LIST_ROLE_PRIV:
        if (AuthorityChecker.SUPER_USER.equals(userName)) {
          return;
        }
        if (AuthorityChecker.checkRole(userName, statement.getRoleName())) {
          return;
        }
        authChecker.checkGlobalPrivilege(userName, TableModelPrivilege.MANAGE_ROLE);
        return;
      case GRANT_ROLE_ANY:
      case GRANT_USER_ANY:
      case REVOKE_ROLE_ANY:
      case REVOKE_USER_ANY:
        if (AuthorityChecker.SUPER_USER.equals(statement.getUserName())) {
          throw new AccessDeniedException("Cannot grant/revoke privileges of admin user");
        }
        if (AuthorityChecker.SUPER_USER.equals(userName)) {
          return;
        }
        for (PrivilegeType privilegeType : statement.getPrivilegeTypes()) {
          authChecker.checkAnyScopePrivilegeGrantOption(
              userName, TableModelPrivilege.getTableModelType(privilegeType));
        }
        return;
      case GRANT_ROLE_ALL:
      case REVOKE_ROLE_ALL:
      case GRANT_USER_ALL:
      case REVOKE_USER_ALL:
        if (AuthorityChecker.SUPER_USER.equals(statement.getUserName())) {
          throw new AccessDeniedException("Cannot grant/revoke all privileges of admin user");
        }
        if (AuthorityChecker.SUPER_USER.equals(userName)) {
          return;
        }
        for (PrivilegeType privilegeType : statement.getPrivilegeTypes()) {
          if (privilegeType.isRelationalPrivilege()) {
            AuthorityChecker.checkAnyScopePermissionGrantOption(userName, privilegeType);
          }
          if (privilegeType.forRelationalSys()) {
            AuthorityChecker.checkSystemPermissionGrantOption(userName, privilegeType);
          }
        }
        return;
      case GRANT_USER_DB:
      case GRANT_ROLE_DB:
      case REVOKE_USER_DB:
      case REVOKE_ROLE_DB:
        if (AuthorityChecker.SUPER_USER.equals(statement.getUserName())) {
          throw new AccessDeniedException("Cannot grant/revoke privileges of admin user");
        }
        if (AuthorityChecker.SUPER_USER.equals(userName)) {
          return;
        }
        for (PrivilegeType privilegeType : statement.getPrivilegeTypes()) {
          authChecker.checkDatabasePrivilegeGrantOption(
              userName,
              statement.getDatabase(),
              TableModelPrivilege.getTableModelType(privilegeType));
        }
        return;
      case GRANT_USER_TB:
      case GRANT_ROLE_TB:
      case REVOKE_USER_TB:
      case REVOKE_ROLE_TB:
        if (AuthorityChecker.SUPER_USER.equals(statement.getUserName())) {
          throw new AccessDeniedException("Cannot grant/revoke privileges of admin user");
        }
        if (AuthorityChecker.SUPER_USER.equals(userName)) {
          return;
        }
        for (PrivilegeType privilegeType : statement.getPrivilegeTypes()) {
          authChecker.checkTablePrivilegeGrantOption(
              userName,
              new QualifiedObjectName(statement.getDatabase(), statement.getTableName()),
              TableModelPrivilege.getTableModelType(privilegeType));
        }
        return;

      case GRANT_USER_SYS:
      case GRANT_ROLE_SYS:
      case REVOKE_USER_SYS:
      case REVOKE_ROLE_SYS:
        if (AuthorityChecker.SUPER_USER.equals(statement.getUserName())) {
          throw new AccessDeniedException("Cannot grant/revoke privileges of admin user");
        }
        if (AuthorityChecker.SUPER_USER.equals(userName)) {
          return;
        }
        for (PrivilegeType privilegeType : statement.getPrivilegeTypes()) {
          authChecker.checkGlobalPrivilegeGrantOption(
              userName, TableModelPrivilege.getTableModelType(privilegeType));
        }
        break;
      default:
        break;
    }
  }

  @Override
  public void checkUserIsAdmin(String userName) {
    if (!AuthorityChecker.SUPER_USER.equals(userName)) {
      throw new AccessDeniedException(ONLY_ADMIN_ALLOWED);
    }
  }
}
