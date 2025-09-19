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

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.auth.entity.PrivilegeType;
import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.exception.auth.AccessDeniedException;
import org.apache.iotdb.commons.path.MeasurementPath;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.schema.table.InformationSchema;
import org.apache.iotdb.db.auth.AuthorityChecker;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.QualifiedObjectName;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.RelationalAuthorStatement;
import org.apache.iotdb.db.queryengine.plan.relational.type.AuthorRType;
import org.apache.iotdb.db.queryengine.plan.statement.Statement;
import org.apache.iotdb.db.queryengine.plan.statement.StatementVisitor;
import org.apache.iotdb.db.schemaengine.table.InformationSchemaUtils;
import org.apache.iotdb.rpc.TSStatusCode;

import org.apache.tsfile.file.metadata.IDeviceID;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

import static org.apache.iotdb.commons.schema.table.Audit.TABLE_MODEL_AUDIT_DATABASE;
import static org.apache.iotdb.commons.schema.table.Audit.includeByAuditTreeDB;
import static org.apache.iotdb.db.auth.AuthorityChecker.ONLY_ADMIN_ALLOWED;
import static org.apache.iotdb.db.auth.AuthorityChecker.SUCCEED;
import static org.apache.iotdb.db.queryengine.plan.relational.security.ITableAuthCheckerImpl.checkCanSelectAuditTable;

public class AccessControlImpl implements AccessControl {

  public static final String READ_ONLY_DB_ERROR_MSG = "The database '%s' is read-only.";

  private final ITableAuthChecker authChecker;

  private final StatementVisitor<TSStatus, TreeAccessCheckContext> treeAccessCheckVisitor;

  public AccessControlImpl(
      ITableAuthChecker authChecker, StatementVisitor<TSStatus, TreeAccessCheckContext> visitor) {
    this.authChecker = authChecker;
    this.treeAccessCheckVisitor = visitor;
  }

  private void checkAuditDatabase(String databaseName) {
    if (TABLE_MODEL_AUDIT_DATABASE.equalsIgnoreCase(databaseName)) {
      throw new AccessDeniedException(
          String.format(READ_ONLY_DB_ERROR_MSG, TABLE_MODEL_AUDIT_DATABASE));
    }
  }

  @Override
  public void checkCanCreateDatabase(String userName, String databaseName) {
    InformationSchemaUtils.checkDBNameInWrite(databaseName);
    authChecker.checkDatabasePrivilege(userName, databaseName, TableModelPrivilege.CREATE);
  }

  @Override
  public void checkCanDropDatabase(String userName, String databaseName) {
    InformationSchemaUtils.checkDBNameInWrite(databaseName);
    authChecker.checkDatabasePrivilege(userName, databaseName, TableModelPrivilege.DROP);
  }

  @Override
  public void checkCanAlterDatabase(String userName, String databaseName) {
    InformationSchemaUtils.checkDBNameInWrite(databaseName);
    authChecker.checkDatabasePrivilege(userName, databaseName, TableModelPrivilege.ALTER);
  }

  @Override
  public void checkCanShowOrUseDatabase(String userName, String databaseName) {
    authChecker.checkDatabaseVisibility(userName, databaseName);
  }

  @Override
  public void checkCanCreateTable(String userName, QualifiedObjectName tableName) {
    InformationSchemaUtils.checkDBNameInWrite(tableName.getDatabaseName());
    checkAuditDatabase(tableName.getDatabaseName());
    if (hasGlobalPrivilege(userName, PrivilegeType.SYSTEM)) {
      return;
    }
    authChecker.checkTablePrivilege(userName, tableName, TableModelPrivilege.CREATE);
  }

  @Override
  public void checkCanDropTable(String userName, QualifiedObjectName tableName) {
    InformationSchemaUtils.checkDBNameInWrite(tableName.getDatabaseName());
    checkAuditDatabase(tableName.getDatabaseName());
    if (hasGlobalPrivilege(userName, PrivilegeType.SYSTEM)) {
      return;
    }
    authChecker.checkTablePrivilege(userName, tableName, TableModelPrivilege.DROP);
  }

  @Override
  public void checkCanAlterTable(String userName, QualifiedObjectName tableName) {
    InformationSchemaUtils.checkDBNameInWrite(tableName.getDatabaseName());
    checkAuditDatabase(tableName.getDatabaseName());
    if (hasGlobalPrivilege(userName, PrivilegeType.SYSTEM)) {
      return;
    }
    authChecker.checkTablePrivilege(userName, tableName, TableModelPrivilege.ALTER);
  }

  @Override
  public void checkCanInsertIntoTable(String userName, QualifiedObjectName tableName) {
    InformationSchemaUtils.checkDBNameInWrite(tableName.getDatabaseName());
    checkAuditDatabase(tableName.getDatabaseName());
    authChecker.checkTablePrivilege(userName, tableName, TableModelPrivilege.INSERT);
  }

  @Override
  public void checkCanSelectFromTable(String userName, QualifiedObjectName tableName) {
    if (tableName.getDatabaseName().equals(InformationSchema.INFORMATION_DATABASE)) {
      return;
    }
    if (TABLE_MODEL_AUDIT_DATABASE.equalsIgnoreCase(tableName.getDatabaseName())) {
      checkCanSelectAuditTable(userName);
    } else {
      authChecker.checkTablePrivilege(userName, tableName, TableModelPrivilege.SELECT);
    }
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
    InformationSchemaUtils.checkDBNameInWrite(tableName.getDatabaseName());
    checkAuditDatabase(tableName.getDatabaseName());
    authChecker.checkTablePrivilege(userName, tableName, TableModelPrivilege.DELETE);
  }

  @Override
  public void checkCanShowOrDescTable(String userName, QualifiedObjectName tableName) {
    // Information_schema is visible to any user
    if (tableName.getDatabaseName().equals(InformationSchema.INFORMATION_DATABASE)) {
      return;
    }
    authChecker.checkTableVisibility(userName, tableName);
  }

  @Override
  public void checkCanCreateViewFromTreePath(final String userName, final PartialPath path) {
    if (AuthorityChecker.SUPER_USER.equals(userName)) {
      return;
    }

    if (includeByAuditTreeDB(path)) {
      checkCanSelectAuditTable(userName);
    }

    TSStatus status =
        AuthorityChecker.getTSStatus(
            AuthorityChecker.checkFullPathOrPatternPermission(
                userName, path, PrivilegeType.READ_SCHEMA),
            PrivilegeType.READ_SCHEMA);
    if (status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      throw new AccessDeniedException(status.getMessage());
    }

    status =
        AuthorityChecker.getTSStatus(
            AuthorityChecker.checkFullPathOrPatternPermission(
                userName, path, PrivilegeType.READ_DATA),
            PrivilegeType.READ_DATA);
    if (status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      throw new AccessDeniedException(status.getMessage());
    }
  }

  @Override
  public void checkUserCanRunRelationalAuthorStatement(
      String userName, RelationalAuthorStatement statement) {
    AuthorRType type = statement.getAuthorType();
    switch (type) {
      case CREATE_USER:
        if (AuthorityChecker.SUPER_USER.equals(userName)) {
          return;
        }
        authChecker.checkGlobalPrivilege(userName, TableModelPrivilege.MANAGE_USER);
        return;
      case DROP_USER:
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
        if (!hasGlobalPrivilege(userName, PrivilegeType.MANAGE_USER)) {
          statement.setUserName(userName);
        }
        return;
      case CREATE_ROLE:
        if (AuthorityChecker.SUPER_USER.equals(userName)) {
          return;
        }
        authChecker.checkGlobalPrivilege(userName, TableModelPrivilege.MANAGE_ROLE);
        return;

      case DROP_ROLE:
        if (AuthorityChecker.SUPER_USER.equals(userName)) {
          return;
        }
        authChecker.checkGlobalPrivilege(userName, TableModelPrivilege.MANAGE_ROLE);
        return;

      case GRANT_USER_ROLE:
        if (AuthorityChecker.SUPER_USER.equals(userName)) {
          return;
        }
        authChecker.checkGlobalPrivilege(userName, TableModelPrivilege.MANAGE_ROLE);
        return;

      case REVOKE_USER_ROLE:
        if (AuthorityChecker.SUPER_USER.equals(userName)) {
          return;
        }
        authChecker.checkGlobalPrivilege(userName, TableModelPrivilege.MANAGE_ROLE);
        return;
      case LIST_ROLE:
        if (statement.getUserName() != null && !statement.getUserName().equals(userName)) {
          authChecker.checkGlobalPrivilege(userName, TableModelPrivilege.MANAGE_ROLE);
          return;
        }
        if (!hasGlobalPrivilege(userName, PrivilegeType.MANAGE_ROLE)) {
          statement.setUserName(userName);
        }
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
        if (hasGlobalPrivilege(userName, PrivilegeType.SECURITY)) {
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
        if (hasGlobalPrivilege(userName, PrivilegeType.SECURITY)) {
          return;
        }
        for (TableModelPrivilege privilege : TableModelPrivilege.values()) {
          PrivilegeType privilegeType = privilege.getPrivilegeType();
          if (privilegeType.isRelationalPrivilege()) {
            authChecker.checkAnyScopePrivilegeGrantOption(userName, privilege);
          }
          if (privilegeType.forRelationalSys()) {
            authChecker.checkGlobalPrivilegeGrantOption(userName, privilege);
          }
        }
        return;
      case GRANT_USER_DB:
      case GRANT_ROLE_DB:
      case REVOKE_USER_DB:
      case REVOKE_ROLE_DB:
        if (hasGlobalPrivilege(userName, PrivilegeType.SECURITY)) {
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
        if (hasGlobalPrivilege(userName, PrivilegeType.SECURITY)) {
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
        if (hasGlobalPrivilege(userName, PrivilegeType.SECURITY)) {
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

  @Override
  public void checkUserGlobalSysPrivilege(String userName) {
    if (!AuthorityChecker.SUPER_USER.equals(userName)) {
      authChecker.checkGlobalPrivilege(userName, TableModelPrivilege.SYSTEM);
    }
  }

  @Override
  public boolean hasGlobalPrivilege(String userName, PrivilegeType privilegeType) {
    return AuthorityChecker.SUPER_USER.equals(userName)
        || AuthorityChecker.checkSystemPermission(userName, privilegeType);
  }

  @Override
  public void checkMissingPrivileges(String username, Collection<PrivilegeType> privilegeTypes) {
    if (AuthorityChecker.SUPER_USER.equals(username)) {
      return;
    }
    authChecker.checkGlobalPrivileges(username, privilegeTypes);
  }

  @Override
  public TSStatus checkPermissionBeforeProcess(Statement statement, String userName) {
    return treeAccessCheckVisitor.process(statement, new TreeAccessCheckContext(userName));
  }

  @Override
  public TSStatus checkFullPathWriteDataPermission(
      String userName, IDeviceID device, String measurementId) {
    if (AuthorityChecker.SUPER_USER.equals(userName)) {
      return SUCCEED;
    }
    try {
      List<PartialPath> paths =
          Collections.singletonList(new MeasurementPath(device, measurementId));
      return AuthorityChecker.getTSStatus(
          AuthorityChecker.checkFullPathOrPatternListPermission(
              userName, paths, PrivilegeType.WRITE_DATA),
          paths,
          PrivilegeType.WRITE_DATA);

    } catch (IllegalPathException e) {
      throw new RuntimeException(e);
    }
  }
}
