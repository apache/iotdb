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
import org.apache.iotdb.commons.audit.AuditLogOperation;
import org.apache.iotdb.commons.audit.IAuditEntity;
import org.apache.iotdb.commons.auth.entity.PrivilegeType;
import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.exception.auth.AccessDeniedException;
import org.apache.iotdb.commons.path.MeasurementPath;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.schema.table.InformationSchema;
import org.apache.iotdb.db.audit.DNAuditLogger;
import org.apache.iotdb.db.auth.AuthorityChecker;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.QualifiedObjectName;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.RelationalAuthorStatement;
import org.apache.iotdb.db.queryengine.plan.relational.type.AuthorRType;
import org.apache.iotdb.db.queryengine.plan.statement.Statement;
import org.apache.iotdb.db.schemaengine.table.InformationSchemaUtils;
import org.apache.iotdb.rpc.TSStatusCode;

import org.apache.tsfile.file.metadata.IDeviceID;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.function.Supplier;

import static org.apache.iotdb.commons.schema.table.Audit.TABLE_MODEL_AUDIT_DATABASE;
import static org.apache.iotdb.commons.schema.table.Audit.TREE_MODEL_AUDIT_DATABASE;
import static org.apache.iotdb.commons.schema.table.Audit.includeByAuditTreeDB;
import static org.apache.iotdb.db.auth.AuthorityChecker.ONLY_ADMIN_ALLOWED;
import static org.apache.iotdb.db.auth.AuthorityChecker.SUCCEED;
import static org.apache.iotdb.db.queryengine.plan.relational.security.ITableAuthCheckerImpl.checkCanSelectAuditTable;
import static org.apache.iotdb.db.queryengine.plan.relational.security.TreeAccessCheckVisitor.checkTimeSeriesPermission;

public class AccessControlImpl implements AccessControl {

  public static final String READ_ONLY_DB_ERROR_MSG = "The database '%s' is read-only.";

  protected final ITableAuthChecker authChecker;

  protected final TreeAccessCheckVisitor treeAccessCheckVisitor;

  public AccessControlImpl(ITableAuthChecker authChecker, TreeAccessCheckVisitor visitor) {
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
  public void checkCanCreateDatabase(
      String userName, String databaseName, IAuditEntity auditEntity) {
    InformationSchemaUtils.checkDBNameInWrite(databaseName);
    authChecker.checkDatabasePrivilege(
        userName, databaseName, TableModelPrivilege.CREATE, auditEntity.setDatabase(databaseName));
  }

  @Override
  public void checkCanDropDatabase(String userName, String databaseName, IAuditEntity auditEntity) {
    InformationSchemaUtils.checkDBNameInWrite(databaseName);
    authChecker.checkDatabasePrivilege(
        userName, databaseName, TableModelPrivilege.DROP, auditEntity.setDatabase(databaseName));
  }

  @Override
  public void checkCanAlterDatabase(
      String userName, String databaseName, IAuditEntity auditEntity) {
    InformationSchemaUtils.checkDBNameInWrite(databaseName);
    authChecker.checkDatabasePrivilege(
        userName, databaseName, TableModelPrivilege.ALTER, auditEntity.setDatabase(databaseName));
  }

  @Override
  public void checkCanShowOrUseDatabase(
      String userName, String databaseName, IAuditEntity auditEntity) {
    authChecker.checkDatabaseVisibility(
        userName, databaseName, auditEntity.setDatabase(databaseName));
  }

  @Override
  public void checkCanCreateTable(
      String userName, QualifiedObjectName tableName, IAuditEntity auditEntity) {
    auditEntity
        .setAuditLogOperation(AuditLogOperation.DDL)
        .setDatabase(tableName.getDatabaseName());
    InformationSchemaUtils.checkDBNameInWrite(tableName.getDatabaseName());
    if (userName.equals(AuthorityChecker.INTERNAL_AUDIT_USER)
        && tableName.getDatabaseName().equals(TABLE_MODEL_AUDIT_DATABASE)) {
      // The internal audit user can create new tables in the audit database
      return;
    }
    checkAuditDatabase(tableName.getDatabaseName());
    if (hasGlobalPrivilege(auditEntity, PrivilegeType.SYSTEM)) {
      DNAuditLogger.getInstance()
          .recordObjectAuthenticationAuditLog(
              auditEntity.setPrivilegeType(PrivilegeType.CREATE).setResult(true),
              tableName::getObjectName);
      return;
    }
    authChecker.checkTablePrivilege(userName, tableName, TableModelPrivilege.CREATE, auditEntity);
  }

  @Override
  public void checkCanDropTable(
      String userName, QualifiedObjectName tableName, IAuditEntity auditEntity) {
    auditEntity
        .setAuditLogOperation(AuditLogOperation.DDL)
        .setDatabase(tableName.getDatabaseName());
    InformationSchemaUtils.checkDBNameInWrite(tableName.getDatabaseName());
    checkAuditDatabase(tableName.getDatabaseName());
    if (hasGlobalPrivilege(auditEntity, PrivilegeType.SYSTEM)) {
      DNAuditLogger.getInstance()
          .recordObjectAuthenticationAuditLog(
              auditEntity.setPrivilegeType(PrivilegeType.DROP).setResult(true),
              tableName::getObjectName);
      return;
    }
    authChecker.checkTablePrivilege(userName, tableName, TableModelPrivilege.DROP, auditEntity);
  }

  @Override
  public void checkCanAlterTable(
      String userName, QualifiedObjectName tableName, IAuditEntity auditEntity) {
    InformationSchemaUtils.checkDBNameInWrite(tableName.getDatabaseName());
    checkAuditDatabase(tableName.getDatabaseName());
    if (hasGlobalPrivilege(auditEntity, PrivilegeType.SYSTEM)) {
      DNAuditLogger.getInstance()
          .recordObjectAuthenticationAuditLog(auditEntity, tableName::getObjectName);
      return;
    }
    authChecker.checkTablePrivilege(userName, tableName, TableModelPrivilege.ALTER, auditEntity);
  }

  @Override
  public void checkCanInsertIntoTable(
      String userName, QualifiedObjectName tableName, IAuditEntity auditEntity) {
    InformationSchemaUtils.checkDBNameInWrite(tableName.getDatabaseName());
    if (userName.equals(AuthorityChecker.INTERNAL_AUDIT_USER)
        && tableName.getDatabaseName().equals(TABLE_MODEL_AUDIT_DATABASE)) {
      // Only the internal audit user can insert into the audit table
      return;
    }
    checkAuditDatabase(tableName.getDatabaseName());
    authChecker.checkTablePrivilege(userName, tableName, TableModelPrivilege.INSERT, auditEntity);
  }

  @Override
  public void checkCanSelectFromTable(
      String userName, QualifiedObjectName tableName, IAuditEntity auditEntity) {
    auditEntity.setDatabase(tableName.getDatabaseName());
    if (tableName.getDatabaseName().equals(InformationSchema.INFORMATION_DATABASE)) {
      return;
    }
    if (TABLE_MODEL_AUDIT_DATABASE.equalsIgnoreCase(tableName.getDatabaseName())) {
      checkCanSelectAuditTable(auditEntity);
    } else {
      authChecker.checkTablePrivilege(userName, tableName, TableModelPrivilege.SELECT, auditEntity);
    }
  }

  @Override
  public void checkCanSelectFromDatabase4Pipe(
      final String userName, final String databaseName, IAuditEntity auditEntity) {
    if (Objects.isNull(userName)) {
      throw new AccessDeniedException("User not exists");
    }
    authChecker.checkDatabasePrivilege(
        userName, databaseName, TableModelPrivilege.SELECT, auditEntity);
  }

  @Override
  public boolean checkCanSelectFromTable4Pipe(
      final String userName, final QualifiedObjectName tableName, IAuditEntity auditEntity) {
    return Objects.nonNull(userName)
        && authChecker.checkTablePrivilege4Pipe(userName, tableName, auditEntity);
  }

  @Override
  public void checkCanDeleteFromTable(
      String userName, QualifiedObjectName tableName, IAuditEntity auditEntity) {
    InformationSchemaUtils.checkDBNameInWrite(tableName.getDatabaseName());
    checkAuditDatabase(tableName.getDatabaseName());
    authChecker.checkTablePrivilege(userName, tableName, TableModelPrivilege.DELETE, auditEntity);
  }

  @Override
  public void checkCanShowOrDescTable(
      String userName, QualifiedObjectName tableName, IAuditEntity auditEntity) {
    // Information_schema is visible to any user
    if (tableName.getDatabaseName().equals(InformationSchema.INFORMATION_DATABASE)) {
      return;
    }
    authChecker.checkTableVisibility(userName, tableName, auditEntity);
  }

  @Override
  public void checkCanCreateViewFromTreePath(final PartialPath path, IAuditEntity auditEntity) {
    if (AuthorityChecker.SUPER_USER_ID == auditEntity.getUserId()) {
      return;
    }

    if (includeByAuditTreeDB(path)) {
      checkCanSelectAuditTable(auditEntity);
    }

    TSStatus status =
        AuthorityChecker.getTSStatus(
            AuthorityChecker.checkFullPathOrPatternPermission(
                auditEntity.getUsername(), path, PrivilegeType.READ_SCHEMA),
            PrivilegeType.READ_SCHEMA);
    if (status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      throw new AccessDeniedException(status.getMessage());
    }

    status =
        AuthorityChecker.getTSStatus(
            AuthorityChecker.checkFullPathOrPatternPermission(
                auditEntity.getUsername(), path, PrivilegeType.READ_DATA),
            PrivilegeType.READ_DATA);
    if (status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      throw new AccessDeniedException(status.getMessage());
    }
  }

  @Override
  public void checkUserCanRunRelationalAuthorStatement(
      String userName, RelationalAuthorStatement statement, IAuditEntity auditEntity) {
    AuthorRType type = statement.getAuthorType();
    switch (type) {
      case CREATE_USER:
      case DROP_USER:
        auditEntity
            .setAuditLogOperation(AuditLogOperation.DDL)
            .setPrivilegeType(PrivilegeType.SECURITY);
        if (AuthorityChecker.SUPER_USER_ID == auditEntity.getUserId()) {
          DNAuditLogger.getInstance()
              .recordObjectAuthenticationAuditLog(
                  auditEntity.setResult(true), statement::getUserName);
          return;
        }
        authChecker.checkGlobalPrivilege(userName, TableModelPrivilege.MANAGE_USER, auditEntity);
        return;
      case RENAME_USER:
      case UPDATE_USER:
        auditEntity.setAuditLogOperation(AuditLogOperation.DDL);
        if (statement.getUserName().equals(userName)) {
          // users can change the username and password of themselves
          DNAuditLogger.getInstance()
              .recordObjectAuthenticationAuditLog(
                  auditEntity.setResult(true), statement::getUserName);
          return;
        }
        if (AuthorityChecker.SUPER_USER_ID
            == AuthorityChecker.getUserId(statement.getUserName()).orElse(-1L)) {
          // Only the superuser can alter him/herself
          DNAuditLogger.getInstance()
              .recordObjectAuthenticationAuditLog(
                  auditEntity.setResult(false), statement::getUserName);
          throw new AccessDeniedException("Only the superuser can alter him/herself.");
        }
        if (AuthorityChecker.SUPER_USER_ID == auditEntity.getUserId()) {
          // the superuser can alter anyone
          DNAuditLogger.getInstance()
              .recordObjectAuthenticationAuditLog(
                  auditEntity.setResult(true), statement::getUserName);
          return;
        }
        authChecker.checkGlobalPrivilege(userName, TableModelPrivilege.MANAGE_USER, auditEntity);
        return;
      case LIST_USER_PRIV:
        auditEntity.setAuditLogOperation(AuditLogOperation.QUERY);
        if (statement.getUserName().equals(userName)) {
          // No need any privilege to list him/herself
          DNAuditLogger.getInstance()
              .recordObjectAuthenticationAuditLog(
                  auditEntity.setResult(true), statement::getUserName);
          return;
        }
        // Require SECURITY privilege to list other users' privileges
        if (AuthorityChecker.SUPER_USER_ID == auditEntity.getUserId()) {
          DNAuditLogger.getInstance()
              .recordObjectAuthenticationAuditLog(
                  auditEntity.setPrivilegeType(PrivilegeType.SECURITY).setResult(true),
                  statement::getUserName);
          return;
        }
        authChecker.checkGlobalPrivilege(userName, TableModelPrivilege.MANAGE_USER, auditEntity);
        return;
      case LIST_USER:
        auditEntity.setAuditLogOperation(AuditLogOperation.QUERY).setResult(true);
        if (!hasGlobalPrivilege(auditEntity, PrivilegeType.MANAGE_USER)) {
          // No need to check privilege to list himself/herself
          statement.setUserName(userName);
          DNAuditLogger.getInstance()
              .recordObjectAuthenticationAuditLog(auditEntity, statement::getUserName);
        } else {
          // Require SECURITY privilege to list other users
          DNAuditLogger.getInstance()
              .recordObjectAuthenticationAuditLog(
                  auditEntity.setPrivilegeType(PrivilegeType.SECURITY), statement::getUserName);
        }
        return;
      case CREATE_ROLE:
      case DROP_ROLE:
        auditEntity
            .setAuditLogOperation(AuditLogOperation.DDL)
            .setPrivilegeType(PrivilegeType.SECURITY);
        if (AuthorityChecker.SUPER_USER_ID == auditEntity.getUserId()) {
          DNAuditLogger.getInstance()
              .recordObjectAuthenticationAuditLog(
                  auditEntity.setResult(true), statement::getRoleName);
          return;
        }
        authChecker.checkGlobalPrivilege(userName, TableModelPrivilege.MANAGE_ROLE, auditEntity);
        return;
      case GRANT_USER_ROLE:
      case REVOKE_USER_ROLE:
        auditEntity
            .setAuditLogOperation(AuditLogOperation.DDL)
            .setPrivilegeType(PrivilegeType.SECURITY);
        if (AuthorityChecker.SUPER_USER_ID == auditEntity.getUserId()) {
          DNAuditLogger.getInstance()
              .recordObjectAuthenticationAuditLog(
                  auditEntity.setResult(true),
                  () -> "user: " + statement.getUserName() + ", role: " + statement.getRoleName());
          return;
        }
        authChecker.checkGlobalPrivilege(userName, TableModelPrivilege.MANAGE_ROLE, auditEntity);
        return;
      case LIST_ROLE:
        auditEntity.setAuditLogOperation(AuditLogOperation.QUERY);
        if (statement.getUserName() != null && !statement.getUserName().equals(userName)) {
          // Require SECURITY privilege to list other users' roles
          authChecker.checkGlobalPrivilege(userName, TableModelPrivilege.MANAGE_ROLE, auditEntity);
          return;
        }
        if (!hasGlobalPrivilege(auditEntity, PrivilegeType.MANAGE_ROLE)) {
          // No need to check privilege to list his/hers own role
          statement.setUserName(userName);
          DNAuditLogger.getInstance()
              .recordObjectAuthenticationAuditLog(
                  auditEntity.setResult(true), statement::getRoleName);
        } else {
          // Require SECURITY privilege to list all roles
          DNAuditLogger.getInstance()
              .recordObjectAuthenticationAuditLog(
                  auditEntity.setPrivilegeType(PrivilegeType.SECURITY).setResult(true),
                  statement::getRoleName);
        }
        return;
      case LIST_ROLE_PRIV:
        auditEntity.setAuditLogOperation(AuditLogOperation.QUERY);
        if (AuthorityChecker.checkRole(userName, statement.getRoleName())) {
          // No need any privilege to list his/hers own role
          DNAuditLogger.getInstance()
              .recordObjectAuthenticationAuditLog(
                  auditEntity.setResult(true), statement::getRoleName);
          return;
        }
        // Require SECURITY privilege to list other roles' privileges
        if (AuthorityChecker.SUPER_USER_ID == auditEntity.getUserId()) {
          DNAuditLogger.getInstance()
              .recordObjectAuthenticationAuditLog(
                  auditEntity.setPrivilegeType(PrivilegeType.SECURITY).setResult(true),
                  statement::getRoleName);
          return;
        }
        authChecker.checkGlobalPrivilege(userName, TableModelPrivilege.MANAGE_ROLE, auditEntity);
        return;
      case GRANT_ROLE_ANY:
      case GRANT_USER_ANY:
      case REVOKE_ROLE_ANY:
      case REVOKE_USER_ANY:
        auditEntity
            .setAuditLogOperation(AuditLogOperation.DDL)
            .setPrivilegeType(PrivilegeType.SECURITY)
            .setDatabase(statement.getDatabase());
        if (hasGlobalPrivilege(auditEntity, PrivilegeType.SECURITY)) {
          DNAuditLogger.getInstance()
              .recordObjectAuthenticationAuditLog(
                  auditEntity.setResult(true),
                  () -> statement.getUserName() + statement.getRoleName());
          return;
        }
        for (PrivilegeType privilegeType : statement.getPrivilegeTypes()) {
          authChecker.checkAnyScopePrivilegeGrantOption(
              userName, TableModelPrivilege.getTableModelType(privilegeType), auditEntity);
        }
        return;
      case GRANT_ROLE_ALL:
      case REVOKE_ROLE_ALL:
      case GRANT_USER_ALL:
      case REVOKE_USER_ALL:
        auditEntity
            .setAuditLogOperation(AuditLogOperation.DDL)
            .setPrivilegeType(PrivilegeType.SECURITY)
            .setDatabase(statement.getDatabase());
        if (hasGlobalPrivilege(auditEntity, PrivilegeType.SECURITY)) {
          DNAuditLogger.getInstance()
              .recordObjectAuthenticationAuditLog(
                  auditEntity.setResult(true),
                  () -> statement.getUserName() + statement.getRoleName());
          return;
        }
        for (TableModelPrivilege privilege : TableModelPrivilege.values()) {
          PrivilegeType privilegeType = privilege.getPrivilegeType();
          if (privilegeType.isRelationalPrivilege()) {
            authChecker.checkAnyScopePrivilegeGrantOption(userName, privilege, auditEntity);
          }
          if (privilegeType.forRelationalSys()) {
            authChecker.checkGlobalPrivilegeGrantOption(userName, privilege, auditEntity);
          }
        }
        return;
      case GRANT_USER_DB:
      case GRANT_ROLE_DB:
      case REVOKE_USER_DB:
      case REVOKE_ROLE_DB:
        auditEntity
            .setAuditLogOperation(AuditLogOperation.DDL)
            .setPrivilegeType(PrivilegeType.SECURITY)
            .setDatabase(statement.getDatabase());
        if (hasGlobalPrivilege(auditEntity, PrivilegeType.SECURITY)) {
          DNAuditLogger.getInstance()
              .recordObjectAuthenticationAuditLog(
                  auditEntity.setResult(true),
                  () -> statement.getUserName() + statement.getRoleName());
          return;
        }
        for (PrivilegeType privilegeType : statement.getPrivilegeTypes()) {
          authChecker.checkDatabasePrivilegeGrantOption(
              userName,
              statement.getDatabase(),
              TableModelPrivilege.getTableModelType(privilegeType),
              auditEntity);
        }
        return;
      case GRANT_USER_TB:
      case GRANT_ROLE_TB:
      case REVOKE_USER_TB:
      case REVOKE_ROLE_TB:
        auditEntity
            .setAuditLogOperation(AuditLogOperation.DDL)
            .setPrivilegeType(PrivilegeType.SECURITY)
            .setDatabase(statement.getDatabase());
        if (hasGlobalPrivilege(auditEntity, PrivilegeType.SECURITY)) {
          DNAuditLogger.getInstance()
              .recordObjectAuthenticationAuditLog(
                  auditEntity.setResult(true),
                  () -> statement.getUserName() + statement.getRoleName());
          return;
        }
        for (PrivilegeType privilegeType : statement.getPrivilegeTypes()) {
          authChecker.checkTablePrivilegeGrantOption(
              userName,
              new QualifiedObjectName(statement.getDatabase(), statement.getTableName()),
              TableModelPrivilege.getTableModelType(privilegeType),
              auditEntity);
        }
        return;

      case GRANT_USER_SYS:
      case GRANT_ROLE_SYS:
      case REVOKE_USER_SYS:
      case REVOKE_ROLE_SYS:
      case ACCOUNT_UNLOCK:
        auditEntity
            .setAuditLogOperation(AuditLogOperation.DDL)
            .setPrivilegeType(PrivilegeType.SECURITY);
        if (hasGlobalPrivilege(auditEntity, PrivilegeType.SECURITY)) {
          DNAuditLogger.getInstance()
              .recordObjectAuthenticationAuditLog(
                  auditEntity.setResult(true),
                  () -> statement.getUserName() + statement.getRoleName());
          return;
        }
        for (PrivilegeType privilegeType : statement.getPrivilegeTypes()) {
          authChecker.checkGlobalPrivilegeGrantOption(
              userName, TableModelPrivilege.getTableModelType(privilegeType), auditEntity);
        }
        break;
      default:
        break;
    }
  }

  @Override
  public void checkUserIsAdmin(IAuditEntity entity) {
    if (AuthorityChecker.SUPER_USER_ID != entity.getUserId()) {
      throw new AccessDeniedException(ONLY_ADMIN_ALLOWED);
    }
  }

  @Override
  public void checkUserGlobalSysPrivilege(IAuditEntity auditEntity) {
    if (AuthorityChecker.SUPER_USER_ID != auditEntity.getUserId()) {
      authChecker.checkGlobalPrivilege(
          auditEntity.getUsername(), TableModelPrivilege.SYSTEM, auditEntity);
    }
  }

  @Override
  public boolean hasGlobalPrivilege(IAuditEntity entity, PrivilegeType privilegeType) {
    return AuthorityChecker.SUPER_USER_ID == entity.getUserId()
        || AuthorityChecker.checkSystemPermission(entity.getUsername(), privilegeType);
  }

  @Override
  public void checkMissingPrivileges(
      String username, Collection<PrivilegeType> privilegeTypes, IAuditEntity auditEntity) {
    if (AuthorityChecker.SUPER_USER_ID == auditEntity.getUserId()) {
      return;
    }
    authChecker.checkGlobalPrivileges(username, privilegeTypes, auditEntity);
  }

  @Override
  public TSStatus checkPermissionBeforeProcess(
      Statement statement, TreeAccessCheckContext context) {
    return treeAccessCheckVisitor.process(statement, context);
  }

  @Override
  public TSStatus checkFullPathWriteDataPermission(
      IAuditEntity auditEntity, IDeviceID device, String measurementId) {
    try {
      PartialPath path = new MeasurementPath(device, measurementId);
      // audit db is read-only
      if (includeByAuditTreeDB(path)
          && !auditEntity.getUsername().equals(AuthorityChecker.INTERNAL_AUDIT_USER)) {
        return new TSStatus(TSStatusCode.NO_PERMISSION.getStatusCode())
            .setMessage(String.format(READ_ONLY_DB_ERROR_MSG, TREE_MODEL_AUDIT_DATABASE));
      }
      return checkTimeSeriesPermission(
          auditEntity, () -> Collections.singletonList(path), PrivilegeType.WRITE_DATA);
    } catch (IllegalPathException e) {
      // should never be here
      throw new IllegalStateException(e);
    }
  }

  @Override
  public TSStatus checkCanCreateDatabaseForTree(IAuditEntity entity, PartialPath databaseName) {
    return treeAccessCheckVisitor.checkCreateOrAlterDatabasePermission(entity, databaseName);
  }

  @Override
  public TSStatus checkCanAlterTemplate(IAuditEntity entity, Supplier<String> auditObject) {
    return treeAccessCheckVisitor.checkCanAlterTemplate(entity, auditObject);
  }

  @Override
  public TSStatus checkCanAlterView(
      IAuditEntity entity, List<PartialPath> sourcePaths, List<PartialPath> targetPaths) {
    if (AuthorityChecker.SUPER_USER_ID == entity.getUserId()) {
      DNAuditLogger.getInstance()
          .recordObjectAuthenticationAuditLog(
              entity
                  .setPrivilegeTypes(
                      Arrays.asList(PrivilegeType.READ_SCHEMA, PrivilegeType.WRITE_SCHEMA))
                  .setResult(true),
              () -> "source: " + sourcePaths + ", target: " + targetPaths);
      return SUCCEED;
    }
    TSStatus status = new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
    if (sourcePaths != null) {
      status = checkTimeSeriesPermission(entity, () -> sourcePaths, PrivilegeType.READ_SCHEMA);
    }
    if (status.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      return checkTimeSeriesPermission(entity, () -> targetPaths, PrivilegeType.WRITE_SCHEMA);
    }
    return status;
  }

  @Override
  public TSStatus checkSeriesPrivilege4Pipe(
      IAuditEntity context, List<? extends PartialPath> checkedPaths, PrivilegeType permission) {
    return TreeAccessCheckVisitor.checkTimeSeriesPermission(
        context, () -> checkedPaths, permission);
  }

  @Override
  public List<Integer> checkSeriesPrivilegeWithIndexes4Pipe(
      IAuditEntity context, List<? extends PartialPath> checkedPaths, PrivilegeType permission) {
    return TreeAccessCheckVisitor.checkTimeSeriesPermission4Pipe(context, checkedPaths, permission);
  }

  @Override
  public TSStatus allowUserToLogin(String userName) {
    return SUCCEED;
  }
}
