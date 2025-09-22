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
import org.apache.iotdb.commons.audit.AuditEventType;
import org.apache.iotdb.commons.audit.AuditLogFields;
import org.apache.iotdb.commons.audit.AuditLogOperation;
import org.apache.iotdb.commons.audit.IAuditEntity;
import org.apache.iotdb.commons.auth.entity.PrivilegeType;
import org.apache.iotdb.commons.exception.auth.AccessDeniedException;
import org.apache.iotdb.commons.schema.table.InformationSchema;
import org.apache.iotdb.db.audit.DNAuditLogger;
import org.apache.iotdb.db.auth.AuthorityChecker;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.QualifiedObjectName;
import org.apache.iotdb.rpc.TSStatusCode;

import java.util.Collection;

import static org.apache.iotdb.commons.schema.table.Audit.TABLE_MODEL_AUDIT_DATABASE;

public class ITableAuthCheckerImpl implements ITableAuthChecker {

  private static final DNAuditLogger AUDIT_LOGGER = DNAuditLogger.getInstance();

  private static final String OBJECT_AUTHENTICATION_AUDIT_STR =
      "User %s (ID=%d) requests authority on object %s with result %s";

  @Override
  public void checkDatabaseVisibility(
      String userName, String databaseName, IAuditEntity auditEntity) {
    if (AuthorityChecker.SUPER_USER_ID == auditEntity.getUserId()) {
      recordAuditLog(
          auditEntity
              .setAuditLogOperation(AuditLogOperation.QUERY)
              .setPrivilegeType(PrivilegeType.READ_SCHEMA)
              .setResult(true),
          databaseName);
      return;
    }
    // Information_schema is visible to any user
    if (databaseName.equals(InformationSchema.INFORMATION_DATABASE)) {
      recordAuditLog(
          auditEntity
              .setAuditLogOperation(AuditLogOperation.QUERY)
              .setPrivilegeType(PrivilegeType.READ_SCHEMA)
              .setResult(true),
          databaseName);
      return;
    }

    if (TABLE_MODEL_AUDIT_DATABASE.equalsIgnoreCase(databaseName)) {
      if (AuthorityChecker.checkSystemPermission(userName, PrivilegeType.AUDIT)) {
        recordAuditLog(
            auditEntity
                .setAuditLogOperation(AuditLogOperation.QUERY)
                .setPrivilegeType(PrivilegeType.READ_SCHEMA)
                .setResult(true),
            databaseName);
        return;
      } else {
        recordAuditLog(
            auditEntity
                .setAuditLogOperation(AuditLogOperation.QUERY)
                .setPrivilegeType(PrivilegeType.READ_SCHEMA)
                .setResult(false),
            databaseName);
        throw new AccessDeniedException("DATABASE " + databaseName);
      }
    }

    if (AuthorityChecker.checkSystemPermission(userName, PrivilegeType.SYSTEM)) {
      recordAuditLog(
          auditEntity
              .setAuditLogOperation(AuditLogOperation.QUERY)
              .setPrivilegeType(PrivilegeType.READ_SCHEMA)
              .setResult(true),
          databaseName);
      return;
    }
    if (!AuthorityChecker.checkDBVisible(userName, databaseName)) {
      recordAuditLog(
          auditEntity
              .setAuditLogOperation(AuditLogOperation.QUERY)
              .setPrivilegeType(PrivilegeType.READ_SCHEMA)
              .setResult(false),
          databaseName);
      throw new AccessDeniedException("DATABASE " + databaseName);
    }
    recordAuditLog(
        auditEntity
            .setAuditLogOperation(AuditLogOperation.QUERY)
            .setPrivilegeType(PrivilegeType.READ_SCHEMA)
            .setResult(true),
        databaseName);
  }

  @Override
  public void checkDatabasePrivilege(
      String userName,
      String databaseName,
      TableModelPrivilege privilege,
      IAuditEntity auditEntity) {
    checkAuditDatabase(userName, privilege, databaseName, auditEntity);
    if (userName.equals(AuthorityChecker.INTERNAL_AUDIT_USER)
        && databaseName.equals(TABLE_MODEL_AUDIT_DATABASE)) {
      // The internal auditor has any privilege to the audit database
      return;
    }

    if (AuthorityChecker.SUPER_USER_ID == auditEntity.getUserId()) {
      recordAuditLog(
          auditEntity
              .setAuditLogOperation(privilege.getAuditLogOperation())
              .setPrivilegeType(privilege.getPrivilegeType())
              .setResult(true),
          databaseName);
      return;
    }

    if (AuthorityChecker.checkSystemPermission(userName, PrivilegeType.SYSTEM)) {
      recordAuditLog(
          auditEntity
              .setAuditLogOperation(privilege.getAuditLogOperation())
              .setPrivilegeType(privilege.getPrivilegeType())
              .setResult(true),
          databaseName);
      return;
    }

    TSStatus result =
        AuthorityChecker.getTSStatus(
            AuthorityChecker.checkDBPermission(
                userName, databaseName, privilege.getPrivilegeType()),
            privilege.getPrivilegeType(),
            databaseName);
    recordAuditLogViaAuthenticationResult(databaseName, privilege, auditEntity, result);
  }

  private static void checkAuditDatabase(
      String userName,
      TableModelPrivilege privilege,
      String databaseName,
      IAuditEntity auditEntity) {
    if (userName.equals(AuthorityChecker.INTERNAL_AUDIT_USER)) {
      return;
    }
    if (TABLE_MODEL_AUDIT_DATABASE.equalsIgnoreCase(databaseName)) {
      if (privilege == TableModelPrivilege.SELECT) {
        checkCanSelectAuditTable(auditEntity);
      } else {
        recordAuditLog(
            auditEntity
                .setAuditLogOperation(privilege.getAuditLogOperation())
                .setPrivilegeType(privilege.getPrivilegeType())
                .setResult(false),
            databaseName);
        throw new AccessDeniedException(
            String.format("The database '%s' is read-only.", TABLE_MODEL_AUDIT_DATABASE));
      }
    }
  }

  public static void checkCanSelectAuditTable(IAuditEntity auditEntity) {
    String userName = auditEntity.getUsername();
    if (AuthorityChecker.SUPER_USER_ID != auditEntity.getUserId()
        && !AuthorityChecker.checkSystemPermission(userName, PrivilegeType.AUDIT)) {
      recordAuditLog(
          auditEntity
              .setAuditLogOperation(AuditLogOperation.QUERY)
              .setPrivilegeType(PrivilegeType.SELECT)
              .setResult(false),
          TABLE_MODEL_AUDIT_DATABASE);
      AUDIT_LOGGER.log(
          new AuditLogFields(
              userName,
              auditEntity.getUserId(),
              auditEntity.getCliHostname(),
              AuditEventType.OBJECT_AUTHENTICATION,
              AuditLogOperation.QUERY,
              PrivilegeType.SELECT,
              false,
              TABLE_MODEL_AUDIT_DATABASE,
              auditEntity.getSqlString()),
          String.format(
              OBJECT_AUTHENTICATION_AUDIT_STR,
              userName,
              auditEntity.getUserId(),
              TABLE_MODEL_AUDIT_DATABASE,
              false));
      throw new AccessDeniedException(
          String.format(
              "The database '%s' can only be queried by AUDIT admin.", TABLE_MODEL_AUDIT_DATABASE));
    }
    recordAuditLog(
        auditEntity
            .setAuditLogOperation(AuditLogOperation.QUERY)
            .setPrivilegeType(PrivilegeType.SELECT)
            .setResult(true),
        TABLE_MODEL_AUDIT_DATABASE);
  }

  @Override
  public void checkDatabasePrivilegeGrantOption(
      String userName,
      String databaseName,
      TableModelPrivilege privilege,
      IAuditEntity auditEntity) {
    if (AuthorityChecker.SUPER_USER_ID == auditEntity.getUserId()) {
      recordAuditLog(
          auditEntity
              .setAuditLogOperation(privilege.getAuditLogOperation())
              .setPrivilegeType(privilege.getPrivilegeType())
              .setResult(true),
          databaseName);
      return;
    }
    TSStatus result =
        AuthorityChecker.getGrantOptTSStatus(
            AuthorityChecker.checkDBPermissionGrantOption(
                userName, databaseName, privilege.getPrivilegeType()),
            privilege.getPrivilegeType(),
            databaseName);
    recordAuditLogViaAuthenticationResult(databaseName, privilege, auditEntity, result);
  }

  @Override
  public void checkTablePrivilege(
      String userName,
      QualifiedObjectName tableName,
      TableModelPrivilege privilege,
      IAuditEntity auditEntity) {
    if (AuthorityChecker.SUPER_USER_ID == auditEntity.getUserId()) {
      recordAuditLog(
          auditEntity
              .setAuditLogOperation(privilege.getAuditLogOperation())
              .setPrivilegeType(privilege.getPrivilegeType())
              .setResult(true),
          tableName.getObjectName());
      return;
    }
    TSStatus result =
        AuthorityChecker.getTSStatus(
            AuthorityChecker.checkTablePermission(
                userName,
                tableName.getDatabaseName(),
                tableName.getObjectName(),
                privilege.getPrivilegeType()),
            privilege.getPrivilegeType(),
            tableName.getDatabaseName(),
            tableName.getObjectName());
    recordAuditLogViaAuthenticationResult(
        tableName.getObjectName(), privilege, auditEntity, result);
  }

  @Override
  public void checkTablePrivilegeGrantOption(
      String userName,
      QualifiedObjectName tableName,
      TableModelPrivilege privilege,
      IAuditEntity auditEntity) {
    if (AuthorityChecker.SUPER_USER_ID == auditEntity.getUserId()) {
      recordAuditLog(
          auditEntity
              .setAuditLogOperation(privilege.getAuditLogOperation())
              .setPrivilegeType(privilege.getPrivilegeType())
              .setResult(true),
          tableName.getObjectName());
      return;
    }
    TSStatus result =
        AuthorityChecker.getGrantOptTSStatus(
            AuthorityChecker.checkTablePermissionGrantOption(
                userName,
                tableName.getDatabaseName(),
                tableName.getObjectName(),
                privilege.getPrivilegeType()),
            privilege.getPrivilegeType(),
            tableName.getDatabaseName(),
            tableName.getObjectName());
    recordAuditLogViaAuthenticationResult(
        tableName.getObjectName(), privilege, auditEntity, result);
  }

  @Override
  public boolean checkTablePrivilege4Pipe(
      final String userName, final QualifiedObjectName tableName, IAuditEntity auditEntity) {
    if (AuthorityChecker.SUPER_USER_ID == auditEntity.getUserId()
        || AuthorityChecker.getTSStatus(
                    AuthorityChecker.checkTablePermission(
                        userName,
                        tableName.getDatabaseName(),
                        tableName.getObjectName(),
                        PrivilegeType.SELECT),
                    PrivilegeType.SELECT,
                    tableName.getDatabaseName(),
                    tableName.getObjectName())
                .getCode()
            == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      recordAuditLog(
          auditEntity
              .setAuditLogOperation(AuditLogOperation.CONTROL)
              .setPrivilegeType(PrivilegeType.SYSTEM)
              .setResult(true),
          tableName.getObjectName());
      return true;
    }
    recordAuditLog(
        auditEntity
            .setAuditLogOperation(AuditLogOperation.CONTROL)
            .setPrivilegeType(PrivilegeType.SYSTEM)
            .setResult(false),
        tableName.getObjectName());
    return false;
  }

  @Override
  public void checkTableVisibility(
      String userName, QualifiedObjectName tableName, IAuditEntity auditEntity) {
    if (AuthorityChecker.SUPER_USER_ID == auditEntity.getUserId()) {
      recordAuditLog(
          auditEntity
              .setAuditLogOperation(AuditLogOperation.QUERY)
              .setPrivilegeType(PrivilegeType.READ_SCHEMA)
              .setResult(true),
          tableName.getObjectName());
      return;
    }

    String databaseName = tableName.getDatabaseName();
    if (TABLE_MODEL_AUDIT_DATABASE.equalsIgnoreCase(databaseName)
        && !AuthorityChecker.checkSystemPermission(userName, PrivilegeType.AUDIT)) {
      recordAuditLog(
          auditEntity
              .setAuditLogOperation(AuditLogOperation.QUERY)
              .setPrivilegeType(PrivilegeType.READ_SCHEMA)
              .setResult(false),
          tableName.getObjectName());
      throw new AccessDeniedException("TABLE " + tableName);
    }

    if (AuthorityChecker.checkSystemPermission(userName, PrivilegeType.SYSTEM)) {
      recordAuditLog(
          auditEntity
              .setAuditLogOperation(AuditLogOperation.QUERY)
              .setPrivilegeType(PrivilegeType.READ_SCHEMA)
              .setResult(true),
          tableName.getObjectName());
      return;
    }
    if (!AuthorityChecker.checkTableVisible(
        userName, tableName.getDatabaseName(), tableName.getObjectName())) {
      recordAuditLog(
          auditEntity
              .setAuditLogOperation(AuditLogOperation.QUERY)
              .setPrivilegeType(PrivilegeType.READ_SCHEMA)
              .setResult(false),
          tableName.getObjectName());
      throw new AccessDeniedException("TABLE " + tableName);
    }
  }

  @Override
  public void checkGlobalPrivilege(
      String userName, TableModelPrivilege privilege, IAuditEntity auditEntity) {
    if (AuthorityChecker.SUPER_USER_ID == auditEntity.getUserId()) {
      recordAuditLog(
          auditEntity
              .setAuditLogOperation(privilege.getAuditLogOperation())
              .setPrivilegeType(privilege.getPrivilegeType())
              .setResult(true),
          AuthorityChecker.ANY_SCOPE);
      return;
    }
    TSStatus result =
        AuthorityChecker.getTSStatus(
            AuthorityChecker.checkSystemPermission(userName, privilege.getPrivilegeType()),
            privilege.getPrivilegeType());
    recordAuditLogViaAuthenticationResult(userName, privilege, auditEntity, result);
  }

  @Override
  public void checkGlobalPrivileges(
      String username, Collection<PrivilegeType> privileges, IAuditEntity auditEntity) {
    if (AuthorityChecker.SUPER_USER_ID == auditEntity.getUserId()) {
      for (PrivilegeType privilege : privileges) {
        recordAuditLog(
            auditEntity
                .setAuditLogOperation(privilege.getAuditLogOperation())
                .setPrivilegeType(privilege)
                .setResult(true),
            AuthorityChecker.ANY_SCOPE);
      }
      return;
    }
    TSStatus result =
        AuthorityChecker.getTSStatus(
            AuthorityChecker.checkUserMissingSystemPermissions(username, privileges));
    if (result.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      throw new AccessDeniedException(result.getMessage());
    }
  }

  @Override
  public void checkGlobalPrivilegeGrantOption(
      String userName, TableModelPrivilege privilege, IAuditEntity auditEntity) {
    if (AuthorityChecker.SUPER_USER_ID == auditEntity.getUserId()) {
      recordAuditLog(
          auditEntity
              .setAuditLogOperation(privilege.getAuditLogOperation())
              .setPrivilegeType(privilege.getPrivilegeType())
              .setResult(true),
          AuthorityChecker.ANY_SCOPE);
      return;
    }
    TSStatus result =
        AuthorityChecker.getGrantOptTSStatus(
            AuthorityChecker.checkSystemPermissionGrantOption(
                userName, privilege.getPrivilegeType()),
            privilege.getPrivilegeType());
    recordAuditLogViaAuthenticationResult(
        AuthorityChecker.ANY_SCOPE, privilege, auditEntity, result);
  }

  @Override
  public void checkAnyScopePrivilegeGrantOption(
      String userName, TableModelPrivilege privilege, IAuditEntity auditEntity) {
    if (AuthorityChecker.SUPER_USER_ID == auditEntity.getUserId()) {
      recordAuditLog(
          auditEntity
              .setAuditLogOperation(privilege.getAuditLogOperation())
              .setPrivilegeType(privilege.getPrivilegeType())
              .setResult(true),
          AuthorityChecker.ANY_SCOPE);
      return;
    }
    TSStatus result =
        AuthorityChecker.getGrantOptTSStatus(
            AuthorityChecker.checkAnyScopePermissionGrantOption(
                userName, privilege.getPrivilegeType()),
            privilege.getPrivilegeType());
    recordAuditLogViaAuthenticationResult(
        AuthorityChecker.ANY_SCOPE, privilege, auditEntity, result);
  }

  private void recordAuditLogViaAuthenticationResult(
      String auditObject,
      TableModelPrivilege privilege,
      IAuditEntity auditEntity,
      TSStatus result) {
    if (result.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      recordAuditLog(
          auditEntity
              .setAuditLogOperation(privilege.getAuditLogOperation())
              .setPrivilegeType(privilege.getPrivilegeType())
              .setResult(false),
          auditObject);
      throw new AccessDeniedException(result.getMessage());
    }
    recordAuditLog(
        auditEntity
            .setAuditLogOperation(privilege.getAuditLogOperation())
            .setPrivilegeType(privilege.getPrivilegeType())
            .setResult(true),
        auditObject);
  }

  private static void recordAuditLog(IAuditEntity auditEntity, String auditObject) {
    AUDIT_LOGGER.log(
        new AuditLogFields(
            auditEntity.getUsername(),
            auditEntity.getUserId(),
            auditEntity.getCliHostname(),
            AuditEventType.OBJECT_AUTHENTICATION,
            auditEntity.getAuditLogOperation(),
            auditEntity.getPrivilegeType(),
            auditEntity.getResult(),
            auditEntity.getDatabase(),
            auditEntity.getSqlString()),
        String.format(
            OBJECT_AUTHENTICATION_AUDIT_STR,
            auditEntity.getUsername(),
            auditEntity.getUserId(),
            auditObject,
            true));
  }
}
