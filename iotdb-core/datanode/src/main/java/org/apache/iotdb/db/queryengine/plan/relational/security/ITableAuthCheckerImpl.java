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
import org.apache.iotdb.commons.exception.auth.AccessDeniedException;
import org.apache.iotdb.commons.schema.table.InformationSchema;
import org.apache.iotdb.db.auth.AuthorityChecker;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.QualifiedObjectName;
import org.apache.iotdb.rpc.TSStatusCode;

import java.util.Collection;

import static org.apache.iotdb.commons.schema.table.Audit.TABLE_MODEL_AUDIT_DATABASE;

public class ITableAuthCheckerImpl implements ITableAuthChecker {

  @Override
  public void checkDatabaseVisibility(String userName, String databaseName) {
    if (AuthorityChecker.SUPER_USER.equals(userName)) {
      return;
    }
    // Information_schema is visible to any user
    if (databaseName.equals(InformationSchema.INFORMATION_DATABASE)) {
      return;
    }

    if (TABLE_MODEL_AUDIT_DATABASE.equalsIgnoreCase(databaseName)) {
      if (AuthorityChecker.checkSystemPermission(userName, PrivilegeType.AUDIT)) {
        return;
      } else {
        throw new AccessDeniedException("DATABASE " + databaseName);
      }
    }

    if (AuthorityChecker.checkSystemPermission(userName, PrivilegeType.SYSTEM)) {
      return;
    }
    if (!AuthorityChecker.checkDBVisible(userName, databaseName)) {
      throw new AccessDeniedException("DATABASE " + databaseName);
    }
  }

  @Override
  public void checkDatabasePrivilege(
      String userName, String databaseName, TableModelPrivilege privilege) {
    checkAuditDatabase(userName, privilege, databaseName);

    if (AuthorityChecker.SUPER_USER.equals(userName)) {
      return;
    }

    if (AuthorityChecker.checkSystemPermission(userName, PrivilegeType.SYSTEM)) {
      return;
    }

    TSStatus result =
        AuthorityChecker.getTSStatus(
            AuthorityChecker.checkDBPermission(
                userName, databaseName, privilege.getPrivilegeType()),
            privilege.getPrivilegeType(),
            databaseName);
    if (result.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      throw new AccessDeniedException(result.getMessage());
    }
  }

  private static void checkAuditDatabase(
      String userName, TableModelPrivilege privilege, String databaseName) {
    if (TABLE_MODEL_AUDIT_DATABASE.equalsIgnoreCase(databaseName)) {
      if (privilege == TableModelPrivilege.SELECT) {
        checkCanSelectAuditTable(userName);
      } else {
        throw new AccessDeniedException(
            String.format("The database '%s' is read-only.", TABLE_MODEL_AUDIT_DATABASE));
      }
    }
  }

  public static void checkCanSelectAuditTable(String userName) {
    if (!AuthorityChecker.SUPER_USER.equals(userName)
        && !AuthorityChecker.checkSystemPermission(userName, PrivilegeType.AUDIT)) {
      throw new AccessDeniedException(
          String.format(
              "The database '%s' can only be queried by AUDIT admin.", TABLE_MODEL_AUDIT_DATABASE));
    }
  }

  @Override
  public void checkDatabasePrivilegeGrantOption(
      String userName, String databaseName, TableModelPrivilege privilege) {
    if (AuthorityChecker.SUPER_USER.equals(userName)) {
      return;
    }
    TSStatus result =
        AuthorityChecker.getGrantOptTSStatus(
            AuthorityChecker.checkDBPermissionGrantOption(
                userName, databaseName, privilege.getPrivilegeType()),
            privilege.getPrivilegeType(),
            databaseName);
    if (result.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      throw new AccessDeniedException(result.getMessage());
    }
  }

  @Override
  public void checkTablePrivilege(
      String userName, QualifiedObjectName tableName, TableModelPrivilege privilege) {
    if (AuthorityChecker.SUPER_USER.equals(userName)) {
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
    if (result.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      throw new AccessDeniedException(result.getMessage());
    }
  }

  @Override
  public void checkTablePrivilegeGrantOption(
      String userName, QualifiedObjectName tableName, TableModelPrivilege privilege) {
    if (AuthorityChecker.SUPER_USER.equals(userName)) {
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
    if (result.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      throw new AccessDeniedException(result.getMessage());
    }
  }

  @Override
  public boolean checkTablePrivilege4Pipe(
      final String userName, final QualifiedObjectName tableName) {
    return AuthorityChecker.SUPER_USER.equals(userName)
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
            == TSStatusCode.SUCCESS_STATUS.getStatusCode();
  }

  @Override
  public void checkTableVisibility(String userName, QualifiedObjectName tableName) {
    if (AuthorityChecker.SUPER_USER.equals(userName)) {
      return;
    }

    String databaseName = tableName.getDatabaseName();
    if (TABLE_MODEL_AUDIT_DATABASE.equalsIgnoreCase(databaseName)
        && !AuthorityChecker.checkSystemPermission(userName, PrivilegeType.AUDIT)) {
      throw new AccessDeniedException("TABLE " + tableName);
    }

    if (AuthorityChecker.checkSystemPermission(userName, PrivilegeType.SYSTEM)) {
      return;
    }
    if (!AuthorityChecker.checkTableVisible(
        userName, tableName.getDatabaseName(), tableName.getObjectName())) {
      throw new AccessDeniedException("TABLE " + tableName);
    }
  }

  @Override
  public void checkGlobalPrivilege(String userName, TableModelPrivilege privilege) {
    if (AuthorityChecker.SUPER_USER.equals(userName)) {
      return;
    }
    TSStatus result =
        AuthorityChecker.getTSStatus(
            AuthorityChecker.checkSystemPermission(userName, privilege.getPrivilegeType()),
            privilege.getPrivilegeType());
    if (result.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      throw new AccessDeniedException(result.getMessage());
    }
  }

  @Override
  public void checkGlobalPrivileges(String username, Collection<PrivilegeType> privileges) {
    if (AuthorityChecker.SUPER_USER.equals(username)) {
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
  public void checkGlobalPrivilegeGrantOption(String userName, TableModelPrivilege privilege) {
    if (AuthorityChecker.SUPER_USER.equals(userName)) {
      return;
    }
    TSStatus result =
        AuthorityChecker.getGrantOptTSStatus(
            AuthorityChecker.checkSystemPermissionGrantOption(
                userName, privilege.getPrivilegeType()),
            privilege.getPrivilegeType());
    if (result.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      throw new AccessDeniedException(result.getMessage());
    }
  }

  @Override
  public void checkAnyScopePrivilegeGrantOption(String userName, TableModelPrivilege privilege) {
    if (AuthorityChecker.SUPER_USER.equals(userName)) {
      return;
    }
    TSStatus result =
        AuthorityChecker.getGrantOptTSStatus(
            AuthorityChecker.checkAnyScopePermissionGrantOption(
                userName, privilege.getPrivilegeType()),
            privilege.getPrivilegeType());
    if (result.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      throw new AccessDeniedException(result.getMessage());
    }
  }
}
