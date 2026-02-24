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
import org.apache.iotdb.commons.audit.IAuditEntity;
import org.apache.iotdb.commons.auth.entity.PrivilegeType;
import org.apache.iotdb.commons.exception.auth.AccessDeniedException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.QualifiedObjectName;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.RelationalAuthorStatement;
import org.apache.iotdb.db.queryengine.plan.statement.Statement;

import org.apache.tsfile.file.metadata.IDeviceID;

import java.util.Collection;
import java.util.List;
import java.util.function.Supplier;

public interface AccessControl {

  // ====================================== TABLE =============================================

  /**
   * Check if user is allowed to create the specified database.
   *
   * @param userName name of user
   * @param databaseName without `root.` prefix, like db
   * @param auditEntity records necessary info for audit log
   * @throws AccessDeniedException if not allowed
   */
  void checkCanCreateDatabase(String userName, String databaseName, IAuditEntity auditEntity);

  /**
   * Check if user is allowed to drop the specified database.
   *
   * @param userName name of user
   * @param databaseName without `root.` prefix, like db
   * @param auditEntity records necessary info for audit log
   * @throws AccessDeniedException if not allowed
   */
  void checkCanDropDatabase(String userName, String databaseName, IAuditEntity auditEntity);

  /**
   * Check if user is allowed to alter the specified database.
   *
   * @param userName name of user
   * @param databaseName without `root.` prefix, like db
   * @param auditEntity records necessary info for audit log
   * @throws AccessDeniedException if not allowed
   */
  void checkCanAlterDatabase(String userName, String databaseName, IAuditEntity auditEntity);

  /**
   * Check if user is allowed to show or use the specified database.
   *
   * @param userName name of user
   * @param databaseName without `root.` prefix, like db
   * @param auditEntity records necessary info for audit log
   * @throws AccessDeniedException if not allowed
   */
  void checkCanShowOrUseDatabase(
      final String userName, final String databaseName, IAuditEntity auditEntity);

  /**
   * Check if user is allowed to create the specified table.
   *
   * @param userName name of user
   * @param tableName qualified name of table without `root.` prefix, like db.table1
   * @param auditEntity records necessary info for audit log
   * @throws AccessDeniedException if not allowed
   */
  void checkCanCreateTable(
      String userName, QualifiedObjectName tableName, IAuditEntity auditEntity);

  /**
   * Check if user is allowed to create the specified table.
   *
   * @param userName name of user
   * @param tableName qualified name of table without `root.` prefix, like db.table1
   * @param auditEntity records necessary info for audit log
   * @throws AccessDeniedException if not allowed
   */
  void checkCanDropTable(String userName, QualifiedObjectName tableName, IAuditEntity auditEntity);

  /**
   * Check if user is allowed to alter the specified table.
   *
   * @param userName name of user
   * @param tableName qualified name of table without `root.` prefix, like db.table1
   * @param auditEntity records necessary info for audit log
   * @throws AccessDeniedException if not allowed
   */
  void checkCanAlterTable(String userName, QualifiedObjectName tableName, IAuditEntity auditEntity);

  /**
   * Check if user is allowed to insert into the specified table.
   *
   * @param userName name of user
   * @param tableName qualified name of table without `root.` prefix, like db.table1
   * @param auditEntity records necessary info for audit log
   * @throws AccessDeniedException if not allowed
   */
  void checkCanInsertIntoTable(
      String userName, QualifiedObjectName tableName, IAuditEntity auditEntity);

  /**
   * Check if user is allowed to select from the specified table.
   *
   * @param userName name of user
   * @param tableName qualified name of table without `root.` prefix, like db.table1
   * @param auditEntity records necessary info for audit log
   * @throws AccessDeniedException if not allowed
   */
  void checkCanSelectFromTable(
      String userName, QualifiedObjectName tableName, IAuditEntity auditEntity);

  /**
   * Check if user is allowed to extract certain data from pipe.
   *
   * @param userName name of user
   * @param databaseName the databaseName
   * @param auditEntity records necessary info for audit log
   * @throws AccessDeniedException if not allowed
   */
  void checkCanSelectFromDatabase4Pipe(
      final String userName, final String databaseName, IAuditEntity auditEntity);

  // This does not throw exception for performance issues
  boolean checkCanSelectFromTable4Pipe(
      final String userName, final QualifiedObjectName tableName, IAuditEntity auditEntity);

  /**
   * Check if user is allowed to delete from the specified table.
   *
   * @param userName name of user
   * @param tableName qualified name of table without `root.` prefix, like db.table1
   * @param auditEntity records necessary info for audit log
   * @throws AccessDeniedException if not allowed
   */
  void checkCanDeleteFromTable(
      String userName, QualifiedObjectName tableName, IAuditEntity auditEntity);

  /**
   * Check if user is allowed to show or describe the specified table.
   *
   * @param userName name of user
   * @param tableName qualified name of table without `root.` prefix, like db.table1
   * @param auditEntity records necessary info for audit log
   * @throws AccessDeniedException if not allowed
   */
  void checkCanShowOrDescTable(
      String userName, QualifiedObjectName tableName, IAuditEntity auditEntity);

  /**
   * Check if user is allowed to create view under the specific tree path.
   *
   * @param path the tree path scope the view can select from
   * @param auditEntity records necessary info for audit log
   * @throws AccessDeniedException if not allowed
   */
  void checkCanCreateViewFromTreePath(final PartialPath path, IAuditEntity auditEntity);

  /**
   * Check if user can run relational author statement.
   *
   * @param userName name of user
   * @param auditEntity records necessary info for audit log
   * @throws AccessDeniedException if not allowed
   */
  void checkUserCanRunRelationalAuthorStatement(
      String userName, RelationalAuthorStatement statement, IAuditEntity auditEntity);

  /**
   * Check if user is admin user
   *
   * @param auditEntity name of user
   * @throws AccessDeniedException if not allowed
   */
  void checkUserIsAdmin(IAuditEntity auditEntity);

  /**
   * Check if user has global SYSTEM privilege
   *
   * @param auditEntity records necessary info for audit log
   * @throws AccessDeniedException if not allowed
   */
  void checkUserGlobalSysPrivilege(IAuditEntity auditEntity);

  /**
   * Check if user has sepecified global privilege
   *
   * @param privilegeType needed privilege
   * @throws AccessDeniedException if not allowed
   */
  boolean hasGlobalPrivilege(IAuditEntity auditEntity, PrivilegeType privilegeType);

  void checkMissingPrivileges(
      String username, Collection<PrivilegeType> privilegeTypes, IAuditEntity auditEntity);

  // ====================================== TREE =============================================

  TSStatus checkPermissionBeforeProcess(Statement statement, TreeAccessCheckContext context);

  /** called by load */
  TSStatus checkFullPathWriteDataPermission(
      IAuditEntity auditEntity, IDeviceID device, String measurementId);

  TSStatus checkCanCreateDatabaseForTree(IAuditEntity entity, PartialPath databaseName);

  TSStatus checkCanAlterTemplate(IAuditEntity entity, Supplier<String> auditObject);

  TSStatus checkCanAlterView(
      IAuditEntity entity, List<PartialPath> sourcePaths, List<PartialPath> targetPaths);

  TSStatus checkSeriesPrivilege4Pipe(
      IAuditEntity context,
      List<? extends PartialPath> checkedPathsSupplier,
      PrivilegeType permission);

  List<Integer> checkSeriesPrivilegeWithIndexes4Pipe(
      IAuditEntity context,
      List<? extends PartialPath> checkedPathsSupplier,
      PrivilegeType permission);

  // ====================================== COMMON =============================================
  TSStatus allowUserToLogin(String userName);
}
