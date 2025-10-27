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

import org.apache.iotdb.commons.audit.IAuditEntity;
import org.apache.iotdb.commons.auth.entity.PrivilegeType;
import org.apache.iotdb.commons.exception.auth.AccessDeniedException;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.QualifiedObjectName;

import java.util.Collection;

public interface ITableAuthChecker {

  /**
   * Check if user has any privilege on ANY, @param{databaseName} or any tables
   * in @param{databaseName}
   *
   * @param userName name of user
   * @param databaseName without `root.` prefix, like db
   * @param auditEntity records necessary info for audit log
   * @throws AccessDeniedException if not allowed
   */
  void checkDatabaseVisibility(String userName, String databaseName, IAuditEntity auditEntity);

  /**
   * Check if user has specified privilege on the specified database or bigger scope (like ANY).
   *
   * @param userName name of user
   * @param databaseName without `root.` prefix, like db
   * @param privilege specified privilege to be checked
   * @param auditEntity records necessary info for audit log
   * @throws AccessDeniedException if not allowed
   */
  void checkDatabasePrivilege(
      String userName,
      String databaseName,
      TableModelPrivilege privilege,
      IAuditEntity auditEntity);

  void checkDatabasePrivilegeGrantOption(
      String userName,
      String databaseName,
      TableModelPrivilege privilege,
      IAuditEntity auditEntity);

  /**
   * Check if user has specified privilege on the specified table or bigger scope (like database or
   * ANY).
   *
   * @param userName name of user
   * @param tableName qualified name of table without `root.` prefix, like db.table1
   * @param privilege specified privilege to be checked
   * @param auditEntity records necessary info for audit log
   * @throws AccessDeniedException if not allowed
   */
  void checkTablePrivilege(
      String userName,
      QualifiedObjectName tableName,
      TableModelPrivilege privilege,
      IAuditEntity auditEntity);

  void checkTablePrivilegeGrantOption(
      String userName,
      QualifiedObjectName tableName,
      TableModelPrivilege privilege,
      IAuditEntity auditEntity);

  // This does not throw exception for performance issue.
  boolean checkTablePrivilege4Pipe(
      final String userName, final QualifiedObjectName tableName, IAuditEntity auditEntity);

  /**
   * Check if user has any privilege on the specified table or bigger scope (like database or ANY).
   *
   * @param userName name of user
   * @param tableName qualified name of table without `root.` prefix, like db.table1
   * @param auditEntity records necessary info for audit log
   * @throws AccessDeniedException if not allowed
   */
  void checkTableVisibility(
      String userName, QualifiedObjectName tableName, IAuditEntity auditEntity);

  /**
   * Check if user has the specified global privilege
   *
   * @param userName name of user
   * @param privilege specified global privilege to be checked
   * @param auditEntity records necessary info for audit log
   * @throws AccessDeniedException if not allowed
   */
  void checkGlobalPrivilege(
      String userName, TableModelPrivilege privilege, IAuditEntity auditEntity);

  void checkGlobalPrivileges(
      String username, Collection<PrivilegeType> privileges, IAuditEntity auditEntity);

  /**
   * Check if user has the specified global privilege
   *
   * @param userName name of user
   * @param privilege specified global privilege to be checked
   * @param auditEntity records necessary info for audit log
   * @throws AccessDeniedException if not allowed
   */
  void checkGlobalPrivilegeGrantOption(
      String userName, TableModelPrivilege privilege, IAuditEntity auditEntity);

  void checkAnyScopePrivilegeGrantOption(
      String userName, TableModelPrivilege privilege, IAuditEntity auditEntity);
}
