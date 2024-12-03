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

import org.apache.iotdb.commons.exception.auth.AccessDeniedException;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.QualifiedObjectName;

public interface ITableAuthChecker {

  /**
   * Check if user has any privilege on ANY, @param{databaseName} or any tables
   * in @param{databaseName}
   *
   * @param userName name of user
   * @param databaseName without `root.` prefix, like db
   * @throws AccessDeniedException if not allowed
   */
  void checkDatabaseVisibility(String userName, String databaseName);

  /**
   * Check if user has specified privilege on the specified database or bigger scope (like ANY).
   *
   * @param userName name of user
   * @param databaseName without `root.` prefix, like db
   * @param privilege specified privilege to be checked
   * @throws AccessDeniedException if not allowed
   */
  void checkDatabasePrivilege(String userName, String databaseName, TableModelPrivilege privilege);

  /**
   * Check if user has specified privilege on the specified table or bigger scope (like database or
   * ANY).
   *
   * @param userName name of user
   * @param tableName qualified name of table without `root.` prefix, like db.table1
   * @param privilege specified privilege to be checked
   * @throws AccessDeniedException if not allowed
   */
  void checkTablePrivilege(
      String userName, QualifiedObjectName tableName, TableModelPrivilege privilege);

  /**
   * Check if user has any privilege on the specified table or bigger scope (like database or ANY).
   *
   * @param userName name of user
   * @param tableName qualified name of table without `root.` prefix, like db.table1
   * @throws AccessDeniedException if not allowed
   */
  void checkTableVisibility(String userName, QualifiedObjectName tableName);

  /**
   * Check if user has the specified global privilege
   *
   * @param userName name of user
   * @param privilege specified global privilege to be checked
   * @throws AccessDeniedException if not allowed
   */
  void checkGlobalPrivilege(String userName, TableModelPrivilege privilege);
}
