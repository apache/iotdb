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
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.QualifiedObjectName;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.RelationalAuthorStatement;

public interface AccessControl {

  /**
   * Check if user is allowed to create the specified database.
   *
   * @param userName name of user
   * @param databaseName without `root.` prefix, like db
   * @throws AccessDeniedException if not allowed
   */
  void checkCanCreateDatabase(String userName, String databaseName);

  /**
   * Check if user is allowed to drop the specified database.
   *
   * @param userName name of user
   * @param databaseName without `root.` prefix, like db
   * @throws AccessDeniedException if not allowed
   */
  void checkCanDropDatabase(String userName, String databaseName);

  /**
   * Check if user is allowed to alter the specified database.
   *
   * @param userName name of user
   * @param databaseName without `root.` prefix, like db
   * @throws AccessDeniedException if not allowed
   */
  void checkCanAlterDatabase(String userName, String databaseName);

  /**
   * Check if user is allowed to show or use the specified database.
   *
   * @param userName name of user
   * @param databaseName without `root.` prefix, like db
   * @throws AccessDeniedException if not allowed
   */
  void checkCanShowOrUseDatabase(final String userName, final String databaseName);

  /**
   * Check if user is allowed to create the specified table.
   *
   * @param userName name of user
   * @param tableName qualified name of table without `root.` prefix, like db.table1
   * @throws AccessDeniedException if not allowed
   */
  void checkCanCreateTable(String userName, QualifiedObjectName tableName);

  /**
   * Check if user is allowed to create the specified table.
   *
   * @param userName name of user
   * @param tableName qualified name of table without `root.` prefix, like db.table1
   * @throws AccessDeniedException if not allowed
   */
  void checkCanDropTable(String userName, QualifiedObjectName tableName);

  /**
   * Check if user is allowed to alter the specified table.
   *
   * @param userName name of user
   * @param tableName qualified name of table without `root.` prefix, like db.table1
   * @throws AccessDeniedException if not allowed
   */
  void checkCanAlterTable(String userName, QualifiedObjectName tableName);

  /**
   * Check if user is allowed to insert into the specified table.
   *
   * @param userName name of user
   * @param tableName qualified name of table without `root.` prefix, like db.table1
   * @throws AccessDeniedException if not allowed
   */
  void checkCanInsertIntoTable(String userName, QualifiedObjectName tableName);

  /**
   * Check if user is allowed to select from the specified table.
   *
   * @param userName name of user
   * @param tableName qualified name of table without `root.` prefix, like db.table1
   * @throws AccessDeniedException if not allowed
   */
  void checkCanSelectFromTable(String userName, QualifiedObjectName tableName);

  /**
   * Check if user is allowed to extract certain data from pipe.
   *
   * @param userName name of user
   * @param databaseName the databaseName
   * @throws AccessDeniedException if not allowed
   */
  void checkCanSelectFromDatabase4Pipe(final String userName, final String databaseName);

  // This does not throw exception for performance issues
  boolean checkCanSelectFromTable4Pipe(final String userName, final QualifiedObjectName tableName);

  /**
   * Check if user is allowed to delete from the specified table.
   *
   * @param userName name of user
   * @param tableName qualified name of table without `root.` prefix, like db.table1
   * @throws AccessDeniedException if not allowed
   */
  void checkCanDeleteFromTable(String userName, QualifiedObjectName tableName);

  /**
   * Check if user is allowed to show or describe the specified table.
   *
   * @param userName name of user
   * @param tableName qualified name of table without `root.` prefix, like db.table1
   * @throws AccessDeniedException if not allowed
   */
  void checkCanShowOrDescTable(String userName, QualifiedObjectName tableName);

  /**
   * Check if user is allowed to create view under the specific tree path.
   *
   * @param userName name of user
   * @param path the tree path scope the view can select from
   * @throws AccessDeniedException if not allowed
   */
  void checkCanCreateViewFromTreePath(final String userName, final PartialPath path);

  /**
   * Check if user can run relational author statement.
   *
   * @param userName name of user
   * @throws AccessDeniedException if not allowed
   */
  void checkUserCanRunRelationalAuthorStatement(
      String userName, RelationalAuthorStatement statement);

  /**
   * Check if user is admin user
   *
   * @param userName name of user
   * @throws AccessDeniedException if not allowed
   */
  void checkUserIsAdmin(String userName);
}
