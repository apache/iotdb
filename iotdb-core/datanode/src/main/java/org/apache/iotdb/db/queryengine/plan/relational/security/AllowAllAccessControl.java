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

import org.apache.iotdb.db.queryengine.plan.relational.metadata.QualifiedObjectName;

public class AllowAllAccessControl implements AccessControl {
  @Override
  public void checkCanCreateDatabase(String userName, String databaseName) {
    // allow anything
  }

  @Override
  public void checkCanDropDatabase(String userName, String databaseName) {
    // allow anything
  }

  @Override
  public void checkCanAlterDatabase(String userName, String databaseName) {
    // allow anything
  }

  @Override
  public void checkCanShowOrUseDatabase(String userName, String databaseName) {
    // allow anything
  }

  @Override
  public void checkCanCreateTable(String userName, QualifiedObjectName tableName) {
    // allow anything
  }

  @Override
  public void checkCanDropTable(String userName, QualifiedObjectName tableName) {
    // allow anything
  }

  @Override
  public void checkCanAlterTable(String userName, QualifiedObjectName tableName) {
    // allow anything
  }

  @Override
  public void checkCanInsertIntoTable(String userName, QualifiedObjectName tableName) {
    // allow anything
  }

  @Override
  public void checkCanSelectFromTable(String userName, QualifiedObjectName tableName) {
    // allow anything
  }

  @Override
  public void checkCanDeleteFromTable(String userName, QualifiedObjectName tableName) {
    // allow anything
  }

  @Override
  public void checkCanShowOrDescTable(String userName, QualifiedObjectName tableName) {
    // allow anything
  }

  @Override
  public void checkUserHasMaintainPrivilege(String userName) {
    // allow anything
  }
}
