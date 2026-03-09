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
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.QualifiedObjectName;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.RelationalAuthorStatement;
import org.apache.iotdb.db.queryengine.plan.statement.Statement;

import org.apache.tsfile.file.metadata.IDeviceID;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.function.Supplier;

import static org.apache.iotdb.db.auth.AuthorityChecker.SUCCEED;

public class AllowAllAccessControl implements AccessControl {
  @Override
  public void checkCanCreateDatabase(
      String userName, String databaseName, IAuditEntity auditEntity) {}

  @Override
  public void checkCanDropDatabase(
      String userName, String databaseName, IAuditEntity auditEntity) {}

  @Override
  public void checkCanAlterDatabase(
      String userName, String databaseName, IAuditEntity auditEntity) {}

  @Override
  public void checkCanShowOrUseDatabase(
      String userName, String databaseName, IAuditEntity auditEntity) {}

  @Override
  public void checkCanCreateTable(
      String userName, QualifiedObjectName tableName, IAuditEntity auditEntity) {}

  @Override
  public void checkCanDropTable(
      String userName, QualifiedObjectName tableName, IAuditEntity auditEntity) {}

  @Override
  public void checkCanAlterTable(
      String userName, QualifiedObjectName tableName, IAuditEntity auditEntity) {}

  @Override
  public void checkCanInsertIntoTable(
      String userName, QualifiedObjectName tableName, IAuditEntity auditEntity) {}

  @Override
  public void checkCanSelectFromTable(
      String userName, QualifiedObjectName tableName, IAuditEntity auditEntity) {}

  @Override
  public void checkCanSelectFromDatabase4Pipe(
      String userName, String databaseName, IAuditEntity auditEntity) {}

  @Override
  public boolean checkCanSelectFromTable4Pipe(
      String userName, QualifiedObjectName tableName, IAuditEntity auditEntity) {
    return false;
  }

  @Override
  public void checkCanDeleteFromTable(
      String userName, QualifiedObjectName tableName, IAuditEntity auditEntity) {}

  @Override
  public void checkCanShowOrDescTable(
      String userName, QualifiedObjectName tableName, IAuditEntity auditEntity) {}

  @Override
  public void checkCanCreateViewFromTreePath(PartialPath path, IAuditEntity auditEntity) {}

  @Override
  public void checkUserCanRunRelationalAuthorStatement(
      String userName, RelationalAuthorStatement statement, IAuditEntity auditEntity) {}

  @Override
  public void checkUserIsAdmin(IAuditEntity entity) {
    // allow anything
  }

  @Override
  public void checkUserGlobalSysPrivilege(IAuditEntity auditEntity) {}

  @Override
  public boolean hasGlobalPrivilege(IAuditEntity entity, PrivilegeType privilegeType) {
    return true;
  }

  @Override
  public void checkMissingPrivileges(
      String username, Collection<PrivilegeType> privilegeTypes, IAuditEntity auditEntity) {}

  @Override
  public TSStatus checkPermissionBeforeProcess(
      Statement statement, TreeAccessCheckContext context) {
    return SUCCEED;
  }

  @Override
  public TSStatus checkFullPathWriteDataPermission(
      IAuditEntity auditEntity, IDeviceID device, String measurementId) {
    return SUCCEED;
  }

  @Override
  public TSStatus checkCanCreateDatabaseForTree(IAuditEntity entity, PartialPath databaseName) {
    return SUCCEED;
  }

  @Override
  public TSStatus checkCanAlterTemplate(IAuditEntity entity, Supplier<String> auditObject) {
    return SUCCEED;
  }

  @Override
  public TSStatus checkCanAlterView(
      IAuditEntity entity, List<PartialPath> sourcePaths, List<PartialPath> targetPaths) {
    return SUCCEED;
  }

  @Override
  public TSStatus checkSeriesPrivilege4Pipe(
      IAuditEntity context,
      List<? extends PartialPath> checkedPathsSupplier,
      PrivilegeType permission) {
    return SUCCEED;
  }

  @Override
  public List<Integer> checkSeriesPrivilegeWithIndexes4Pipe(
      IAuditEntity context,
      List<? extends PartialPath> checkedPathsSupplier,
      PrivilegeType permission) {
    return Collections.emptyList();
  }

  @Override
  public TSStatus allowUserToLogin(String userName) {
    return SUCCEED;
  }
}
