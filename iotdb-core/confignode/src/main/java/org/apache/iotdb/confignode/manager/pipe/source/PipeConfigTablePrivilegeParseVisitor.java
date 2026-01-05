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

package org.apache.iotdb.confignode.manager.pipe.source;

import org.apache.iotdb.commons.audit.IAuditEntity;
import org.apache.iotdb.commons.auth.entity.PrivilegeType;
import org.apache.iotdb.commons.auth.entity.PrivilegeUnion;
import org.apache.iotdb.confignode.audit.CNAuditLogger;
import org.apache.iotdb.confignode.consensus.request.ConfigPhysicalPlan;
import org.apache.iotdb.confignode.consensus.request.ConfigPhysicalPlanVisitor;
import org.apache.iotdb.confignode.consensus.request.write.auth.AuthorRelationalPlan;
import org.apache.iotdb.confignode.consensus.request.write.database.DatabaseSchemaPlan;
import org.apache.iotdb.confignode.consensus.request.write.database.DeleteDatabasePlan;
import org.apache.iotdb.confignode.consensus.request.write.pipe.payload.PipeCreateTableOrViewPlan;
import org.apache.iotdb.confignode.consensus.request.write.pipe.payload.PipeDeleteDevicesPlan;
import org.apache.iotdb.confignode.consensus.request.write.table.AbstractTablePlan;
import org.apache.iotdb.confignode.consensus.request.write.table.AddTableColumnPlan;
import org.apache.iotdb.confignode.consensus.request.write.table.AlterColumnDataTypePlan;
import org.apache.iotdb.confignode.consensus.request.write.table.CommitDeleteColumnPlan;
import org.apache.iotdb.confignode.consensus.request.write.table.CommitDeleteTablePlan;
import org.apache.iotdb.confignode.consensus.request.write.table.RenameTableColumnPlan;
import org.apache.iotdb.confignode.consensus.request.write.table.RenameTablePlan;
import org.apache.iotdb.confignode.consensus.request.write.table.SetTableColumnCommentPlan;
import org.apache.iotdb.confignode.consensus.request.write.table.SetTableCommentPlan;
import org.apache.iotdb.confignode.consensus.request.write.table.SetTablePropertiesPlan;
import org.apache.iotdb.confignode.manager.ConfigManager;
import org.apache.iotdb.confignode.service.ConfigNode;
import org.apache.iotdb.rpc.TSStatusCode;

import java.util.Optional;

public class PipeConfigTablePrivilegeParseVisitor
    extends ConfigPhysicalPlanVisitor<Optional<ConfigPhysicalPlan>, IAuditEntity> {

  @Override
  public Optional<ConfigPhysicalPlan> visitPlan(
      final ConfigPhysicalPlan plan, final IAuditEntity userEntity) {
    return Optional.of(plan);
  }

  @Override
  public Optional<ConfigPhysicalPlan> visitCreateDatabase(
      final DatabaseSchemaPlan createDatabasePlan, final IAuditEntity userEntity) {
    return visitDatabaseSchemaPlan(createDatabasePlan, userEntity);
  }

  @Override
  public Optional<ConfigPhysicalPlan> visitAlterDatabase(
      final DatabaseSchemaPlan alterDatabasePlan, final IAuditEntity userEntity) {
    return visitDatabaseSchemaPlan(alterDatabasePlan, userEntity);
  }

  public Optional<ConfigPhysicalPlan> visitDatabaseSchemaPlan(
      final DatabaseSchemaPlan databaseSchemaPlan, final IAuditEntity userEntity) {
    return isDatabaseVisible(userEntity, databaseSchemaPlan.getSchema().getName())
        ? Optional.of(databaseSchemaPlan)
        : Optional.empty();
  }

  @Override
  public Optional<ConfigPhysicalPlan> visitDeleteDatabase(
      final DeleteDatabasePlan deleteDatabasePlan, final IAuditEntity userEntity) {
    return isDatabaseVisible(userEntity, deleteDatabasePlan.getName())
        ? Optional.of(deleteDatabasePlan)
        : Optional.empty();
  }

  private boolean isDatabaseVisible(final IAuditEntity userEntity, final String database) {
    final ConfigManager configManager = ConfigNode.getInstance().getConfigManager();
    final CNAuditLogger logger = configManager.getAuditLogger();
    final boolean result =
        configManager
                .getPermissionManager()
                .checkUserPrivileges(userEntity.getUsername(), new PrivilegeUnion(database, null))
                .getStatus()
                .getCode()
            == TSStatusCode.SUCCESS_STATUS.getStatusCode();
    if (result) {
      logger.recordObjectAuthenticationAuditLog(
          userEntity.setPrivilegeType(PrivilegeType.READ_SCHEMA).setResult(true), () -> database);
      return true;
    }
    return PipeConfigTreePrivilegeParseVisitor.hasGlobalPrivilege(
        userEntity, PrivilegeType.SYSTEM, database, true);
  }

  @Override
  public Optional<ConfigPhysicalPlan> visitPipeCreateTableOrView(
      final PipeCreateTableOrViewPlan pipeCreateTableOrViewPlan, final IAuditEntity userEntity) {
    return isTableVisible(
            userEntity,
            pipeCreateTableOrViewPlan.getDatabase(),
            pipeCreateTableOrViewPlan.getTable().getTableName())
        ? Optional.of(pipeCreateTableOrViewPlan)
        : Optional.empty();
  }

  @Override
  public Optional<ConfigPhysicalPlan> visitAddTableColumn(
      final AddTableColumnPlan addTableColumnPlan, final IAuditEntity userEntity) {
    return visitAbstractTablePlan(addTableColumnPlan, userEntity);
  }

  @Override
  public Optional<ConfigPhysicalPlan> visitSetTableProperties(
      final SetTablePropertiesPlan setTablePropertiesPlan, final IAuditEntity userEntity) {
    return visitAbstractTablePlan(setTablePropertiesPlan, userEntity);
  }

  @Override
  public Optional<ConfigPhysicalPlan> visitCommitDeleteColumn(
      final CommitDeleteColumnPlan commitDeleteColumnPlan, final IAuditEntity userEntity) {
    return visitAbstractTablePlan(commitDeleteColumnPlan, userEntity);
  }

  @Override
  public Optional<ConfigPhysicalPlan> visitRenameTableColumn(
      final RenameTableColumnPlan renameTableColumnPlan, final IAuditEntity userEntity) {
    return visitAbstractTablePlan(renameTableColumnPlan, userEntity);
  }

  @Override
  public Optional<ConfigPhysicalPlan> visitCommitDeleteTable(
      final CommitDeleteTablePlan commitDeleteTablePlan, final IAuditEntity userEntity) {
    return visitAbstractTablePlan(commitDeleteTablePlan, userEntity);
  }

  @Override
  public Optional<ConfigPhysicalPlan> visitPipeDeleteDevices(
      final PipeDeleteDevicesPlan pipeDeleteDevicesPlan, final IAuditEntity userEntity) {
    return visitAbstractTablePlan(pipeDeleteDevicesPlan, userEntity);
  }

  @Override
  public Optional<ConfigPhysicalPlan> visitSetTableComment(
      final SetTableCommentPlan setTableCommentPlan, final IAuditEntity userEntity) {
    return visitAbstractTablePlan(setTableCommentPlan, userEntity);
  }

  @Override
  public Optional<ConfigPhysicalPlan> visitSetTableColumnComment(
      final SetTableColumnCommentPlan setTableColumnCommentPlan, final IAuditEntity userEntity) {
    return visitAbstractTablePlan(setTableColumnCommentPlan, userEntity);
  }

  @Override
  public Optional<ConfigPhysicalPlan> visitRenameTable(
      final RenameTablePlan renameTablePlan, final IAuditEntity userEntity) {
    return visitAbstractTablePlan(renameTablePlan, userEntity);
  }

  private Optional<ConfigPhysicalPlan> visitAbstractTablePlan(
      final AbstractTablePlan plan, final IAuditEntity userEntity) {
    return isTableVisible(userEntity, plan.getDatabase(), plan.getTableName())
        ? Optional.of(plan)
        : Optional.empty();
  }

  private boolean isTableVisible(
      final IAuditEntity userEntity, final String database, final String tableName) {
    final ConfigManager configManager = ConfigNode.getInstance().getConfigManager();
    final CNAuditLogger logger = configManager.getAuditLogger();
    final boolean result =
        configManager
                .getPermissionManager()
                .checkUserPrivileges(
                    userEntity.getUsername(), new PrivilegeUnion(database, tableName, null))
                .getStatus()
                .getCode()
            == TSStatusCode.SUCCESS_STATUS.getStatusCode();
    if (result) {
      logger.recordObjectAuthenticationAuditLog(
          userEntity.setPrivilegeType(PrivilegeType.READ_SCHEMA).setResult(true), () -> database);
      return true;
    }
    return PipeConfigTreePrivilegeParseVisitor.hasGlobalPrivilege(
        userEntity, PrivilegeType.SYSTEM, tableName, true);
  }

  @Override
  public Optional<ConfigPhysicalPlan> visitRCreateUser(
      final AuthorRelationalPlan rCreateUserPlan, final IAuditEntity userEntity) {
    return visitUserPlan(rCreateUserPlan, userEntity);
  }

  @Override
  public Optional<ConfigPhysicalPlan> visitRCreateRole(
      final AuthorRelationalPlan rCreateRolePlan, final IAuditEntity userEntity) {
    return visitRolePlan(rCreateRolePlan, userEntity);
  }

  @Override
  public Optional<ConfigPhysicalPlan> visitRUpdateUser(
      final AuthorRelationalPlan rUpdateUserPlan, final IAuditEntity userEntity) {
    return visitUserPlan(rUpdateUserPlan, userEntity);
  }

  @Override
  public Optional<ConfigPhysicalPlan> visitRDropUserPlan(
      final AuthorRelationalPlan rDropUserPlan, final IAuditEntity userEntity) {
    return visitUserPlan(rDropUserPlan, userEntity);
  }

  @Override
  public Optional<ConfigPhysicalPlan> visitRDropRolePlan(
      final AuthorRelationalPlan rDropRolePlan, final IAuditEntity userEntity) {
    return visitRolePlan(rDropRolePlan, userEntity);
  }

  @Override
  public Optional<ConfigPhysicalPlan> visitRGrantUserRole(
      final AuthorRelationalPlan rGrantUserRolePlan, final IAuditEntity userEntity) {
    return visitUserRolePlan(rGrantUserRolePlan, userEntity);
  }

  @Override
  public Optional<ConfigPhysicalPlan> visitRRevokeUserRole(
      final AuthorRelationalPlan rRevokeUserRolePlan, final IAuditEntity userEntity) {
    return visitUserRolePlan(rRevokeUserRolePlan, userEntity);
  }

  @Override
  public Optional<ConfigPhysicalPlan> visitRGrantUserAny(
      final AuthorRelationalPlan rGrantUserAnyPlan, final IAuditEntity userEntity) {
    return visitUserPlan(rGrantUserAnyPlan, userEntity);
  }

  @Override
  public Optional<ConfigPhysicalPlan> visitRGrantRoleAny(
      final AuthorRelationalPlan rGrantRoleAnyPlan, final IAuditEntity userEntity) {
    return visitRolePlan(rGrantRoleAnyPlan, userEntity);
  }

  @Override
  public Optional<ConfigPhysicalPlan> visitRGrantUserAll(
      final AuthorRelationalPlan rGrantUserAllPlan, final IAuditEntity userEntity) {
    return visitUserPlan(rGrantUserAllPlan, userEntity);
  }

  @Override
  public Optional<ConfigPhysicalPlan> visitRGrantRoleAll(
      final AuthorRelationalPlan rGrantRoleAllPlan, final IAuditEntity userEntity) {
    return visitRolePlan(rGrantRoleAllPlan, userEntity);
  }

  @Override
  public Optional<ConfigPhysicalPlan> visitRGrantUserDB(
      final AuthorRelationalPlan rGrantUserDBPlan, final IAuditEntity userEntity) {
    return visitUserPlan(rGrantUserDBPlan, userEntity);
  }

  @Override
  public Optional<ConfigPhysicalPlan> visitRGrantUserTB(
      final AuthorRelationalPlan rGrantUserTBPlan, final IAuditEntity userEntity) {
    return visitUserPlan(rGrantUserTBPlan, userEntity);
  }

  @Override
  public Optional<ConfigPhysicalPlan> visitRGrantRoleDB(
      final AuthorRelationalPlan rGrantRoleDBPlan, final IAuditEntity userEntity) {
    return visitRolePlan(rGrantRoleDBPlan, userEntity);
  }

  @Override
  public Optional<ConfigPhysicalPlan> visitRGrantRoleTB(
      final AuthorRelationalPlan rGrantRoleTBPlan, final IAuditEntity userEntity) {
    return visitRolePlan(rGrantRoleTBPlan, userEntity);
  }

  @Override
  public Optional<ConfigPhysicalPlan> visitRRevokeUserAny(
      final AuthorRelationalPlan rRevokeUserAnyPlan, final IAuditEntity userEntity) {
    return visitUserPlan(rRevokeUserAnyPlan, userEntity);
  }

  @Override
  public Optional<ConfigPhysicalPlan> visitRRevokeRoleAny(
      final AuthorRelationalPlan rRevokeRoleAnyPlan, final IAuditEntity userEntity) {
    return visitRolePlan(rRevokeRoleAnyPlan, userEntity);
  }

  @Override
  public Optional<ConfigPhysicalPlan> visitRRevokeUserAll(
      final AuthorRelationalPlan rRevokeUserAllPlan, final IAuditEntity userEntity) {
    return visitUserPlan(rRevokeUserAllPlan, userEntity);
  }

  @Override
  public Optional<ConfigPhysicalPlan> visitRRevokeRoleAll(
      final AuthorRelationalPlan rRevokeRoleAllPlan, final IAuditEntity userEntity) {
    return visitRolePlan(rRevokeRoleAllPlan, userEntity);
  }

  @Override
  public Optional<ConfigPhysicalPlan> visitRRevokeUserDBPrivilege(
      final AuthorRelationalPlan rRevokeUserDBPrivilegePlan, final IAuditEntity userEntity) {
    return visitUserPlan(rRevokeUserDBPrivilegePlan, userEntity);
  }

  @Override
  public Optional<ConfigPhysicalPlan> visitRRevokeUserTBPrivilege(
      final AuthorRelationalPlan rRevokeUserTBPrivilegePlan, final IAuditEntity userEntity) {
    return visitUserPlan(rRevokeUserTBPrivilegePlan, userEntity);
  }

  @Override
  public Optional<ConfigPhysicalPlan> visitRRevokeRoleDBPrivilege(
      final AuthorRelationalPlan rRevokeRoleTBPrivilegePlan, final IAuditEntity userEntity) {
    return visitRolePlan(rRevokeRoleTBPrivilegePlan, userEntity);
  }

  @Override
  public Optional<ConfigPhysicalPlan> visitRRevokeRoleTBPrivilege(
      final AuthorRelationalPlan rRevokeRoleTBPrivilegePlan, final IAuditEntity userEntity) {
    return visitRolePlan(rRevokeRoleTBPrivilegePlan, userEntity);
  }

  @Override
  public Optional<ConfigPhysicalPlan> visitRGrantUserSysPrivilege(
      final AuthorRelationalPlan rGrantUserSysPrivilegePlan, final IAuditEntity userEntity) {
    return visitUserPlan(rGrantUserSysPrivilegePlan, userEntity);
  }

  @Override
  public Optional<ConfigPhysicalPlan> visitRGrantRoleSysPrivilege(
      final AuthorRelationalPlan rGrantRoleSysPrivilegePlan, final IAuditEntity userEntity) {
    return visitRolePlan(rGrantRoleSysPrivilegePlan, userEntity);
  }

  @Override
  public Optional<ConfigPhysicalPlan> visitRRevokeUserSysPrivilege(
      final AuthorRelationalPlan rRevokeUserSysPrivilegePlan, final IAuditEntity userEntity) {
    return visitUserPlan(rRevokeUserSysPrivilegePlan, userEntity);
  }

  @Override
  public Optional<ConfigPhysicalPlan> visitRRevokeRoleSysPrivilege(
      final AuthorRelationalPlan rRevokeRoleSysPrivilegePlan, final IAuditEntity userEntity) {
    return visitRolePlan(rRevokeRoleSysPrivilegePlan, userEntity);
  }

  private Optional<ConfigPhysicalPlan> visitUserPlan(
      final AuthorRelationalPlan plan, final IAuditEntity userEntity) {
    return PipeConfigTreePrivilegeParseVisitor.visitUserPlan(plan, userEntity);
  }

  private Optional<ConfigPhysicalPlan> visitRolePlan(
      final AuthorRelationalPlan plan, final IAuditEntity userEntity) {
    return PipeConfigTreePrivilegeParseVisitor.visitRolePlan(plan, userEntity);
  }

  private Optional<ConfigPhysicalPlan> visitUserRolePlan(
      final AuthorRelationalPlan plan, final IAuditEntity userEntity) {
    return PipeConfigTreePrivilegeParseVisitor.visitUserRolePlan(plan, userEntity);
  }

  public Optional<ConfigPhysicalPlan> visitAlterColumnDataType(
      final AlterColumnDataTypePlan alterColumnDataTypePlan, final IAuditEntity userEntity) {
    return visitAbstractTablePlan(alterColumnDataTypePlan, userEntity);
  }
}
