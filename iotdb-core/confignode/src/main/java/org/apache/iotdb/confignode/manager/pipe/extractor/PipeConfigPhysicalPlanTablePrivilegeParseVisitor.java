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

package org.apache.iotdb.confignode.manager.pipe.extractor;

import org.apache.iotdb.commons.auth.entity.PrivilegeType;
import org.apache.iotdb.commons.auth.entity.PrivilegeUnion;
import org.apache.iotdb.confignode.consensus.request.ConfigPhysicalPlan;
import org.apache.iotdb.confignode.consensus.request.ConfigPhysicalPlanVisitor;
import org.apache.iotdb.confignode.consensus.request.write.auth.AuthorRelationalPlan;
import org.apache.iotdb.confignode.consensus.request.write.database.DatabaseSchemaPlan;
import org.apache.iotdb.confignode.consensus.request.write.database.DeleteDatabasePlan;
import org.apache.iotdb.confignode.consensus.request.write.pipe.payload.PipeCreateTablePlan;
import org.apache.iotdb.confignode.consensus.request.write.pipe.payload.PipeDeleteDevicesPlan;
import org.apache.iotdb.confignode.consensus.request.write.table.AbstractTablePlan;
import org.apache.iotdb.confignode.consensus.request.write.table.AddTableColumnPlan;
import org.apache.iotdb.confignode.consensus.request.write.table.CommitDeleteColumnPlan;
import org.apache.iotdb.confignode.consensus.request.write.table.CommitDeleteTablePlan;
import org.apache.iotdb.confignode.consensus.request.write.table.RenameTableColumnPlan;
import org.apache.iotdb.confignode.consensus.request.write.table.SetTableColumnCommentPlan;
import org.apache.iotdb.confignode.consensus.request.write.table.SetTableCommentPlan;
import org.apache.iotdb.confignode.consensus.request.write.table.SetTablePropertiesPlan;
import org.apache.iotdb.confignode.service.ConfigNode;
import org.apache.iotdb.rpc.TSStatusCode;

import java.util.Optional;

public class PipeConfigPhysicalPlanTablePrivilegeParseVisitor
    extends ConfigPhysicalPlanVisitor<Optional<ConfigPhysicalPlan>, String> {

  @Override
  public Optional<ConfigPhysicalPlan> visitPlan(
      final ConfigPhysicalPlan plan, final String userName) {
    return Optional.of(plan);
  }

  @Override
  public Optional<ConfigPhysicalPlan> visitCreateDatabase(
      final DatabaseSchemaPlan createDatabasePlan, final String userName) {
    return visitDatabaseSchemaPlan(createDatabasePlan, userName);
  }

  @Override
  public Optional<ConfigPhysicalPlan> visitAlterDatabase(
      final DatabaseSchemaPlan alterDatabasePlan, final String userName) {
    return visitDatabaseSchemaPlan(alterDatabasePlan, userName);
  }

  public Optional<ConfigPhysicalPlan> visitDatabaseSchemaPlan(
      final DatabaseSchemaPlan databaseSchemaPlan, final String userName) {
    return ConfigNode.getInstance()
                .getConfigManager()
                .getPermissionManager()
                .checkUserPrivileges(
                    userName, new PrivilegeUnion(databaseSchemaPlan.getSchema().getName(), null))
                .getStatus()
                .getCode()
            == TSStatusCode.SUCCESS_STATUS.getStatusCode()
        ? Optional.of(databaseSchemaPlan)
        : Optional.empty();
  }

  @Override
  public Optional<ConfigPhysicalPlan> visitDeleteDatabase(
      final DeleteDatabasePlan deleteDatabasePlan, final String userName) {
    return ConfigNode.getInstance()
                .getConfigManager()
                .getPermissionManager()
                .checkUserPrivileges(
                    userName, new PrivilegeUnion(deleteDatabasePlan.getName(), null))
                .getStatus()
                .getCode()
            == TSStatusCode.SUCCESS_STATUS.getStatusCode()
        ? Optional.of(deleteDatabasePlan)
        : Optional.empty();
  }

  @Override
  public Optional<ConfigPhysicalPlan> visitPipeCreateTable(
      final PipeCreateTablePlan pipeCreateTablePlan, final String userName) {
    return ConfigNode.getInstance()
                .getConfigManager()
                .getPermissionManager()
                .checkUserPrivileges(
                    userName,
                    new PrivilegeUnion(
                        pipeCreateTablePlan.getDatabase(),
                        pipeCreateTablePlan.getTable().getTableName(),
                        null))
                .getStatus()
                .getCode()
            == TSStatusCode.SUCCESS_STATUS.getStatusCode()
        ? Optional.of(pipeCreateTablePlan)
        : Optional.empty();
  }

  @Override
  public Optional<ConfigPhysicalPlan> visitAddTableColumn(
      final AddTableColumnPlan addTableColumnPlan, final String userName) {
    return visitAbstractTablePlan(addTableColumnPlan, userName);
  }

  @Override
  public Optional<ConfigPhysicalPlan> visitSetTableProperties(
      final SetTablePropertiesPlan setTablePropertiesPlan, final String userName) {
    return visitAbstractTablePlan(setTablePropertiesPlan, userName);
  }

  @Override
  public Optional<ConfigPhysicalPlan> visitCommitDeleteColumn(
      final CommitDeleteColumnPlan commitDeleteColumnPlan, final String userName) {
    return visitAbstractTablePlan(commitDeleteColumnPlan, userName);
  }

  @Override
  public Optional<ConfigPhysicalPlan> visitRenameTableColumn(
      final RenameTableColumnPlan renameTableColumnPlan, final String userName) {
    return visitAbstractTablePlan(renameTableColumnPlan, userName);
  }

  @Override
  public Optional<ConfigPhysicalPlan> visitCommitDeleteTable(
      final CommitDeleteTablePlan commitDeleteTablePlan, final String userName) {
    return visitAbstractTablePlan(commitDeleteTablePlan, userName);
  }

  @Override
  public Optional<ConfigPhysicalPlan> visitPipeDeleteDevices(
      final PipeDeleteDevicesPlan pipeDeleteDevicesPlan, final String userName) {
    return visitAbstractTablePlan(pipeDeleteDevicesPlan, userName);
  }

  @Override
  public Optional<ConfigPhysicalPlan> visitSetTableComment(
      final SetTableCommentPlan setTableCommentPlan, final String userName) {
    return visitAbstractTablePlan(setTableCommentPlan, userName);
  }

  @Override
  public Optional<ConfigPhysicalPlan> visitSetTableColumnComment(
      final SetTableColumnCommentPlan setTableColumnCommentPlan, final String userName) {
    return visitAbstractTablePlan(setTableColumnCommentPlan, userName);
  }

  private Optional<ConfigPhysicalPlan> visitAbstractTablePlan(
      final AbstractTablePlan plan, final String userName) {
    return ConfigNode.getInstance()
                .getConfigManager()
                .getPermissionManager()
                .checkUserPrivileges(
                    userName, new PrivilegeUnion(plan.getDatabase(), plan.getTableName(), null))
                .getStatus()
                .getCode()
            == TSStatusCode.SUCCESS_STATUS.getStatusCode()
        ? Optional.of(plan)
        : Optional.empty();
  }

  @Override
  public Optional<ConfigPhysicalPlan> visitRCreateUser(
      final AuthorRelationalPlan rCreateUserPlan, final String userName) {
    return visitUserPlan(rCreateUserPlan, userName);
  }

  @Override
  public Optional<ConfigPhysicalPlan> visitRCreateRole(
      final AuthorRelationalPlan rCreateRolePlan, final String userName) {
    return visitRolePlan(rCreateRolePlan, userName);
  }

  @Override
  public Optional<ConfigPhysicalPlan> visitRUpdateUser(
      final AuthorRelationalPlan rUpdateUserPlan, final String userName) {
    return visitUserPlan(rUpdateUserPlan, userName);
  }

  @Override
  public Optional<ConfigPhysicalPlan> visitRDropUserPlan(
      final AuthorRelationalPlan rDropUserPlan, final String userName) {
    return visitUserPlan(rDropUserPlan, userName);
  }

  @Override
  public Optional<ConfigPhysicalPlan> visitRDropRolePlan(
      final AuthorRelationalPlan rDropRolePlan, final String userName) {
    return visitRolePlan(rDropRolePlan, userName);
  }

  @Override
  public Optional<ConfigPhysicalPlan> visitRGrantUserRole(
      final AuthorRelationalPlan rGrantUserRolePlan, final String userName) {
    return visitUserRolePlan(rGrantUserRolePlan, userName);
  }

  @Override
  public Optional<ConfigPhysicalPlan> visitRRevokeUserRole(
      final AuthorRelationalPlan rRevokeUserRolePlan, final String userName) {
    return visitUserRolePlan(rRevokeUserRolePlan, userName);
  }

  @Override
  public Optional<ConfigPhysicalPlan> visitRGrantUserAny(
      final AuthorRelationalPlan rGrantUserAnyPlan, final String userName) {
    return visitUserPlan(rGrantUserAnyPlan, userName);
  }

  @Override
  public Optional<ConfigPhysicalPlan> visitRGrantRoleAny(
      final AuthorRelationalPlan rGrantRoleAnyPlan, final String userName) {
    return visitRolePlan(rGrantRoleAnyPlan, userName);
  }

  @Override
  public Optional<ConfigPhysicalPlan> visitRGrantUserAll(
      final AuthorRelationalPlan rGrantUserAllPlan, final String userName) {
    return visitUserPlan(rGrantUserAllPlan, userName);
  }

  @Override
  public Optional<ConfigPhysicalPlan> visitRGrantRoleAll(
      final AuthorRelationalPlan rGrantRoleAllPlan, final String userName) {
    return visitRolePlan(rGrantRoleAllPlan, userName);
  }

  @Override
  public Optional<ConfigPhysicalPlan> visitRGrantUserDB(
      final AuthorRelationalPlan rGrantUserDBPlan, final String userName) {
    return visitUserPlan(rGrantUserDBPlan, userName);
  }

  @Override
  public Optional<ConfigPhysicalPlan> visitRGrantUserTB(
      final AuthorRelationalPlan rGrantUserTBPlan, final String userName) {
    return visitUserPlan(rGrantUserTBPlan, userName);
  }

  @Override
  public Optional<ConfigPhysicalPlan> visitRGrantRoleDB(
      final AuthorRelationalPlan rGrantRoleDBPlan, final String userName) {
    return visitRolePlan(rGrantRoleDBPlan, userName);
  }

  @Override
  public Optional<ConfigPhysicalPlan> visitRGrantRoleTB(
      final AuthorRelationalPlan rGrantRoleTBPlan, final String userName) {
    return visitRolePlan(rGrantRoleTBPlan, userName);
  }

  @Override
  public Optional<ConfigPhysicalPlan> visitRRevokeUserAny(
      final AuthorRelationalPlan rRevokeUserAnyPlan, final String userName) {
    return visitUserPlan(rRevokeUserAnyPlan, userName);
  }

  @Override
  public Optional<ConfigPhysicalPlan> visitRRevokeRoleAny(
      final AuthorRelationalPlan rRevokeRoleAnyPlan, final String userName) {
    return visitRolePlan(rRevokeRoleAnyPlan, userName);
  }

  @Override
  public Optional<ConfigPhysicalPlan> visitRRevokeUserAll(
      final AuthorRelationalPlan rRevokeUserAllPlan, final String userName) {
    return visitUserPlan(rRevokeUserAllPlan, userName);
  }

  @Override
  public Optional<ConfigPhysicalPlan> visitRRevokeRoleAll(
      final AuthorRelationalPlan rRevokeRoleAllPlan, final String userName) {
    return visitRolePlan(rRevokeRoleAllPlan, userName);
  }

  @Override
  public Optional<ConfigPhysicalPlan> visitRRevokeUserDBPrivilege(
      final AuthorRelationalPlan rRevokeUserDBPrivilegePlan, final String userName) {
    return visitUserPlan(rRevokeUserDBPrivilegePlan, userName);
  }

  @Override
  public Optional<ConfigPhysicalPlan> visitRRevokeUserTBPrivilege(
      final AuthorRelationalPlan rRevokeUserTBPrivilegePlan, final String userName) {
    return visitUserPlan(rRevokeUserTBPrivilegePlan, userName);
  }

  @Override
  public Optional<ConfigPhysicalPlan> visitRRevokeRoleDBPrivilege(
      final AuthorRelationalPlan rRevokeRoleTBPrivilegePlan, final String userName) {
    return visitRolePlan(rRevokeRoleTBPrivilegePlan, userName);
  }

  @Override
  public Optional<ConfigPhysicalPlan> visitRRevokeRoleTBPrivilege(
      final AuthorRelationalPlan rRevokeRoleTBPrivilegePlan, final String userName) {
    return visitRolePlan(rRevokeRoleTBPrivilegePlan, userName);
  }

  @Override
  public Optional<ConfigPhysicalPlan> visitRGrantUserSysPrivilege(
      final AuthorRelationalPlan rGrantUserSysPrivilegePlan, final String userName) {
    return visitUserPlan(rGrantUserSysPrivilegePlan, userName);
  }

  @Override
  public Optional<ConfigPhysicalPlan> visitRGrantRoleSysPrivilege(
      final AuthorRelationalPlan rGrantRoleSysPrivilegePlan, final String userName) {
    return visitRolePlan(rGrantRoleSysPrivilegePlan, userName);
  }

  @Override
  public Optional<ConfigPhysicalPlan> visitRRevokeUserSysPrivilege(
      final AuthorRelationalPlan rRevokeUserSysPrivilegePlan, final String userName) {
    return visitUserPlan(rRevokeUserSysPrivilegePlan, userName);
  }

  @Override
  public Optional<ConfigPhysicalPlan> visitRRevokeRoleSysPrivilege(
      final AuthorRelationalPlan rRevokeRoleSysPrivilegePlan, final String userName) {
    return visitRolePlan(rRevokeRoleSysPrivilegePlan, userName);
  }

  private Optional<ConfigPhysicalPlan> visitUserPlan(
      final AuthorRelationalPlan plan, final String userName) {
    return ConfigNode.getInstance()
                .getConfigManager()
                .getPermissionManager()
                .checkUserPrivileges(userName, new PrivilegeUnion(PrivilegeType.MANAGE_USER))
                .getStatus()
                .getCode()
            == TSStatusCode.SUCCESS_STATUS.getStatusCode()
        ? Optional.of(plan)
        : Optional.empty();
  }

  private Optional<ConfigPhysicalPlan> visitRolePlan(
      final AuthorRelationalPlan plan, final String userName) {
    return ConfigNode.getInstance()
                .getConfigManager()
                .getPermissionManager()
                .checkUserPrivileges(userName, new PrivilegeUnion(PrivilegeType.MANAGE_ROLE))
                .getStatus()
                .getCode()
            == TSStatusCode.SUCCESS_STATUS.getStatusCode()
        ? Optional.of(plan)
        : Optional.empty();
  }

  private Optional<ConfigPhysicalPlan> visitUserRolePlan(
      final AuthorRelationalPlan plan, final String userName) {
    return ConfigNode.getInstance()
                    .getConfigManager()
                    .getPermissionManager()
                    .checkUserPrivileges(userName, new PrivilegeUnion(PrivilegeType.MANAGE_ROLE))
                    .getStatus()
                    .getCode()
                == TSStatusCode.SUCCESS_STATUS.getStatusCode()
            || ConfigNode.getInstance()
                    .getConfigManager()
                    .getPermissionManager()
                    .checkUserPrivileges(userName, new PrivilegeUnion(PrivilegeType.MANAGE_USER))
                    .getStatus()
                    .getCode()
                == TSStatusCode.SUCCESS_STATUS.getStatusCode()
        ? Optional.of(plan)
        : Optional.empty();
  }
}
