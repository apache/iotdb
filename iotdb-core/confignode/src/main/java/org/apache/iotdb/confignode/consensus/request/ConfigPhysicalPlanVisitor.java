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

package org.apache.iotdb.confignode.consensus.request;

import org.apache.iotdb.confignode.consensus.request.write.auth.AuthorPlan;
import org.apache.iotdb.confignode.consensus.request.write.database.DatabaseSchemaPlan;
import org.apache.iotdb.confignode.consensus.request.write.database.DeleteDatabasePlan;
import org.apache.iotdb.confignode.consensus.request.write.database.SetTTLPlan;
import org.apache.iotdb.confignode.consensus.request.write.pipe.payload.PipeCreateTablePlan;
import org.apache.iotdb.confignode.consensus.request.write.pipe.payload.PipeDeactivateTemplatePlan;
import org.apache.iotdb.confignode.consensus.request.write.pipe.payload.PipeDeleteDevicesPlan;
import org.apache.iotdb.confignode.consensus.request.write.pipe.payload.PipeDeleteLogicalViewPlan;
import org.apache.iotdb.confignode.consensus.request.write.pipe.payload.PipeDeleteTimeSeriesPlan;
import org.apache.iotdb.confignode.consensus.request.write.pipe.payload.PipeUnsetSchemaTemplatePlan;
import org.apache.iotdb.confignode.consensus.request.write.table.AddTableColumnPlan;
import org.apache.iotdb.confignode.consensus.request.write.table.CommitDeleteColumnPlan;
import org.apache.iotdb.confignode.consensus.request.write.table.CommitDeleteTablePlan;
import org.apache.iotdb.confignode.consensus.request.write.table.RenameTableColumnPlan;
import org.apache.iotdb.confignode.consensus.request.write.table.SetTablePropertiesPlan;
import org.apache.iotdb.confignode.consensus.request.write.template.CommitSetSchemaTemplatePlan;
import org.apache.iotdb.confignode.consensus.request.write.template.CreateSchemaTemplatePlan;
import org.apache.iotdb.confignode.consensus.request.write.template.DropSchemaTemplatePlan;
import org.apache.iotdb.confignode.consensus.request.write.template.ExtendSchemaTemplatePlan;

public abstract class ConfigPhysicalPlanVisitor<R, C> {
  public R process(final ConfigPhysicalPlan plan, final C context) {
    switch (plan.getType()) {
      case CreateDatabase:
        return visitCreateDatabase((DatabaseSchemaPlan) plan, context);
      case AlterDatabase:
        return visitAlterDatabase((DatabaseSchemaPlan) plan, context);
      case DeleteDatabase:
        return visitDeleteDatabase((DeleteDatabasePlan) plan, context);
      case CreateSchemaTemplate:
        return visitCreateSchemaTemplate((CreateSchemaTemplatePlan) plan, context);
      case CommitSetSchemaTemplate:
        return visitCommitSetSchemaTemplate((CommitSetSchemaTemplatePlan) plan, context);
      case ExtendSchemaTemplate:
        return visitExtendSchemaTemplate((ExtendSchemaTemplatePlan) plan, context);
      case DropSchemaTemplate:
        return visitDropSchemaTemplate((DropSchemaTemplatePlan) plan, context);
      case PipeUnsetTemplate:
        return visitPipeUnsetSchemaTemplate((PipeUnsetSchemaTemplatePlan) plan, context);
      case PipeDeleteTimeSeries:
        return visitPipeDeleteTimeSeries((PipeDeleteTimeSeriesPlan) plan, context);
      case PipeDeleteLogicalView:
        return visitPipeDeleteLogicalView((PipeDeleteLogicalViewPlan) plan, context);
      case PipeDeactivateTemplate:
        return visitPipeDeactivateTemplate((PipeDeactivateTemplatePlan) plan, context);
      case CreateRole:
        return visitCreateRole((AuthorPlan) plan, context);
      case DropRole:
        return visitDropRole((AuthorPlan) plan, context);
      case GrantRole:
        return visitGrantRole((AuthorPlan) plan, context);
      case RevokeRole:
        return visitRevokeRole((AuthorPlan) plan, context);
      case CreateUser:
        return visitCreateUser((AuthorPlan) plan, context);
      case CreateUserWithRawPassword:
        return visitCreateRawUser((AuthorPlan) plan, context);
      case UpdateUser:
        return visitUpdateUser((AuthorPlan) plan, context);
      case DropUser:
        return visitDropUser((AuthorPlan) plan, context);
      case GrantUser:
        return visitGrantUser((AuthorPlan) plan, context);
      case RevokeUser:
        return visitRevokeUser((AuthorPlan) plan, context);
      case GrantRoleToUser:
        return visitGrantRoleToUser((AuthorPlan) plan, context);
      case RevokeRoleFromUser:
        return visitRevokeRoleFromUser((AuthorPlan) plan, context);
      case RCreateUser:
        return visitRCreateUser((AuthorPlan) plan, context);
      case RCreateRole:
        return visitRCreateRole((AuthorPlan) plan, context);
      case RUpdateUser:
        return visitRUpdateUser((AuthorPlan) plan, context);
      case RDropUser:
        return visitRDropUserPlan((AuthorPlan) plan, context);
      case RDropRole:
        return visitRDropRolePlan((AuthorPlan) plan, context);
      case RGrantUserRole:
        return visitRGrantUserRole((AuthorPlan) plan, context);
      case RRevokeUserRole:
        return visitRRevokeUserRole((AuthorPlan) plan, context);
      case RGrantUserAny:
        return visitRGrantUserAny((AuthorPlan) plan, context);
      case RGrantRoleAny:
        return visitRGrantRoleAny((AuthorPlan) plan, context);
      case RGrantUserAll:
        return visitRGrantUserAll((AuthorPlan) plan, context);
      case RGrantRoleAll:
        return visitRGrantRoleAll((AuthorPlan) plan, context);
      case RGrantUserDBPriv:
        return visitRGrantUserDB((AuthorPlan) plan, context);
      case RGrantUserTBPriv:
        return visitRGrantUserTB((AuthorPlan) plan, context);
      case RGrantRoleDBPriv:
        return visitRGrantRoleDB((AuthorPlan) plan, context);
      case RGrantRoleTBPriv:
        return visitRGrantRoleTB((AuthorPlan) plan, context);
      case RRevokeUserAny:
        return visitRRevokeUserAny((AuthorPlan) plan, context);
      case RRevokeRoleAny:
        return visitRRevokeRoleAny((AuthorPlan) plan, context);
      case RRevokeUserAll:
        return visitRRevokeUserAll((AuthorPlan) plan, context);
      case RRevokeRoleAll:
        return visitRRevokeRoleAll((AuthorPlan) plan, context);
      case RRevokeUserDBPriv:
        return visitRRevokeUserDBPrivilegePlan((AuthorPlan) plan, context);
      case RRevokeUserTBPriv:
        return visitRRevokeUserTBPrivilege((AuthorPlan) plan, context);
      case RRevokeRoleDBPriv:
        return visitRRevokeRoleDBPrivilege((AuthorPlan) plan, context);
      case RRevokeRoleTBPriv:
        return visitRRevokeRoleTBPrivilege((AuthorPlan) plan, context);
      case RGrantUserSysPri:
        return visitRGrantUserSysPrivilege((AuthorPlan) plan, context);
      case RGrantRoleSysPri:
        return visitRGrantRoleSysPrivilege((AuthorPlan) plan, context);
      case RRevokeUserSysPri:
        return visitRRevokeUserSysPrivilege((AuthorPlan) plan, context);
      case RRevokeRoleSysPri:
        return visitRRevokeRoleSysPrivilege((AuthorPlan) plan, context);
      case SetTTL:
        return visitTTL((SetTTLPlan) plan, context);
      case PipeCreateTable:
        return visitPipeCreateTable((PipeCreateTablePlan) plan, context);
      case AddTableColumn:
        return visitAddTableColumn((AddTableColumnPlan) plan, context);
      case SetTableProperties:
        return visitSetTableProperties((SetTablePropertiesPlan) plan, context);
      case RenameTableColumn:
        return visitRenameTableColumn((RenameTableColumnPlan) plan, context);
      case CommitDeleteColumn:
        return visitCommitDeleteColumn((CommitDeleteColumnPlan) plan, context);
      case CommitDeleteTable:
        return visitCommitDeleteTable((CommitDeleteTablePlan) plan, context);
      case PipeDeleteDevices:
        return visitPipeDeleteDevices((PipeDeleteDevicesPlan) plan, context);
      default:
        return visitPlan(plan, context);
    }
  }

  /** Top Level Description */
  public abstract R visitPlan(final ConfigPhysicalPlan plan, final C context);

  public R visitCreateDatabase(final DatabaseSchemaPlan createDatabasePlan, final C context) {
    return visitPlan(createDatabasePlan, context);
  }

  public R visitAlterDatabase(final DatabaseSchemaPlan alterDatabasePlan, final C context) {
    return visitPlan(alterDatabasePlan, context);
  }

  public R visitDeleteDatabase(final DeleteDatabasePlan deleteDatabasePlan, final C context) {
    return visitPlan(deleteDatabasePlan, context);
  }

  public R visitCreateSchemaTemplate(
      final CreateSchemaTemplatePlan createSchemaTemplatePlan, final C context) {
    return visitPlan(createSchemaTemplatePlan, context);
  }

  public R visitCommitSetSchemaTemplate(
      final CommitSetSchemaTemplatePlan commitSetSchemaTemplatePlan, final C context) {
    return visitPlan(commitSetSchemaTemplatePlan, context);
  }

  public R visitPipeUnsetSchemaTemplate(
      final PipeUnsetSchemaTemplatePlan pipeUnsetSchemaTemplatePlan, final C context) {
    return visitPlan(pipeUnsetSchemaTemplatePlan, context);
  }

  public R visitExtendSchemaTemplate(
      final ExtendSchemaTemplatePlan extendSchemaTemplatePlan, final C context) {
    return visitPlan(extendSchemaTemplatePlan, context);
  }

  public R visitDropSchemaTemplate(
      final DropSchemaTemplatePlan dropSchemaTemplatePlan, final C context) {
    return visitPlan(dropSchemaTemplatePlan, context);
  }

  public R visitPipeDeleteTimeSeries(
      final PipeDeleteTimeSeriesPlan pipeDeleteTimeSeriesPlan, final C context) {
    return visitPlan(pipeDeleteTimeSeriesPlan, context);
  }

  public R visitPipeDeleteLogicalView(
      final PipeDeleteLogicalViewPlan pipeDeleteLogicalViewPlan, final C context) {
    return visitPlan(pipeDeleteLogicalViewPlan, context);
  }

  public R visitPipeDeactivateTemplate(
      final PipeDeactivateTemplatePlan pipeDeactivateTemplatePlan, final C context) {
    return visitPlan(pipeDeactivateTemplatePlan, context);
  }

  public R visitCreateUser(final AuthorPlan createUserPlan, final C context) {
    return visitPlan(createUserPlan, context);
  }

  public R visitCreateRawUser(final AuthorPlan createRawUserPlan, final C context) {
    return visitPlan(createRawUserPlan, context);
  }

  public R visitUpdateUser(final AuthorPlan updateUserPlan, final C context) {
    return visitPlan(updateUserPlan, context);
  }

  public R visitDropUser(final AuthorPlan dropUserPlan, final C context) {
    return visitPlan(dropUserPlan, context);
  }

  public R visitGrantUser(final AuthorPlan grantUserPlan, final C context) {
    return visitPlan(grantUserPlan, context);
  }

  public R visitRevokeUser(final AuthorPlan revokeUserPlan, final C context) {
    return visitPlan(revokeUserPlan, context);
  }

  public R visitCreateRole(final AuthorPlan createRolePlan, final C context) {
    return visitPlan(createRolePlan, context);
  }

  public R visitDropRole(final AuthorPlan dropRolePlan, final C context) {
    return visitPlan(dropRolePlan, context);
  }

  public R visitGrantRole(final AuthorPlan grantRolePlan, final C context) {
    return visitPlan(grantRolePlan, context);
  }

  public R visitRevokeRole(final AuthorPlan revokeRolePlan, final C context) {
    return visitPlan(revokeRolePlan, context);
  }

  public R visitGrantRoleToUser(final AuthorPlan grantRoleToUserPlan, final C context) {
    return visitPlan(grantRoleToUserPlan, context);
  }

  public R visitRCreateUser(final AuthorPlan rCreateUserPlan, final C context) {
    return visitPlan(rCreateUserPlan, context);
  }

  public R visitRCreateRole(final AuthorPlan rCreateRolePlan, final C context) {
    return visitPlan(rCreateRolePlan, context);
  }

  public R visitRUpdateUser(final AuthorPlan rUpdateUserPlan, final C context) {
    return visitPlan(rUpdateUserPlan, context);
  }

  public R visitRDropUserPlan(final AuthorPlan rDropUserPlan, final C context) {
    return visitPlan(rDropUserPlan, context);
  }

  public R visitRDropRolePlan(final AuthorPlan rDropRolePlan, final C context) {
    return visitPlan(rDropRolePlan, context);
  }

  public R visitRGrantUserRole(final AuthorPlan rGrantUserRolePlan, final C context) {
    return visitPlan(rGrantUserRolePlan, context);
  }

  public R visitRRevokeUserRole(final AuthorPlan rRevokeUserRolePlan, final C context) {
    return visitPlan(rRevokeUserRolePlan, context);
  }

  public R visitRGrantUserAny(final AuthorPlan rGrantUserAnyPlan, final C context) {
    return visitPlan(rGrantUserAnyPlan, context);
  }

  public R visitRGrantRoleAny(final AuthorPlan rGrantRoleAnyPlan, final C context) {
    return visitPlan(rGrantRoleAnyPlan, context);
  }

  public R visitRGrantUserAll(final AuthorPlan rGrantUserAllPlan, final C context) {
    return visitPlan(rGrantUserAllPlan, context);
  }

  public R visitRGrantRoleAll(final AuthorPlan rGrantRoleAllPlan, final C context) {
    return visitPlan(rGrantRoleAllPlan, context);
  }

  public R visitRGrantUserDB(final AuthorPlan rGrantUserDBPlan, final C context) {
    return visitPlan(rGrantUserDBPlan, context);
  }

  public R visitRGrantUserTB(final AuthorPlan rGrantUserTBPlan, final C context) {
    return visitPlan(rGrantUserTBPlan, context);
  }

  public R visitRGrantRoleDB(final AuthorPlan rGrantRoleDBPlan, final C context) {
    return visitPlan(rGrantRoleDBPlan, context);
  }

  public R visitRGrantRoleTB(final AuthorPlan rGrantRoleTBPlan, final C context) {
    return visitPlan(rGrantRoleTBPlan, context);
  }

  public R visitRRevokeUserAny(final AuthorPlan rRevokeUserAnyPlan, final C context) {
    return visitPlan(rRevokeUserAnyPlan, context);
  }

  public R visitRRevokeRoleAny(final AuthorPlan rRevokeRoleAnyPlan, final C context) {
    return visitPlan(rRevokeRoleAnyPlan, context);
  }

  public R visitRRevokeUserAll(final AuthorPlan rRevokeUserAllPlan, final C context) {
    return visitPlan(rRevokeUserAllPlan, context);
  }

  public R visitRRevokeRoleAll(final AuthorPlan rRevokeRoleAllPlan, final C context) {
    return visitPlan(rRevokeRoleAllPlan, context);
  }

  public R visitRRevokeUserDBPrivilegePlan(
      final AuthorPlan rRevokeUserDBPrivilegePlan, final C context) {
    return visitPlan(rRevokeUserDBPrivilegePlan, context);
  }

  public R visitRRevokeUserTBPrivilege(
      final AuthorPlan rRevokeUserTBPrivilegePlan, final C context) {
    return visitPlan(rRevokeUserTBPrivilegePlan, context);
  }

  public R visitRRevokeRoleDBPrivilege(
      final AuthorPlan rRevokeRoleTBPrivilegePlan, final C context) {
    return visitPlan(rRevokeRoleTBPrivilegePlan, context);
  }

  public R visitRRevokeRoleTBPrivilege(
      final AuthorPlan rRevokeRoleTBPrivilegePlan, final C context) {
    return visitPlan(rRevokeRoleTBPrivilegePlan, context);
  }

  public R visitRGrantUserSysPrivilege(
      final AuthorPlan rGrantUserSysPrivilegePlan, final C context) {
    return visitPlan(rGrantUserSysPrivilegePlan, context);
  }

  public R visitRGrantRoleSysPrivilege(
      final AuthorPlan rGrantRoleSysPrivilegePlan, final C context) {
    return visitPlan(rGrantRoleSysPrivilegePlan, context);
  }

  public R visitRRevokeUserSysPrivilege(
      final AuthorPlan rRevokeUserSysPrivilegePlan, final C context) {
    return visitPlan(rRevokeUserSysPrivilegePlan, context);
  }

  public R visitRRevokeRoleSysPrivilege(
      final AuthorPlan rRevokeRoleSysPrivilegePlan, final C context) {
    return visitPlan(rRevokeRoleSysPrivilegePlan, context);
  }

  public R visitTTL(final SetTTLPlan setTTLPlan, final C context) {
    return visitPlan(setTTLPlan, context);
  }

  public R visitPipeCreateTable(final PipeCreateTablePlan pipeCreateTablePlan, final C context) {
    return visitPlan(pipeCreateTablePlan, context);
  }

  public R visitAddTableColumn(final AddTableColumnPlan addTableColumnPlan, final C context) {
    return visitPlan(addTableColumnPlan, context);
  }

  public R visitSetTableProperties(
      final SetTablePropertiesPlan setTablePropertiesPlan, final C context) {
    return visitPlan(setTablePropertiesPlan, context);
  }

  public R visitCommitDeleteColumn(
      final CommitDeleteColumnPlan commitDeleteColumnPlan, final C context) {
    return visitPlan(commitDeleteColumnPlan, context);
  }

  public R visitRenameTableColumn(
      final RenameTableColumnPlan renameTableColumnPlan, final C context) {
    return visitPlan(renameTableColumnPlan, context);
  }

  public R visitCommitDeleteTable(
      final CommitDeleteTablePlan commitDeleteTablePlan, final C context) {
    return visitPlan(commitDeleteTablePlan, context);
  }

  public R visitPipeDeleteDevices(
      final PipeDeleteDevicesPlan pipeDeleteDevicesPlan, final C context) {
    return visitPlan(pipeDeleteDevicesPlan, context);
  }
}
