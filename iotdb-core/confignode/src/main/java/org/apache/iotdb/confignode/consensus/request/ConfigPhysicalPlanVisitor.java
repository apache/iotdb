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

import org.apache.iotdb.confignode.consensus.request.write.auth.AuthorRelationalPlan;
import org.apache.iotdb.confignode.consensus.request.write.auth.AuthorTreePlan;
import org.apache.iotdb.confignode.consensus.request.write.database.DatabaseSchemaPlan;
import org.apache.iotdb.confignode.consensus.request.write.database.DeleteDatabasePlan;
import org.apache.iotdb.confignode.consensus.request.write.database.SetTTLPlan;
import org.apache.iotdb.confignode.consensus.request.write.pipe.payload.PipeAlterEncodingCompressorPlan;
import org.apache.iotdb.confignode.consensus.request.write.pipe.payload.PipeAlterTimeSeriesPlan;
import org.apache.iotdb.confignode.consensus.request.write.pipe.payload.PipeCreateTableOrViewPlan;
import org.apache.iotdb.confignode.consensus.request.write.pipe.payload.PipeDeactivateTemplatePlan;
import org.apache.iotdb.confignode.consensus.request.write.pipe.payload.PipeDeleteDevicesPlan;
import org.apache.iotdb.confignode.consensus.request.write.pipe.payload.PipeDeleteLogicalViewPlan;
import org.apache.iotdb.confignode.consensus.request.write.pipe.payload.PipeDeleteTimeSeriesPlan;
import org.apache.iotdb.confignode.consensus.request.write.pipe.payload.PipeUnsetSchemaTemplatePlan;
import org.apache.iotdb.confignode.consensus.request.write.table.AddTableColumnPlan;
import org.apache.iotdb.confignode.consensus.request.write.table.AlterColumnDataTypePlan;
import org.apache.iotdb.confignode.consensus.request.write.table.CommitDeleteColumnPlan;
import org.apache.iotdb.confignode.consensus.request.write.table.CommitDeleteTablePlan;
import org.apache.iotdb.confignode.consensus.request.write.table.RenameTableColumnPlan;
import org.apache.iotdb.confignode.consensus.request.write.table.RenameTablePlan;
import org.apache.iotdb.confignode.consensus.request.write.table.SetTableColumnCommentPlan;
import org.apache.iotdb.confignode.consensus.request.write.table.SetTableCommentPlan;
import org.apache.iotdb.confignode.consensus.request.write.table.SetTablePropertiesPlan;
import org.apache.iotdb.confignode.consensus.request.write.table.view.AddTableViewColumnPlan;
import org.apache.iotdb.confignode.consensus.request.write.table.view.CommitDeleteViewColumnPlan;
import org.apache.iotdb.confignode.consensus.request.write.table.view.CommitDeleteViewPlan;
import org.apache.iotdb.confignode.consensus.request.write.table.view.RenameViewColumnPlan;
import org.apache.iotdb.confignode.consensus.request.write.table.view.RenameViewPlan;
import org.apache.iotdb.confignode.consensus.request.write.table.view.SetViewCommentPlan;
import org.apache.iotdb.confignode.consensus.request.write.table.view.SetViewPropertiesPlan;
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
        return visitCreateRole((AuthorTreePlan) plan, context);
      case DropRole:
        return visitDropRole((AuthorTreePlan) plan, context);
      case GrantRole:
        return visitGrantRole((AuthorTreePlan) plan, context);
      case RevokeRole:
        return visitRevokeRole((AuthorTreePlan) plan, context);
      case CreateUser:
        return visitCreateUser((AuthorTreePlan) plan, context);
      case CreateUserWithRawPassword:
        return visitCreateRawUser((AuthorTreePlan) plan, context);
      case UpdateUser:
      case UpdateUserV2:
        return visitUpdateUser((AuthorTreePlan) plan, context);
      case DropUser:
      case DropUserV2:
        return visitDropUser((AuthorTreePlan) plan, context);
      case GrantUser:
        return visitGrantUser((AuthorTreePlan) plan, context);
      case RevokeUser:
        return visitRevokeUser((AuthorTreePlan) plan, context);
      case GrantRoleToUser:
        return visitGrantRoleToUser((AuthorTreePlan) plan, context);
      case RevokeRoleFromUser:
        return visitRevokeRoleFromUser((AuthorTreePlan) plan, context);
      case RCreateUser:
        return visitRCreateUser((AuthorRelationalPlan) plan, context);
      case RCreateRole:
        return visitRCreateRole((AuthorRelationalPlan) plan, context);
      case RUpdateUser:
      case RUpdateUserV2:
        return visitRUpdateUser((AuthorRelationalPlan) plan, context);
      case RDropUser:
      case RDropUserV2:
        return visitRDropUserPlan((AuthorRelationalPlan) plan, context);
      case RDropRole:
        return visitRDropRolePlan((AuthorRelationalPlan) plan, context);
      case RGrantUserRole:
        return visitRGrantUserRole((AuthorRelationalPlan) plan, context);
      case RRevokeUserRole:
        return visitRRevokeUserRole((AuthorRelationalPlan) plan, context);
      case RGrantUserAny:
        return visitRGrantUserAny((AuthorRelationalPlan) plan, context);
      case RGrantRoleAny:
        return visitRGrantRoleAny((AuthorRelationalPlan) plan, context);
      case RGrantUserAll:
        return visitRGrantUserAll((AuthorRelationalPlan) plan, context);
      case RGrantRoleAll:
        return visitRGrantRoleAll((AuthorRelationalPlan) plan, context);
      case RGrantUserDBPriv:
        return visitRGrantUserDB((AuthorRelationalPlan) plan, context);
      case RGrantUserTBPriv:
        return visitRGrantUserTB((AuthorRelationalPlan) plan, context);
      case RGrantRoleDBPriv:
        return visitRGrantRoleDB((AuthorRelationalPlan) plan, context);
      case RGrantRoleTBPriv:
        return visitRGrantRoleTB((AuthorRelationalPlan) plan, context);
      case RRevokeUserAny:
        return visitRRevokeUserAny((AuthorRelationalPlan) plan, context);
      case RRevokeRoleAny:
        return visitRRevokeRoleAny((AuthorRelationalPlan) plan, context);
      case RRevokeUserAll:
        return visitRRevokeUserAll((AuthorRelationalPlan) plan, context);
      case RRevokeRoleAll:
        return visitRRevokeRoleAll((AuthorRelationalPlan) plan, context);
      case RRevokeUserDBPriv:
        return visitRRevokeUserDBPrivilege((AuthorRelationalPlan) plan, context);
      case RRevokeUserTBPriv:
        return visitRRevokeUserTBPrivilege((AuthorRelationalPlan) plan, context);
      case RRevokeRoleDBPriv:
        return visitRRevokeRoleDBPrivilege((AuthorRelationalPlan) plan, context);
      case RRevokeRoleTBPriv:
        return visitRRevokeRoleTBPrivilege((AuthorRelationalPlan) plan, context);
      case RGrantUserSysPri:
        return visitRGrantUserSysPrivilege((AuthorRelationalPlan) plan, context);
      case RGrantRoleSysPri:
        return visitRGrantRoleSysPrivilege((AuthorRelationalPlan) plan, context);
      case RRevokeUserSysPri:
        return visitRRevokeUserSysPrivilege((AuthorRelationalPlan) plan, context);
      case RRevokeRoleSysPri:
        return visitRRevokeRoleSysPrivilege((AuthorRelationalPlan) plan, context);
      case SetTTL:
        return visitTTL((SetTTLPlan) plan, context);
      case PipeCreateTableOrView:
        return visitPipeCreateTableOrView((PipeCreateTableOrViewPlan) plan, context);
      case AddTableColumn:
        return visitAddTableColumn((AddTableColumnPlan) plan, context);
      case AddViewColumn:
        return visitAddTableViewColumn((AddTableViewColumnPlan) plan, context);
      case SetTableProperties:
        return visitSetTableProperties((SetTablePropertiesPlan) plan, context);
      case SetViewProperties:
        return visitSetViewProperties((SetViewPropertiesPlan) plan, context);
      case RenameTableColumn:
        return visitRenameTableColumn((RenameTableColumnPlan) plan, context);
      case RenameViewColumn:
        return visitRenameViewColumn((RenameViewColumnPlan) plan, context);
      case CommitDeleteColumn:
        return visitCommitDeleteColumn((CommitDeleteColumnPlan) plan, context);
      case CommitDeleteViewColumn:
        return visitCommitDeleteViewColumn((CommitDeleteViewColumnPlan) plan, context);
      case CommitDeleteTable:
        return visitCommitDeleteTable((CommitDeleteTablePlan) plan, context);
      case CommitDeleteView:
        return visitCommitDeleteView((CommitDeleteViewPlan) plan, context);
      case PipeDeleteDevices:
        return visitPipeDeleteDevices((PipeDeleteDevicesPlan) plan, context);
      case SetTableComment:
        return visitSetTableComment((SetTableCommentPlan) plan, context);
      case SetViewComment:
        return visitSetViewComment((SetViewCommentPlan) plan, context);
      case SetTableColumnComment:
        return visitSetTableColumnComment((SetTableColumnCommentPlan) plan, context);
      case AlterColumnDataType:
        return visitAlterColumnDataType((AlterColumnDataTypePlan) plan, context);
      case RenameTable:
        return visitRenameTable((RenameTablePlan) plan, context);
      case RenameView:
        return visitRenameView((RenameViewPlan) plan, context);
      case PipeAlterEncodingCompressor:
        return visitPipeAlterEncodingCompressor((PipeAlterEncodingCompressorPlan) plan, context);
      case PipeAlterTimeSeries:
        return visitPipeAlterTimeSeries((PipeAlterTimeSeriesPlan) plan, context);
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

  public R visitCreateUser(final AuthorTreePlan createUserPlan, final C context) {
    return visitPlan(createUserPlan, context);
  }

  public R visitCreateRawUser(final AuthorTreePlan createRawUserPlan, final C context) {
    return visitPlan(createRawUserPlan, context);
  }

  public R visitUpdateUser(final AuthorTreePlan updateUserPlan, final C context) {
    return visitPlan(updateUserPlan, context);
  }

  public R visitDropUser(final AuthorTreePlan dropUserPlan, final C context) {
    return visitPlan(dropUserPlan, context);
  }

  public R visitGrantUser(final AuthorTreePlan grantUserPlan, final C context) {
    return visitPlan(grantUserPlan, context);
  }

  public R visitRevokeUser(final AuthorTreePlan revokeUserPlan, final C context) {
    return visitPlan(revokeUserPlan, context);
  }

  public R visitCreateRole(final AuthorTreePlan createRolePlan, final C context) {
    return visitPlan(createRolePlan, context);
  }

  public R visitDropRole(final AuthorTreePlan dropRolePlan, final C context) {
    return visitPlan(dropRolePlan, context);
  }

  public R visitGrantRole(final AuthorTreePlan grantRolePlan, final C context) {
    return visitPlan(grantRolePlan, context);
  }

  public R visitRevokeRole(final AuthorTreePlan revokeRolePlan, final C context) {
    return visitPlan(revokeRolePlan, context);
  }

  public R visitGrantRoleToUser(final AuthorTreePlan grantRoleToUserPlan, final C context) {
    return visitPlan(grantRoleToUserPlan, context);
  }

  public R visitRevokeRoleFromUser(final AuthorTreePlan revokeRoleFromUserPlan, final C context) {
    return visitPlan(revokeRoleFromUserPlan, context);
  }

  public R visitRCreateUser(final AuthorRelationalPlan rCreateUserPlan, final C context) {
    return visitPlan(rCreateUserPlan, context);
  }

  public R visitRCreateRole(final AuthorRelationalPlan rCreateRolePlan, final C context) {
    return visitPlan(rCreateRolePlan, context);
  }

  public R visitRUpdateUser(final AuthorRelationalPlan rUpdateUserPlan, final C context) {
    return visitPlan(rUpdateUserPlan, context);
  }

  public R visitRDropUserPlan(final AuthorRelationalPlan rDropUserPlan, final C context) {
    return visitPlan(rDropUserPlan, context);
  }

  public R visitRDropRolePlan(final AuthorRelationalPlan rDropRolePlan, final C context) {
    return visitPlan(rDropRolePlan, context);
  }

  public R visitRGrantUserRole(final AuthorRelationalPlan rGrantUserRolePlan, final C context) {
    return visitPlan(rGrantUserRolePlan, context);
  }

  public R visitRRevokeUserRole(final AuthorRelationalPlan rRevokeUserRolePlan, final C context) {
    return visitPlan(rRevokeUserRolePlan, context);
  }

  public R visitRGrantUserAny(final AuthorRelationalPlan rGrantUserAnyPlan, final C context) {
    return visitPlan(rGrantUserAnyPlan, context);
  }

  public R visitRGrantRoleAny(final AuthorRelationalPlan rGrantRoleAnyPlan, final C context) {
    return visitPlan(rGrantRoleAnyPlan, context);
  }

  public R visitRGrantUserAll(final AuthorRelationalPlan rGrantUserAllPlan, final C context) {
    return visitPlan(rGrantUserAllPlan, context);
  }

  public R visitRGrantRoleAll(final AuthorRelationalPlan rGrantRoleAllPlan, final C context) {
    return visitPlan(rGrantRoleAllPlan, context);
  }

  public R visitRGrantUserDB(final AuthorRelationalPlan rGrantUserDBPlan, final C context) {
    return visitPlan(rGrantUserDBPlan, context);
  }

  public R visitRGrantUserTB(final AuthorRelationalPlan rGrantUserTBPlan, final C context) {
    return visitPlan(rGrantUserTBPlan, context);
  }

  public R visitRGrantRoleDB(final AuthorRelationalPlan rGrantRoleDBPlan, final C context) {
    return visitPlan(rGrantRoleDBPlan, context);
  }

  public R visitRGrantRoleTB(final AuthorRelationalPlan rGrantRoleTBPlan, final C context) {
    return visitPlan(rGrantRoleTBPlan, context);
  }

  public R visitRRevokeUserAny(final AuthorRelationalPlan rRevokeUserAnyPlan, final C context) {
    return visitPlan(rRevokeUserAnyPlan, context);
  }

  public R visitRRevokeRoleAny(final AuthorRelationalPlan rRevokeRoleAnyPlan, final C context) {
    return visitPlan(rRevokeRoleAnyPlan, context);
  }

  public R visitRRevokeUserAll(final AuthorRelationalPlan rRevokeUserAllPlan, final C context) {
    return visitPlan(rRevokeUserAllPlan, context);
  }

  public R visitRRevokeRoleAll(final AuthorRelationalPlan rRevokeRoleAllPlan, final C context) {
    return visitPlan(rRevokeRoleAllPlan, context);
  }

  public R visitRRevokeUserDBPrivilege(
      final AuthorRelationalPlan rRevokeUserDBPrivilegePlan, final C context) {
    return visitPlan(rRevokeUserDBPrivilegePlan, context);
  }

  public R visitRRevokeUserTBPrivilege(
      final AuthorRelationalPlan rRevokeUserTBPrivilegePlan, final C context) {
    return visitPlan(rRevokeUserTBPrivilegePlan, context);
  }

  public R visitRRevokeRoleDBPrivilege(
      final AuthorRelationalPlan rRevokeRoleTBPrivilegePlan, final C context) {
    return visitPlan(rRevokeRoleTBPrivilegePlan, context);
  }

  public R visitRRevokeRoleTBPrivilege(
      final AuthorRelationalPlan rRevokeRoleTBPrivilegePlan, final C context) {
    return visitPlan(rRevokeRoleTBPrivilegePlan, context);
  }

  public R visitRGrantUserSysPrivilege(
      final AuthorRelationalPlan rGrantUserSysPrivilegePlan, final C context) {
    return visitPlan(rGrantUserSysPrivilegePlan, context);
  }

  public R visitRGrantRoleSysPrivilege(
      final AuthorRelationalPlan rGrantRoleSysPrivilegePlan, final C context) {
    return visitPlan(rGrantRoleSysPrivilegePlan, context);
  }

  public R visitRRevokeUserSysPrivilege(
      final AuthorRelationalPlan rRevokeUserSysPrivilegePlan, final C context) {
    return visitPlan(rRevokeUserSysPrivilegePlan, context);
  }

  public R visitRRevokeRoleSysPrivilege(
      final AuthorRelationalPlan rRevokeRoleSysPrivilegePlan, final C context) {
    return visitPlan(rRevokeRoleSysPrivilegePlan, context);
  }

  public R visitTTL(final SetTTLPlan setTTLPlan, final C context) {
    return visitPlan(setTTLPlan, context);
  }

  public R visitPipeCreateTableOrView(
      final PipeCreateTableOrViewPlan pipeCreateTableOrViewPlan, final C context) {
    return visitPlan(pipeCreateTableOrViewPlan, context);
  }

  public R visitAddTableColumn(final AddTableColumnPlan addTableColumnPlan, final C context) {
    return visitPlan(addTableColumnPlan, context);
  }

  // Use add table column by default
  public R visitAddTableViewColumn(
      final AddTableViewColumnPlan addTableViewColumnPlan, final C context) {
    return visitAddTableColumn(addTableViewColumnPlan, context);
  }

  public R visitSetTableProperties(
      final SetTablePropertiesPlan setTablePropertiesPlan, final C context) {
    return visitPlan(setTablePropertiesPlan, context);
  }

  // Use set table properties by default
  public R visitSetViewProperties(
      final SetViewPropertiesPlan setViewPropertiesPlan, final C context) {
    return visitSetTableProperties(setViewPropertiesPlan, context);
  }

  public R visitCommitDeleteColumn(
      final CommitDeleteColumnPlan commitDeleteColumnPlan, final C context) {
    return visitPlan(commitDeleteColumnPlan, context);
  }

  public R visitCommitDeleteViewColumn(
      final CommitDeleteViewColumnPlan commitDeleteViewColumnPlan, final C context) {
    return visitCommitDeleteColumn(commitDeleteViewColumnPlan, context);
  }

  public R visitRenameTableColumn(
      final RenameTableColumnPlan renameTableColumnPlan, final C context) {
    return visitPlan(renameTableColumnPlan, context);
  }

  // Use commit delete table by default
  public R visitRenameViewColumn(final RenameViewColumnPlan renameViewColumnPlan, final C context) {
    return visitRenameTableColumn(renameViewColumnPlan, context);
  }

  public R visitCommitDeleteTable(
      final CommitDeleteTablePlan commitDeleteTablePlan, final C context) {
    return visitPlan(commitDeleteTablePlan, context);
  }

  // Use commit delete table by default
  public R visitCommitDeleteView(final CommitDeleteViewPlan commitDeleteViewPlan, final C context) {
    return visitCommitDeleteTable(commitDeleteViewPlan, context);
  }

  public R visitPipeDeleteDevices(
      final PipeDeleteDevicesPlan pipeDeleteDevicesPlan, final C context) {
    return visitPlan(pipeDeleteDevicesPlan, context);
  }

  public R visitSetTableComment(final SetTableCommentPlan setTableCommentPlan, final C context) {
    return visitPlan(setTableCommentPlan, context);
  }

  // Use set table by default
  public R visitSetViewComment(final SetViewCommentPlan setViewCommentPlan, final C context) {
    return visitSetTableComment(setViewCommentPlan, context);
  }

  public R visitSetTableColumnComment(
      final SetTableColumnCommentPlan setTableColumnCommentPlan, final C context) {
    return visitPlan(setTableColumnCommentPlan, context);
  }

  public R visitRenameTable(final RenameTablePlan renameTablePlan, final C context) {
    return visitPlan(renameTablePlan, context);
  }

  // Use rename table by default
  public R visitRenameView(final RenameViewPlan renameViewPlan, final C context) {
    return visitRenameTable(renameViewPlan, context);
  }

  public R visitPipeAlterEncodingCompressor(
      final PipeAlterEncodingCompressorPlan pipeAlterEncodingCompressorPlan, final C context) {
    return visitPlan(pipeAlterEncodingCompressorPlan, context);
  }

  public R visitPipeAlterTimeSeries(
      final PipeAlterTimeSeriesPlan pipeAlterTimeSeriesPlan, final C context) {
    return visitPlan(pipeAlterTimeSeriesPlan, context);
  }

  public R visitAlterColumnDataType(
      final AlterColumnDataTypePlan alterColumnDataTypePlan, final C context) {
    return visitPlan(alterColumnDataTypePlan, context);
  }
}
