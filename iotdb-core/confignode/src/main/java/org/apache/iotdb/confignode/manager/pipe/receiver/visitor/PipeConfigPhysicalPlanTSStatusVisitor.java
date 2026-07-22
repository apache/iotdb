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

package org.apache.iotdb.confignode.manager.pipe.receiver.visitor;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.confignode.consensus.request.ConfigPhysicalPlan;
import org.apache.iotdb.confignode.consensus.request.ConfigPhysicalPlanVisitor;
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
import org.apache.iotdb.confignode.consensus.request.write.template.CommitSetSchemaTemplatePlan;
import org.apache.iotdb.confignode.consensus.request.write.template.CreateSchemaTemplatePlan;
import org.apache.iotdb.confignode.consensus.request.write.template.DropSchemaTemplatePlan;
import org.apache.iotdb.confignode.consensus.request.write.template.ExtendSchemaTemplatePlan;
import org.apache.iotdb.rpc.TSStatusCode;

/**
 * This visitor translated some {@link TSStatus} to pipe related status to help sender classify them
 * and apply different error handling tactics. Please DO NOT modify the {@link TSStatus} returned by
 * the processes that generate the following {@link TSStatus}es in the class.
 */
public class PipeConfigPhysicalPlanTSStatusVisitor
    extends ConfigPhysicalPlanVisitor<TSStatus, TSStatus> {

  @Override
  public TSStatus visitPlan(final ConfigPhysicalPlan plan, final TSStatus context) {
    return context;
  }

  @Override
  public TSStatus visitCreateDatabase(final DatabaseSchemaPlan plan, final TSStatus context) {
    if (context.getCode() == TSStatusCode.DATABASE_ALREADY_EXISTS.getStatusCode()) {
      return new TSStatus(TSStatusCode.PIPE_RECEIVER_IDEMPOTENT_CONFLICT_EXCEPTION.getStatusCode())
          .setMessage(context.getMessage());
    } else if (context.getCode() == TSStatusCode.SCHEMA_QUOTA_EXCEEDED.getStatusCode()
        || context.getCode() == TSStatusCode.DATABASE_CONFLICT.getStatusCode()) {
      return new TSStatus(TSStatusCode.PIPE_RECEIVER_USER_CONFLICT_EXCEPTION.getStatusCode())
          .setMessage(context.getMessage());
    } else if (context.getCode() == TSStatusCode.METADATA_ERROR.getStatusCode()) {
      return new TSStatus(
              TSStatusCode.PIPE_RECEIVER_TEMPORARY_UNAVAILABLE_EXCEPTION.getStatusCode())
          .setMessage(context.getMessage());
    }
    return super.visitCreateDatabase(plan, context);
  }

  @Override
  public TSStatus visitAlterDatabase(final DatabaseSchemaPlan plan, final TSStatus context) {
    if (context.getCode() == TSStatusCode.DATABASE_CONFIG_ERROR.getStatusCode()) {
      return new TSStatus(TSStatusCode.PIPE_RECEIVER_IDEMPOTENT_CONFLICT_EXCEPTION.getStatusCode())
          .setMessage(context.getMessage());
    } else if (context.getCode() == TSStatusCode.DATABASE_NOT_EXIST.getStatusCode()) {
      return new TSStatus(TSStatusCode.PIPE_RECEIVER_USER_CONFLICT_EXCEPTION.getStatusCode())
          .setMessage(context.getMessage());
    }
    return super.visitAlterDatabase(plan, context);
  }

  @Override
  public TSStatus visitDeleteDatabase(final DeleteDatabasePlan plan, final TSStatus context) {
    if (context.getCode() == TSStatusCode.PATH_NOT_EXIST.getStatusCode()) {
      return new TSStatus(TSStatusCode.PIPE_RECEIVER_IDEMPOTENT_CONFLICT_EXCEPTION.getStatusCode())
          .setMessage(context.getMessage());
    }
    return super.visitDeleteDatabase(plan, context);
  }

  @Override
  public TSStatus visitCreateSchemaTemplate(
      final CreateSchemaTemplatePlan plan, final TSStatus context) {
    if (context.getCode() == TSStatusCode.METADATA_ERROR.getStatusCode()) {
      return new TSStatus(TSStatusCode.PIPE_RECEIVER_IDEMPOTENT_CONFLICT_EXCEPTION.getStatusCode())
          .setMessage(context.getMessage());
    }
    return super.visitCreateSchemaTemplate(plan, context);
  }

  @Override
  public TSStatus visitExtendSchemaTemplate(
      final ExtendSchemaTemplatePlan plan, final TSStatus context) {
    if (context.getCode() == TSStatusCode.MEASUREMENT_ALREADY_EXISTS_IN_TEMPLATE.getStatusCode()) {
      return new TSStatus(TSStatusCode.PIPE_RECEIVER_IDEMPOTENT_CONFLICT_EXCEPTION.getStatusCode())
          .setMessage(context.getMessage());
    } else if (context.getCode() == TSStatusCode.METADATA_ERROR.getStatusCode()) {
      return new TSStatus(TSStatusCode.PIPE_RECEIVER_USER_CONFLICT_EXCEPTION.getStatusCode())
          .setMessage(context.getMessage());
    }
    return super.visitExtendSchemaTemplate(plan, context);
  }

  @Override
  public TSStatus visitCommitSetSchemaTemplate(
      final CommitSetSchemaTemplatePlan plan, final TSStatus context) {
    if (context.getCode() == TSStatusCode.METADATA_ERROR.getStatusCode()) {
      if (context.getMessage().contains("Template already exists")) {
        return new TSStatus(
                TSStatusCode.PIPE_RECEIVER_IDEMPOTENT_CONFLICT_EXCEPTION.getStatusCode())
            .setMessage(context.getMessage());
      } else if (context.getMessage().contains("Template t1 does not exist")) {
        return new TSStatus(TSStatusCode.PIPE_RECEIVER_USER_CONFLICT_EXCEPTION.getStatusCode())
            .setMessage(context.getMessage());
      }
    } else if (context.getCode() == TSStatusCode.DATABASE_NOT_EXIST.getStatusCode()) {
      return new TSStatus(TSStatusCode.PIPE_RECEIVER_USER_CONFLICT_EXCEPTION.getStatusCode())
          .setMessage(context.getMessage());
    }
    return super.visitCommitSetSchemaTemplate(plan, context);
  }

  @Override
  public TSStatus visitPipeUnsetSchemaTemplate(
      final PipeUnsetSchemaTemplatePlan pipeUnsetSchemaTemplatePlan, final TSStatus context) {
    if (context.getCode() == TSStatusCode.TEMPLATE_NOT_SET.getStatusCode()) {
      return new TSStatus(TSStatusCode.PIPE_RECEIVER_IDEMPOTENT_CONFLICT_EXCEPTION.getStatusCode())
          .setMessage(context.getMessage());
    } else if (context.getCode() == TSStatusCode.UNDEFINED_TEMPLATE.getStatusCode()
        || context.getCode() == TSStatusCode.TEMPLATE_IS_IN_USE.getStatusCode()) {
      return new TSStatus(TSStatusCode.PIPE_RECEIVER_USER_CONFLICT_EXCEPTION.getStatusCode())
          .setMessage(context.getMessage());
    }
    return super.visitPipeUnsetSchemaTemplate(pipeUnsetSchemaTemplatePlan, context);
  }

  @Override
  public TSStatus visitDropSchemaTemplate(
      final DropSchemaTemplatePlan dropSchemaTemplatePlan, final TSStatus context) {
    if (context.getCode() == TSStatusCode.UNDEFINED_TEMPLATE.getStatusCode()) {
      return new TSStatus(TSStatusCode.PIPE_RECEIVER_IDEMPOTENT_CONFLICT_EXCEPTION.getStatusCode())
          .setMessage(context.getMessage());
    }
    return super.visitDropSchemaTemplate(dropSchemaTemplatePlan, context);
  }

  @Override
  public TSStatus visitPipeDeleteTimeSeries(
      final PipeDeleteTimeSeriesPlan pipeDeleteTimeSeriesPlan, final TSStatus context) {
    if (context.getCode() == TSStatusCode.PATH_NOT_EXIST.getStatusCode()) {
      return new TSStatus(TSStatusCode.PIPE_RECEIVER_IDEMPOTENT_CONFLICT_EXCEPTION.getStatusCode())
          .setMessage(context.getMessage());
    }
    return super.visitPipeDeleteTimeSeries(pipeDeleteTimeSeriesPlan, context);
  }

  @Override
  public TSStatus visitPipeDeleteLogicalView(
      final PipeDeleteLogicalViewPlan pipeDeleteLogicalViewPlan, final TSStatus context) {
    if (context.getCode() == TSStatusCode.PATH_NOT_EXIST.getStatusCode()) {
      return new TSStatus(TSStatusCode.PIPE_RECEIVER_IDEMPOTENT_CONFLICT_EXCEPTION.getStatusCode())
          .setMessage(context.getMessage());
    }
    return super.visitPipeDeleteLogicalView(pipeDeleteLogicalViewPlan, context);
  }

  @Override
  public TSStatus visitPipeDeactivateTemplate(
      final PipeDeactivateTemplatePlan pipeDeactivateTemplatePlan, final TSStatus context) {
    if (context.getCode() == TSStatusCode.TEMPLATE_NOT_ACTIVATED.getStatusCode()) {
      return new TSStatus(TSStatusCode.PIPE_RECEIVER_IDEMPOTENT_CONFLICT_EXCEPTION.getStatusCode())
          .setMessage(context.getMessage());
    }
    return super.visitPipeDeactivateTemplate(pipeDeactivateTemplatePlan, context);
  }

  @Override
  public TSStatus visitPipeAlterTimeSeries(
      final PipeAlterTimeSeriesPlan pipeAlterTimeSeriesPlan, final TSStatus context) {
    if (context.getCode() == TSStatusCode.PATH_NOT_EXIST.getStatusCode()) {
      return new TSStatus(TSStatusCode.PIPE_RECEIVER_IDEMPOTENT_CONFLICT_EXCEPTION.getStatusCode())
          .setMessage(context.getMessage());
    }
    return super.visitPipeAlterTimeSeries(pipeAlterTimeSeriesPlan, context);
  }

  @Override
  public TSStatus visitCreateUser(final AuthorTreePlan plan, final TSStatus context) {
    if (context.getCode() == TSStatusCode.USER_ALREADY_EXIST.getStatusCode()) {
      return new TSStatus(TSStatusCode.PIPE_RECEIVER_IDEMPOTENT_CONFLICT_EXCEPTION.getStatusCode())
          .setMessage(context.getMessage());
    }
    return super.visitCreateUser(plan, context);
  }

  @Override
  public TSStatus visitCreateRawUser(final AuthorTreePlan plan, final TSStatus context) {
    if (context.getCode() == TSStatusCode.USER_ALREADY_EXIST.getStatusCode()) {
      return new TSStatus(TSStatusCode.PIPE_RECEIVER_IDEMPOTENT_CONFLICT_EXCEPTION.getStatusCode())
          .setMessage(context.getMessage());
    }
    return super.visitCreateRawUser(plan, context);
  }

  @Override
  public TSStatus visitUpdateUser(final AuthorTreePlan plan, final TSStatus context) {
    if (context.getCode() == TSStatusCode.USER_NOT_EXIST.getStatusCode()) {
      return new TSStatus(TSStatusCode.PIPE_RECEIVER_USER_CONFLICT_EXCEPTION.getStatusCode())
          .setMessage(context.getMessage());
    }
    return super.visitUpdateUser(plan, context);
  }

  @Override
  public TSStatus visitDropUser(final AuthorTreePlan plan, final TSStatus context) {
    if (context.getCode() == TSStatusCode.USER_NOT_EXIST.getStatusCode()) {
      return new TSStatus(TSStatusCode.PIPE_RECEIVER_IDEMPOTENT_CONFLICT_EXCEPTION.getStatusCode())
          .setMessage(context.getMessage());
    }
    return super.visitDropUser(plan, context);
  }

  @Override
  public TSStatus visitGrantUser(final AuthorTreePlan plan, final TSStatus context) {
    if (context.getCode() == TSStatusCode.NO_PERMISSION.getStatusCode()) {
      // Admin user
      return new TSStatus(TSStatusCode.PIPE_RECEIVER_IDEMPOTENT_CONFLICT_EXCEPTION.getStatusCode())
          .setMessage(context.getMessage());
    }
    if (context.getCode() == TSStatusCode.USER_NOT_EXIST.getStatusCode()) {
      return new TSStatus(TSStatusCode.PIPE_RECEIVER_USER_CONFLICT_EXCEPTION.getStatusCode())
          .setMessage(context.getMessage());
    }
    return super.visitGrantUser(plan, context);
  }

  @Override
  public TSStatus visitRevokeUser(final AuthorTreePlan plan, final TSStatus context) {
    if (context.getCode() == TSStatusCode.NOT_HAS_PRIVILEGE.getStatusCode()) {
      return new TSStatus(TSStatusCode.PIPE_RECEIVER_IDEMPOTENT_CONFLICT_EXCEPTION.getStatusCode())
          .setMessage(context.getMessage());
    } else if (context.getCode() == TSStatusCode.USER_NOT_EXIST.getStatusCode()
        // Admin user
        || context.getCode() == TSStatusCode.NO_PERMISSION.getStatusCode()) {
      return new TSStatus(TSStatusCode.PIPE_RECEIVER_USER_CONFLICT_EXCEPTION.getStatusCode())
          .setMessage(context.getMessage());
    }
    return super.visitRevokeUser(plan, context);
  }

  @Override
  public TSStatus visitCreateRole(final AuthorTreePlan plan, final TSStatus context) {
    if (context.getCode() == TSStatusCode.ROLE_ALREADY_EXIST.getStatusCode()) {
      return new TSStatus(TSStatusCode.PIPE_RECEIVER_IDEMPOTENT_CONFLICT_EXCEPTION.getStatusCode())
          .setMessage(context.getMessage());
    }
    return super.visitCreateRole(plan, context);
  }

  @Override
  public TSStatus visitDropRole(final AuthorTreePlan plan, final TSStatus context) {
    if (context.getCode() == TSStatusCode.ROLE_NOT_EXIST.getStatusCode()) {
      return new TSStatus(TSStatusCode.PIPE_RECEIVER_IDEMPOTENT_CONFLICT_EXCEPTION.getStatusCode())
          .setMessage(context.getMessage());
    }
    return super.visitDropRole(plan, context);
  }

  @Override
  public TSStatus visitGrantRole(final AuthorTreePlan plan, final TSStatus context) {
    if (context.getCode() == TSStatusCode.ROLE_NOT_EXIST.getStatusCode()) {
      return new TSStatus(TSStatusCode.PIPE_RECEIVER_USER_CONFLICT_EXCEPTION.getStatusCode())
          .setMessage(context.getMessage());
    }
    return super.visitGrantRole(plan, context);
  }

  @Override
  public TSStatus visitRevokeRole(final AuthorTreePlan plan, final TSStatus context) {
    if (context.getCode() == TSStatusCode.NOT_HAS_PRIVILEGE.getStatusCode()) {
      return new TSStatus(TSStatusCode.PIPE_RECEIVER_IDEMPOTENT_CONFLICT_EXCEPTION.getStatusCode())
          .setMessage(context.getMessage());
    } else if (context.getCode() == TSStatusCode.ROLE_NOT_EXIST.getStatusCode()) {
      return new TSStatus(TSStatusCode.PIPE_RECEIVER_USER_CONFLICT_EXCEPTION.getStatusCode())
          .setMessage(context.getMessage());
    }
    return super.visitRevokeRole(plan, context);
  }

  @Override
  public TSStatus visitGrantRoleToUser(final AuthorTreePlan plan, final TSStatus context) {
    if (context.getCode() == TSStatusCode.USER_ALREADY_HAS_ROLE.getStatusCode()) {
      return new TSStatus(TSStatusCode.PIPE_RECEIVER_IDEMPOTENT_CONFLICT_EXCEPTION.getStatusCode())
          .setMessage(context.getMessage());
    }
    return super.visitGrantRoleToUser(plan, context);
  }

  @Override
  public TSStatus visitRevokeRoleFromUser(
      final AuthorTreePlan revokeRoleFromUserPlan, final TSStatus context) {
    if (context.getCode() == TSStatusCode.USER_NOT_HAS_ROLE.getStatusCode()) {
      return new TSStatus(TSStatusCode.PIPE_RECEIVER_IDEMPOTENT_CONFLICT_EXCEPTION.getStatusCode())
          .setMessage(context.getMessage());
    } else if (context.getCode() == TSStatusCode.ROLE_NOT_EXIST.getStatusCode()) {
      return new TSStatus(TSStatusCode.PIPE_RECEIVER_USER_CONFLICT_EXCEPTION.getStatusCode())
          .setMessage(context.getMessage());
    }
    return super.visitRevokeRoleFromUser(revokeRoleFromUserPlan, context);
  }

  @Override
  public TSStatus visitRCreateUser(
      final AuthorRelationalPlan rCreateUserPlan, final TSStatus context) {
    return visitAuthorRelationalPlan(rCreateUserPlan, context);
  }

  @Override
  public TSStatus visitRCreateRole(
      final AuthorRelationalPlan rCreateRolePlan, final TSStatus context) {
    return visitAuthorRelationalPlan(rCreateRolePlan, context);
  }

  @Override
  public TSStatus visitRUpdateUser(
      final AuthorRelationalPlan rUpdateUserPlan, final TSStatus context) {
    return visitAuthorRelationalPlan(rUpdateUserPlan, context);
  }

  @Override
  public TSStatus visitRDropUserPlan(
      final AuthorRelationalPlan rDropUserPlan, final TSStatus context) {
    return visitAuthorRelationalPlan(rDropUserPlan, context);
  }

  @Override
  public TSStatus visitRDropRolePlan(
      final AuthorRelationalPlan rDropRolePlan, final TSStatus context) {
    return visitAuthorRelationalPlan(rDropRolePlan, context);
  }

  @Override
  public TSStatus visitRGrantUserRole(
      final AuthorRelationalPlan rGrantUserRolePlan, final TSStatus context) {
    return visitAuthorRelationalPlan(rGrantUserRolePlan, context);
  }

  @Override
  public TSStatus visitRRevokeUserRole(
      final AuthorRelationalPlan rRevokeUserRolePlan, final TSStatus context) {
    return visitAuthorRelationalPlan(rRevokeUserRolePlan, context);
  }

  @Override
  public TSStatus visitRGrantUserAny(
      final AuthorRelationalPlan rGrantUserAnyPlan, final TSStatus context) {
    return visitAuthorRelationalPlan(rGrantUserAnyPlan, context);
  }

  @Override
  public TSStatus visitRGrantRoleAny(
      final AuthorRelationalPlan rGrantRoleAnyPlan, final TSStatus context) {
    return visitAuthorRelationalPlan(rGrantRoleAnyPlan, context);
  }

  @Override
  public TSStatus visitRGrantUserAll(
      final AuthorRelationalPlan rGrantUserAllPlan, final TSStatus context) {
    return visitAuthorRelationalPlan(rGrantUserAllPlan, context);
  }

  @Override
  public TSStatus visitRGrantRoleAll(
      final AuthorRelationalPlan rGrantRoleAllPlan, final TSStatus context) {
    return visitAuthorRelationalPlan(rGrantRoleAllPlan, context);
  }

  @Override
  public TSStatus visitRGrantUserDB(
      final AuthorRelationalPlan rGrantUserDBPlan, final TSStatus context) {
    return visitAuthorRelationalPlan(rGrantUserDBPlan, context);
  }

  @Override
  public TSStatus visitRGrantUserTB(
      final AuthorRelationalPlan rGrantUserTBPlan, final TSStatus context) {
    return visitAuthorRelationalPlan(rGrantUserTBPlan, context);
  }

  @Override
  public TSStatus visitRGrantRoleDB(
      final AuthorRelationalPlan rGrantRoleDBPlan, final TSStatus context) {
    return visitAuthorRelationalPlan(rGrantRoleDBPlan, context);
  }

  @Override
  public TSStatus visitRGrantRoleTB(
      final AuthorRelationalPlan rGrantRoleTBPlan, final TSStatus context) {
    return visitAuthorRelationalPlan(rGrantRoleTBPlan, context);
  }

  @Override
  public TSStatus visitRRevokeUserAny(
      final AuthorRelationalPlan rRevokeUserAnyPlan, final TSStatus context) {
    return visitAuthorRelationalPlan(rRevokeUserAnyPlan, context);
  }

  @Override
  public TSStatus visitRRevokeRoleAny(
      final AuthorRelationalPlan rRevokeRoleAnyPlan, final TSStatus context) {
    return visitAuthorRelationalPlan(rRevokeRoleAnyPlan, context);
  }

  @Override
  public TSStatus visitRRevokeUserAll(
      final AuthorRelationalPlan rRevokeUserAllPlan, final TSStatus context) {
    return visitAuthorRelationalPlan(rRevokeUserAllPlan, context);
  }

  @Override
  public TSStatus visitRRevokeRoleAll(
      final AuthorRelationalPlan rRevokeRoleAllPlan, final TSStatus context) {
    return visitAuthorRelationalPlan(rRevokeRoleAllPlan, context);
  }

  @Override
  public TSStatus visitRRevokeUserDBPrivilege(
      final AuthorRelationalPlan rRevokeUserDBPrivilegePlan, final TSStatus context) {
    return visitAuthorRelationalPlan(rRevokeUserDBPrivilegePlan, context);
  }

  @Override
  public TSStatus visitRRevokeUserTBPrivilege(
      final AuthorRelationalPlan rRevokeUserTBPrivilegePlan, final TSStatus context) {
    return visitAuthorRelationalPlan(rRevokeUserTBPrivilegePlan, context);
  }

  @Override
  public TSStatus visitRRevokeRoleDBPrivilege(
      final AuthorRelationalPlan rRevokeRoleTBPrivilegePlan, final TSStatus context) {
    return visitAuthorRelationalPlan(rRevokeRoleTBPrivilegePlan, context);
  }

  @Override
  public TSStatus visitRRevokeRoleTBPrivilege(
      final AuthorRelationalPlan rRevokeRoleTBPrivilegePlan, final TSStatus context) {
    return visitAuthorRelationalPlan(rRevokeRoleTBPrivilegePlan, context);
  }

  @Override
  public TSStatus visitRGrantUserSysPrivilege(
      final AuthorRelationalPlan rGrantUserSysPrivilegePlan, final TSStatus context) {
    return visitAuthorRelationalPlan(rGrantUserSysPrivilegePlan, context);
  }

  @Override
  public TSStatus visitRGrantRoleSysPrivilege(
      final AuthorRelationalPlan rGrantRoleSysPrivilegePlan, final TSStatus context) {
    return visitAuthorRelationalPlan(rGrantRoleSysPrivilegePlan, context);
  }

  @Override
  public TSStatus visitRRevokeUserSysPrivilege(
      final AuthorRelationalPlan rRevokeUserSysPrivilegePlan, final TSStatus context) {
    return visitAuthorRelationalPlan(rRevokeUserSysPrivilegePlan, context);
  }

  @Override
  public TSStatus visitRRevokeRoleSysPrivilege(
      final AuthorRelationalPlan rRevokeRoleSysPrivilegePlan, final TSStatus context) {
    return visitAuthorRelationalPlan(rRevokeRoleSysPrivilegePlan, context);
  }

  private TSStatus visitAuthorRelationalPlan(
      final AuthorRelationalPlan plan, final TSStatus context) {
    if (context.getCode() == TSStatusCode.USER_NOT_EXIST.getStatusCode()
        || context.getCode() == TSStatusCode.USER_ALREADY_EXIST.getStatusCode()
        || context.getCode() == TSStatusCode.ROLE_NOT_EXIST.getStatusCode()
        || context.getCode() == TSStatusCode.ROLE_ALREADY_EXIST.getStatusCode()
        || context.getCode() == TSStatusCode.USER_ALREADY_HAS_ROLE.getStatusCode()
        || context.getCode() == TSStatusCode.USER_NOT_HAS_ROLE.getStatusCode()
        || context.getCode() == TSStatusCode.NO_PERMISSION.getStatusCode()) {
      return new TSStatus(TSStatusCode.PIPE_RECEIVER_IDEMPOTENT_CONFLICT_EXCEPTION.getStatusCode())
          .setMessage(context.getMessage());
    }
    return visitPlan(plan, context);
  }

  @Override
  public TSStatus visitTTL(final SetTTLPlan plan, final TSStatus context) {
    if (context.getCode() == TSStatusCode.DATABASE_NOT_EXIST.getStatusCode()) {
      return new TSStatus(TSStatusCode.PIPE_RECEIVER_IDEMPOTENT_CONFLICT_EXCEPTION.getStatusCode())
          .setMessage(context.getMessage());
    }
    return super.visitTTL(plan, context);
  }

  @Override
  public TSStatus visitPipeCreateTableOrView(
      final PipeCreateTableOrViewPlan pipeCreateTableOrViewPlan, final TSStatus context) {
    return visitCommonTablePlan(pipeCreateTableOrViewPlan, context);
  }

  @Override
  public TSStatus visitAddTableColumn(
      final AddTableColumnPlan addTableColumnPlan, final TSStatus context) {
    return visitCommonTablePlan(addTableColumnPlan, context);
  }

  @Override
  public TSStatus visitSetTableProperties(
      final SetTablePropertiesPlan setTablePropertiesPlan, final TSStatus context) {
    return visitCommonTablePlan(setTablePropertiesPlan, context);
  }

  @Override
  public TSStatus visitCommitDeleteColumn(
      final CommitDeleteColumnPlan commitDeleteColumnPlan, final TSStatus context) {
    return visitCommonTablePlan(commitDeleteColumnPlan, context);
  }

  @Override
  public TSStatus visitRenameTableColumn(
      final RenameTableColumnPlan renameTableColumnPlan, final TSStatus context) {
    return visitCommonTablePlan(renameTableColumnPlan, context);
  }

  @Override
  public TSStatus visitAlterColumnDataType(
      final AlterColumnDataTypePlan alterColumnDataTypePlan, final TSStatus context) {
    return visitCommonTablePlan(alterColumnDataTypePlan, context);
  }

  @Override
  public TSStatus visitCommitDeleteTable(
      final CommitDeleteTablePlan commitDeleteTablePlan, final TSStatus context) {
    return visitCommonTablePlan(commitDeleteTablePlan, context);
  }

  @Override
  public TSStatus visitPipeDeleteDevices(
      final PipeDeleteDevicesPlan pipeDeleteDevicesPlan, final TSStatus context) {
    return visitCommonTablePlan(pipeDeleteDevicesPlan, context);
  }

  @Override
  public TSStatus visitSetTableComment(
      final SetTableCommentPlan setTableCommentPlan, final TSStatus context) {
    return visitCommonTablePlan(setTableCommentPlan, context);
  }

  @Override
  public TSStatus visitSetTableColumnComment(
      final SetTableColumnCommentPlan setTableColumnCommentPlan, final TSStatus context) {
    return visitCommonTablePlan(setTableColumnCommentPlan, context);
  }

  @Override
  public TSStatus visitRenameTable(final RenameTablePlan renameTablePlan, final TSStatus context) {
    return visitCommonTablePlan(renameTablePlan, context);
  }

  private TSStatus visitCommonTablePlan(final ConfigPhysicalPlan plan, final TSStatus context) {
    if (context.getCode() == TSStatusCode.DATABASE_NOT_EXIST.getStatusCode()
        || context.getCode() == TSStatusCode.TABLE_ALREADY_EXISTS.getStatusCode()
        || context.getCode() == TSStatusCode.TABLE_NOT_EXISTS.getStatusCode()
        || context.getCode() == TSStatusCode.COLUMN_ALREADY_EXISTS.getStatusCode()
        || context.getCode() == TSStatusCode.COLUMN_NOT_EXISTS.getStatusCode()) {
      return new TSStatus(TSStatusCode.PIPE_RECEIVER_IDEMPOTENT_CONFLICT_EXCEPTION.getStatusCode())
          .setMessage(context.getMessage());
    }
    if (context.getCode() == TSStatusCode.SEMANTIC_ERROR.getStatusCode()) {
      return new TSStatus(TSStatusCode.PIPE_RECEIVER_USER_CONFLICT_EXCEPTION.getStatusCode())
          .setMessage(context.getMessage());
    }
    return visitPlan(plan, context);
  }

  @Override
  public TSStatus visitPipeAlterEncodingCompressor(
      final PipeAlterEncodingCompressorPlan plan, final TSStatus context) {
    if (context.getCode() == TSStatusCode.PATH_NOT_EXIST.getStatusCode()) {
      return new TSStatus(TSStatusCode.PIPE_RECEIVER_IDEMPOTENT_CONFLICT_EXCEPTION.getStatusCode())
          .setMessage(context.getMessage());
    }
    return visitPlan(plan, context);
  }
}
