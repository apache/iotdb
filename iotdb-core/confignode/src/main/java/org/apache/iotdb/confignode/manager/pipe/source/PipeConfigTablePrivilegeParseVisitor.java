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
import org.apache.iotdb.commons.schema.table.WritableView;
import org.apache.iotdb.confignode.audit.CNAuditLogger;
import org.apache.iotdb.confignode.consensus.request.ConfigPhysicalPlan;
import org.apache.iotdb.confignode.consensus.request.ConfigPhysicalPlanVisitor;
import org.apache.iotdb.confignode.consensus.request.TimechoConfigPhysicalPlanVisitor;
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

import com.timecho.iotdb.confignode.consensus.request.write.table.view.writable.AddWritableViewColumnPlan;
import com.timecho.iotdb.confignode.consensus.request.write.table.view.writable.AlterWritableViewColumnDataTypePlan;
import com.timecho.iotdb.confignode.consensus.request.write.table.view.writable.CommitDeleteWritableViewColumnPlan;
import com.timecho.iotdb.confignode.consensus.request.write.table.view.writable.CommitDeleteWritableViewPlan;
import com.timecho.iotdb.confignode.consensus.request.write.table.view.writable.SetWritableViewColumnCommentPlan;
import com.timecho.iotdb.confignode.consensus.request.write.table.view.writable.SetWritableViewCommentPlan;
import com.timecho.iotdb.confignode.consensus.request.write.table.view.writable.SetWritableViewPropertiesPlan;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

public class PipeConfigTablePrivilegeParseVisitor
    extends ConfigPhysicalPlanVisitor<Optional<ConfigPhysicalPlan>, IAuditEntity> {

  private final TimechoConfigPhysicalPlanVisitor<Optional<ConfigPhysicalPlan>, IAuditEntity>
      timechoVisitor =
          new TimechoConfigPhysicalPlanVisitor<Optional<ConfigPhysicalPlan>, IAuditEntity>(this) {
            @Override
            public Optional<ConfigPhysicalPlan> visitAddWritableViewColumn(
                final AddWritableViewColumnPlan addWritableViewColumnPlan,
                final IAuditEntity userEntity) {
              return Objects.nonNull(addWritableViewColumnPlan.getOriginalDatabase())
                  ? getAddWritableViewColumnPlan(
                      addWritableViewColumnPlan,
                      isTableVisible(
                          userEntity,
                          addWritableViewColumnPlan.getDatabase(),
                          addWritableViewColumnPlan.getTableName()),
                      isTableVisible(
                          userEntity,
                          addWritableViewColumnPlan.getOriginalDatabase(),
                          addWritableViewColumnPlan.getOriginalTableName()))
                  : PipeConfigTablePrivilegeParseVisitor.this.visitAbstractTablePlan(
                      addWritableViewColumnPlan, userEntity);
            }

            @Override
            public Optional<ConfigPhysicalPlan> visitSetWritableViewProperties(
                final SetWritableViewPropertiesPlan setWritableViewPropertiesPlan,
                final IAuditEntity userEntity) {
              return Objects.nonNull(setWritableViewPropertiesPlan.getOriginalDatabase())
                  ? getSetWritableViewPropertiesPlan(
                      setWritableViewPropertiesPlan,
                      isTableVisible(
                          userEntity,
                          setWritableViewPropertiesPlan.getDatabase(),
                          setWritableViewPropertiesPlan.getTableName()),
                      isTableVisible(
                          userEntity,
                          setWritableViewPropertiesPlan.getOriginalDatabase(),
                          setWritableViewPropertiesPlan.getOriginalTableName()))
                  : PipeConfigTablePrivilegeParseVisitor.this.visitAbstractTablePlan(
                      setWritableViewPropertiesPlan, userEntity);
            }

            @Override
            public Optional<ConfigPhysicalPlan> visitCommitDeleteWritableViewColumn(
                final CommitDeleteWritableViewColumnPlan commitDeleteWritableViewColumnPlan,
                final IAuditEntity userEntity) {
              return Objects.nonNull(commitDeleteWritableViewColumnPlan.getOriginalDatabase())
                  ? getCommitDeleteWritableViewColumnPlan(
                      commitDeleteWritableViewColumnPlan,
                      isTableVisible(
                          userEntity,
                          commitDeleteWritableViewColumnPlan.getDatabase(),
                          commitDeleteWritableViewColumnPlan.getTableName()),
                      isTableVisible(
                          userEntity,
                          commitDeleteWritableViewColumnPlan.getOriginalDatabase(),
                          commitDeleteWritableViewColumnPlan.getOriginalTableName()))
                  : PipeConfigTablePrivilegeParseVisitor.this.visitAbstractTablePlan(
                      commitDeleteWritableViewColumnPlan, userEntity);
            }

            @Override
            public Optional<ConfigPhysicalPlan> visitCommitDeleteWritableView(
                final CommitDeleteWritableViewPlan commitDeleteWritableViewPlan,
                final IAuditEntity userEntity) {
              return Objects.nonNull(commitDeleteWritableViewPlan.getOriginalDatabase())
                  ? getCommitDeleteWritableViewPlan(
                      commitDeleteWritableViewPlan,
                      isTableVisible(
                          userEntity,
                          commitDeleteWritableViewPlan.getDatabase(),
                          commitDeleteWritableViewPlan.getTableName()),
                      isTableVisible(
                          userEntity,
                          commitDeleteWritableViewPlan.getOriginalDatabase(),
                          commitDeleteWritableViewPlan.getOriginalTableName()))
                  : PipeConfigTablePrivilegeParseVisitor.this.visitAbstractTablePlan(
                      commitDeleteWritableViewPlan, userEntity);
            }

            @Override
            public Optional<ConfigPhysicalPlan> visitSetWritableViewComment(
                final SetWritableViewCommentPlan setWritableViewCommentPlan,
                final IAuditEntity userEntity) {
              return Objects.nonNull(setWritableViewCommentPlan.getOriginalDatabase())
                  ? getSetWritableViewCommentPlan(
                      setWritableViewCommentPlan,
                      isTableVisible(
                          userEntity,
                          setWritableViewCommentPlan.getDatabase(),
                          setWritableViewCommentPlan.getTableName()),
                      isTableVisible(
                          userEntity,
                          setWritableViewCommentPlan.getOriginalDatabase(),
                          setWritableViewCommentPlan.getOriginalTableName()))
                  : PipeConfigTablePrivilegeParseVisitor.this.visitAbstractTablePlan(
                      setWritableViewCommentPlan, userEntity);
            }

            @Override
            public Optional<ConfigPhysicalPlan> visitSetWritableViewColumnComment(
                final SetWritableViewColumnCommentPlan setWritableViewColumnCommentPlan,
                final IAuditEntity userEntity) {
              return Objects.nonNull(setWritableViewColumnCommentPlan.getOriginalDatabase())
                  ? getSetWritableViewColumnCommentPlan(
                      setWritableViewColumnCommentPlan,
                      isTableVisible(
                          userEntity,
                          setWritableViewColumnCommentPlan.getDatabase(),
                          setWritableViewColumnCommentPlan.getTableName()),
                      isTableVisible(
                          userEntity,
                          setWritableViewColumnCommentPlan.getOriginalDatabase(),
                          setWritableViewColumnCommentPlan.getOriginalTableName()))
                  : PipeConfigTablePrivilegeParseVisitor.this.visitAbstractTablePlan(
                      setWritableViewColumnCommentPlan, userEntity);
            }

            @Override
            public Optional<ConfigPhysicalPlan> visitAlterWritableViewColumnDataType(
                final AlterWritableViewColumnDataTypePlan alterWritableViewColumnDataTypePlan,
                final IAuditEntity userEntity) {
              return Objects.nonNull(alterWritableViewColumnDataTypePlan.getOriginalDatabase())
                  ? getAlterWritableViewColumnDataTypePlan(
                      alterWritableViewColumnDataTypePlan,
                      isTableVisible(
                          userEntity,
                          alterWritableViewColumnDataTypePlan.getDatabase(),
                          alterWritableViewColumnDataTypePlan.getTableName()),
                      isTableVisible(
                          userEntity,
                          alterWritableViewColumnDataTypePlan.getOriginalDatabase(),
                          alterWritableViewColumnDataTypePlan.getOriginalTableName()))
                  : PipeConfigTablePrivilegeParseVisitor.this.visitAbstractTablePlan(
                      alterWritableViewColumnDataTypePlan, userEntity);
            }
          };

  @Override
  protected TimechoConfigPhysicalPlanVisitor<Optional<ConfigPhysicalPlan>, IAuditEntity>
      getTimechoVisitor() {
    return timechoVisitor;
  }

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

  public static Optional<ConfigPhysicalPlan> getAddWritableViewColumnPlan(
      final AddWritableViewColumnPlan addWritableViewColumnPlan,
      final boolean isWritableViewVisible,
      final boolean isSourceTableVisible) {
    if (isWritableViewVisible) {
      return isSourceTableVisible
          ? Optional.of(addWritableViewColumnPlan)
          : Optional.of(
              new AddWritableViewColumnPlan(
                  addWritableViewColumnPlan.getDatabase(),
                  addWritableViewColumnPlan.getTableName(),
                  addWritableViewColumnPlan.getColumnSchemaList(),
                  addWritableViewColumnPlan.isRollback(),
                  null,
                  null,
                  null));
    } else {
      return isSourceTableVisible
          ? Optional.of(
              new AddTableColumnPlan(
                  addWritableViewColumnPlan.getOriginalDatabase(),
                  addWritableViewColumnPlan.getOriginalTableName(),
                  addWritableViewColumnPlan.getOriginalColumnSchemaList(),
                  addWritableViewColumnPlan.isRollback()))
          : Optional.empty();
    }
  }

  @Override
  public Optional<ConfigPhysicalPlan> visitSetTableProperties(
      final SetTablePropertiesPlan setTablePropertiesPlan, final IAuditEntity userEntity) {
    return visitAbstractTablePlan(setTablePropertiesPlan, userEntity);
  }

  public static Optional<ConfigPhysicalPlan> getSetWritableViewPropertiesPlan(
      final SetWritableViewPropertiesPlan setWritableViewPropertiesPlan,
      final boolean isWritableViewVisible,
      final boolean isSourceTableVisible) {
    if (isWritableViewVisible) {
      return isSourceTableVisible
          ? Optional.of(setWritableViewPropertiesPlan)
          : Optional.of(
              new SetWritableViewPropertiesPlan(
                  setWritableViewPropertiesPlan.getDatabase(),
                  setWritableViewPropertiesPlan.getTableName(),
                  setWritableViewPropertiesPlan.getProperties(),
                  null,
                  null));
    } else {
      // The writable view itself is hidden, but its source table may still need the cascaded
      // property update.
      return isSourceTableVisible
          ? Optional.of(
              new SetTablePropertiesPlan(
                  setWritableViewPropertiesPlan.getOriginalDatabase(),
                  setWritableViewPropertiesPlan.getOriginalTableName(),
                  copyPropertiesForOriginalTable(setWritableViewPropertiesPlan.getProperties())))
          : Optional.empty();
    }
  }

  public static Map<String, String> copyPropertiesForOriginalTable(
      final Map<String, String> properties) {
    if (Objects.isNull(properties)) {
      return null;
    }
    final Map<String, String> copiedProperties = new HashMap<>(properties);
    copiedProperties.remove(WritableView.SCHEMA_CASCADE);
    return copiedProperties;
  }

  @Override
  public Optional<ConfigPhysicalPlan> visitCommitDeleteColumn(
      final CommitDeleteColumnPlan commitDeleteColumnPlan, final IAuditEntity userEntity) {
    return visitAbstractTablePlan(commitDeleteColumnPlan, userEntity);
  }

  public static Optional<ConfigPhysicalPlan> getCommitDeleteWritableViewColumnPlan(
      final CommitDeleteWritableViewColumnPlan commitDeleteWritableViewColumnPlan,
      final boolean isWritableViewVisible,
      final boolean isSourceTableVisible) {
    if (isWritableViewVisible) {
      return isSourceTableVisible
          ? Optional.of(commitDeleteWritableViewColumnPlan)
          : Optional.of(
              new CommitDeleteWritableViewColumnPlan(
                  commitDeleteWritableViewColumnPlan.getDatabase(),
                  commitDeleteWritableViewColumnPlan.getTableName(),
                  commitDeleteWritableViewColumnPlan.getColumnName(),
                  null,
                  null,
                  null));
    } else {
      return isSourceTableVisible
          ? Optional.of(
              new CommitDeleteColumnPlan(
                  commitDeleteWritableViewColumnPlan.getOriginalDatabase(),
                  commitDeleteWritableViewColumnPlan.getOriginalTableName(),
                  commitDeleteWritableViewColumnPlan.getOriginalColumnName()))
          : Optional.empty();
    }
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

  public static Optional<ConfigPhysicalPlan> getCommitDeleteWritableViewPlan(
      final CommitDeleteWritableViewPlan commitDeleteWritableViewPlan,
      final boolean isWritableViewVisible,
      final boolean isSourceTableVisible) {
    if (isWritableViewVisible) {
      return isSourceTableVisible
          ? Optional.of(commitDeleteWritableViewPlan)
          : Optional.of(
              new CommitDeleteWritableViewPlan(
                  commitDeleteWritableViewPlan.getDatabase(),
                  commitDeleteWritableViewPlan.getTableName(),
                  null,
                  null));
    }
    return isSourceTableVisible
        ? Optional.of(
            new CommitDeleteTablePlan(
                commitDeleteWritableViewPlan.getOriginalDatabase(),
                commitDeleteWritableViewPlan.getOriginalTableName()))
        : Optional.empty();
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

  public static Optional<ConfigPhysicalPlan> getSetWritableViewCommentPlan(
      final SetWritableViewCommentPlan setWritableViewCommentPlan,
      final boolean isWritableViewVisible,
      final boolean isSourceTableVisible) {
    if (isWritableViewVisible) {
      return isSourceTableVisible
          ? Optional.of(setWritableViewCommentPlan)
          : Optional.of(
              new SetWritableViewCommentPlan(
                  setWritableViewCommentPlan.getDatabase(),
                  setWritableViewCommentPlan.getTableName(),
                  setWritableViewCommentPlan.getComment(),
                  null,
                  null));
    } else {
      return isSourceTableVisible
          ? Optional.of(
              new SetTableCommentPlan(
                  setWritableViewCommentPlan.getOriginalDatabase(),
                  setWritableViewCommentPlan.getOriginalTableName(),
                  setWritableViewCommentPlan.getComment()))
          : Optional.empty();
    }
  }

  @Override
  public Optional<ConfigPhysicalPlan> visitSetTableColumnComment(
      final SetTableColumnCommentPlan setTableColumnCommentPlan, final IAuditEntity userEntity) {
    return visitAbstractTablePlan(setTableColumnCommentPlan, userEntity);
  }

  public static Optional<ConfigPhysicalPlan> getSetWritableViewColumnCommentPlan(
      final SetWritableViewColumnCommentPlan setWritableViewColumnCommentPlan,
      final boolean isWritableViewVisible,
      final boolean isSourceTableVisible) {
    if (isWritableViewVisible) {
      return isSourceTableVisible
          ? Optional.of(setWritableViewColumnCommentPlan)
          : Optional.of(
              new SetWritableViewColumnCommentPlan(
                  setWritableViewColumnCommentPlan.getDatabase(),
                  setWritableViewColumnCommentPlan.getTableName(),
                  setWritableViewColumnCommentPlan.getColumnName(),
                  setWritableViewColumnCommentPlan.getComment(),
                  null,
                  null,
                  null));
    } else {
      return isSourceTableVisible
          ? Optional.of(
              new SetTableColumnCommentPlan(
                  setWritableViewColumnCommentPlan.getOriginalDatabase(),
                  setWritableViewColumnCommentPlan.getOriginalTableName(),
                  setWritableViewColumnCommentPlan.getOriginalColumnName(),
                  setWritableViewColumnCommentPlan.getComment()))
          : Optional.empty();
    }
  }

  public Optional<ConfigPhysicalPlan> visitAlterColumnDataType(
      final AlterColumnDataTypePlan alterColumnDataTypePlan, final IAuditEntity userEntity) {
    return visitAbstractTablePlan(alterColumnDataTypePlan, userEntity);
  }

  public static Optional<ConfigPhysicalPlan> getAlterWritableViewColumnDataTypePlan(
      final AlterWritableViewColumnDataTypePlan alterWritableViewColumnDataTypePlan,
      final boolean isWritableViewVisible,
      final boolean isSourceTableVisible) {
    if (isWritableViewVisible) {
      return isSourceTableVisible
          ? Optional.of(alterWritableViewColumnDataTypePlan)
          : Optional.of(
              new AlterWritableViewColumnDataTypePlan(
                  alterWritableViewColumnDataTypePlan.getDatabase(),
                  alterWritableViewColumnDataTypePlan.getTableName(),
                  alterWritableViewColumnDataTypePlan.getColumnName(),
                  alterWritableViewColumnDataTypePlan.getNewType(),
                  null,
                  null,
                  null));
    } else {
      return isSourceTableVisible
          ? Optional.of(
              new AlterColumnDataTypePlan(
                  alterWritableViewColumnDataTypePlan.getOriginalDatabase(),
                  alterWritableViewColumnDataTypePlan.getOriginalTableName(),
                  alterWritableViewColumnDataTypePlan.getOriginalColumnName(),
                  alterWritableViewColumnDataTypePlan.getNewType()))
          : Optional.empty();
    }
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
}
