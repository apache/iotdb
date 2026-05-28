/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.confignode.manager.pipe.source;

import org.apache.iotdb.commons.pipe.datastructure.pattern.TablePattern;
import org.apache.iotdb.commons.utils.PathUtils;
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

import com.timecho.iotdb.confignode.consensus.request.write.table.view.writable.AddWritableViewColumnPlan;
import com.timecho.iotdb.confignode.consensus.request.write.table.view.writable.AlterWritableViewColumnDataTypePlan;
import com.timecho.iotdb.confignode.consensus.request.write.table.view.writable.CommitDeleteWritableViewColumnPlan;
import com.timecho.iotdb.confignode.consensus.request.write.table.view.writable.CommitDeleteWritableViewPlan;
import com.timecho.iotdb.confignode.consensus.request.write.table.view.writable.SetWritableViewColumnCommentPlan;
import com.timecho.iotdb.confignode.consensus.request.write.table.view.writable.SetWritableViewCommentPlan;
import com.timecho.iotdb.confignode.consensus.request.write.table.view.writable.SetWritableViewPropertiesPlan;

import java.util.Objects;
import java.util.Optional;

import static org.apache.iotdb.confignode.manager.pipe.source.PipeConfigTablePrivilegeParseVisitor.getAddWritableViewColumnPlan;
import static org.apache.iotdb.confignode.manager.pipe.source.PipeConfigTablePrivilegeParseVisitor.getAlterWritableViewColumnDataTypePlan;
import static org.apache.iotdb.confignode.manager.pipe.source.PipeConfigTablePrivilegeParseVisitor.getCommitDeleteWritableViewColumnPlan;
import static org.apache.iotdb.confignode.manager.pipe.source.PipeConfigTablePrivilegeParseVisitor.getCommitDeleteWritableViewPlan;
import static org.apache.iotdb.confignode.manager.pipe.source.PipeConfigTablePrivilegeParseVisitor.getSetWritableViewColumnCommentPlan;
import static org.apache.iotdb.confignode.manager.pipe.source.PipeConfigTablePrivilegeParseVisitor.getSetWritableViewCommentPlan;
import static org.apache.iotdb.confignode.manager.pipe.source.PipeConfigTablePrivilegeParseVisitor.getSetWritableViewPropertiesPlan;

public class PipeConfigTablePatternParseVisitor
    extends ConfigPhysicalPlanVisitor<Optional<ConfigPhysicalPlan>, TablePattern> {

  private final TimechoConfigPhysicalPlanVisitor<Optional<ConfigPhysicalPlan>, TablePattern>
      timechoVisitor =
          new TimechoConfigPhysicalPlanVisitor<Optional<ConfigPhysicalPlan>, TablePattern>(this) {
            @Override
            public Optional<ConfigPhysicalPlan> visitAddWritableViewColumn(
                final AddWritableViewColumnPlan addWritableViewColumnPlan,
                final TablePattern pattern) {
              return Objects.nonNull(addWritableViewColumnPlan.getOriginalDatabase())
                  ? getAddWritableViewColumnPlan(
                      addWritableViewColumnPlan,
                      pattern.matchesDatabase(
                              PathUtils.unQualifyDatabaseName(
                                  addWritableViewColumnPlan.getDatabase()))
                          && pattern.matchesTable(addWritableViewColumnPlan.getTableName()),
                      pattern.matchesDatabase(
                              PathUtils.unQualifyDatabaseName(
                                  addWritableViewColumnPlan.getOriginalDatabase()))
                          && pattern.matchesTable(addWritableViewColumnPlan.getOriginalTableName()))
                  : PipeConfigTablePatternParseVisitor.this.visitAbstractTablePlan(
                      addWritableViewColumnPlan, pattern);
            }

            @Override
            public Optional<ConfigPhysicalPlan> visitSetWritableViewProperties(
                final SetWritableViewPropertiesPlan setWritableViewPropertiesPlan,
                final TablePattern pattern) {
              return Objects.nonNull(setWritableViewPropertiesPlan.getOriginalDatabase())
                  ? getSetWritableViewPropertiesPlan(
                      setWritableViewPropertiesPlan,
                      pattern.matchesDatabase(
                              PathUtils.unQualifyDatabaseName(
                                  setWritableViewPropertiesPlan.getDatabase()))
                          && pattern.matchesTable(setWritableViewPropertiesPlan.getTableName()),
                      pattern.matchesDatabase(
                              PathUtils.unQualifyDatabaseName(
                                  setWritableViewPropertiesPlan.getOriginalDatabase()))
                          && pattern.matchesTable(
                              setWritableViewPropertiesPlan.getOriginalTableName()))
                  : PipeConfigTablePatternParseVisitor.this.visitAbstractTablePlan(
                      setWritableViewPropertiesPlan, pattern);
            }

            @Override
            public Optional<ConfigPhysicalPlan> visitCommitDeleteWritableViewColumn(
                final CommitDeleteWritableViewColumnPlan commitDeleteWritableViewColumnPlan,
                final TablePattern pattern) {
              return Objects.nonNull(commitDeleteWritableViewColumnPlan.getOriginalDatabase())
                  ? getCommitDeleteWritableViewColumnPlan(
                      commitDeleteWritableViewColumnPlan,
                      pattern.matchesDatabase(
                              PathUtils.unQualifyDatabaseName(
                                  commitDeleteWritableViewColumnPlan.getDatabase()))
                          && pattern.matchesTable(
                              commitDeleteWritableViewColumnPlan.getTableName()),
                      pattern.matchesDatabase(
                              PathUtils.unQualifyDatabaseName(
                                  commitDeleteWritableViewColumnPlan.getOriginalDatabase()))
                          && pattern.matchesTable(
                              commitDeleteWritableViewColumnPlan.getOriginalTableName()))
                  : PipeConfigTablePatternParseVisitor.this.visitAbstractTablePlan(
                      commitDeleteWritableViewColumnPlan, pattern);
            }

            @Override
            public Optional<ConfigPhysicalPlan> visitAlterWritableViewColumnDataType(
                final AlterWritableViewColumnDataTypePlan alterWritableViewColumnDataTypePlan,
                final TablePattern pattern) {
              return Objects.nonNull(alterWritableViewColumnDataTypePlan.getOriginalDatabase())
                  ? getAlterWritableViewColumnDataTypePlan(
                      alterWritableViewColumnDataTypePlan,
                      pattern.matchesDatabase(
                              PathUtils.unQualifyDatabaseName(
                                  alterWritableViewColumnDataTypePlan.getDatabase()))
                          && pattern.matchesTable(
                              alterWritableViewColumnDataTypePlan.getTableName()),
                      pattern.matchesDatabase(
                              PathUtils.unQualifyDatabaseName(
                                  alterWritableViewColumnDataTypePlan.getOriginalDatabase()))
                          && pattern.matchesTable(
                              alterWritableViewColumnDataTypePlan.getOriginalTableName()))
                  : PipeConfigTablePatternParseVisitor.this.visitAbstractTablePlan(
                      alterWritableViewColumnDataTypePlan, pattern);
            }

            @Override
            public Optional<ConfigPhysicalPlan> visitCommitDeleteWritableView(
                final CommitDeleteWritableViewPlan commitDeleteWritableViewPlan,
                final TablePattern pattern) {
              return Objects.nonNull(commitDeleteWritableViewPlan.getOriginalDatabase())
                  ? getCommitDeleteWritableViewPlan(
                      commitDeleteWritableViewPlan,
                      pattern.matchesDatabase(
                              PathUtils.unQualifyDatabaseName(
                                  commitDeleteWritableViewPlan.getDatabase()))
                          && pattern.matchesTable(commitDeleteWritableViewPlan.getTableName()),
                      pattern.matchesDatabase(
                              PathUtils.unQualifyDatabaseName(
                                  commitDeleteWritableViewPlan.getOriginalDatabase()))
                          && pattern.matchesTable(
                              commitDeleteWritableViewPlan.getOriginalTableName()))
                  : PipeConfigTablePatternParseVisitor.this.visitAbstractTablePlan(
                      commitDeleteWritableViewPlan, pattern);
            }

            @Override
            public Optional<ConfigPhysicalPlan> visitSetWritableViewComment(
                final SetWritableViewCommentPlan setWritableViewCommentPlan,
                final TablePattern pattern) {
              return Objects.nonNull(setWritableViewCommentPlan.getOriginalDatabase())
                  ? getSetWritableViewCommentPlan(
                      setWritableViewCommentPlan,
                      pattern.matchesDatabase(
                              PathUtils.unQualifyDatabaseName(
                                  setWritableViewCommentPlan.getDatabase()))
                          && pattern.matchesTable(setWritableViewCommentPlan.getTableName()),
                      pattern.matchesDatabase(
                              PathUtils.unQualifyDatabaseName(
                                  setWritableViewCommentPlan.getOriginalDatabase()))
                          && pattern.matchesTable(
                              setWritableViewCommentPlan.getOriginalTableName()))
                  : PipeConfigTablePatternParseVisitor.this.visitAbstractTablePlan(
                      setWritableViewCommentPlan, pattern);
            }

            @Override
            public Optional<ConfigPhysicalPlan> visitSetWritableViewColumnComment(
                final SetWritableViewColumnCommentPlan setWritableViewColumnCommentPlan,
                final TablePattern pattern) {
              return Objects.nonNull(setWritableViewColumnCommentPlan.getOriginalDatabase())
                  ? getSetWritableViewColumnCommentPlan(
                      setWritableViewColumnCommentPlan,
                      pattern.matchesDatabase(
                              PathUtils.unQualifyDatabaseName(
                                  setWritableViewColumnCommentPlan.getDatabase()))
                          && pattern.matchesTable(setWritableViewColumnCommentPlan.getTableName()),
                      pattern.matchesDatabase(
                              PathUtils.unQualifyDatabaseName(
                                  setWritableViewColumnCommentPlan.getOriginalDatabase()))
                          && pattern.matchesTable(
                              setWritableViewColumnCommentPlan.getOriginalTableName()))
                  : PipeConfigTablePatternParseVisitor.this.visitAbstractTablePlan(
                      setWritableViewColumnCommentPlan, pattern);
            }
          };

  @Override
  protected TimechoConfigPhysicalPlanVisitor<Optional<ConfigPhysicalPlan>, TablePattern>
      getTimechoVisitor() {
    return timechoVisitor;
  }

  @Override
  public Optional<ConfigPhysicalPlan> visitPlan(
      final ConfigPhysicalPlan plan, final TablePattern pattern) {
    return Optional.of(plan);
  }

  @Override
  public Optional<ConfigPhysicalPlan> visitCreateDatabase(
      final DatabaseSchemaPlan createDatabasePlan, final TablePattern pattern) {
    return visitDatabaseSchemaPlan(createDatabasePlan, pattern);
  }

  @Override
  public Optional<ConfigPhysicalPlan> visitAlterDatabase(
      final DatabaseSchemaPlan alterDatabasePlan, final TablePattern pattern) {
    return visitDatabaseSchemaPlan(alterDatabasePlan, pattern);
  }

  public Optional<ConfigPhysicalPlan> visitDatabaseSchemaPlan(
      final DatabaseSchemaPlan databaseSchemaPlan, final TablePattern pattern) {
    return pattern.matchesDatabase(
            PathUtils.unQualifyDatabaseName(databaseSchemaPlan.getSchema().getName()))
        ? Optional.of(databaseSchemaPlan)
        : Optional.empty();
  }

  @Override
  public Optional<ConfigPhysicalPlan> visitDeleteDatabase(
      final DeleteDatabasePlan deleteDatabasePlan, final TablePattern pattern) {
    return pattern.matchesDatabase(PathUtils.unQualifyDatabaseName(deleteDatabasePlan.getName()))
        ? Optional.of(deleteDatabasePlan)
        : Optional.empty();
  }

  @Override
  public Optional<ConfigPhysicalPlan> visitPipeCreateTableOrView(
      final PipeCreateTableOrViewPlan pipeCreateTableOrViewPlan, final TablePattern pattern) {
    return pattern.matchesDatabase(
                PathUtils.unQualifyDatabaseName(pipeCreateTableOrViewPlan.getDatabase()))
            && pattern.matchesTable(pipeCreateTableOrViewPlan.getTable().getTableName())
        ? Optional.of(pipeCreateTableOrViewPlan)
        : Optional.empty();
  }

  @Override
  public Optional<ConfigPhysicalPlan> visitAddTableColumn(
      final AddTableColumnPlan addTableColumnPlan, final TablePattern pattern) {
    return visitAbstractTablePlan(addTableColumnPlan, pattern);
  }

  @Override
  public Optional<ConfigPhysicalPlan> visitSetTableProperties(
      final SetTablePropertiesPlan setTablePropertiesPlan, final TablePattern pattern) {
    return visitAbstractTablePlan(setTablePropertiesPlan, pattern);
  }

  @Override
  public Optional<ConfigPhysicalPlan> visitCommitDeleteColumn(
      final CommitDeleteColumnPlan commitDeleteColumnPlan, final TablePattern pattern) {
    return visitAbstractTablePlan(commitDeleteColumnPlan, pattern);
  }

  @Override
  public Optional<ConfigPhysicalPlan> visitRenameTableColumn(
      final RenameTableColumnPlan renameTableColumnPlan, final TablePattern pattern) {
    return visitAbstractTablePlan(renameTableColumnPlan, pattern);
  }

  @Override
  public Optional<ConfigPhysicalPlan> visitAlterColumnDataType(
      final AlterColumnDataTypePlan alterColumnDataTypePlan, final TablePattern pattern) {
    return visitAbstractTablePlan(alterColumnDataTypePlan, pattern);
  }

  @Override
  public Optional<ConfigPhysicalPlan> visitCommitDeleteTable(
      final CommitDeleteTablePlan commitDeleteTablePlan, final TablePattern pattern) {
    return visitAbstractTablePlan(commitDeleteTablePlan, pattern);
  }

  @Override
  public Optional<ConfigPhysicalPlan> visitPipeDeleteDevices(
      final PipeDeleteDevicesPlan pipeDeleteDevicesPlan, final TablePattern pattern) {
    return visitAbstractTablePlan(pipeDeleteDevicesPlan, pattern);
  }

  @Override
  public Optional<ConfigPhysicalPlan> visitSetTableComment(
      final SetTableCommentPlan setTableCommentPlan, final TablePattern pattern) {
    return visitAbstractTablePlan(setTableCommentPlan, pattern);
  }

  @Override
  public Optional<ConfigPhysicalPlan> visitSetTableColumnComment(
      final SetTableColumnCommentPlan setTableColumnCommentPlan, final TablePattern pattern) {
    return visitAbstractTablePlan(setTableColumnCommentPlan, pattern);
  }

  @Override
  public Optional<ConfigPhysicalPlan> visitRenameTable(
      final RenameTablePlan renameTablePlan, final TablePattern pattern) {
    return visitAbstractTablePlan(renameTablePlan, pattern);
  }

  private Optional<ConfigPhysicalPlan> visitAbstractTablePlan(
      final AbstractTablePlan plan, final TablePattern pattern) {
    return pattern.matchesDatabase(PathUtils.unQualifyDatabaseName(plan.getDatabase()))
            && pattern.matchesTable(plan.getTableName())
        ? Optional.of(plan)
        : Optional.empty();
  }

  @Override
  public Optional<ConfigPhysicalPlan> visitRGrantUserDB(
      final AuthorRelationalPlan plan, final TablePattern pattern) {
    return visitAuthorDBPlan(plan, pattern);
  }

  @Override
  public Optional<ConfigPhysicalPlan> visitRGrantRoleDB(
      final AuthorRelationalPlan plan, final TablePattern pattern) {
    return visitAuthorDBPlan(plan, pattern);
  }

  @Override
  public Optional<ConfigPhysicalPlan> visitRRevokeUserDBPrivilege(
      final AuthorRelationalPlan plan, final TablePattern pattern) {
    return visitAuthorDBPlan(plan, pattern);
  }

  @Override
  public Optional<ConfigPhysicalPlan> visitRRevokeRoleDBPrivilege(
      final AuthorRelationalPlan plan, final TablePattern pattern) {
    return visitAuthorDBPlan(plan, pattern);
  }

  private Optional<ConfigPhysicalPlan> visitAuthorDBPlan(
      final AuthorRelationalPlan plan, final TablePattern pattern) {
    return pattern.matchesDatabase(plan.getDatabaseName()) ? Optional.of(plan) : Optional.empty();
  }

  @Override
  public Optional<ConfigPhysicalPlan> visitRGrantUserTB(
      final AuthorRelationalPlan plan, final TablePattern pattern) {
    return visitAuthorTBPlan(plan, pattern);
  }

  @Override
  public Optional<ConfigPhysicalPlan> visitRGrantRoleTB(
      final AuthorRelationalPlan plan, final TablePattern pattern) {
    return visitAuthorTBPlan(plan, pattern);
  }

  @Override
  public Optional<ConfigPhysicalPlan> visitRRevokeUserTBPrivilege(
      final AuthorRelationalPlan plan, final TablePattern pattern) {
    return visitAuthorTBPlan(plan, pattern);
  }

  @Override
  public Optional<ConfigPhysicalPlan> visitRRevokeRoleTBPrivilege(
      final AuthorRelationalPlan plan, final TablePattern pattern) {
    return visitAuthorTBPlan(plan, pattern);
  }

  private Optional<ConfigPhysicalPlan> visitAuthorTBPlan(
      final AuthorRelationalPlan plan, final TablePattern pattern) {
    return pattern.matchesDatabase(plan.getDatabaseName())
            && pattern.matchesTable(plan.getTableName())
        ? Optional.of(plan)
        : Optional.empty();
  }
}
