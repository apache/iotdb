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

package org.apache.iotdb.confignode.manager.pipe.extractor;

import org.apache.iotdb.commons.pipe.datastructure.pattern.TablePattern;
import org.apache.iotdb.commons.utils.PathUtils;
import org.apache.iotdb.confignode.consensus.request.ConfigPhysicalPlan;
import org.apache.iotdb.confignode.consensus.request.ConfigPhysicalPlanVisitor;
import org.apache.iotdb.confignode.consensus.request.write.database.DatabaseSchemaPlan;
import org.apache.iotdb.confignode.consensus.request.write.pipe.payload.PipeCreateTablePlan;
import org.apache.iotdb.confignode.consensus.request.write.table.AddTableColumnPlan;
import org.apache.iotdb.confignode.consensus.request.write.table.CommitDeleteColumnPlan;
import org.apache.iotdb.confignode.consensus.request.write.table.CommitDeleteTablePlan;
import org.apache.iotdb.confignode.consensus.request.write.table.RenameTableColumnPlan;
import org.apache.iotdb.confignode.consensus.request.write.table.SetTablePropertiesPlan;

import java.util.Optional;

public class PipeConfigPhysicalPlanTablePatternParseVisitor
    extends ConfigPhysicalPlanVisitor<Optional<ConfigPhysicalPlan>, TablePattern> {

  @Override
  public Optional<ConfigPhysicalPlan> visitPlan(
      final ConfigPhysicalPlan plan, final TablePattern pattern) {
    return Optional.of(plan);
  }

  @Override
  public Optional<ConfigPhysicalPlan> visitCreateDatabase(
      final DatabaseSchemaPlan createDatabasePlan, final TablePattern pattern) {
    return pattern.matchesDatabase(
            PathUtils.unQualifyDatabaseName(createDatabasePlan.getSchema().getName()))
        ? Optional.of(createDatabasePlan)
        : Optional.empty();
  }

  @Override
  public Optional<ConfigPhysicalPlan> visitAlterDatabase(
      final DatabaseSchemaPlan alterDatabasePlan, final TablePattern pattern) {
    return pattern.matchesDatabase(
            PathUtils.unQualifyDatabaseName(alterDatabasePlan.getSchema().getName()))
        ? Optional.of(alterDatabasePlan)
        : Optional.empty();
  }

  @Override
  public Optional<ConfigPhysicalPlan> visitPipeCreateTable(
      final PipeCreateTablePlan pipeCreateTablePlan, final TablePattern pattern) {
    return matchDatabaseAndTableName(
            pipeCreateTablePlan.getDatabase(),
            pipeCreateTablePlan.getTable().getTableName(),
            pattern)
        ? Optional.of(pipeCreateTablePlan)
        : Optional.empty();
  }

  @Override
  public Optional<ConfigPhysicalPlan> visitAddTableColumn(
      final AddTableColumnPlan addTableColumnPlan, final TablePattern pattern) {
    return matchDatabaseAndTableName(
            addTableColumnPlan.getDatabase(), addTableColumnPlan.getTableName(), pattern)
        ? Optional.of(addTableColumnPlan)
        : Optional.empty();
  }

  @Override
  public Optional<ConfigPhysicalPlan> visitSetTableProperties(
      final SetTablePropertiesPlan setTablePropertiesPlan, final TablePattern pattern) {
    return matchDatabaseAndTableName(
            setTablePropertiesPlan.getDatabase(), setTablePropertiesPlan.getTableName(), pattern)
        ? Optional.of(setTablePropertiesPlan)
        : Optional.empty();
  }

  @Override
  public Optional<ConfigPhysicalPlan> visitCommitDeleteColumn(
      final CommitDeleteColumnPlan commitDeleteColumnPlan, final TablePattern pattern) {
    return matchDatabaseAndTableName(
            commitDeleteColumnPlan.getDatabase(), commitDeleteColumnPlan.getTableName(), pattern)
        ? Optional.of(commitDeleteColumnPlan)
        : Optional.empty();
  }

  @Override
  public Optional<ConfigPhysicalPlan> visitRenameTableColumn(
      final RenameTableColumnPlan renameTableColumnPlan, final TablePattern pattern) {
    return matchDatabaseAndTableName(
            renameTableColumnPlan.getDatabase(), renameTableColumnPlan.getTableName(), pattern)
        ? Optional.of(renameTableColumnPlan)
        : Optional.empty();
  }

  @Override
  public Optional<ConfigPhysicalPlan> visitCommitDeleteTable(
      final CommitDeleteTablePlan commitDeleteTablePlan, final TablePattern pattern) {
    return matchDatabaseAndTableName(
            commitDeleteTablePlan.getDatabase(), commitDeleteTablePlan.getTableName(), pattern)
        ? Optional.of(commitDeleteTablePlan)
        : Optional.empty();
  }

  private boolean matchDatabaseAndTableName(
      final String database, final String tableName, final TablePattern pattern) {
    return pattern.matchesDatabase(PathUtils.unQualifyDatabaseName(database))
        && pattern.matchesTable(tableName);
  }
}