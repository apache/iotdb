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

package com.timecho.iotdb.confignode.procedure.impl.schema.table.view.writable;

import org.apache.iotdb.confignode.consensus.request.ConfigPhysicalPlan;
import org.apache.iotdb.confignode.i18n.ProcedureMessages;
import org.apache.iotdb.confignode.procedure.impl.schema.table.DropTableColumnProcedure;
import org.apache.iotdb.confignode.procedure.impl.schema.table.TableSchemaObjectType;
import org.apache.iotdb.confignode.procedure.store.ProcedureType;

import com.timecho.iotdb.confignode.consensus.request.write.table.view.writable.CommitDeleteWritableViewColumnPlan;
import com.timecho.iotdb.confignode.consensus.request.write.table.view.writable.PreDeleteWritableViewColumnPlan;

import java.io.DataOutputStream;
import java.io.IOException;

public class DropWritableViewColumnProcedure extends DropTableColumnProcedure {
  public DropWritableViewColumnProcedure(final boolean isGeneratedByPipe) {
    super(isGeneratedByPipe);
  }

  public DropWritableViewColumnProcedure(
      final String database,
      final String tableName,
      final String queryId,
      final String columnName,
      final boolean isGeneratedByPipe) {
    super(database, tableName, queryId, columnName, isGeneratedByPipe);
  }

  @Override
  protected TableSchemaObjectType getTableSchemaObjectType() {
    return TableSchemaObjectType.WRITABLE_VIEW;
  }

  @Override
  protected String getActionMessage() {
    return ProcedureMessages.DROP_WRITABLE_VIEW_COLUMN;
  }

  @Override
  protected ConfigPhysicalPlan createPreDeleteColumnPlan() {
    return new PreDeleteWritableViewColumnPlan(
        database,
        tableName,
        columnName,
        getOriginalDatabaseForColumn(),
        getOriginalTableNameForColumn(),
        getOriginalColumnName());
  }

  @Override
  protected boolean shouldIncludeOriginalColumnInCacheInvalidation() {
    return !isOriginalTagCascadeSkipped();
  }

  @Override
  protected String getRegionOperationDatabase() {
    return getOriginalDatabase();
  }

  @Override
  protected String getRegionOperationTableName() {
    return getOriginalTableName();
  }

  @Override
  protected String getRegionOperationColumnName() {
    return getOriginalColumnName();
  }

  @Override
  protected boolean shouldExecuteOnRegions() {
    return !isOriginalTagCascadeSkipped() && super.shouldExecuteOnRegions();
  }

  @Override
  protected ConfigPhysicalPlan createCommitDeleteColumnPlan() {
    return new CommitDeleteWritableViewColumnPlan(
        database,
        tableName,
        columnName,
        isOriginalTagCascadeSkipped() ? null : getOriginalDatabaseForColumn(),
        isOriginalTagCascadeSkipped() ? null : getOriginalTableNameForColumn(),
        isOriginalTagCascadeSkipped() ? null : getOriginalColumnName());
  }

  @Override
  public void serialize(final DataOutputStream stream) throws IOException {
    stream.writeShort(
        isGeneratedByPipe
            ? ProcedureType.PIPE_ENRICHED_DROP_WRITABLE_VIEW_COLUMN_PROCEDURE.getTypeCode()
            : ProcedureType.DROP_WRITABLE_VIEW_COLUMN_PROCEDURE.getTypeCode());
    innerSerialize(stream);
  }
}
