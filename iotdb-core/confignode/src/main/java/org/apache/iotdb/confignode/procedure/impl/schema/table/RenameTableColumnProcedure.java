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

package org.apache.iotdb.confignode.procedure.impl.schema.table;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.exception.IoTDBException;
import org.apache.iotdb.commons.exception.MetadataException;
import org.apache.iotdb.commons.schema.table.TableType;
import org.apache.iotdb.commons.schema.table.TsTable;
import org.apache.iotdb.commons.schema.table.ViewColumnSchemaUtils;
import org.apache.iotdb.commons.schema.table.WritableView;
import org.apache.iotdb.commons.schema.table.column.TsTableColumnSchema;
import org.apache.iotdb.confignode.consensus.request.ConfigPhysicalPlan;
import org.apache.iotdb.confignode.consensus.request.write.table.RenameTableColumnPlan;
import org.apache.iotdb.confignode.consensus.request.write.table.view.RenameViewColumnPlan;
import org.apache.iotdb.confignode.i18n.ProcedureMessages;
import org.apache.iotdb.confignode.procedure.env.ConfigNodeProcedureEnv;
import org.apache.iotdb.confignode.procedure.exception.ProcedureException;
import org.apache.iotdb.confignode.procedure.state.schema.RenameTableColumnState;
import org.apache.iotdb.confignode.procedure.store.ProcedureType;
import org.apache.iotdb.rpc.TSStatusCode;

import com.timecho.iotdb.confignode.consensus.request.write.table.view.writable.RenameWritableViewColumnPlan;
import org.apache.tsfile.utils.Pair;
import org.apache.tsfile.utils.ReadWriteIOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Objects;

public class RenameTableColumnProcedure
    extends AbstractAlterOrDropTableColumnProcedure<RenameTableColumnState> {
  private static final Logger LOGGER = LoggerFactory.getLogger(RenameTableColumnProcedure.class);
  private String newName;
  protected String originalColumnName;

  public RenameTableColumnProcedure(final boolean isGeneratedByPipe) {
    super(isGeneratedByPipe);
  }

  public RenameTableColumnProcedure(
      final String database,
      final String tableName,
      final String queryId,
      final String columnName,
      final String newName,
      final boolean isGeneratedByPipe) {
    super(database, tableName, queryId, columnName, isGeneratedByPipe);
    this.newName = newName;
  }

  @Override
  protected Flow executeFromState(
      final ConfigNodeProcedureEnv env, final RenameTableColumnState state)
      throws InterruptedException {
    final long startTime = System.currentTimeMillis();
    try {
      switch (state) {
        case COLUMN_CHECK:
          LOGGER.info(
              ProcedureMessages.COLUMN_CHECK_FOR_TABLE_WHEN_RENAMING_COLUMN, database, tableName);
          columnCheck(env);
          break;
        case PRE_RELEASE:
          LOGGER.info(
              ProcedureMessages.PRE_RELEASE_INFO_OF_TABLE_WHEN_RENAMING_COLUMN,
              database,
              tableName);
          preRelease(env);
          break;
        case RENAME_COLUMN:
          LOGGER.info(ProcedureMessages.RENAME_COLUMN_TO_TABLE_ON_CONFIG_NODE, database, tableName);
          renameColumn(env);
          break;
        case COMMIT_RELEASE:
          LOGGER.info(
              ProcedureMessages.COMMIT_RELEASE_INFO_OF_TABLE_WHEN_RENAMING_COLUMN,
              database,
              tableName);
          commitRelease(env);
          return Flow.NO_MORE_STATE;
        default:
          setFailure(
              new ProcedureException(
                  ProcedureMessages.UNRECOGNIZED_RENAMETABLECOLUMNSTATE + state));
          return Flow.NO_MORE_STATE;
      }
      return Flow.HAS_MORE_STATE;
    } finally {
      LOGGER.info(
          ProcedureMessages.RENAMETABLECOLUMN_COSTS_MS,
          database,
          tableName,
          state,
          (System.currentTimeMillis() - startTime));
    }
  }

  private void columnCheck(final ConfigNodeProcedureEnv env) {
    try {
      final String checkedOriginalColumnName = getCascadeOriginalColumnName();
      if (isFailed()) {
        return;
      }
      final Pair<TSStatus, TsTable> result =
          env.getConfigManager()
              .getClusterSchemaManager()
              .tableColumnCheckForColumnRenaming(
                  database,
                  tableName,
                  columnName,
                  newName,
                  getTableSchemaObjectType().getClusterSchemaTableType());
      final TSStatus status = result.getLeft();
      if (status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        setFailure(new ProcedureException(new IoTDBException(status)));
        return;
      }
      final TsTable checkedTable = result.getRight();
      if (Objects.nonNull(checkedOriginalColumnName)) {
        final Pair<TSStatus, TsTable> originalResult =
            checkOriginalColumnRenaming(env, checkedOriginalColumnName);
        if (isFailed()) {
          return;
        }
        originalTable = originalResult.getRight();
        originalColumnName = checkedOriginalColumnName;
        updateWritableViewSourceColumnMapping(
            checkedTable, newName, getRenamedOriginalColumnName());
      }
      table = checkedTable;
      setNextState(RenameTableColumnState.PRE_RELEASE);
    } catch (final MetadataException e) {
      setFailure(new ProcedureException(e));
    }
  }

  private String getCascadeOriginalColumnName() {
    if (getTableSchemaObjectType() != TableSchemaObjectType.WRITABLE_VIEW) {
      return null;
    }
    mayInitOriginal();
    if (isFailed() || Objects.isNull(originalDatabase) || !(table instanceof WritableView)) {
      return null;
    }
    return ((WritableView) table).getOriginalColumnName(columnName);
  }

  private Pair<TSStatus, TsTable> checkOriginalColumnRenaming(
      final ConfigNodeProcedureEnv env, final String checkedOriginalColumnName)
      throws MetadataException {
    if (checkedOriginalColumnName.equals(getRenamedOriginalColumnName())) {
      return new Pair<>(
          new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode()), getOriginalTable());
    }
    final Pair<TSStatus, TsTable> originalResult =
        env.getConfigManager()
            .getClusterSchemaManager()
            .tableColumnCheckForColumnRenaming(
                getOriginalDatabase(),
                getOriginalTableName(),
                checkedOriginalColumnName,
                getRenamedOriginalColumnName(),
                TableType.BASE_TABLE);
    final TSStatus originalStatus = originalResult.getLeft();
    if (originalStatus.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      setFailure(new ProcedureException(new IoTDBException(originalStatus)));
    }
    return originalResult;
  }

  @Override
  protected void preRelease(final ConfigNodeProcedureEnv env) {
    prepareOriginalTableForCascade(env);
    if (isFailed()) {
      return;
    }
    super.preRelease(env);
    setNextState(RenameTableColumnState.RENAME_COLUMN);
  }

  private void prepareOriginalTableForCascade(final ConfigNodeProcedureEnv env) {
    if (getTableSchemaObjectType() != TableSchemaObjectType.WRITABLE_VIEW
        || Objects.isNull(originalColumnName)) {
      return;
    }
    try {
      final Pair<TSStatus, TsTable> originalResult =
          checkOriginalColumnRenaming(env, originalColumnName);
      if (isFailed()) {
        return;
      }
      originalTable = originalResult.getRight();
      updateWritableViewSourceColumnMapping(table, newName, getRenamedOriginalColumnName());
    } catch (final MetadataException e) {
      setFailure(new ProcedureException(e));
    }
  }

  private void renameColumn(final ConfigNodeProcedureEnv env) {
    final TSStatus status =
        env.getConfigManager()
            .getClusterSchemaManager()
            .executePlan(createRenameColumnPlan(columnName, newName), isGeneratedByPipe);
    if (status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      setFailure(new ProcedureException(new IoTDBException(status)));
    } else {
      setNextState(RenameTableColumnState.COMMIT_RELEASE);
    }
  }

  @Override
  protected void rollbackState(final ConfigNodeProcedureEnv env, final RenameTableColumnState state)
      throws IOException, InterruptedException, ProcedureException {
    final long startTime = System.currentTimeMillis();
    try {
      switch (state) {
        case RENAME_COLUMN:
          LOGGER.info(
              ProcedureMessages.START_ROLLBACK_RENAMING_COLUMN_TO_TABLE_ON_CONFIGNODE,
              database,
              table.getTableName());
          rollbackRenameColumn(env);
          break;
        case PRE_RELEASE:
          LOGGER.info(
              ProcedureMessages.START_ROLLBACK_PRE_RELEASE_INFO_OF_TABLE,
              database,
              table.getTableName());
          rollbackPreRelease(env);
          break;
      }
    } finally {
      LOGGER.info(
          ProcedureMessages.ROLLBACK_RENAMETABLECOLUMN_COSTS_MS,
          state,
          (System.currentTimeMillis() - startTime));
    }
  }

  private void rollbackRenameColumn(final ConfigNodeProcedureEnv env) {
    if (table == null) {
      return;
    }
    final TSStatus status =
        env.getConfigManager()
            .getClusterSchemaManager()
            .executePlan(createRenameColumnPlan(newName, columnName), isGeneratedByPipe);
    if (status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      setFailure(new ProcedureException(new IoTDBException(status)));
    }
  }

  private ConfigPhysicalPlan createRenameColumnPlan(
      final String oldColumnName, final String renamedColumnName) {
    switch (getTableSchemaObjectType()) {
      case VIEW:
        return new RenameViewColumnPlan(database, tableName, oldColumnName, renamedColumnName);
      case WRITABLE_VIEW:
        final String originalOldName;
        final String originalNewName;
        if (Objects.isNull(originalColumnName)) {
          originalOldName = null;
          originalNewName = null;
        } else if (Objects.equals(oldColumnName, columnName)) {
          originalOldName = originalColumnName;
          originalNewName = getRenamedOriginalColumnName();
        } else {
          originalOldName = getRenamedOriginalColumnName();
          originalNewName = originalColumnName;
        }
        return new RenameWritableViewColumnPlan(
            database,
            tableName,
            oldColumnName,
            renamedColumnName,
            Objects.nonNull(originalColumnName) ? getOriginalDatabase() : null,
            Objects.nonNull(originalColumnName) ? getOriginalTableName() : null,
            originalOldName,
            originalNewName);
      default:
        return new RenameTableColumnPlan(database, tableName, oldColumnName, renamedColumnName);
    }
  }

  private String getRenamedOriginalColumnName() {
    return newName;
  }

  private void updateWritableViewSourceColumnMapping(
      final TsTable table, final String viewColumnName, final String sourceColumnName) {
    if (!(table instanceof WritableView)
        || Objects.isNull(viewColumnName)
        || Objects.isNull(sourceColumnName)) {
      return;
    }
    ((WritableView) table).putViewColumnSourceColumnMapping(viewColumnName, sourceColumnName);
    final TsTableColumnSchema columnSchema = table.getColumnSchema(viewColumnName);
    if (Objects.isNull(columnSchema)) {
      return;
    }
    if (Objects.equals(viewColumnName, sourceColumnName)) {
      columnSchema.getProps().remove(ViewColumnSchemaUtils.SOURCE_NAME);
    } else {
      ViewColumnSchemaUtils.setSourceName(columnSchema, sourceColumnName);
    }
  }

  @Override
  protected RenameTableColumnState getState(final int stateId) {
    return RenameTableColumnState.values()[stateId];
  }

  @Override
  protected int getStateId(final RenameTableColumnState state) {
    return state.ordinal();
  }

  @Override
  protected RenameTableColumnState getInitialState() {
    return RenameTableColumnState.COLUMN_CHECK;
  }

  @Override
  protected String getActionMessage() {
    return "rename table column";
  }

  @Override
  public void serialize(final DataOutputStream stream) throws IOException {
    stream.writeShort(
        isGeneratedByPipe
            ? ProcedureType.PIPE_ENRICHED_RENAME_TABLE_COLUMN_PROCEDURE.getTypeCode()
            : ProcedureType.RENAME_TABLE_COLUMN_PROCEDURE.getTypeCode());
    innerSerialize(stream);
  }

  @Override
  protected void innerSerialize(final DataOutputStream stream) throws IOException {
    super.innerSerialize(stream);
    ReadWriteIOUtils.write(newName, stream);
  }

  @Override
  public void deserialize(final ByteBuffer byteBuffer) {
    super.deserialize(byteBuffer);
    this.newName = ReadWriteIOUtils.readString(byteBuffer);
  }

  @Override
  public boolean equals(final Object o) {
    return super.equals(o)
        && Objects.equals(newName, ((RenameTableColumnProcedure) o).newName)
        && Objects.equals(originalColumnName, ((RenameTableColumnProcedure) o).originalColumnName);
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), newName, originalColumnName);
  }
}
