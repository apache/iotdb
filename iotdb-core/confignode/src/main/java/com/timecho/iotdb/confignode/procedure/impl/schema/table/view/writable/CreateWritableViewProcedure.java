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

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.exception.IoTDBException;
import org.apache.iotdb.commons.exception.MetadataException;
import org.apache.iotdb.commons.schema.table.TableNodeStatus;
import org.apache.iotdb.commons.schema.table.TsTable;
import org.apache.iotdb.commons.schema.table.ViewColumnSchemaUtils;
import org.apache.iotdb.commons.schema.table.ViewTableUtils;
import org.apache.iotdb.commons.schema.table.WritableView;
import org.apache.iotdb.commons.schema.table.column.TimeColumnSchema;
import org.apache.iotdb.commons.schema.table.column.TsTableColumnSchema;
import org.apache.iotdb.confignode.i18n.ProcedureMessages;
import org.apache.iotdb.confignode.procedure.env.ConfigNodeProcedureEnv;
import org.apache.iotdb.confignode.procedure.exception.ProcedureException;
import org.apache.iotdb.confignode.procedure.impl.schema.SchemaUtils;
import org.apache.iotdb.confignode.procedure.impl.schema.table.TableSchemaObjectType;
import org.apache.iotdb.confignode.procedure.impl.schema.table.view.CreateTableViewProcedure;
import org.apache.iotdb.confignode.procedure.state.schema.CreateTableState;
import org.apache.iotdb.confignode.procedure.store.ProcedureType;

import com.timecho.iotdb.confignode.consensus.request.write.table.view.writable.RollbackCreateWritableViewPlan;
import org.apache.tsfile.utils.Pair;
import org.apache.tsfile.utils.ReadWriteIOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

import static org.apache.iotdb.commons.conf.IoTDBConstant.TTL_INFINITE;
import static org.apache.iotdb.rpc.TSStatusCode.COLUMN_NOT_EXISTS;
import static org.apache.iotdb.rpc.TSStatusCode.SEMANTIC_ERROR;
import static org.apache.iotdb.rpc.TSStatusCode.SUCCESS_STATUS;
import static org.apache.iotdb.rpc.TSStatusCode.TABLE_ALREADY_EXISTS;
import static org.apache.iotdb.rpc.TSStatusCode.TABLE_INCOMPATIBLE;
import static org.apache.iotdb.rpc.TSStatusCode.TABLE_NOT_EXISTS;

public class CreateWritableViewProcedure extends CreateTableViewProcedure {
  private static final Logger LOGGER = LoggerFactory.getLogger(CreateWritableViewProcedure.class);

  // Only populated for schema_cascade=true. "Original" follows the AbstractTablePlan naming:
  // originalDatabase locates the source table, and originalTable carries the source-table snapshot
  // to commit after applying cascade changes.
  private TsTable oldOriginalTable;
  private TsTable originalTable;
  private TsTable rollbackOriginalTable;
  private Map<String, String> viewColumnCommentMap;

  public CreateWritableViewProcedure(final boolean isGeneratedByPipe) {
    super(isGeneratedByPipe);
  }

  // No need to rewrite "createTablePlans"
  // Because the table itself can tell whether it's a writable view
  public CreateWritableViewProcedure(
      final String database,
      final WritableView table,
      final boolean replace,
      final boolean isGeneratedByPipe) {
    this(database, table, replace, isGeneratedByPipe, null);
  }

  public CreateWritableViewProcedure(
      final String database,
      final WritableView table,
      final boolean replace,
      final boolean isGeneratedByPipe,
      final Map<String, String> viewColumnCommentMap) {
    super(database, table, replace, isGeneratedByPipe);
    this.viewColumnCommentMap =
        Objects.nonNull(viewColumnCommentMap) ? new LinkedHashMap<>(viewColumnCommentMap) : null;
  }

  @Override
  protected TableSchemaObjectType getTableSchemaObjectType() {
    return TableSchemaObjectType.WRITABLE_VIEW;
  }

  @Override
  protected void checkTableExistence(final ConfigNodeProcedureEnv env) {
    if (!replace) {
      superCheckTableExistence(env);
    } else {
      try {
        final Optional<Pair<TsTable, TableNodeStatus>> oldTableAndStatus =
            env.getConfigManager()
                .getClusterSchemaManager()
                .getTableAndStatusIfExists(database, table.getTableName());
        if (oldTableAndStatus.isPresent()) {
          if (!(oldTableAndStatus.get().getLeft() instanceof WritableView)) {
            setFailure(
                new ProcedureException(
                    new IoTDBException(
                        String.format(
                            ProcedureMessages.TABLE_ALREADY_EXISTS, database, table.getTableName()),
                        TABLE_ALREADY_EXISTS.getStatusCode())));
            return;
          } else {
            oldView = oldTableAndStatus.get().getLeft();
            oldStatus = oldTableAndStatus.get().getRight();
          }
        }
      } catch (final MetadataException e) {
        setFailure(new ProcedureException(e));
      }
    }

    // Writable view only borrows column-level alias metadata from other view families. Its source
    // object is intentionally constrained to a base table until cross-view cascade/rewrite
    // semantics are designed explicitly.
    try {
      final WritableView view = (WritableView) table;
      // The analyzer currently enforces same-database creation. Read the source identity from the
      // writable-view metadata here anyway so future cross-database support only needs one
      // semantic gate for source eligibility.
      final String sourceDatabase = view.getSourceTableDatabase();
      final String sourceTableName = view.getSourceTableName();
      final Optional<TsTable> sourceTable =
          env.getConfigManager()
              .getClusterSchemaManager()
              .getTableIfExists(sourceDatabase, sourceTableName);
      if (!sourceTable.isPresent()) {
        setFailure(
            new ProcedureException(
                new IoTDBException(
                    String.format(
                        ProcedureMessages.SOURCE_TABLE_DOES_NOT_EXIST,
                        sourceDatabase,
                        sourceTableName),
                    TABLE_NOT_EXISTS.getStatusCode())));
      } else if (!ViewTableUtils.canUseAsWritableViewSource(sourceTable.get())) {
        setFailure(
            new ProcedureException(
                new IoTDBException(
                    String.format(
                        ProcedureMessages.SOURCE_TABLE_IS_NOT_BASE_TABLE,
                        sourceDatabase,
                        sourceTableName),
                    TABLE_INCOMPATIBLE.getStatusCode())));
      } else {
        final TsTable source = sourceTable.get();
        final Optional<String> explicitViewComment = view.getPropValue(TsTable.COMMENT_KEY);
        if (!explicitViewComment.isPresent()) {
          view.addProp(TsTable.COMMENT_KEY, source.getPropValue(TsTable.COMMENT_KEY).orElse(null));
        } else {
          applyOriginalTableCommentForCascade(source, explicitViewComment.get());
        }
        if (Objects.isNull(view.getViewColumnToSourceColumnMap())) {
          // `CREATE WRITABLE VIEW ... AS SELECT * ...` reaches ConfigNode without explicit column
          // mappings. Materialize both the mapping and the copied schemas here.
          materializeSelectStarColumns(source, view);
        } else if (view.getColumnNum() == 0) {
          // The analyzer already resolved the view-to-source aliases, but the concrete column
          // schemas are still lazily copied from the source table on ConfigNode.
          materializeProjectedColumns(source, sourceDatabase, sourceTableName, view);
        } else {
          // Replay/recovery may already carry materialized view schemas. In that case only verify
          // the source mapping and rebuild cascade metadata for the original table.
          validateMaterializedColumnsForCascade(source, sourceDatabase, sourceTableName, view);
        }
        if (isFailed()) {
          return;
        }

        if (!table.getPropValue(TsTable.TTL_PROPERTY).isPresent()) {
          table.addProp(
              TsTable.TTL_PROPERTY, source.getPropValue(TsTable.TTL_PROPERTY).orElse(TTL_INFINITE));
        } else if (((WritableView) table).isSchemaCascade()) {
          initOriginalTableForCascade(source);
          originalTable.addProp(
              TsTable.TTL_PROPERTY, table.getPropValue(TsTable.TTL_PROPERTY).orElse(TTL_INFINITE));
        }
        setNextState(CreateTableState.PRE_CREATE);
      }
    } catch (final MetadataException e) {
      setFailure(new ProcedureException(e));
    }
  }

  /** Returns the source database only when source-side cascade changes need to be committed. */
  @Override
  public String getOriginalDatabase() {
    return Objects.nonNull(originalTable) ? ((WritableView) table).getSourceTableDatabase() : null;
  }

  /** Returns the source-table snapshot after cascade updates, not the live source table object. */
  @Override
  public TsTable getOriginalTable() {
    return originalTable;
  }

  @Override
  public void serialize(final DataOutputStream stream) throws IOException {
    stream.writeShort(
        isGeneratedByPipe
            ? ProcedureType.PIPE_ENRICHED_CREATE_WRITABLE_VIEW_PROCEDURE.getTypeCode()
            : ProcedureType.CREATE_WRITABLE_VIEW_PROCEDURE.getTypeCode());
    innerSerialize(stream);

    ReadWriteIOUtils.write(Objects.nonNull(oldOriginalTable), stream);
    if (Objects.nonNull(oldOriginalTable)) {
      oldOriginalTable.serialize(stream);
    }

    ReadWriteIOUtils.write(Objects.nonNull(originalTable), stream);
    if (Objects.nonNull(originalTable)) {
      originalTable.serialize(stream);
    }
    ReadWriteIOUtils.write(Objects.nonNull(rollbackOriginalTable), stream);
    if (Objects.nonNull(rollbackOriginalTable)) {
      rollbackOriginalTable.serialize(stream);
    }
  }

  @Override
  protected void innerSerialize(final DataOutputStream stream) throws IOException {
    super.innerSerialize(stream);
    ReadWriteIOUtils.write(viewColumnCommentMap, stream);
  }

  @Override
  public void deserialize(final ByteBuffer byteBuffer) {
    super.deserialize(byteBuffer);
    viewColumnCommentMap = ReadWriteIOUtils.readMap(byteBuffer);
    if (ReadWriteIOUtils.readBoolObject(byteBuffer)) {
      oldOriginalTable = TsTable.deserialize(byteBuffer);
    }
    if (ReadWriteIOUtils.readBoolObject(byteBuffer)) {
      originalTable = TsTable.deserialize(byteBuffer);
    }
    if (byteBuffer.hasRemaining() && ReadWriteIOUtils.readBoolObject(byteBuffer)) {
      rollbackOriginalTable = TsTable.deserialize(byteBuffer);
    }
  }

  @Override
  public boolean equals(final Object o) {
    return super.equals(o)
        && Objects.equals(
            viewColumnCommentMap, ((CreateWritableViewProcedure) o).viewColumnCommentMap)
        && Objects.equals(oldOriginalTable, ((CreateWritableViewProcedure) o).oldOriginalTable)
        && Objects.equals(originalTable, ((CreateWritableViewProcedure) o).originalTable)
        && Objects.equals(
            rollbackOriginalTable, ((CreateWritableViewProcedure) o).rollbackOriginalTable);
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        super.hashCode(),
        viewColumnCommentMap,
        oldOriginalTable,
        originalTable,
        rollbackOriginalTable);
  }

  private void initOriginalTableForCascade(final TsTable source) {
    if (Objects.nonNull(originalTable)) {
      return;
    }
    oldOriginalTable = new TsTable(source);
    rollbackOriginalTable = new TsTable(source);
    originalTable = new TsTable(source);
  }

  private void applyOriginalTableCommentForCascade(final TsTable source, final String comment) {
    if (!((WritableView) table).isSchemaCascade()) {
      return;
    }
    initOriginalTableForCascade(source);
    if (Objects.isNull(comment)) {
      originalTable.removeProp(TsTable.COMMENT_KEY);
    } else {
      originalTable.addProp(TsTable.COMMENT_KEY, comment);
    }
  }

  private void applyExplicitColumnCommentForCascade(
      final TsTable source,
      final String sourceColumnName,
      final String viewColumnName,
      final TsTableColumnSchema viewColumnSchema) {
    if (!hasExplicitColumnComment(viewColumnName)) {
      return;
    }
    final String comment = viewColumnCommentMap.get(viewColumnName);
    applyColumnComment(viewColumnSchema, comment);
    applyOriginalColumnCommentForCascade(source, sourceColumnName, comment);
  }

  private void materializeSelectStarColumns(final TsTable source, final WritableView view) {
    view.setViewColumnToSourceColumnMap(new LinkedHashMap<>());
    source
        .getColumnList()
        .forEach(
            column -> {
              view.getViewColumnToSourceColumnMap()
                  .put(column.getColumnName(), column.getColumnName());
              view.addColumnSchema(
                  copySourceColumnSchemaForView(
                      source, column.getColumnName(), column.getColumnName()));
            });
  }

  private void materializeProjectedColumns(
      final TsTable source,
      final String sourceDatabase,
      final String sourceTableName,
      final WritableView view) {
    if (!validateNoDuplicateSourceColumnMapping(view)) {
      return;
    }
    boolean hasTime = false;
    for (final Map.Entry<String, String> entry : view.getViewColumnToSourceColumnMap().entrySet()) {
      if (Objects.isNull(
          validateSourceColumnExists(source, sourceDatabase, sourceTableName, entry.getValue()))) {
        return;
      }
      final TsTableColumnSchema viewColumnSchema =
          copySourceColumnSchemaForView(source, entry.getValue(), entry.getKey());
      hasTime |= viewColumnSchema instanceof TimeColumnSchema;
      view.addColumnSchema(viewColumnSchema);
    }
    if (!hasTime) {
      view.addColumnSchema(source.getTimeColumnSchema().copy());
      view.getViewColumnToSourceColumnMap()
          .put(
              source.getTimeColumnSchema().getColumnName(),
              source.getTimeColumnSchema().getColumnName());
    }
  }

  private boolean validateNoDuplicateSourceColumnMapping(final WritableView view) {
    final Set<String> sourceColumnNames = new HashSet<>();
    for (final Map.Entry<String, String> entry : view.getViewColumnToSourceColumnMap().entrySet()) {
      if (!sourceColumnNames.add(entry.getValue())) {
        setFailure(
            new ProcedureException(
                new IoTDBException(
                    String.format(
                        ProcedureMessages
                            .WRITABLE_VIEW_CANNOT_MAP_MULTIPLE_COLUMNS_FROM_SOURCE_COLUMN,
                        entry.getValue()),
                    SEMANTIC_ERROR.getStatusCode())));
        return false;
      }
    }
    return true;
  }

  private void validateMaterializedColumnsForCascade(
      final TsTable source,
      final String sourceDatabase,
      final String sourceTableName,
      final WritableView view) {
    for (final Map.Entry<String, String> entry : view.getViewColumnToSourceColumnMap().entrySet()) {
      if (Objects.isNull(
          validateSourceColumnExists(source, sourceDatabase, sourceTableName, entry.getValue()))) {
        return;
      }
      final TsTableColumnSchema viewColumnSchema = view.getColumnSchema(entry.getKey());
      if (Objects.nonNull(viewColumnSchema)) {
        applyMaterializedColumnCommentForCascade(
            source, entry.getValue(), entry.getKey(), viewColumnSchema);
      }
    }
  }

  private TsTableColumnSchema validateSourceColumnExists(
      final TsTable source,
      final String sourceDatabase,
      final String sourceTableName,
      final String sourceColumnName) {
    final TsTableColumnSchema sourceColumnSchema = source.getColumnSchema(sourceColumnName);
    if (Objects.isNull(sourceColumnSchema)) {
      setFailure(
          new ProcedureException(
              new IoTDBException(
                  String.format(
                      ProcedureMessages.SOURCE_COLUMN_DOES_NOT_EXIST,
                      sourceDatabase,
                      sourceTableName,
                      sourceColumnName),
                  COLUMN_NOT_EXISTS.getStatusCode())));
      return null;
    }
    return sourceColumnSchema;
  }

  private TsTableColumnSchema copySourceColumnSchemaForView(
      final TsTable source, final String sourceColumnName, final String viewColumnName) {
    final TsTableColumnSchema viewColumnSchema =
        source.getColumnSchema(sourceColumnName).copy().setColumnName(viewColumnName);
    setViewColumnSourceName(viewColumnSchema, sourceColumnName);
    applyExplicitColumnCommentForCascade(
        source, sourceColumnName, viewColumnName, viewColumnSchema);
    return viewColumnSchema;
  }

  private void applyMaterializedColumnCommentForCascade(
      final TsTable source,
      final String sourceColumnName,
      final String viewColumnName,
      final TsTableColumnSchema viewColumnSchema) {
    if (hasExplicitColumnComment(viewColumnName)) {
      applyOriginalColumnCommentForCascade(
          source, sourceColumnName, viewColumnCommentMap.get(viewColumnName));
      return;
    }
    final String materializedViewColumnComment =
        viewColumnSchema.getProps().get(TsTable.COMMENT_KEY);
    if (Objects.nonNull(materializedViewColumnComment)) {
      applyOriginalColumnCommentForCascade(source, sourceColumnName, materializedViewColumnComment);
    }
  }

  private boolean hasExplicitColumnComment(final String viewColumnName) {
    return Objects.nonNull(viewColumnCommentMap)
        && viewColumnCommentMap.containsKey(viewColumnName);
  }

  private void applyOriginalColumnCommentForCascade(
      final TsTable source, final String sourceColumnName, final String comment) {
    if (!((WritableView) table).isSchemaCascade()) {
      return;
    }
    initOriginalTableForCascade(source);
    final TsTableColumnSchema originalColumnSchema =
        originalTable.getColumnSchema(sourceColumnName);
    if (Objects.nonNull(originalColumnSchema)) {
      applyColumnComment(originalColumnSchema, comment);
    }
  }

  private void applyColumnComment(final TsTableColumnSchema schema, final String comment) {
    if (Objects.isNull(comment)) {
      schema.getProps().remove(TsTable.COMMENT_KEY);
    } else {
      schema.getProps().put(TsTable.COMMENT_KEY, comment);
    }
  }

  private void setViewColumnSourceName(
      final TsTableColumnSchema viewColumnSchema, final String sourceColumnName) {
    if (!sourceColumnName.equals(viewColumnSchema.getColumnName())) {
      ViewColumnSchemaUtils.setSourceName(viewColumnSchema, sourceColumnName);
    } else {
      viewColumnSchema.getProps().remove(ViewColumnSchemaUtils.SOURCE_NAME);
    }
  }

  @Override
  protected void rollbackCreate(final ConfigNodeProcedureEnv env) {
    if (Objects.isNull(rollbackOriginalTable)) {
      super.rollbackCreate(env);
      return;
    }
    final TSStatus status =
        SchemaUtils.executeInConsensusLayer(
            new RollbackCreateWritableViewPlan(
                database,
                Objects.nonNull(oldView) ? oldView : table,
                Objects.nonNull(oldStatus) ? oldStatus : TableNodeStatus.PRE_CREATE,
                ((WritableView) table).getSourceTableDatabase(),
                rollbackOriginalTable,
                Objects.nonNull(oldView)),
            env,
            LOGGER);
    if (status.getCode() != SUCCESS_STATUS.getStatusCode()) {
      LOGGER.warn(
          ProcedureMessages.FAILED_TO_ROLLBACK_TABLE_CREATION, database, table.getTableName());
      setFailure(new ProcedureException(new IoTDBException(status)));
    }
  }
}
