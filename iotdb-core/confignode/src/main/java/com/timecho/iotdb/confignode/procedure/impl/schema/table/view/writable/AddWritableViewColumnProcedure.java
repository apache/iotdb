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
import org.apache.iotdb.commons.schema.table.TableType;
import org.apache.iotdb.commons.schema.table.TreeViewSchema;
import org.apache.iotdb.commons.schema.table.TsTable;
import org.apache.iotdb.commons.schema.table.ViewColumnSchemaUtils;
import org.apache.iotdb.commons.schema.table.WritableView;
import org.apache.iotdb.commons.schema.table.column.FieldColumnSchema;
import org.apache.iotdb.commons.schema.table.column.TsTableColumnSchema;
import org.apache.iotdb.commons.schema.table.column.TsTableColumnSchemaUtil;
import org.apache.iotdb.confignode.i18n.ProcedureMessages;
import org.apache.iotdb.confignode.procedure.env.ConfigNodeProcedureEnv;
import org.apache.iotdb.confignode.procedure.exception.ProcedureException;
import org.apache.iotdb.confignode.procedure.impl.schema.table.AddTableColumnProcedure;
import org.apache.iotdb.confignode.procedure.impl.schema.table.TableSchemaObjectType;
import org.apache.iotdb.confignode.procedure.state.ProcedureState;
import org.apache.iotdb.confignode.procedure.state.schema.AddTableColumnState;
import org.apache.iotdb.confignode.procedure.store.ProcedureType;
import org.apache.iotdb.rpc.TSStatusCode;

import com.timecho.iotdb.confignode.consensus.request.write.table.view.writable.AddWritableViewColumnPlan;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.utils.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

public class AddWritableViewColumnProcedure extends AddTableColumnProcedure {
  private static final Logger LOGGER =
      LoggerFactory.getLogger(AddWritableViewColumnProcedure.class);
  private List<TsTableColumnSchema> originalAddedColumnList = new ArrayList<>();

  public AddWritableViewColumnProcedure(final boolean isGeneratedByPipe) {
    super(isGeneratedByPipe);
  }

  public AddWritableViewColumnProcedure(
      final String database,
      final String tableName,
      final String queryId,
      final List<TsTableColumnSchema> addedColumnList,
      final boolean isGeneratedByPipe) {
    super(database, tableName, queryId, addedColumnList, isGeneratedByPipe);
  }

  @Override
  protected TableSchemaObjectType getTableSchemaObjectType() {
    return TableSchemaObjectType.WRITABLE_VIEW;
  }

  @Override
  protected void columnCheck(final ConfigNodeProcedureEnv env) {
    final Optional<WritableView> writableViewOptional = getWritableView(env);
    if (!writableViewOptional.isPresent()) {
      return;
    }

    final WritableView writableView = writableViewOptional.get();
    if (!validateNoDuplicateSourceColumnMapping(writableView)) {
      return;
    }
    final Optional<TsTable> sourceTableOptional = getSourceTable(env, writableView);
    if (getState().equals(ProcedureState.FAILED)) {
      return;
    }
    final TsTable sourceTable = sourceTableOptional.orElse(null);

    final List<TsTableColumnSchema> resolvedViewColumnList = new ArrayList<>();
    final List<TsTableColumnSchema> originalColumnCandidateList = new ArrayList<>();
    for (final TsTableColumnSchema addedColumnSchema : addedColumnList) {
      if (isTreeViewFromSyntax(addedColumnSchema)) {
        setTreeViewFromSyntaxFailure();
        return;
      }
      // Writable views persist source-column aliases in shared view-column metadata, while the
      // source table itself is still required to be a base table.
      final String sourceColumnName = ViewColumnSchemaUtils.getSourceName(addedColumnSchema);
      final TsTableColumnSchema sourceColumnSchema =
          Objects.nonNull(sourceTable) ? sourceTable.getColumnSchema(sourceColumnName) : null;
      if (Objects.isNull(sourceColumnSchema)) {
        if (Objects.isNull(sourceTable)) {
          if (isUnknownField(addedColumnSchema)) {
            setMissingSourceTableFailure(writableView, sourceColumnName, addedColumnSchema);
            return;
          }
          resolvedViewColumnList.add(prepareViewColumnSchema(addedColumnSchema, sourceColumnName));
          continue;
        }
        if (!writableView.isSchemaCascade() || isUnknownField(addedColumnSchema)) {
          setSourceColumnMissingFailure(writableView, sourceColumnName, addedColumnSchema);
          return;
        }
        originalColumnCandidateList.add(toSourceColumnSchema(addedColumnSchema, sourceColumnName));
        resolvedViewColumnList.add(prepareViewColumnSchema(addedColumnSchema, sourceColumnName));
        continue;
      }
      if (!isUnknownField(addedColumnSchema)
          && !isSameCategoryAndDataType(sourceColumnSchema, addedColumnSchema)) {
        setSourceColumnMismatchFailure(
            writableView, sourceTable, sourceColumnName, addedColumnSchema);
        return;
      }
      resolvedViewColumnList.add(
          copySourceColumnSchemaForView(addedColumnSchema, sourceColumnSchema, sourceColumnName));
    }

    addedColumnList = resolvedViewColumnList;
    originalAddedColumnList = originalColumnCandidateList;
    super.columnCheck(env);
    if (getState().equals(ProcedureState.FAILED)) {
      return;
    }

    // Only columns absent from the source table need to be cascaded into the source. Existing
    // source columns were already copied into the view schema above.
    if (!writableView.isSchemaCascade() || originalAddedColumnList.isEmpty()) {
      return;
    }

    final String originalDatabase = getOriginalDatabase();
    final String originalTableName = getOriginalTableName();
    if (Objects.isNull(originalDatabase) || Objects.isNull(originalTableName)) {
      // The source table can disappear after the initial lookup, so keep the view-side change
      // and skip the source-side cascade best-effort.
      return;
    }

    try {
      final Pair<TSStatus, TsTable> result =
          env.getConfigManager()
              .getClusterSchemaManager()
              .tableColumnCheckForColumnExtension(
                  originalDatabase,
                  originalTableName,
                  originalAddedColumnList,
                  // The cascaded table is always the writable view's source base table.
                  TableType.BASE_TABLE,
                  true);
      final TSStatus status = result.getLeft();
      if (status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        if (!WritableViewUtils.isIdempotent(status)) {
          setFailure(new ProcedureException(new IoTDBException(status)));
        }
      } else {
        originalTable = result.getRight();
      }
    } catch (final MetadataException e) {
      if (!WritableViewUtils.isIdempotent(e)) {
        setFailure(new ProcedureException(e));
      }
    }
  }

  private Optional<WritableView> getWritableView(final ConfigNodeProcedureEnv env) {
    if (table instanceof WritableView) {
      return Optional.of((WritableView) table);
    }
    try {
      final Optional<TsTable> tableOptional =
          env.getConfigManager().getClusterSchemaManager().getTableIfExists(database, tableName);
      if (!tableOptional.isPresent()) {
        setFailure(
            new ProcedureException(
                new IoTDBException(
                    String.format(ProcedureMessages.TABLE_DOES_NOT_EXIST, database, tableName),
                    TSStatusCode.TABLE_NOT_EXISTS.getStatusCode())));
        return Optional.empty();
      }
      table = tableOptional.get();
      if (!(table instanceof WritableView)) {
        setFailure(
            new ProcedureException(
                new IoTDBException(
                    String.format(
                        ProcedureMessages.TABLE_IS_NOT_WRITABLE_VIEW_CHECK_IMPLEMENTATION,
                        database,
                        tableName),
                    TSStatusCode.INTERNAL_SERVER_ERROR.getStatusCode())));
        return Optional.empty();
      }
      return Optional.of((WritableView) table);
    } catch (final MetadataException e) {
      setFailure(new ProcedureException(e));
      return Optional.empty();
    }
  }

  private Optional<TsTable> getSourceTable(
      final ConfigNodeProcedureEnv env, final WritableView writableView) {
    try {
      final String sourceDatabase = writableView.getSourceTableDatabase();
      final String sourceTableName = writableView.getSourceTableName();
      final Optional<TsTable> sourceTableOptional =
          env.getConfigManager()
              .getClusterSchemaManager()
              .getTableIfExists(sourceDatabase, sourceTableName);
      if (!sourceTableOptional.isPresent()) {
        LOGGER.warn(
            ProcedureMessages.SKIP_SCHEMA_CASCADE_FOR_WRITABLE_VIEW_MISSING_SOURCE,
            database,
            tableName,
            sourceDatabase,
            sourceTableName);
        return Optional.empty();
      }
      return sourceTableOptional;
    } catch (final MetadataException e) {
      if (e.getErrorCode() == TSStatusCode.TABLE_NOT_EXISTS.getStatusCode()
          || e.getErrorCode() == TSStatusCode.DATABASE_NOT_EXIST.getStatusCode()) {
        LOGGER.warn(
            ProcedureMessages.SKIP_SCHEMA_CASCADE_FOR_WRITABLE_VIEW_MISSING_SOURCE_DETAIL,
            database,
            tableName,
            writableView.getSourceTableDatabase(),
            writableView.getSourceTableName(),
            e.getMessage());
        return Optional.empty();
      }
      setFailure(new ProcedureException(e));
      return Optional.empty();
    }
  }

  private static boolean isTreeViewFromSyntax(final TsTableColumnSchema schema) {
    return Objects.nonNull(schema.getProps().remove(TreeViewSchema.EXPLICIT_FROM));
  }

  private void setTreeViewFromSyntaxFailure() {
    setFailure(
        new ProcedureException(
            new IoTDBException(
                ProcedureMessages.WRITABLE_VIEW_ADD_COLUMN_UNSUPPORTED_TREE_VIEW_FROM,
                TSStatusCode.SEMANTIC_ERROR.getStatusCode())));
  }

  private void setSourceColumnMissingFailure(
      final WritableView writableView,
      final String sourceColumnName,
      final TsTableColumnSchema addedColumnSchema) {
    final String message =
        writableView.isSchemaCascade()
            ? String.format(
                ProcedureMessages.SOURCE_COLUMN_DOES_NOT_EXIST_CANNOT_INFER_SCHEMA,
                writableView.getSourceTableDatabase(),
                writableView.getSourceTableName(),
                sourceColumnName,
                addedColumnSchema.getColumnName())
            : String.format(
                ProcedureMessages.WRITABLE_VIEW_COLUMN_MUST_EXIST_WHEN_SCHEMA_CASCADE_FALSE,
                addedColumnSchema.getColumnName(),
                writableView.getSourceTableDatabase(),
                writableView.getSourceTableName());
    setFailure(
        new ProcedureException(
            new IoTDBException(message, TSStatusCode.SEMANTIC_ERROR.getStatusCode())));
  }

  private void setMissingSourceTableFailure(
      final WritableView writableView,
      final String sourceColumnName,
      final TsTableColumnSchema addedColumnSchema) {
    setFailure(
        new ProcedureException(
            new IoTDBException(
                String.format(
                    ProcedureMessages.SOURCE_TABLE_DOES_NOT_EXIST_CANNOT_INFER_SOURCE_COLUMN,
                    writableView.getSourceTableDatabase(),
                    writableView.getSourceTableName(),
                    addedColumnSchema.getColumnName(),
                    sourceColumnName),
                TSStatusCode.SEMANTIC_ERROR.getStatusCode())));
  }

  private void setSourceColumnMismatchFailure(
      final WritableView writableView,
      final TsTable sourceTable,
      final String sourceColumnName,
      final TsTableColumnSchema addedColumnSchema) {
    setFailure(
        new ProcedureException(
            new IoTDBException(
                String.format(
                    ProcedureMessages.WRITABLE_VIEW_COLUMN_MUST_MATCH_SOURCE_COLUMN,
                    addedColumnSchema.getColumnName(),
                    writableView.getSourceTableDatabase(),
                    sourceTable.getTableName(),
                    sourceColumnName),
                TSStatusCode.SEMANTIC_ERROR.getStatusCode())));
  }

  private static boolean isSameCategoryAndDataType(
      final TsTableColumnSchema left, final TsTableColumnSchema right) {
    return left.getColumnCategory() == right.getColumnCategory()
        && left.getDataType() == right.getDataType();
  }

  private static boolean isUnknownField(final TsTableColumnSchema schema) {
    return schema instanceof FieldColumnSchema && schema.getDataType() == TSDataType.UNKNOWN;
  }

  private static TsTableColumnSchema copySourceColumnSchemaForView(
      final TsTableColumnSchema requestedViewColumn,
      final TsTableColumnSchema sourceColumn,
      final String sourceColumnName) {
    // Keep source encoding/compressor/category/type authoritative. Only the exposed view name and
    // explicit view comment come from the ADD COLUMN statement.
    final TsTableColumnSchema viewColumn =
        sourceColumn.copy().setColumnName(requestedViewColumn.getColumnName());
    setViewColumnSourceName(viewColumn, sourceColumnName);
    applyExplicitComment(requestedViewColumn, viewColumn);
    return viewColumn;
  }

  private static TsTableColumnSchema prepareViewColumnSchema(
      final TsTableColumnSchema viewColumn, final String sourceColumnName) {
    final TsTableColumnSchema schema = viewColumn.copy();
    setViewColumnSourceName(schema, sourceColumnName);
    return schema;
  }

  private static TsTableColumnSchema toSourceColumnSchema(
      final TsTableColumnSchema viewColumn, final String sourceColumnName) {
    final TsTableColumnSchema sourceColumn = viewColumn.copy().setColumnName(sourceColumnName);
    // The source table stores its own column name; the temporary view-side source marker must not
    // be written into the source table schema.
    sourceColumn.getProps().remove(ViewColumnSchemaUtils.SOURCE_NAME);
    return sourceColumn;
  }

  private static void setViewColumnSourceName(
      final TsTableColumnSchema viewColumn, final String sourceColumnName) {
    // Persist the displayed-column to source-column alias in the shared view metadata so
    // describe/show-create logic stays consistent.
    if (!sourceColumnName.equals(viewColumn.getColumnName())) {
      ViewColumnSchemaUtils.setSourceName(viewColumn, sourceColumnName);
    } else {
      viewColumn.getProps().remove(ViewColumnSchemaUtils.SOURCE_NAME);
    }
  }

  private static void applyExplicitComment(
      final TsTableColumnSchema requestedColumn, final TsTableColumnSchema targetColumn) {
    final String explicitComment = requestedColumn.getProps().get(TsTable.COMMENT_KEY);
    if (Objects.nonNull(explicitComment)) {
      targetColumn.getProps().put(TsTable.COMMENT_KEY, explicitComment);
    }
  }

  private boolean validateNoDuplicateSourceColumnMapping(final WritableView writableView) {
    final Map<String, String> sourceColumnToViewColumnMap = new HashMap<>();
    for (final TsTableColumnSchema columnSchema : writableView.getColumnList()) {
      final String viewColumnName = columnSchema.getColumnName();
      final String sourceColumnName = writableView.getOriginalColumnName(viewColumnName);
      final String previousViewColumn =
          sourceColumnToViewColumnMap.put(sourceColumnName, viewColumnName);
      if (Objects.nonNull(previousViewColumn)
          && !Objects.equals(previousViewColumn, viewColumnName)) {
        setDuplicateSourceColumnFailure(previousViewColumn, viewColumnName, sourceColumnName);
        return false;
      }
    }
    for (final TsTableColumnSchema addedColumnSchema : addedColumnList) {
      final String viewColumnName = addedColumnSchema.getColumnName();
      final String sourceColumnName = ViewColumnSchemaUtils.getSourceName(addedColumnSchema);
      final String previousViewColumn =
          sourceColumnToViewColumnMap.put(sourceColumnName, viewColumnName);
      if (Objects.nonNull(previousViewColumn)
          && !Objects.equals(previousViewColumn, viewColumnName)) {
        setDuplicateSourceColumnFailure(previousViewColumn, viewColumnName, sourceColumnName);
        return false;
      }
    }
    return true;
  }

  private void setDuplicateSourceColumnFailure(
      final String previousViewColumn, final String viewColumnName, final String sourceColumnName) {
    setFailure(
        new ProcedureException(
            new IoTDBException(
                String.format(
                    ProcedureMessages.VIEW_COLUMNS_CANNOT_FROM_SAME_SOURCE_COLUMN,
                    previousViewColumn,
                    viewColumnName,
                    sourceColumnName),
                TSStatusCode.SEMANTIC_ERROR.getStatusCode())));
  }

  @Override
  protected void addColumn(final ConfigNodeProcedureEnv env) {
    final List<TsTableColumnSchema> originalColumnSchemas =
        Objects.nonNull(originalAddedColumnList)
            ? originalAddedColumnList
            : Collections.emptyList();
    final boolean needOriginal =
        Objects.nonNull(getOriginalDatabase()) && !originalColumnSchemas.isEmpty();
    final TSStatus status =
        env.getConfigManager()
            .getClusterSchemaManager()
            .executePlan(
                new AddWritableViewColumnPlan(
                    database,
                    tableName,
                    addedColumnList,
                    false,
                    needOriginal ? getOriginalDatabase() : null,
                    needOriginal ? getOriginalTableName() : null,
                    originalColumnSchemas),
                isGeneratedByPipe);
    if (status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      setFailure(new ProcedureException(new IoTDBException(status)));
    } else {
      setNextState(AddTableColumnState.COMMIT_RELEASE);
    }
  }

  @Override
  protected void rollbackAddColumn(final ConfigNodeProcedureEnv env) {
    if (table == null) {
      return;
    }

    final List<TsTableColumnSchema> originalColumnSchemas =
        Objects.nonNull(originalAddedColumnList)
            ? originalAddedColumnList
            : Collections.emptyList();
    final boolean needOriginal =
        Objects.nonNull(getOriginalDatabase()) && !originalColumnSchemas.isEmpty();
    final TSStatus status =
        env.getConfigManager()
            .getClusterSchemaManager()
            .executePlan(
                new AddWritableViewColumnPlan(
                    database,
                    tableName,
                    addedColumnList,
                    true,
                    needOriginal ? getOriginalDatabase() : null,
                    needOriginal ? getOriginalTableName() : null,
                    originalColumnSchemas),
                isGeneratedByPipe);
    if (status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      setFailure(new ProcedureException(new IoTDBException(status)));
    }
  }

  @Override
  protected String getActionMessage() {
    return String.format(
        ProcedureMessages.ALTER_WRITABLE_VIEW_COLUMN_ON,
        database,
        tableName,
        getAddedColumnNames());
  }

  private String getAddedColumnNames() {
    if (Objects.isNull(addedColumnList) || addedColumnList.isEmpty()) {
      return ProcedureMessages.UNKNOWN_COLUMNS;
    }
    return addedColumnList.stream()
        .map(TsTableColumnSchema::getColumnName)
        .collect(Collectors.joining(", "));
  }

  @Override
  public void serialize(final DataOutputStream stream) throws IOException {
    stream.writeShort(
        isGeneratedByPipe
            ? ProcedureType.PIPE_ENRICHED_ADD_WRITABLE_VIEW_COLUMN_PROCEDURE.getTypeCode()
            : ProcedureType.ADD_WRITABLE_VIEW_COLUMN_PROCEDURE.getTypeCode());
    innerSerialize(stream);
  }

  @Override
  protected void innerSerialize(final DataOutputStream stream) throws IOException {
    super.innerSerialize(stream);
    TsTableColumnSchemaUtil.serialize(originalAddedColumnList, stream);
  }

  @Override
  public void deserialize(final ByteBuffer byteBuffer) {
    super.deserialize(byteBuffer);
    this.originalAddedColumnList = TsTableColumnSchemaUtil.deserializeColumnSchemaList(byteBuffer);
  }

  @Override
  public boolean equals(final Object o) {
    return super.equals(o)
        && Objects.equals(
            originalAddedColumnList, ((AddWritableViewColumnProcedure) o).originalAddedColumnList);
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), originalAddedColumnList);
  }
}
