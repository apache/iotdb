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

package org.apache.iotdb.db.queryengine.plan.relational.metadata.fetcher;

import org.apache.iotdb.commons.exception.IoTDBException;
import org.apache.iotdb.commons.exception.IoTDBRuntimeException;
import org.apache.iotdb.commons.exception.MetadataException;
import org.apache.iotdb.commons.schema.table.InsertNodeMeasurementInfo;
import org.apache.iotdb.commons.schema.table.TreeViewSchema;
import org.apache.iotdb.commons.schema.table.TsTable;
import org.apache.iotdb.commons.schema.table.column.AttributeColumnSchema;
import org.apache.iotdb.commons.schema.table.column.FieldColumnSchema;
import org.apache.iotdb.commons.schema.table.column.TagColumnSchema;
import org.apache.iotdb.commons.schema.table.column.TsTableColumnCategory;
import org.apache.iotdb.commons.schema.table.column.TsTableColumnSchema;
import org.apache.iotdb.db.auth.AuthorityChecker;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.load.LoadAnalyzeTableColumnDisorderException;
import org.apache.iotdb.db.exception.sql.ColumnCreationFailException;
import org.apache.iotdb.db.exception.sql.SemanticException;
import org.apache.iotdb.db.queryengine.common.MPPQueryContext;
import org.apache.iotdb.db.queryengine.plan.analyze.lock.DataNodeSchemaLockManager;
import org.apache.iotdb.db.queryengine.plan.analyze.lock.SchemaLockType;
import org.apache.iotdb.db.queryengine.plan.execution.config.ConfigTaskResult;
import org.apache.iotdb.db.queryengine.plan.execution.config.executor.ClusterConfigTaskExecutor;
import org.apache.iotdb.db.queryengine.plan.execution.config.metadata.relational.AlterTableAddColumnTask;
import org.apache.iotdb.db.queryengine.plan.execution.config.metadata.relational.CreateTableTask;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.ColumnSchema;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.QualifiedObjectName;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.TableMetadataImpl;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.TableSchema;
import org.apache.iotdb.db.queryengine.plan.relational.type.InternalTypeManager;
import org.apache.iotdb.db.schemaengine.table.DataNodeTableCache;
import org.apache.iotdb.db.schemaengine.table.DataNodeTreeViewSchemaUtils;
import org.apache.iotdb.rpc.TSStatusCode;

import com.google.common.util.concurrent.ListenableFuture;
import org.apache.tsfile.common.conf.TSFileDescriptor;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.type.StringType;
import org.apache.tsfile.read.common.type.TypeFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import static org.apache.iotdb.db.queryengine.plan.relational.type.InternalTypeManager.getTSDataType;
import static org.apache.iotdb.db.utils.EncodingInferenceUtils.getDefaultEncoding;

public class TableHeaderSchemaValidator {

  private static final Logger LOGGER = LoggerFactory.getLogger(TableHeaderSchemaValidator.class);

  private final ClusterConfigTaskExecutor configTaskExecutor =
      ClusterConfigTaskExecutor.getInstance();

  private TableHeaderSchemaValidator() {
    // do nothing
  }

  private static class TableHeaderSchemaValidatorHolder {

    private static final TableHeaderSchemaValidator INSTANCE = new TableHeaderSchemaValidator();
  }

  public static TableHeaderSchemaValidator getInstance() {
    return TableHeaderSchemaValidatorHolder.INSTANCE;
  }

  public Optional<TableSchema> validateTableHeaderSchema(
      final String database,
      final TableSchema tableSchema,
      final MPPQueryContext context,
      final boolean allowCreateTable,
      final boolean isStrictTagColumn)
      throws LoadAnalyzeTableColumnDisorderException {
    // The schema cache R/W and fetch operation must be locked together thus the cache clean
    // operation executed by delete timeSeries will be effective.
    DataNodeSchemaLockManager.getInstance()
        .takeReadLock(context, SchemaLockType.VALIDATE_VS_DELETION_TABLE);

    final List<ColumnSchema> inputColumnList = tableSchema.getColumns();
    if (inputColumnList == null || inputColumnList.isEmpty()) {
      throw new SemanticException("No column other than Time present, please check the request");
    }
    // Get directly if there is a table because we do not want "addColumn" to affect
    // original writings
    TsTable table =
        DataNodeTableCache.getInstance().getTableInWrite(database, tableSchema.getTableName());
    final List<ColumnSchema> missingColumnList = new ArrayList<>();

    final boolean isAutoCreateSchemaEnabled =
        IoTDBDescriptor.getInstance().getConfig().isAutoCreateSchemaEnabled();
    // first round validate, check existing schema
    if (table == null) {
      // auto create missing table
      // it's ok that many write requests concurrently auto create same table, the thread safety
      // will be guaranteed by ProcedureManager.createTable in CN
      if (allowCreateTable && isAutoCreateSchemaEnabled) {
        autoCreateTable(context, database, tableSchema);
        table =
            DataNodeTableCache.getInstance().getTable(database, tableSchema.getTableName(), false);
        if (table == null) {
          throw new IllegalStateException(
              "auto create table succeed, but cannot get table schema in current node's DataNodeTableCache, may be caused by concurrently auto creating table");
        }
      } else {
        TableMetadataImpl.throwTableNotExistsException(database, tableSchema.getTableName());
      }
    } else {
      DataNodeTreeViewSchemaUtils.checkTableInWrite(database, table);
      // If table with this name already exists and isStrictTagColumn is true, make sure the
      // existing
      // id columns are the prefix of the incoming tag columns, or vice versa
      if (isStrictTagColumn) {
        final List<TsTableColumnSchema> realTagColumns = table.getTagColumnSchemaList();
        final List<ColumnSchema> incomingTagColumns = tableSchema.getTagColumns();
        if (realTagColumns.size() <= incomingTagColumns.size()) {
          // When incoming table has more TAG columns, the existing tag columns
          // should be the prefix of the incoming tag columns (or equal)
          for (int indexReal = 0; indexReal < realTagColumns.size(); indexReal++) {
            final String tagName = realTagColumns.get(indexReal).getColumnName();
            final int indexIncoming = tableSchema.getIndexAmongTagColumns(tagName);
            if (indexIncoming != indexReal) {
              throw new LoadAnalyzeTableColumnDisorderException(
                  String.format(
                      "Can not create table because incoming table has no less tag columns than existing table, "
                          + "and the existing tag columns are not the prefix of the incoming tag columns. "
                          + "Existing tag column: %s, index in existing table: %s, index in incoming table: %s",
                      tagName, indexReal, indexIncoming));
            }
          }
        } else {
          // When existing table has more TAG columns, the incoming tag columns
          // should be the prefix of the existing tag columns
          for (int indexIncoming = 0; indexIncoming < incomingTagColumns.size(); indexIncoming++) {
            final String tagName = incomingTagColumns.get(indexIncoming).getName();
            final int indexReal = table.getTagColumnOrdinal(tagName);
            if (indexReal != indexIncoming) {
              throw new LoadAnalyzeTableColumnDisorderException(
                  String.format(
                      "Can not create table because existing table has more tag columns than incoming table, "
                          + "and the incoming tag columns are not the prefix of the existing tag columns. "
                          + "Incoming tag column: %s, index in existing table: %s, index in incoming table: %s",
                      tagName, indexReal, indexIncoming));
            }
          }
        }
      }
    }

    boolean refreshed = false;
    boolean noField = true;
    for (final ColumnSchema columnSchema : inputColumnList) {
      TsTableColumnSchema existingColumn = table.getColumnSchema(columnSchema.getName());
      if (Objects.isNull(existingColumn)) {
        if (!refreshed) {
          // Refresh because there may be new columns added and failed to commit
          // Allow refresh only once to avoid too much failure columns in sql when there are column
          // procedures
          refreshed = true;
          table = DataNodeTableCache.getInstance().getTable(database, tableSchema.getTableName());
          existingColumn = table.getColumnSchema(columnSchema.getName());
        }
        if (Objects.isNull(existingColumn)) {
          // check arguments for column auto creation
          if (columnSchema.getColumnCategory() == null) {
            throw new SemanticException(
                String.format(
                    "Unknown column category for %s. Cannot auto create column.",
                    columnSchema.getName()),
                TSStatusCode.COLUMN_NOT_EXISTS.getStatusCode());
          }
          if (columnSchema.getType() == null) {
            throw new SemanticException(
                String.format(
                    "Unknown column data type for %s. Cannot auto create column.",
                    columnSchema.getName()),
                TSStatusCode.COLUMN_NOT_EXISTS.getStatusCode());
          }
          missingColumnList.add(columnSchema);
        }
        if (noField
            && columnSchema.getColumnCategory() != null
            && columnSchema.getColumnCategory() == TsTableColumnCategory.FIELD) {
          noField = false;
        }
      } else {
        // leave measurement columns' dataType checking to the caller, then the caller can decide
        // whether to do partial insert

        // only check column category
        if (columnSchema.getColumnCategory() != null
            && !existingColumn.getColumnCategory().equals(columnSchema.getColumnCategory())) {
          throw new SemanticException(
              String.format("Wrong category at column %s.", columnSchema.getName()),
              TSStatusCode.COLUMN_CATEGORY_MISMATCH.getStatusCode());
        }
        if (noField && existingColumn.getColumnCategory() == TsTableColumnCategory.FIELD) {
          noField = false;
        }
      }
    }
    if (noField) {
      throw new SemanticException("No Field column present, please check the request");
    }

    final List<ColumnSchema> resultColumnList = new ArrayList<>();
    if (!missingColumnList.isEmpty() && isAutoCreateSchemaEnabled) {
      // TODO table metadata: authority check for table alter
      // check id or attribute column data type in this method
      autoCreateColumn(database, tableSchema.getTableName(), missingColumnList, context);
      table = DataNodeTableCache.getInstance().getTable(database, tableSchema.getTableName());
    } else if (!missingColumnList.isEmpty()
        && !IoTDBDescriptor.getInstance().getConfig().isEnablePartialInsert()) {
      throw new SemanticException(
          String.format(
              "Missing columns %s.",
              missingColumnList.stream().map(ColumnSchema::getName).collect(Collectors.toList())),
          TSStatusCode.COLUMN_NOT_EXISTS.getStatusCode());
    }

    table
        .getColumnList()
        .forEach(
            o ->
                resultColumnList.add(
                    new ColumnSchema(
                        o.getColumnName(),
                        TypeFactory.getType(o.getDataType()),
                        false,
                        o.getColumnCategory())));
    return Optional.of(new TableSchema(tableSchema.getTableName(), resultColumnList));
  }

  /** Handler for validating and processing a single measurement column */
  @FunctionalInterface
  public interface MeasurementValidator {
    /**
     * Validate a measurement column
     *
     * @param index measurement index in the array
     * @param columnCategory column category
     * @param existingColumn existing column in table, null if not exists
     */
    void validate(
        final int index,
        final String measurement,
        final TSDataType dataType,
        final TsTableColumnCategory columnCategory,
        final TsTableColumnSchema existingColumn);
  }

  /** Handler for processing TAG columns during validation */
  @FunctionalInterface
  public interface TagColumnHandler {
    /**
     * Adjust the order of TAG columns in this insertion to be consistent with that from the schema
     * region, similar to adjustIdColumns logic.
     *
     * @param tagColumnIndexMap LinkedHashMap of incoming TAG columns, key is column name, value is
     *     measurement index in the array (maintains insertion order)
     * @param existingTagColumnIndexMap LinkedHashMap of existing TAG columns in TsTable, key is
     *     column name, value is TAG column index in table (maintains schema region order)
     */
    void handle(
        LinkedHashMap<String, Integer> tagColumnIndexMap,
        LinkedHashMap<String, Integer> existingTagColumnIndexMap);
  }

  /**
   * Optimized validation method with custom handlers for measurement validation and TAG column
   * processing
   *
   * @param database database name
   * @param measurementInfo measurement information from InsertNode
   * @param context query context
   * @param allowCreateTable whether to allow table auto-creation
   * @param measurementValidator custom validator for each measurement, null to use default
   * @param tagColumnHandler custom handler for TAG columns, null to skip TAG processing
   * @return validated TsTable, or empty if table doesn't exist and cannot be created
   */
  public void validateInsertNodeMeasurements(
      final String database,
      final InsertNodeMeasurementInfo measurementInfo,
      final MPPQueryContext context,
      final boolean allowCreateTable,
      final MeasurementValidator measurementValidator,
      final TagColumnHandler tagColumnHandler) {

    DataNodeSchemaLockManager.getInstance()
        .takeReadLock(context, SchemaLockType.VALIDATE_VS_DELETION_TABLE);

    final TsTableColumnCategory[] columnCategories = measurementInfo.getColumnCategories();
    final int measurementCount = measurementInfo.getMeasurementCount();

    if (measurementCount == 0) {
      throw new SemanticException("No column other than Time present, please check the request");
    }

    TsTable table =
        DataNodeTableCache.getInstance().getTableInWrite(database, measurementInfo.getTableName());
    final List<Integer> missingMeasurementIndices = new ArrayList<>();

    final boolean isAutoCreateSchemaEnabled =
        IoTDBDescriptor.getInstance().getConfig().isAutoCreateSchemaEnabled();

    // First round validate, check existing schema
    if (table == null) {
      // Auto create missing table
      if (allowCreateTable && isAutoCreateSchemaEnabled) {
        measurementInfo.toLowerCase();
        measurementInfo.semanticCheck();
        autoCreateTableFromMeasurementInfo(context, database, measurementInfo);
        table =
            DataNodeTableCache.getInstance()
                .getTable(database, measurementInfo.getTableName(), false);
        if (table == null) {
          throw new IllegalStateException(
              "auto create table succeed, but cannot get table schema in current node's DataNodeTableCache, may be caused by concurrently auto creating table");
        }
      } else {
        TableMetadataImpl.throwTableNotExistsException(database, measurementInfo.getTableName());
      }
    } else {
      DataNodeTreeViewSchemaUtils.checkTableInWrite(database, table);
      // Note: isStrictTagColumn is always false for InsertNode, so TAG column order validation is
      // skipped
    }

    boolean refreshed = false;
    boolean noField = true;
    boolean hasAttribute = false;

    // Track TAG column measurement indices for batch processing after validation loop
    // LinkedHashMap maintains insertion order, key is column name, value is measurement index
    final LinkedHashMap<String, Integer> tagColumnIndexMap = new LinkedHashMap<>();

    // Validate each measurement
    for (int i = 0; i < measurementCount; i++) {
      String measurementName = measurementInfo.getMeasurementName(i);
      if (measurementName == null) {
        continue;
      }

      final TsTableColumnCategory category =
          columnCategories != null && i < columnCategories.length ? columnCategories[i] : null;

      hasAttribute = hasAttribute || category == TsTableColumnCategory.ATTRIBUTE;

      TsTableColumnSchema existingColumn = table.getColumnSchema(measurementName);
      if (existingColumn == null) {
        measurementInfo.toLowerCase();
        measurementInfo.semanticCheck();
        measurementName = measurementInfo.getMeasurementName(i);
        existingColumn = table.getColumnSchema(measurementName);
      }

      if (Objects.isNull(existingColumn)) {
        if (!refreshed) {
          // Refresh because there may be new columns added and failed to commit
          refreshed = true;
          table =
              DataNodeTableCache.getInstance().getTable(database, measurementInfo.getTableName());
          existingColumn = table.getColumnSchema(measurementName);
        }

        if (Objects.isNull(existingColumn)) {
          // Check arguments for column auto creation
          if (category == null) {
            throw new SemanticException(
                String.format(
                    "Unknown column category for %s. Cannot auto create column.", measurementName),
                TSStatusCode.COLUMN_NOT_EXISTS.getStatusCode());
          }
          missingMeasurementIndices.add(i);
        } else {
          // Custom validation handler - get MeasurementSchema on demand
          if (measurementValidator != null) {
            measurementValidator.validate(
                i, measurementName, measurementInfo.getType(i), category, existingColumn);
          }
        }

        if (noField && category == TsTableColumnCategory.FIELD) {
          noField = false;
        }
      } else {
        // Only check column category
        if (category != null && !existingColumn.getColumnCategory().equals(category)) {
          throw new SemanticException(
              String.format("Wrong category at column %s.", measurementName),
              TSStatusCode.COLUMN_CATEGORY_MISMATCH.getStatusCode());
        }
        if (noField && existingColumn.getColumnCategory() == TsTableColumnCategory.FIELD) {
          noField = false;
        }

        hasAttribute =
            hasAttribute || existingColumn.getColumnCategory() == TsTableColumnCategory.ATTRIBUTE;

        // Custom validation handler - get MeasurementSchema on demand
        if (measurementValidator != null) {
          measurementValidator.validate(
              i, measurementName, measurementInfo.getType(i), category, existingColumn);
        }
      }

      boolean isTagColumn =
          category == TsTableColumnCategory.TAG
              || (category == null
                  && existingColumn != null
                  && existingColumn.getColumnCategory() == TsTableColumnCategory.TAG);

      // Record TAG column measurement index during validation loop
      if (tagColumnHandler != null && isTagColumn) {
        tagColumnIndexMap.put(measurementName, i); // Store measurement index
      }
    }

    if (noField) {
      throw new SemanticException("No Field column present, please check the request");
    }

    measurementInfo.setAttributeColumnsPresent(hasAttribute);
    if (missingMeasurementIndices.isEmpty()) {
      measurementInfo.setToLowerCaseApplied(true);
    } else {
      measurementInfo.toLowerCase();
    }
    measurementInfo.semanticCheck();

    // Auto create missing columns
    if (!missingMeasurementIndices.isEmpty() && isAutoCreateSchemaEnabled) {
      autoCreateColumnsFromMeasurements(
          database, measurementInfo, missingMeasurementIndices, context);
      table = DataNodeTableCache.getInstance().getTable(database, measurementInfo.getTableName());
    } else if (!missingMeasurementIndices.isEmpty()
        && !IoTDBDescriptor.getInstance().getConfig().isEnablePartialInsert()) {
      final List<String> missingNames = new ArrayList<>();
      for (int idx : missingMeasurementIndices) {
        missingNames.add(measurementInfo.getMeasurementName(idx));
      }
      throw new SemanticException(
          String.format("Missing columns %s.", missingNames),
          TSStatusCode.COLUMN_NOT_EXISTS.getStatusCode());
    }

    for (int i : missingMeasurementIndices) {
      if (measurementValidator != null) {
        measurementValidator.validate(
            i,
            measurementInfo.getMeasurementName(i),
            measurementInfo.getType(i),
            columnCategories[i],
            table.getColumnSchema(measurementInfo.getMeasurementName(i)));
      }
    }

    // Handle TAG columns after validation loop
    if (tagColumnHandler != null) {
      final List<TsTableColumnSchema> existingTagColumns = table.getTagColumnSchemaList();
      // Build existing TAG column index map from TsTable
      final LinkedHashMap<String, Integer> existingTagColumnIndexMap =
          new LinkedHashMap<>(existingTagColumns.size());
      for (int i = 0; i < existingTagColumns.size(); i++) {
        existingTagColumnIndexMap.put(existingTagColumns.get(i).getColumnName(), i);
      }

      tagColumnHandler.handle(tagColumnIndexMap, existingTagColumnIndexMap);
    }
  }

  private void autoCreateTableFromMeasurementInfo(
      final MPPQueryContext context,
      final String database,
      final InsertNodeMeasurementInfo measurementInfo) {
    DataNodeSchemaLockManager.getInstance().releaseReadLock(context);
    final TsTable tsTable = toTsTable(measurementInfo);
    AuthorityChecker.getAccessControl()
        .checkCanCreateTable(
            context.getSession().getUserName(),
            new QualifiedObjectName(database, measurementInfo.getTableName()),
            context);
    final CreateTableTask createTableTask = new CreateTableTask(tsTable, database, true);
    try {
      final ListenableFuture<ConfigTaskResult> future = createTableTask.execute(configTaskExecutor);
      final ConfigTaskResult result = future.get();
      if (result.getStatusCode().getStatusCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        throw new RuntimeException(
            new IoTDBException(
                "Auto create table failed.", result.getStatusCode().getStatusCode()));
      }
      DataNodeSchemaLockManager.getInstance()
          .takeReadLock(context, SchemaLockType.VALIDATE_VS_DELETION_TABLE);
    } catch (final ExecutionException e) {
      throw new RuntimeException(e);
    } catch (final InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException(e);
    }
  }

  private void autoCreateColumnsFromMeasurements(
      final String database,
      final InsertNodeMeasurementInfo measurementInfo,
      final List<Integer> missingIndices,
      final MPPQueryContext context) {
    DataNodeSchemaLockManager.getInstance().releaseReadLock(context);
    AuthorityChecker.getAccessControl()
        .checkCanAlterTable(
            context.getSession().getUserName(),
            new QualifiedObjectName(database, measurementInfo.getTableName()),
            context);

    final TsTableColumnCategory[] columnCategories = measurementInfo.getColumnCategories();

    final List<TsTableColumnSchema> columnSchemaList = new ArrayList<>(missingIndices.size());
    for (int idx : missingIndices) {
      final String columnName = measurementInfo.getMeasurementName(idx);
      final TSDataType dataType = measurementInfo.getType(idx);
      final TsTableColumnCategory category =
          columnCategories != null && idx < columnCategories.length
              ? columnCategories[idx]
              : TsTableColumnCategory.FIELD;

      columnSchemaList.add(
          generateColumnSchema(
              category,
              columnName,
              dataType == null ? measurementInfo.getTypeForFirstValue(idx) : dataType,
              null,
              null));
    }

    final AlterTableAddColumnTask task =
        new AlterTableAddColumnTask(
            database,
            measurementInfo.getTableName(),
            columnSchemaList,
            context.getQueryId().getId(),
            true,
            true,
            false);
    try {
      final ListenableFuture<ConfigTaskResult> future = task.execute(configTaskExecutor);
      final ConfigTaskResult result = future.get();
      if (result.getStatusCode().getStatusCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        throw new RuntimeException(
            new IoTDBException(
                String.format(
                    "Auto add table column failed: %s.%s",
                    database, measurementInfo.getTableName()),
                result.getStatusCode().getStatusCode()));
      }
      DataNodeSchemaLockManager.getInstance()
          .takeReadLock(context, SchemaLockType.VALIDATE_VS_DELETION_TABLE);
    } catch (final ExecutionException | InterruptedException e) {
      LOGGER.warn("Auto add table column failed.", e);
      throw new RuntimeException(e);
    }
  }

  private void autoCreateTable(
      final MPPQueryContext context, final String database, final TableSchema tableSchema) {
    // Release to avoid deadlock
    DataNodeSchemaLockManager.getInstance().releaseReadLock(context);
    final TsTable tsTable = new TsTable(tableSchema.getTableName());
    addColumnSchema(tableSchema.getColumns(), tsTable);
    AuthorityChecker.getAccessControl()
        .checkCanCreateTable(
            context.getSession().getUserName(),
            new QualifiedObjectName(database, tableSchema.getTableName()),
            context);
    final CreateTableTask createTableTask = new CreateTableTask(tsTable, database, true);
    try {
      final ListenableFuture<ConfigTaskResult> future = createTableTask.execute(configTaskExecutor);
      final ConfigTaskResult result = future.get();
      if (result.getStatusCode().getStatusCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        throw new RuntimeException(
            new IoTDBException(
                "Auto create table column failed.", result.getStatusCode().getStatusCode()));
      }
      DataNodeSchemaLockManager.getInstance()
          .takeReadLock(context, SchemaLockType.VALIDATE_VS_DELETION_TABLE);
    } catch (final ExecutionException e) {
      throw new RuntimeException(e);
    } catch (final InterruptedException e) {
      /* Clean up whatever needs to be handled before interrupting  */
      Thread.currentThread().interrupt();
      throw new RuntimeException(e);
    }
  }

  /**
   * Convert to TsTable object
   *
   * @return converted TsTable object
   */
  public TsTable toTsTable(InsertNodeMeasurementInfo measurementInfo) {
    final TsTable tsTable = new TsTable(measurementInfo.getTableName());
    final String[] measurements = measurementInfo.getMeasurements();
    final TsTableColumnCategory[] columnCategories = measurementInfo.getColumnCategories();

    if (measurements == null || measurements.length == 0) {
      return tsTable;
    }

    boolean hasObject = false;
    for (int i = 0; i < measurements.length; i++) {
      if (measurements[i] == null) {
        continue;
      }

      final String columnName = measurements[i];
      // Determine column category
      final TsTableColumnCategory category =
          measurementInfo.getColumnCategories() != null
                  && i < columnCategories.length
                  && columnCategories[i] != null
              ? columnCategories[i]
              : null;
      if (category == null) {
        throw new ColumnCreationFailException(
            "Cannot create column " + columnName + " category is not provided");
      }

      if (tsTable.getColumnSchema(columnName) != null) {
        throw new SemanticException(
            String.format("Columns in table shall not share the same name %s.", columnName));
      }

      TSDataType dataType = measurementInfo.getType(i);
      if (dataType == null && (dataType = measurementInfo.getTypeForFirstValue(i)) == null) {
        throw new ColumnCreationFailException(
            "Cannot create column " + columnName + " datatype is not provided");
      }
      hasObject |= dataType == TSDataType.OBJECT;
      tsTable.addColumnSchema(generateColumnSchema(category, columnName, dataType, null, null));
    }
    if (hasObject) {
      try {
        tsTable.checkTableNameAndObjectNames4Object();
      } catch (final MetadataException e) {
        throw new IoTDBRuntimeException(
            e.getMessage(), TSStatusCode.SEMANTIC_ERROR.getStatusCode());
      }
    }
    return tsTable;
  }

  private void addColumnSchema(final List<ColumnSchema> columnSchemas, final TsTable tsTable) {
    for (final ColumnSchema columnSchema : columnSchemas) {
      TsTableColumnCategory category = columnSchema.getColumnCategory();
      if (category == null) {
        throw new ColumnCreationFailException(
            "Cannot create column " + columnSchema.getName() + " category is not provided");
      }
      final String columnName = columnSchema.getName();
      if (tsTable.getColumnSchema(columnName) != null) {
        throw new SemanticException(
            String.format("Columns in table shall not share the same name %s.", columnName));
      }
      final TSDataType dataType = getTSDataType(columnSchema.getType());
      if (dataType == null) {
        throw new ColumnCreationFailException(
            "Cannot create column " + columnSchema.getName() + " datatype is not provided");
      }
      tsTable.addColumnSchema(generateColumnSchema(category, columnName, dataType, null, null));
    }
  }

  public static TsTableColumnSchema generateColumnSchema(
      final TsTableColumnCategory category,
      final String columnName,
      final TSDataType dataType,
      final @Nullable String comment,
      final String from) {
    final TsTableColumnSchema schema;
    switch (category) {
      case TAG:
        if (!TSDataType.STRING.equals(dataType)) {
          throw new SemanticException(
              "DataType of TAG Column should only be STRING, current is " + dataType);
        }
        schema = new TagColumnSchema(columnName, dataType);
        break;
      case ATTRIBUTE:
        if (!TSDataType.STRING.equals(dataType)) {
          throw new SemanticException(
              "DataType of ATTRIBUTE Column should only be STRING, current is " + dataType);
        }
        schema = new AttributeColumnSchema(columnName, dataType);
        break;
      case TIME:
        throw new SemanticException(
            "Create table or add column statement shall not specify column category TIME");
      case FIELD:
        schema =
            dataType != TSDataType.UNKNOWN
                ? new FieldColumnSchema(
                    columnName,
                    dataType,
                    getDefaultEncoding(dataType),
                    TSFileDescriptor.getInstance().getConfig().getCompressor(dataType))
                // Unknown appears only for tree view field when the type needs auto-detection
                // Skip encoding & compressors because view query does not need these
                : new FieldColumnSchema(columnName, dataType);
        if (Objects.nonNull(from)) {
          TreeViewSchema.setOriginalName(schema, from);
        }
        break;
      default:
        throw new IllegalArgumentException();
    }
    if (Objects.nonNull(comment)) {
      schema.getProps().put(TsTable.COMMENT_KEY, comment);
    }
    return schema;
  }

  private void autoCreateColumn(
      final String database,
      final String tableName,
      final List<ColumnSchema> inputColumnList,
      final MPPQueryContext context) {
    DataNodeSchemaLockManager.getInstance().releaseReadLock(context);
    AuthorityChecker.getAccessControl()
        .checkCanAlterTable(
            context.getSession().getUserName(),
            new QualifiedObjectName(database, tableName),
            context);
    final AlterTableAddColumnTask task =
        new AlterTableAddColumnTask(
            database,
            tableName,
            parseInputColumnSchema(inputColumnList),
            context.getQueryId().getId(),
            true,
            true,
            false);
    try {
      final ListenableFuture<ConfigTaskResult> future = task.execute(configTaskExecutor);
      final ConfigTaskResult result = future.get();
      if (result.getStatusCode().getStatusCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        throw new RuntimeException(
            new IoTDBException(
                String.format(
                    "Auto add table column failed: %s.%s, %s",
                    database, tableName, inputColumnList),
                result.getStatusCode().getStatusCode()));
      }
      DataNodeSchemaLockManager.getInstance()
          .takeReadLock(context, SchemaLockType.VALIDATE_VS_DELETION_TABLE);
    } catch (final ExecutionException | InterruptedException e) {
      LOGGER.warn("Auto add table column failed.", e);
      throw new RuntimeException(e);
    }
  }

  private List<TsTableColumnSchema> parseInputColumnSchema(
      final List<ColumnSchema> inputColumnList) {
    final List<TsTableColumnSchema> columnSchemaList = new ArrayList<>(inputColumnList.size());
    for (final ColumnSchema inputColumn : inputColumnList) {
      switch (inputColumn.getColumnCategory()) {
        case TAG:
          if (!inputColumn.getType().equals(StringType.STRING)) {
            throw new SemanticException("Tag column only support data type STRING.");
          }
          columnSchemaList.add(new TagColumnSchema(inputColumn.getName(), TSDataType.STRING));
          break;
        case ATTRIBUTE:
          if (!inputColumn.getType().equals(StringType.STRING)) {
            throw new SemanticException("Attribute column only support data type STRING.");
          }
          columnSchemaList.add(new AttributeColumnSchema(inputColumn.getName(), TSDataType.STRING));
          break;
        case FIELD:
          final TSDataType dataType = InternalTypeManager.getTSDataType(inputColumn.getType());
          columnSchemaList.add(
              new FieldColumnSchema(
                  inputColumn.getName(),
                  dataType,
                  getDefaultEncoding(dataType),
                  TSFileDescriptor.getInstance().getConfig().getCompressor(dataType)));
          break;
        case TIME:
          throw new SemanticException(
              "Adding column for column category "
                  + inputColumn.getColumnCategory()
                  + " is not supported");
        default:
          throw new IllegalStateException(
              "Unknown ColumnCategory for adding column: " + inputColumn.getColumnCategory());
      }
    }
    return columnSchemaList;
  }
}
