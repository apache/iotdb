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

package org.apache.iotdb.db.queryengine.plan.relational.sql.ast;

import org.apache.iotdb.commons.schema.table.InsertNodeMeasurementInfo;
import org.apache.iotdb.commons.schema.table.column.TsTableColumnCategory;
import org.apache.iotdb.commons.schema.table.column.TsTableColumnSchema;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.metadata.DataTypeMismatchException;
import org.apache.iotdb.db.exception.metadata.PathNotExistException;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.exception.sql.SemanticException;
import org.apache.iotdb.db.queryengine.common.MPPQueryContext;
import org.apache.iotdb.db.queryengine.plan.analyze.AnalyzeUtils;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.ColumnSchema;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.ITableDeviceSchemaValidation;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.Metadata;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.TableSchema;
import org.apache.iotdb.db.queryengine.plan.statement.crud.InsertBaseStatement;
import org.apache.iotdb.db.queryengine.plan.statement.crud.InsertRowStatement;
import org.apache.iotdb.db.utils.TypeInferenceUtils;
import org.apache.iotdb.rpc.TSStatusCode;

import org.apache.tsfile.common.conf.TSFileDescriptor;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.type.TypeFactory;
import org.apache.tsfile.write.schema.MeasurementSchema;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.iotdb.db.queryengine.plan.execution.config.TableConfigTaskVisitor.DATABASE_NOT_SPECIFIED;
import static org.apache.iotdb.db.utils.EncodingInferenceUtils.getDefaultEncoding;

public abstract class WrappedInsertStatement extends WrappedStatement
    implements ITableDeviceSchemaValidation {

  protected TableSchema tableSchema;

  protected WrappedInsertStatement(
      InsertBaseStatement innerTreeStatement, MPPQueryContext context) {
    super(innerTreeStatement, context);
  }

  @Override
  public InsertBaseStatement getInnerTreeStatement() {
    return ((InsertBaseStatement) super.getInnerTreeStatement());
  }

  public abstract void updateAfterSchemaValidation(MPPQueryContext context)
      throws QueryProcessException;

  public TableSchema getTableSchema() {
    if (tableSchema == null) {
      InsertBaseStatement insertBaseStatement = getInnerTreeStatement();
      tableSchema = toTableSchema(insertBaseStatement);
    }

    return tableSchema;
  }

  public InsertNodeMeasurementInfo getInsertNodeMeasurementInfo() {
    InsertBaseStatement insertBaseStatement = getInnerTreeStatement();
    return toInsertNodeMeasurementInfo(insertBaseStatement);
  }

  protected TableSchema toTableSchema(InsertBaseStatement insertBaseStatement) {
    String tableName = insertBaseStatement.getDevicePath().getFullPath();
    List<ColumnSchema> columnSchemas =
        new ArrayList<>(insertBaseStatement.getMeasurements().length);
    for (int i = 0; i < insertBaseStatement.getMeasurements().length; i++) {
      if (insertBaseStatement.getMeasurements()[i] != null) {
        TSDataType dataType = insertBaseStatement.getDataType(i);
        if (dataType == null) {
          dataType =
              TypeInferenceUtils.getPredictedDataType(
                  insertBaseStatement.getFirstValueOfIndex(i), true);
        }
        columnSchemas.add(
            new ColumnSchema(
                insertBaseStatement.getMeasurements()[i],
                dataType != null ? TypeFactory.getType(dataType) : null,
                false,
                insertBaseStatement.getColumnCategory(i)));
      } else {
        columnSchemas.add(null);
      }
    }
    return new TableSchema(tableName, columnSchemas);
  }

  protected InsertNodeMeasurementInfo toInsertNodeMeasurementInfo(
      InsertBaseStatement insertBaseStatement) {
    String tableName = insertBaseStatement.getDevicePath().getFullPath().toLowerCase();

    // Use lazy initialization with measurements and dataTypes
    return new InsertNodeMeasurementInfo(
        tableName,
        insertBaseStatement.getColumnCategories(),
        insertBaseStatement.getMeasurements(),
        insertBaseStatement.getDataTypes(),
        index ->
            TypeInferenceUtils.getPredictedDataType(
                insertBaseStatement.getFirstValueOfIndex(index), true),
        insertBaseStatement::toLowerCase,
        insertBaseStatement::semanticCheck,
        insertBaseStatement::setAttributeColumnsPresent,
        insertBaseStatement::setToLowerCaseApplied,
        insertBaseStatement::setSemanticChecked);
  }

  public void validateTableSchema(Metadata metadata, MPPQueryContext context) {
    String databaseName = getDatabase();
    metadata.validateInsertNodeMeasurements(
        databaseName,
        getInsertNodeMeasurementInfo(),
        context,
        true,
        this::validate,
        this::adjustTagColumns);
  }

  public void validateTableSchema(
      final Metadata metadata,
      final MPPQueryContext context,
      final InsertRowStatement insertRowStatement,
      final String databaseName,
      final boolean allowCreateTable) {
    metadata.validateInsertNodeMeasurements(
        databaseName,
        toInsertNodeMeasurementInfo(insertRowStatement),
        context,
        allowCreateTable,
        (index, measurement, dataType, columnCategory, existingColumn) ->
            validate(
                index, insertRowStatement, measurement, dataType, columnCategory, existingColumn),
        (tagColumnIndexMap, existingTagColumnIndexMap) ->
            adjustTagColumns(insertRowStatement, tagColumnIndexMap, existingTagColumnIndexMap));
  }

  private void validate(
      final int index,
      final String measurement,
      final TSDataType dataType,
      final TsTableColumnCategory columnCategory,
      final TsTableColumnSchema existingColumn) {
    final InsertBaseStatement innerTreeStatement = getInnerTreeStatement();
    validate(index, innerTreeStatement, measurement, dataType, columnCategory, existingColumn);
  }

  private void validate(
      final int index,
      final InsertBaseStatement innerTreeStatement,
      final String measurement,
      final TSDataType dataType,
      final TsTableColumnCategory columnCategory,
      final TsTableColumnSchema existingColumn) {
    if (existingColumn == null) {
      processNonExistColumn(measurement, columnCategory, innerTreeStatement, index);
      return; // Exit early if column doesn't exist
    }

    // check data type
    if (dataType == null || columnCategory != TsTableColumnCategory.FIELD) {
      // sql insertion does not provide type
      // the type is inferred and can be inconsistent with the existing one
      innerTreeStatement.setDataType(existingColumn.getDataType(), index);
    } else if (!existingColumn.getDataType().isCompatible(dataType)
        && !innerTreeStatement.isForceTypeConversion()) {
      processTypeConflictColumn(
          measurement, dataType, columnCategory, existingColumn, index, innerTreeStatement);
      return;
    }

    // check column category
    if (columnCategory == null) {
      // sql insertion does not provide category
      innerTreeStatement.setColumnCategory(existingColumn.getColumnCategory(), index);
    } else if (!columnCategory.equals(existingColumn.getColumnCategory())) {
      throw new SemanticException(
          String.format(
              "Inconsistent column category of column %s: %s/%s",
              measurement, columnCategory, existingColumn.getColumnCategory()),
          TSStatusCode.COLUMN_CATEGORY_MISMATCH.getStatusCode());
    }

    // construct measurement schema
    TSDataType tsDataType = existingColumn.getDataType();
    MeasurementSchema newMeasurementSchema =
        new MeasurementSchema(
            existingColumn.getColumnName(),
            tsDataType,
            getDefaultEncoding(tsDataType),
            TSFileDescriptor.getInstance().getConfig().getCompressor(tsDataType));
    innerTreeStatement.setMeasurementSchema(newMeasurementSchema, index);
    try {
      innerTreeStatement.selfCheckDataTypes(index);
    } catch (DataTypeMismatchException | PathNotExistException e) {
      throw new SemanticException(e);
    }
  }

  void adjustTagColumns(
      final LinkedHashMap<String, Integer> tagColumnIndexMap,
      final LinkedHashMap<String, Integer> existingTagColumnIndexMap) {
    adjustTagColumns(getInnerTreeStatement(), tagColumnIndexMap, existingTagColumnIndexMap);
  }

  /**
   * Adjust the order of TAG columns in this insertion to be consistent with that from the schema
   * region. Optimized for performance: one expansion if needed, one reordering pass.
   *
   * @param tagColumnIndexMap LinkedHashMap of incoming TAG columns, key is column name, value is
   *     measurement index in the array (will be updated during adjustment)
   * @param existingTagColumnIndexMap LinkedHashMap of existing TAG columns in TsTable, key is
   *     column name, value is TAG column index in table (maintains schema region order)
   */
  void adjustTagColumns(
      final InsertBaseStatement baseStatement,
      final LinkedHashMap<String, Integer> tagColumnIndexMap,
      final LinkedHashMap<String, Integer> existingTagColumnIndexMap) {
    if (baseStatement == null || existingTagColumnIndexMap.isEmpty()) {
      return;
    }

    // Phase 1: Analyze and determine action in one pass
    final int oldLength = baseStatement.getMeasurements().length;
    final int totalTagCount = existingTagColumnIndexMap.size();

    // Build mapping, count existing TAG columns, and check if reordering is needed - all in one
    // pass
    int existingTagCount = 0;
    boolean needReorder = false;
    final Map<String, Integer> tagOldIndexMap =
        new LinkedHashMap<>(existingTagColumnIndexMap.size());

    int targetPos = 0;
    for (String tagColumnName : existingTagColumnIndexMap.keySet()) {
      final Integer oldIndex = tagColumnIndexMap.get(tagColumnName);
      if (oldIndex != null) {
        tagOldIndexMap.put(tagColumnName, oldIndex);
        existingTagCount++;
        // Check if this TAG column is at the correct position
        if (oldIndex != targetPos) {
          needReorder = true;
        }
      }
      targetPos++;
    }

    final boolean needExpansion = (existingTagCount < totalTagCount);

    if (needExpansion) {
      // Case 1: Expansion needed - create new arrays and reorder in one pass
      expandAndReorderTagColumns(
          baseStatement, existingTagColumnIndexMap, tagColumnIndexMap, oldLength, totalTagCount);
    } else if (needReorder) {
      // Case 2: No expansion but reordering needed - use swaps
      reorderTagColumnsWithSwap(baseStatement, tagColumnIndexMap, existingTagColumnIndexMap);
    }
    // else: No expansion and already in correct order - no operation needed

    // Clear cached table schema to force regeneration
    tableSchema = null;
  }

  /**
   * Expand arrays and reorder TAG columns in one pass. Array layout: [TAG area: 0~totalTagCount] +
   * [non-TAG area: totalTagCount~newLength]
   */
  private void expandAndReorderTagColumns(
      final InsertBaseStatement baseStatement,
      final LinkedHashMap<String, Integer> existingTagColumnIndexMap,
      final Map<String, Integer> tagOldIndexMap,
      final int oldLength,
      final int totalTagCount) {
    // Get old arrays
    final String[] oldMeasurements = baseStatement.getMeasurements();

    final int missingCount = totalTagCount - tagOldIndexMap.size();
    final int newLength = oldLength + missingCount;
    final int[] newToOldMapping = new int[newLength];
    final String[] newMeasurements = new String[newLength];

    // Fill TAG area [0, totalTagCount) in schema order
    int tagIdx = 0;
    final Set<Integer> tagOldIndices = new HashSet<>(tagOldIndexMap.values());
    for (String tagColumnName : existingTagColumnIndexMap.keySet()) {
      final Integer oldIdx = tagOldIndexMap.get(tagColumnName);
      newMeasurements[tagIdx] = tagColumnName;
      if (oldIdx == null) {
        newToOldMapping[tagIdx++] = -1;
      } else {
        newToOldMapping[tagIdx++] = oldIdx;
      }
    }

    for (int oldIdx = 0; oldIdx < oldLength; oldIdx++) {
      if (!tagOldIndices.contains(oldIdx)) {
        newMeasurements[tagIdx] = oldMeasurements[oldIdx];
        newToOldMapping[tagIdx++] = oldIdx;
      }
    }
    baseStatement.rebuildArraysAfterExpansion(newToOldMapping, newMeasurements);
  }

  /**
   * Reorder TAG columns using swap operations when no expansion is needed. This method is only
   * called when reordering is confirmed to be needed, so no need to check again.
   */
  private void reorderTagColumnsWithSwap(
      final InsertBaseStatement baseStatement,
      final LinkedHashMap<String, Integer> tagColumnIndexMap,
      final LinkedHashMap<String, Integer> existingTagColumnIndexMap) {

    // Swap columns to correct positions
    int targetPos = 0;
    for (String tagColumnName : existingTagColumnIndexMap.keySet()) {
      final Integer currentPos = tagColumnIndexMap.get(tagColumnName);

      if (currentPos != null && currentPos != targetPos) {
        baseStatement.swapColumn(currentPos, targetPos);

        // Update tagColumnIndexMap
        String columnAtTarget = findColumnAtPosition(tagColumnIndexMap, targetPos);
        tagColumnIndexMap.put(tagColumnName, targetPos);
        if (columnAtTarget != null) {
          tagColumnIndexMap.put(columnAtTarget, currentPos);
        }
      }
      targetPos++;
    }
  }

  /** Find column name at the given position */
  private String findColumnAtPosition(
      final LinkedHashMap<String, Integer> tagColumnIndexMap, final int position) {
    for (Map.Entry<String, Integer> entry : tagColumnIndexMap.entrySet()) {
      if (entry.getValue() == position) {
        return entry.getKey();
      }
    }
    return null;
  }

  /** Fill a missing TAG column with default value */
  private void fillMissingTagColumn(
      final String[] measurements,
      final MeasurementSchema[] measurementSchemas,
      final TSDataType[] dataTypes,
      final TsTableColumnCategory[] columnCategories,
      final int index,
      final String columnName) {
    measurements[index] = columnName;
    if (measurementSchemas != null) {
      measurementSchemas[index] = new MeasurementSchema(columnName, TSDataType.STRING);
    }
    if (dataTypes != null) {
      dataTypes[index] = TSDataType.STRING;
    }
    if (columnCategories != null) {
      columnCategories[index] = TsTableColumnCategory.TAG;
    }
  }

  /** Copy column data from old array to new array */
  private void copyColumn(
      final String[] oldMeasurements,
      final MeasurementSchema[] oldMeasurementSchemas,
      final TSDataType[] oldDataTypes,
      final TsTableColumnCategory[] oldColumnCategories,
      final String[] newMeasurements,
      final MeasurementSchema[] newMeasurementSchemas,
      final TSDataType[] newDataTypes,
      final TsTableColumnCategory[] newColumnCategories,
      final int oldIdx,
      final int newIdx) {
    newMeasurements[newIdx] = oldMeasurements[oldIdx];
    if (newMeasurementSchemas != null) {
      newMeasurementSchemas[newIdx] = oldMeasurementSchemas[oldIdx];
    }
    if (newDataTypes != null) {
      newDataTypes[newIdx] = oldDataTypes[oldIdx];
    }
    if (newColumnCategories != null) {
      newColumnCategories[newIdx] = oldColumnCategories[oldIdx];
    }
  }

  public static void processNonExistColumn(
      String name,
      TsTableColumnCategory columnCategory,
      InsertBaseStatement innerTreeStatement,
      int i) {
    // the column does not exist and auto-creation is disabled
    SemanticException semanticException =
        new SemanticException(
            "Column " + name + " does not exists or fails to be " + "created",
            TSStatusCode.COLUMN_NOT_EXISTS.getStatusCode());
    if (columnCategory != TsTableColumnCategory.FIELD
        || !IoTDBDescriptor.getInstance().getConfig().isEnablePartialInsert()) {
      // non-measurement columns cannot be partially inserted
      throw semanticException;
    } else {
      // partial insertion
      innerTreeStatement.markFailedMeasurement(i, semanticException);
    }
  }

  public static void processTypeConflictColumn(
      ColumnSchema incoming, ColumnSchema real, int i, InsertBaseStatement innerTreeStatement) {
    SemanticException semanticException =
        new SemanticException(
            String.format(
                "Incompatible data type of column %s: %s/%s",
                incoming.getName(), incoming.getType(), real.getType()),
            TSStatusCode.DATA_TYPE_MISMATCH.getStatusCode());
    if (incoming.getColumnCategory() != TsTableColumnCategory.FIELD
        || !IoTDBDescriptor.getInstance().getConfig().isEnablePartialInsert()) {
      // non-measurement columns cannot be partially inserted
      throw semanticException;
    } else {
      // partial insertion
      innerTreeStatement.markFailedMeasurement(i, semanticException);
    }
  }

  public static void processTypeConflictColumn(
      String incomingMeasurement,
      TSDataType incomingDataType,
      TsTableColumnCategory columnCategory,
      TsTableColumnSchema real,
      int i,
      InsertBaseStatement innerTreeStatement) {
    SemanticException semanticException =
        new SemanticException(
            String.format(
                "Incompatible data type of column %s: %s/%s",
                incomingMeasurement, incomingDataType, real.getDataType()),
            TSStatusCode.DATA_TYPE_MISMATCH.getStatusCode());
    if (columnCategory != TsTableColumnCategory.FIELD
        || !IoTDBDescriptor.getInstance().getConfig().isEnablePartialInsert()) {
      // non-measurement columns cannot be partially inserted
      throw semanticException;
    } else {
      // partial insertion
      innerTreeStatement.markFailedMeasurement(i, semanticException);
    }
  }

  public void validateDeviceSchema(Metadata metadata, MPPQueryContext context) {
    metadata.validateDeviceSchema(this, context);
  }

  public String getDatabase() {
    String databaseName = AnalyzeUtils.getDatabaseName(getInnerTreeStatement(), context);
    if (databaseName == null) {
      throw new SemanticException(DATABASE_NOT_SPECIFIED);
    }
    return databaseName;
  }

  public void toLowerCase() {
    getInnerTreeStatement().toLowerCase();
  }

  public void removeAttributeColumns() {
    getInnerTreeStatement().removeAttributeColumns();
  }
}
