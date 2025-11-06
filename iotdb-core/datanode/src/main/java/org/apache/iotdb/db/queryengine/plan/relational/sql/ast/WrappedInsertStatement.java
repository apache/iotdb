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
import org.apache.iotdb.db.queryengine.plan.relational.type.InternalTypeManager;
import org.apache.iotdb.db.queryengine.plan.statement.crud.InsertBaseStatement;
import org.apache.iotdb.db.utils.TypeInferenceUtils;
import org.apache.iotdb.rpc.TSStatusCode;

import org.apache.tsfile.common.conf.TSFileDescriptor;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.type.TypeFactory;
import org.apache.tsfile.write.schema.MeasurementSchema;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
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
    String tableName = insertBaseStatement.getDevicePath().getFullPath();

    // Use lazy initialization with measurements and dataTypes
    return new InsertNodeMeasurementInfo(
        tableName,
        insertBaseStatement.getColumnCategories(),
        insertBaseStatement.getMeasurements(),
        insertBaseStatement.getDataTypes(),
        index ->
            TypeInferenceUtils.getPredictedDataType(
                insertBaseStatement.getFirstValueOfIndex(index), true));
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

  private void validate(
      int index,
      String measurement,
      TSDataType dataType,
      TsTableColumnCategory columnCategory,
      TsTableColumnSchema existingColumn) {
    InsertBaseStatement innerTreeStatement = getInnerTreeStatement();
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
            tsDataType.name(),
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

  public void validateTableSchema(
      TableSchema realSchema,
      TableSchema incomingTableSchema,
      InsertBaseStatement innerTreeStatement) {
    final List<ColumnSchema> incomingSchemaColumns = incomingTableSchema.getColumns();
    Map<String, ColumnSchema> realSchemaMap = new HashMap<>();
    realSchema.getColumns().forEach(c -> realSchemaMap.put(c.getName(), c));
    // incoming schema should be consistent with real schema
    for (int i = 0, incomingSchemaColumnsSize = incomingSchemaColumns.size();
        i < incomingSchemaColumnsSize;
        i++) {
      ColumnSchema incomingSchemaColumn = incomingSchemaColumns.get(i);
      final ColumnSchema realSchemaColumn = realSchemaMap.get(incomingSchemaColumn.getName());
      validateTableSchema(incomingSchemaColumn, realSchemaColumn, i, innerTreeStatement);
    }
    // incoming schema should contain all id columns in real schema and have consistent order
    final List<ColumnSchema> realIdColumns = realSchema.getTagColumns();
    adjustIdColumns(realIdColumns, innerTreeStatement);
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
      final LinkedHashMap<String, Integer> tagColumnIndexMap,
      final LinkedHashMap<String, Integer> existingTagColumnIndexMap) {
    InsertBaseStatement baseStatement = getInnerTreeStatement();
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
    final Map<String, Integer> tagOldIndexMap = new LinkedHashMap<>();

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
          baseStatement,
          tagColumnIndexMap,
          existingTagColumnIndexMap,
          tagOldIndexMap,
          oldLength,
          totalTagCount);
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
      final LinkedHashMap<String, Integer> tagColumnIndexMap,
      final LinkedHashMap<String, Integer> existingTagColumnIndexMap,
      final Map<String, Integer> tagOldIndexMap,
      final int oldLength,
      final int totalTagCount) {

    // Build old-to-new index mapping array
    final int[] oldToNewMapping = new int[oldLength];

    // Get old arrays
    final String[] oldMeasurements = baseStatement.getMeasurements();
    final MeasurementSchema[] oldMeasurementSchemas = baseStatement.getMeasurementSchemas();
    final TSDataType[] oldDataTypes = baseStatement.getDataTypes();
    final TsTableColumnCategory[] oldColumnCategories = baseStatement.getColumnCategories();

    final int missingCount = totalTagCount - tagOldIndexMap.size();
    final int newLength = oldLength + missingCount;

    // Build mapping for existing TAG columns
    int tagIdx = 0;
    for (String tagColumnName : existingTagColumnIndexMap.keySet()) {
      final Integer oldIdx = tagOldIndexMap.get(tagColumnName);
      if (oldIdx != null) {
        oldToNewMapping[oldIdx] = tagIdx; // oldIdx maps to tagIdx in new array
      }
      tagIdx++;
    }

    // Create new arrays with final size
    final String[] newMeasurements = new String[newLength];
    final MeasurementSchema[] newMeasurementSchemas =
        oldMeasurementSchemas != null ? new MeasurementSchema[newLength] : null;
    final TSDataType[] newDataTypes = oldDataTypes != null ? new TSDataType[newLength] : null;
    final TsTableColumnCategory[] newColumnCategories =
        oldColumnCategories != null ? new TsTableColumnCategory[newLength] : null;

    // Fill TAG area [0, totalTagCount) in schema order
    tagIdx = 0;
    for (String tagColumnName : existingTagColumnIndexMap.keySet()) {
      final Integer oldIdx = tagOldIndexMap.get(tagColumnName);

      if (oldIdx == null) {
        // Missing TAG column, fill with default value
        fillMissingTagColumn(
            newMeasurements,
            newMeasurementSchemas,
            newDataTypes,
            newColumnCategories,
            tagIdx,
            tagColumnName);
      } else {
        // Existing TAG column, copy from old array
        copyColumn(
            oldMeasurements,
            oldMeasurementSchemas,
            oldDataTypes,
            oldColumnCategories,
            newMeasurements,
            newMeasurementSchemas,
            newDataTypes,
            newColumnCategories,
            oldIdx,
            tagIdx);
      }
      tagIdx++;
    }

    // Fill non-TAG area [totalTagCount, newLength) and complete the mapping
    final Set<Integer> tagOldIndices = new HashSet<>(tagOldIndexMap.values());
    int nonTagIdx = totalTagCount;
    for (int oldIdx = 0; oldIdx < oldLength; oldIdx++) {
      if (!tagOldIndices.contains(oldIdx)) {
        // Non-TAG column, copy to back section
        copyColumn(
            oldMeasurements,
            oldMeasurementSchemas,
            oldDataTypes,
            oldColumnCategories,
            newMeasurements,
            newMeasurementSchemas,
            newDataTypes,
            newColumnCategories,
            oldIdx,
            nonTagIdx);
        oldToNewMapping[oldIdx] = nonTagIdx;
        nonTagIdx++;
      }
    }

    // Replace old arrays with new arrays
    baseStatement.setMeasurements(newMeasurements);
    if (newMeasurementSchemas != null) {
      baseStatement.setMeasurementSchemas(newMeasurementSchemas);
    }
    if (newDataTypes != null) {
      baseStatement.setDataTypes(newDataTypes);
    }
    if (newColumnCategories != null) {
      baseStatement.setColumnCategories(newColumnCategories);
    }

    // Notify subclass to rebuild its arrays with the mapping
    baseStatement.rebuildArraysAfterExpansion(oldToNewMapping, totalTagCount);

    // Update tagColumnIndexMap with new positions
    tagColumnIndexMap.clear();
    int idx = 0;
    for (String tagColumnName : existingTagColumnIndexMap.keySet()) {
      tagColumnIndexMap.put(tagColumnName, idx++);
    }
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

  /**
   * Adjust the order of ID columns in this insertion to be consistent with that from the schema
   * region.
   *
   * @param realIdColumnSchemas id column order from the schema region
   */
  public void adjustIdColumns(
      List<ColumnSchema> realIdColumnSchemas, final InsertBaseStatement baseStatement) {
    List<ColumnSchema> incomingColumnSchemas = toTableSchema(baseStatement).getColumns();
    for (int realIdColPos = 0; realIdColPos < realIdColumnSchemas.size(); realIdColPos++) {
      ColumnSchema realColumn = realIdColumnSchemas.get(realIdColPos);
      int incomingIdColPos = incomingColumnSchemas.indexOf(realColumn);
      if (incomingIdColPos == -1) {
        // if the realIdColPos-th id column in the table is missing, insert an empty column in the
        // tablet
        baseStatement.insertColumn(realIdColPos, realColumn);
        incomingColumnSchemas.add(realIdColPos, realColumn);
      } else {
        // move the id column in the tablet to the proper position
        baseStatement.swapColumn(incomingIdColPos, realIdColPos);
        Collections.swap(incomingColumnSchemas, incomingIdColPos, realIdColPos);
      }
    }
    tableSchema = null;
  }

  public static void processNonExistColumn(
      ColumnSchema incoming, InsertBaseStatement innerTreeStatement, int i) {
    // the column does not exist and auto-creation is disabled
    SemanticException semanticException =
        new SemanticException(
            "Column " + incoming.getName() + " does not exists or fails to be " + "created",
            TSStatusCode.COLUMN_NOT_EXISTS.getStatusCode());
    if (incoming.getColumnCategory() != TsTableColumnCategory.FIELD
        || !IoTDBDescriptor.getInstance().getConfig().isEnablePartialInsert()) {
      // non-measurement columns cannot be partially inserted
      throw semanticException;
    } else {
      // partial insertion
      innerTreeStatement.markFailedMeasurement(i, semanticException);
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

  public static void validateTableSchema(
      ColumnSchema incoming, ColumnSchema real, int i, InsertBaseStatement innerTreeStatement) {
    if (real == null) {
      processNonExistColumn(incoming, innerTreeStatement, i);
      return;
    }

    // check data type
    if (incoming.getType() == null || incoming.getColumnCategory() != TsTableColumnCategory.FIELD) {
      // sql insertion does not provide type
      // the type is inferred and can be inconsistent with the existing one
      innerTreeStatement.setDataType(InternalTypeManager.getTSDataType(real.getType()), i);
    } else if (!InternalTypeManager.getTSDataType(real.getType())
            .isCompatible(InternalTypeManager.getTSDataType(incoming.getType()))
        && !innerTreeStatement.isForceTypeConversion()) {
      processTypeConflictColumn(incoming, real, i, innerTreeStatement);
      return;
    }

    // check column category
    if (incoming.getColumnCategory() == null) {
      // sql insertion does not provide category
      innerTreeStatement.setColumnCategory(real.getColumnCategory(), i);
    } else if (!incoming.getColumnCategory().equals(real.getColumnCategory())) {
      throw new SemanticException(
          String.format(
              "Inconsistent column category of column %s: %s/%s",
              incoming.getName(), incoming.getColumnCategory(), real.getColumnCategory()),
          TSStatusCode.COLUMN_CATEGORY_MISMATCH.getStatusCode());
    }

    // construct measurement schema
    TSDataType tsDataType = InternalTypeManager.getTSDataType(real.getType());
    MeasurementSchema measurementSchema =
        new MeasurementSchema(
            real.getName(),
            tsDataType,
            getDefaultEncoding(tsDataType),
            TSFileDescriptor.getInstance().getConfig().getCompressor(tsDataType));
    innerTreeStatement.setMeasurementSchema(measurementSchema, i);
    try {
      innerTreeStatement.selfCheckDataTypes(i);
    } catch (DataTypeMismatchException | PathNotExistException e) {
      throw new SemanticException(e);
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
