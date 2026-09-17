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

package org.apache.iotdb.db.protocol.session;

import org.apache.iotdb.commons.exception.SemanticException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.queryengine.utils.TimestampPrecisionUtils;
import org.apache.iotdb.commons.schema.table.TsTable;
import org.apache.iotdb.commons.schema.table.column.TsTableColumnCategory;
import org.apache.iotdb.commons.schema.table.column.TsTableColumnSchema;
import org.apache.iotdb.db.i18n.DataNodeMiscMessages;
import org.apache.iotdb.db.pipe.resource.memory.InsertNodeMemoryEstimator;
import org.apache.iotdb.db.queryengine.plan.statement.crud.InsertNumericRowStatement;
import org.apache.iotdb.db.schemaengine.table.DataNodeTableCache;
import org.apache.iotdb.db.schemaengine.table.DataNodeTreeViewSchemaUtils;
import org.apache.iotdb.db.utils.NumericRowValues;
import org.apache.iotdb.service.rpc.thrift.TSPrepareNumericRowReq;

import org.apache.tsfile.common.conf.TSFileDescriptor;
import org.apache.tsfile.enums.ColumnCategory;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.utils.Pair;
import org.apache.tsfile.utils.RamUsageEstimator;
import org.apache.tsfile.utils.ReadWriteIOUtils;
import org.apache.tsfile.write.schema.MeasurementSchema;

import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.Locale;
import java.util.Set;

import static org.apache.iotdb.db.utils.EncodingInferenceUtils.getDefaultEncoding;

/**
 * Session-owned metadata. Each execution gets fresh arrays because schema analysis mutates them.
 */
public final class PreparedNumericRowInfo {
  private final String database;
  private final String table;
  private final String[] measurements;
  private final TSDataType[] types;
  private final TsTableColumnCategory[] categories;
  private final int tagCount;
  private final int valueBytes;
  private volatile Pair<Long, Long> tableVersion;
  private final MeasurementSchema[] measurementSchemas;

  public PreparedNumericRowInfo(String database, TSPrepareNumericRowReq req) {
    if (req == null) {
      throw invalid(DataNodeMiscMessages.EXCEPTION_PREPARED_NUMERIC_ROW_REQUEST_IS_NULL_5A9F9BA9);
    }
    if (database == null) {
      throw invalid(DataNodeMiscMessages.EXCEPTION_PREPARED_NUMERIC_ROW_DATABASE_IS_NULL_F2A2E0AC);
    }
    if (req.getTableName() == null || req.getTableName().isEmpty()) {
      throw invalid(
          DataNodeMiscMessages.EXCEPTION_PREPARED_NUMERIC_ROW_TABLE_NAME_IS_NULL_OR_EMPTY_F2D38BE5);
    }
    if (req.getMeasurements() == null) {
      throw invalid(
          DataNodeMiscMessages.EXCEPTION_PREPARED_NUMERIC_ROW_MEASUREMENTS_ARE_NULL_830DE646);
    }
    if (req.getDataTypes() == null) {
      throw invalid(
          DataNodeMiscMessages.EXCEPTION_PREPARED_NUMERIC_ROW_DATA_TYPES_ARE_NULL_7C15F477);
    }
    if (req.getColumnCategories() == null) {
      throw invalid(
          DataNodeMiscMessages.EXCEPTION_PREPARED_NUMERIC_ROW_COLUMN_CATEGORIES_ARE_NULL_6F2EE1BE);
    }
    if (req.getMeasurementsSize() == 0) {
      throw invalid(DataNodeMiscMessages.EXCEPTION_PREPARED_NUMERIC_ROW_HAS_NO_COLUMNS_D4E40365);
    }
    if (req.getMeasurementsSize() != req.getDataTypesSize()
        || req.getMeasurementsSize() != req.getColumnCategoriesSize()) {
      throw invalid(
          DataNodeMiscMessages
              .EXCEPTION_PREPARED_NUMERIC_ROW_COLUMN_METADATA_SIZE_MISMATCH_MEASUREMENTS_ARG_DATA_TYPES_ARG_CATEGORIES_ARG_23DAB34B,
          req.getMeasurementsSize(),
          req.getDataTypesSize(),
          req.getColumnCategoriesSize());
    }
    this.database = database;
    table = req.getTableName();
    measurements = req.getMeasurements().toArray(new String[0]);
    types = new TSDataType[measurements.length];
    categories = new TsTableColumnCategory[measurements.length];
    int tags = 0;
    int bytes = 0;
    Set<String> names = new HashSet<>();
    for (int i = 0; i < measurements.length; i++) {
      Integer type = req.getDataTypes().get(i);
      Byte category = req.getColumnCategories().get(i);
      if (measurements[i] == null || measurements[i].isEmpty()) {
        throw invalid(
            DataNodeMiscMessages
                .EXCEPTION_PREPARED_NUMERIC_ROW_COLUMN_NAME_IS_NULL_OR_EMPTY_AT_INDEX_ARG_DEBDCFF8,
            i);
      }
      if (!names.add(measurements[i].toLowerCase(Locale.ROOT))) {
        throw invalid(
            DataNodeMiscMessages
                .EXCEPTION_PREPARED_NUMERIC_ROW_COLUMN_NAME_IS_DUPLICATED_AT_INDEX_ARG_ARG_73AAFECC,
            i,
            measurements[i]);
      }
      if (type == null) {
        throw invalid(
            DataNodeMiscMessages
                .EXCEPTION_PREPARED_NUMERIC_ROW_DATA_TYPE_IS_NULL_AT_INDEX_ARG_FB2A1FD6,
            i);
      }
      if (type < 0 || type >= TSDataType.values().length) {
        throw invalid(
            DataNodeMiscMessages
                .EXCEPTION_PREPARED_NUMERIC_ROW_DATA_TYPE_ORDINAL_IS_INVALID_AT_INDEX_ARG_ARG_BB5059BB,
            i,
            type);
      }
      if (category == null) {
        throw invalid(
            DataNodeMiscMessages
                .EXCEPTION_PREPARED_NUMERIC_ROW_COLUMN_CATEGORY_IS_NULL_AT_INDEX_ARG_3ED8315E,
            i);
      }
      types[i] = TSDataType.values()[type];
      if (category == ColumnCategory.TAG.ordinal() && types[i] == TSDataType.STRING && tags == i) {
        tags++;
        categories[i] = TsTableColumnCategory.TAG;
      } else if (category == ColumnCategory.FIELD.ordinal()
          && NumericRowValues.supports(types[i])) {
        categories[i] = TsTableColumnCategory.FIELD;
        bytes = Math.addExact(bytes, types[i].getDataTypeSize());
      } else {
        throw invalid(
            DataNodeMiscMessages
                .EXCEPTION_PREPARED_NUMERIC_ROW_COLUMN_IS_INVALID_AT_INDEX_ARG_CATEGORY_ARG_AND_TYPE_ARG_9DAEEA10,
            i,
            category,
            types[i]);
      }
    }
    tagCount = tags;
    valueBytes = bytes;
    if (tagCount == measurements.length) {
      throw invalid(
          DataNodeMiscMessages.EXCEPTION_PREPARED_NUMERIC_ROW_HAS_NO_FIELD_COLUMNS_A76209AE);
    }
    TsTable currentTable = DataNodeTableCache.getInstance().getTableInWrite(database, table);
    Pair<Long, Long> version = currentTable == null ? null : currentTable.getInstanceVersion();
    MeasurementSchema[] schemas = prepareMeasurementSchemas(currentTable);
    // Never retain a snapshot assembled across concurrent schema revisions.
    if (schemas != null && currentTable.matchesInstanceVersion(version)) {
      tableVersion = version;
      measurementSchemas = schemas;
    } else {
      tableVersion = null;
      measurementSchemas = null;
    }
  }

  private MeasurementSchema[] prepareMeasurementSchemas(TsTable currentTable) {
    if (!matchesTableSchema(currentTable)) {
      return null;
    }
    MeasurementSchema[] schemas = new MeasurementSchema[measurements.length];
    for (int i = 0; i < measurements.length; i++) {
      TsTableColumnSchema column = currentTable.getColumnSchema(measurements[i]);
      if (column == null) {
        return null;
      }
      schemas[i] =
          new MeasurementSchema(
              column.getColumnName(),
              types[i],
              getDefaultEncoding(types[i]),
              TSFileDescriptor.getInstance().getConfig().getCompressor(types[i]));
    }
    return schemas;
  }

  private boolean matchesTableSchema(TsTable currentTable) {
    // Views and requests needing TAG padding/reordering or type conversion use normal analysis.
    if (currentTable == null
        || currentTable.getClass() != TsTable.class
        || DataNodeTreeViewSchemaUtils.isTreeViewTable(currentTable)
        || currentTable.getTagColumnSchemaList().size() != tagCount) {
      return false;
    }
    for (int i = 0; i < measurements.length; i++) {
      TsTableColumnSchema column = currentTable.getColumnSchema(measurements[i]);
      if (column == null
          || column.getDataType() != types[i]
          || column.getColumnCategory() != categories[i]
          || (i < tagCount && currentTable.getTagColumnOrdinal(measurements[i]) != i)) {
        return false;
      }
    }
    return true;
  }

  /** Returns a stable compatible version, retaining the cached schemas and wire layout. */
  public Pair<Long, Long> refreshTableVersionIfCompatible(TsTable currentTable) {
    if (measurementSchemas == null || currentTable == null) {
      return null;
    }
    Pair<Long, Long> version = currentTable.getInstanceVersion();
    if (!matchesTableSchema(currentTable) || !currentTable.matchesInstanceVersion(version)) {
      return null;
    }
    // Publish only a compatible candidate. Each execution rechecks it under the schema lock;
    // retaining the old candidate on failure also keeps the session's memory reservation stable.
    tableVersion = version;
    return version;
  }

  public InsertNumericRowStatement bind(
      long timestamp, ByteBuffer tags, ByteBuffer values, ByteBuffer nullBitmap) {
    if (tags == null) {
      throw invalid(
          DataNodeMiscMessages.EXCEPTION_PREPARED_NUMERIC_ROW_TAG_BUFFER_IS_NULL_3FF27BF7);
    }
    if (values == null) {
      throw invalid(
          DataNodeMiscMessages.EXCEPTION_PREPARED_NUMERIC_ROW_VALUE_BUFFER_IS_NULL_A928F919);
    }
    NumericRowValues.validateNullBitmap(nullBitmap, types.length - tagCount);
    // Preserve the cached fixed-width length for the common non-null path.
    int expectedBytes = valueBytes;
    if (nullBitmap != null) {
      for (int i = tagCount; i < types.length; i++) {
        if (NumericRowValues.isNullField(nullBitmap, i - tagCount)) {
          expectedBytes -= types[i].getDataTypeSize();
        }
      }
    }
    if (values.remaining() != expectedBytes) {
      throw invalid(
          DataNodeMiscMessages
              .EXCEPTION_PREPARED_NUMERIC_ROW_VALUE_BUFFER_SIZE_MISMATCH_EXPECTED_ARG_BYTES_BUT_GOT_ARG_B75D48C8,
          expectedBytes,
          values.remaining());
    }
    TimestampPrecisionUtils.checkTimestampPrecision(timestamp);
    NumericRowValues row = new NumericRowValues(tagCount, types.length - tagCount);
    // Duplicate cursors so validation never changes a caller's buffers, even on failure.
    tags = tags.duplicate();
    values = values.duplicate();
    try {
      for (int i = 0; i < tagCount; i++) {
        if (tags.remaining() < Integer.BYTES) {
          throw invalid(
              DataNodeMiscMessages
                  .EXCEPTION_PREPARED_NUMERIC_ROW_TAG_LENGTH_PREFIX_IS_INCOMPLETE_AT_INDEX_ARG_ARG_BYTES_REMAINING_6D342F3A,
              i,
              tags.remaining());
        }
        int length = tags.getInt(tags.position());
        if (length < -1 || length > tags.remaining() - Integer.BYTES) {
          throw invalid(
              DataNodeMiscMessages
                  .EXCEPTION_PREPARED_NUMERIC_ROW_TAG_LENGTH_IS_INVALID_AT_INDEX_ARG_LENGTH_ARG_ARG_BYTES_REMAINING_DEDD1E8F,
              i,
              length,
              tags.remaining());
        }
        row.setTag(i, ReadWriteIOUtils.readString(tags));
      }
      if (tags.hasRemaining()) {
        throw invalid(
            DataNodeMiscMessages
                .EXCEPTION_PREPARED_NUMERIC_ROW_TAG_BUFFER_HAS_ARG_TRAILING_BYTES_042CF4D2,
            tags.remaining());
      }
      for (int i = tagCount; i < types.length; i++) {
        if (NumericRowValues.isNullField(nullBitmap, i - tagCount)) {
          row.setNull(i);
        } else {
          row.read(i, types[i], values);
        }
      }
    } catch (BufferUnderflowException e) {
      throw invalid(
          DataNodeMiscMessages.EXCEPTION_PREPARED_NUMERIC_ROW_PAYLOAD_DECODING_FAILED_ARG_F87689DD,
          e.getMessage());
    }
    InsertNumericRowStatement statement = new InsertNumericRowStatement();
    statement.setDevicePath(new PartialPath(table, false));
    statement.setDatabaseName(database);
    statement.setMeasurements(measurements.clone());
    statement.setDataTypes(types.clone());
    statement.setColumnCategories(categories.clone());
    statement.setTime(timestamp);
    statement.setNumericValues(row);
    if (measurementSchemas != null) {
      statement.setPreparedNumericRowInfo(this);
      statement.setPreparedTableVersion(tableVersion);
      statement.setMeasurementSchemas(measurementSchemas.clone());
    }
    return statement;
  }

  public long ramBytesUsed() {
    return RamUsageEstimator.shallowSizeOfInstance(PreparedNumericRowInfo.class)
        + RamUsageEstimator.sizeOf(database)
        + RamUsageEstimator.sizeOf(table)
        + RamUsageEstimator.sizeOf(measurements)
        + RamUsageEstimator.shallowSizeOf(types)
        + RamUsageEstimator.shallowSizeOf(categories)
        + InsertNodeMemoryEstimator.sizeOfMeasurementSchemas(measurementSchemas)
        + (tableVersion == null
            ? 0
            : RamUsageEstimator.shallowSizeOf(tableVersion)
                + 2 * RamUsageEstimator.shallowSizeOfInstance(Long.class))
        // Account for the map entry and boxed ID retained by the session.
        + 64;
  }

  private static SemanticException invalid() {
    return new SemanticException(
        DataNodeMiscMessages.EXCEPTION_INVALID_PREPARED_NUMERIC_ROW_METADATA_OR_PAYLOAD_AECCCEAA);
  }

  private static SemanticException invalid(String message, Object... args) {
    return new SemanticException(String.format(message, args));
  }
}
