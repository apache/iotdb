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

package org.apache.iotdb.db.pipe.sink.util;

import org.apache.iotdb.commons.exception.MetadataException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.schema.table.column.TsTableColumnCategory;
import org.apache.iotdb.db.queryengine.plan.statement.crud.InsertTabletStatement;

import org.apache.tsfile.enums.ColumnCategory;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.utils.BitMap;
import org.apache.tsfile.utils.BytesUtils;
import org.apache.tsfile.utils.DateUtils;
import org.apache.tsfile.utils.Pair;
import org.apache.tsfile.utils.ReadWriteIOUtils;
import org.apache.tsfile.write.UnSupportedDataTypeException;
import org.apache.tsfile.write.record.Tablet;
import org.apache.tsfile.write.schema.IMeasurementSchema;
import org.apache.tsfile.write.schema.MeasurementSchema;

import java.nio.ByteBuffer;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Utility class for converting between InsertTabletStatement and Tablet format ByteBuffer. This
 * avoids creating intermediate Tablet objects and directly converts between formats with only the
 * fields needed.
 */
public class TabletStatementConverter {

  private TabletStatementConverter() {
    // Utility class, no instantiation
  }

  /**
   * Convert Tablet to InsertTabletStatement.
   *
   * @param tablet Tablet object to convert
   * @param isAligned whether the tablet is aligned
   * @param databaseName database name (optional, for table model)
   * @return InsertTabletStatement
   * @throws MetadataException if conversion fails
   */
  private static InsertTabletStatement convertTabletToStatement(
      final Tablet tablet, final boolean isAligned, final String databaseName)
      throws MetadataException {
    try {
      final String deviceId = tablet.getDeviceId();
      final int rowSize = tablet.getRowSize();
      final List<IMeasurementSchema> schemas = tablet.getSchemas();
      final int schemaSize = schemas.size();
      final long[] times = tablet.getTimestamps();
      final Object[] values = tablet.getValues();
      final BitMap[] bitMaps = tablet.getBitMaps();
      final List<ColumnCategory> columnCategories = tablet.getColumnTypes();

      // Construct InsertTabletStatement
      final InsertTabletStatement statement = new InsertTabletStatement();

      // Set device path based on databaseName
      // For table model, use getTableName() which returns the table name
      // For tree model, use getDeviceId() which returns the device path
      if (databaseName != null) {
        // For table model, deviceId is actually the table name
        statement.setDevicePath(new PartialPath(deviceId, false));
        statement.setDatabaseName(databaseName);
        statement.setWriteToTable(true);
      } else {
        statement.setDevicePath(new PartialPath(deviceId));
        statement.setWriteToTable(false);
      }

      // Set measurements, dataTypes, and measurementSchemas
      final String[] measurements = new String[schemaSize];
      final TSDataType[] statementDataTypes = new TSDataType[schemaSize];
      final MeasurementSchema[] measurementSchemas = new MeasurementSchema[schemaSize];
      for (int i = 0; i < schemaSize; i++) {
        final IMeasurementSchema schema = schemas.get(i);
        if (schema != null) {
          measurements[i] = schema.getMeasurementName();
          statementDataTypes[i] = schema.getType();
          measurementSchemas[i] = (MeasurementSchema) schema;
        } else {
          measurements[i] = null;
          statementDataTypes[i] = null;
          measurementSchemas[i] = null;
        }
      }
      statement.setMeasurements(measurements);
      statement.setDataTypes(statementDataTypes);
      statement.setMeasurementSchemas(measurementSchemas);

      // Set column categories if databaseName is provided
      if (databaseName != null && columnCategories != null) {
        final TsTableColumnCategory[] statementColumnCategories =
            new TsTableColumnCategory[schemaSize];
        for (int i = 0; i < schemaSize; i++) {
          if (i < columnCategories.size() && columnCategories.get(i) != null) {
            statementColumnCategories[i] =
                TsTableColumnCategory.fromTsFileColumnCategory(columnCategories.get(i));
          } else {
            statementColumnCategories[i] = null;
          }
        }
        statement.setColumnCategories(statementColumnCategories);
      }

      // Set times, rowCount, columns, bitMaps
      // Tablet.getTimestamps() returns an array of size maxRowNumber, but we only need rowSize
      // elements
      final long[] statementTimes;
      if (times != null && times.length >= rowSize && rowSize > 0) {
        statementTimes = new long[rowSize];
        System.arraycopy(times, 0, statementTimes, 0, rowSize);
      } else {
        // If times array is null or too small, create empty array
        statementTimes = new long[0];
      }
      statement.setTimes(statementTimes);
      statement.setRowCount(rowSize);
      statement.setColumns(values);
      statement.setBitMaps(bitMaps);
      statement.setAligned(isAligned);

      return statement;
    } catch (final Exception e) {
      throw new MetadataException("Failed to convert Tablet to InsertTabletStatement", e);
    }
  }

  /**
   * Convert InsertTabletStatement to Tablet. This method constructs a Tablet object from
   * InsertTabletStatement, converting all necessary fields.
   *
   * @param statement InsertTabletStatement to convert
   * @return Tablet object
   * @throws MetadataException if conversion fails
   */
  public static Tablet convertStatementToTablet(final InsertTabletStatement statement)
      throws MetadataException {
    try {
      // Get deviceId/tableName from devicePath
      final String deviceIdOrTableName =
          statement.getDevicePath() != null ? statement.getDevicePath().getFullPath() : "";

      // Get schemas from measurementSchemas
      final MeasurementSchema[] measurementSchemas = statement.getMeasurementSchemas();
      final String[] measurements = statement.getMeasurements();
      final TSDataType[] dataTypes = statement.getDataTypes();
      final int schemaSize = measurements != null ? measurements.length : 0;

      final List<IMeasurementSchema> schemas = new ArrayList<>();
      for (int i = 0; i < schemaSize; i++) {
        if (measurementSchemas != null && measurementSchemas[i] != null) {
          schemas.add(measurementSchemas[i]);
        } else if (measurements[i] != null && dataTypes[i] != null) {
          // Create MeasurementSchema if not present
          schemas.add(new MeasurementSchema(measurements[i], dataTypes[i]));
        } else {
          schemas.add(null);
        }
      }

      // Get columnTypes (for table model)
      final TsTableColumnCategory[] columnCategories = statement.getColumnCategories();
      final List<ColumnCategory> tabletColumnTypes = new ArrayList<>();
      if (columnCategories != null && columnCategories.length > 0) {
        for (int i = 0; i < schemaSize; i++) {
          if (columnCategories[i] != null) {
            tabletColumnTypes.add(columnCategories[i].toTsFileColumnType());
          } else {
            tabletColumnTypes.add(ColumnCategory.FIELD);
          }
        }
      } else {
        // Default to FIELD for all columns if not specified
        for (int i = 0; i < schemaSize; i++) {
          tabletColumnTypes.add(ColumnCategory.FIELD);
        }
      }

      // Get timestamps
      final long[] times = statement.getTimes();
      final int rowSize = statement.getRowCount();
      final long[] timestamps;
      if (times != null && times.length >= rowSize) {
        timestamps = new long[rowSize];
        System.arraycopy(times, 0, timestamps, 0, rowSize);
      } else {
        timestamps = new long[0];
      }

      // Get values - convert Statement columns to Tablet format
      final Object[] statementColumns = statement.getColumns();
      final Object[] tabletValues = new Object[schemaSize];
      if (statementColumns != null && statementColumns.length > 0) {
        for (int i = 0; i < schemaSize; i++) {
          if (statementColumns[i] != null && dataTypes[i] != null) {
            tabletValues[i] = convertStatementColumnToTablet(statementColumns[i], dataTypes[i]);
          } else {
            tabletValues[i] = null;
          }
        }
      }

      // Get bitMaps
      final BitMap[] bitMaps = statement.getBitMaps();

      // Create Tablet using the full constructor
      // Tablet(String tableName, List<IMeasurementSchema> schemas, List<ColumnCategory>
      // columnTypes,
      //        long[] timestamps, Object[] values, BitMap[] bitMaps, int rowSize)
      final Tablet tablet =
          new Tablet(
              deviceIdOrTableName,
              schemas,
              tabletColumnTypes,
              timestamps,
              tabletValues,
              bitMaps,
              rowSize);

      // Note: isAligned is not a property of Tablet, it's handled separately during insertion
      return tablet;
    } catch (final Exception e) {
      throw new MetadataException("Failed to convert InsertTabletStatement to Tablet", e);
    }
  }

  /**
   * Convert a single column value from Statement format to Tablet format. Statement uses primitive
   * arrays (e.g., int[], long[], float[]), while Tablet may need different format.
   *
   * @param columnValue column value from Statement (primitive array)
   * @param dataType data type of the column
   * @return column value in Tablet format
   */
  private static Object convertStatementColumnToTablet(
      final Object columnValue, final TSDataType dataType) {

    if (TSDataType.DATE.equals(dataType)) {
      int[] values = (int[]) columnValue;
      final LocalDate[] localDateValue = new LocalDate[values.length];
      for (int i = 0; i < values.length; i++) {
        localDateValue[i] = DateUtils.parseIntToLocalDate(values[i]);
      }

      return localDateValue;
    }
    // For primitive arrays (boolean[], int[], long[], float[], double[]), return as-is
    return columnValue;
  }

  public static Pair<InsertTabletStatement, String> deserializeStatementFromTabletFormat(
      ByteBuffer byteBuffer) {
    InsertTabletStatement statement = new InsertTabletStatement();
    final String insertTargetName = ReadWriteIOUtils.readString(byteBuffer);

    int rowSize = ReadWriteIOUtils.readInt(byteBuffer);

    // deserialize schemas
    final int schemaSize =
        BytesUtils.byteToBool(ReadWriteIOUtils.readByte(byteBuffer))
            ? ReadWriteIOUtils.readInt(byteBuffer)
            : 0;
    final String[] measurement = new String[schemaSize];
    final TsTableColumnCategory[] columnCategories = new TsTableColumnCategory[schemaSize];
    final TSDataType[] dataTypes = new TSDataType[schemaSize];

    for (int i = 0; i < schemaSize; i++) {
      boolean hasSchema = BytesUtils.byteToBool(ReadWriteIOUtils.readByte(byteBuffer));
      if (hasSchema) {
        Pair<String, TSDataType> pair = readMeasurement(byteBuffer);
        measurement[i] = pair.getLeft();
        dataTypes[i] = pair.getRight();
        columnCategories[i] =
            TsTableColumnCategory.fromTsFileColumnCategory(
                ColumnCategory.values()[byteBuffer.get()]);
      }
    }

    // deserialize times
    long[] times = new long[rowSize];
    boolean isTimesNotNull = BytesUtils.byteToBool(ReadWriteIOUtils.readByte(byteBuffer));
    if (isTimesNotNull) {
      for (int i = 0; i < rowSize; i++) {
        times[i] = ReadWriteIOUtils.readLong(byteBuffer);
      }
    }

    // deserialize bitmaps
    BitMap[] bitMaps = new BitMap[schemaSize];
    boolean isBitMapsNotNull = BytesUtils.byteToBool(ReadWriteIOUtils.readByte(byteBuffer));
    if (isBitMapsNotNull) {
      bitMaps = readBitMapsFromBuffer(byteBuffer, schemaSize);
    }

    Object[] values = new Object[schemaSize];
    boolean isValuesNotNull = BytesUtils.byteToBool(ReadWriteIOUtils.readByte(byteBuffer));
    if (isValuesNotNull) {
      values = readvaluesFromBuffer(byteBuffer, dataTypes, schemaSize, rowSize);
    }

    final boolean isAligned = ReadWriteIOUtils.readBoolean(byteBuffer);

    statement.setMeasurements(measurement);
    statement.setColumnCategories(columnCategories);
    statement.setTimes(times);
    statement.setBitMaps(bitMaps);
    statement.setDataTypes(dataTypes);
    statement.setColumns(values);
    statement.setRowCount(rowSize);
    statement.setAligned(isAligned);
    return new Pair<>(statement, insertTargetName);
  }

  private static Pair<String, TSDataType> readMeasurement(ByteBuffer buffer) {

    final Pair<String, TSDataType> pair =
        new Pair<>(ReadWriteIOUtils.readString(buffer), TSDataType.deserializeFrom(buffer));

    ReadWriteIOUtils.readByte(buffer);

    ReadWriteIOUtils.readByte(buffer);

    int size = ReadWriteIOUtils.readInt(buffer);
    if (size > 0) {
      for (int i = 0; i < size; i++) {
        ReadWriteIOUtils.readString(buffer);
        ReadWriteIOUtils.readString(buffer);
      }
    }

    return pair;
  }

  /** deserialize bitmaps */
  private static BitMap[] readBitMapsFromBuffer(ByteBuffer byteBuffer, int columns) {
    BitMap[] bitMaps = new BitMap[columns];
    for (int i = 0; i < columns; i++) {
      boolean hasBitMap = BytesUtils.byteToBool(ReadWriteIOUtils.readByte(byteBuffer));
      if (hasBitMap) {
        final int size = ReadWriteIOUtils.readInt(byteBuffer);
        final Binary valueBinary = ReadWriteIOUtils.readBinary(byteBuffer);
        bitMaps[i] = new BitMap(size, valueBinary.getValues());
      }
    }
    return bitMaps;
  }

  /**
   * @param byteBuffer data values
   * @param columns column number
   */
  @SuppressWarnings("squid:S3776") // Suppress high Cognitive Complexity warning
  private static Object[] readvaluesFromBuffer(
      ByteBuffer byteBuffer, TSDataType[] types, int columns, int rowSize) {
    Object[] values = new Object[columns];
    for (int i = 0; i < columns; i++) {
      boolean isValueColumnsNotNull = BytesUtils.byteToBool(ReadWriteIOUtils.readByte(byteBuffer));
      if (isValueColumnsNotNull && types[i] == null) {
        continue;
      }
      switch (types[i]) {
        case BOOLEAN:
          boolean[] boolValues = new boolean[rowSize];
          if (isValueColumnsNotNull) {
            for (int index = 0; index < rowSize; index++) {
              boolValues[index] = BytesUtils.byteToBool(ReadWriteIOUtils.readByte(byteBuffer));
            }
          }
          values[i] = boolValues;
          break;
        case INT32:
        case DATE:
          int[] intValues = new int[rowSize];
          if (isValueColumnsNotNull) {
            for (int index = 0; index < rowSize; index++) {
              intValues[index] = ReadWriteIOUtils.readInt(byteBuffer);
            }
          }
          values[i] = intValues;
          break;
        case INT64:
        case TIMESTAMP:
          long[] longValues = new long[rowSize];
          if (isValueColumnsNotNull) {
            for (int index = 0; index < rowSize; index++) {
              longValues[index] = ReadWriteIOUtils.readLong(byteBuffer);
            }
          }
          values[i] = longValues;
          break;
        case FLOAT:
          float[] floatValues = new float[rowSize];
          if (isValueColumnsNotNull) {
            for (int index = 0; index < rowSize; index++) {
              floatValues[index] = ReadWriteIOUtils.readFloat(byteBuffer);
            }
          }
          values[i] = floatValues;
          break;
        case DOUBLE:
          double[] doubleValues = new double[rowSize];
          if (isValueColumnsNotNull) {
            for (int index = 0; index < rowSize; index++) {
              doubleValues[index] = ReadWriteIOUtils.readDouble(byteBuffer);
            }
          }
          values[i] = doubleValues;
          break;
        case TEXT:
        case STRING:
        case BLOB:
          Binary[] binaryValues = new Binary[rowSize];
          if (isValueColumnsNotNull) {
            for (int index = 0; index < rowSize; index++) {
              boolean isNotNull = BytesUtils.byteToBool(ReadWriteIOUtils.readByte(byteBuffer));
              if (isNotNull) {
                binaryValues[index] = ReadWriteIOUtils.readBinary(byteBuffer);
              } else {
                binaryValues[index] = Binary.EMPTY_VALUE;
              }
            }
          } else {
            Arrays.fill(binaryValues, Binary.EMPTY_VALUE);
          }
          values[i] = binaryValues;
          break;
        default:
          throw new UnSupportedDataTypeException(
              String.format("data type %s is not supported when convert data at client", types[i]));
      }
    }
    return values;
  }
}
