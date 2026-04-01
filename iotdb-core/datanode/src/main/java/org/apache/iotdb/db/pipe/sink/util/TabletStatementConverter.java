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

import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.schema.table.column.TsTableColumnCategory;
import org.apache.iotdb.db.pipe.resource.memory.InsertNodeMemoryEstimator;
import org.apache.iotdb.db.queryengine.plan.analyze.cache.schema.DataNodeDevicePathCache;
import org.apache.iotdb.db.queryengine.plan.statement.crud.InsertTabletStatement;

import org.apache.tsfile.enums.ColumnCategory;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.utils.BitMap;
import org.apache.tsfile.utils.BytesUtils;
import org.apache.tsfile.utils.Pair;
import org.apache.tsfile.utils.ReadWriteIOUtils;
import org.apache.tsfile.write.UnSupportedDataTypeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.Arrays;

/**
 * Utility class for converting between InsertTabletStatement and Tablet format ByteBuffer. This
 * avoids creating intermediate Tablet objects and directly converts between formats with only the
 * fields needed.
 */
public class TabletStatementConverter {

  private static final Logger LOGGER = LoggerFactory.getLogger(TabletStatementConverter.class);

  // Memory calculation constants - extracted from RamUsageEstimator for better performance
  private static final long NUM_BYTES_ARRAY_HEADER =
      org.apache.tsfile.utils.RamUsageEstimator.NUM_BYTES_ARRAY_HEADER;
  private static final long NUM_BYTES_OBJECT_REF =
      org.apache.tsfile.utils.RamUsageEstimator.NUM_BYTES_OBJECT_REF;
  private static final long NUM_BYTES_OBJECT_HEADER =
      org.apache.tsfile.utils.RamUsageEstimator.NUM_BYTES_OBJECT_HEADER;
  private static final long SIZE_OF_ARRAYLIST =
      org.apache.tsfile.utils.RamUsageEstimator.shallowSizeOfInstance(java.util.ArrayList.class);
  private static final long SIZE_OF_BITMAP =
      org.apache.tsfile.utils.RamUsageEstimator.shallowSizeOfInstance(
          org.apache.tsfile.utils.BitMap.class);

  private TabletStatementConverter() {
    // Utility class, no instantiation
  }

  /**
   * Deserialize InsertTabletStatement from Tablet format ByteBuffer.
   *
   * @param byteBuffer ByteBuffer containing serialized data
   * @param readDatabaseName whether to read databaseName from buffer (for V2 format)
   * @return InsertTabletStatement with all fields set, including devicePath
   */
  public static InsertTabletStatement deserializeStatementFromTabletFormat(
      final ByteBuffer byteBuffer, final boolean readDatabaseName) throws IllegalPathException {
    final InsertTabletStatement statement = new InsertTabletStatement();

    // Calculate memory size during deserialization, use INSTANCE_SIZE constant
    long memorySize = InsertTabletStatement.getInstanceSize();

    final String insertTargetName = ReadWriteIOUtils.readString(byteBuffer);

    final int rowSize = ReadWriteIOUtils.readInt(byteBuffer);

    // deserialize schemas
    final int schemaSize =
        BytesUtils.byteToBool(ReadWriteIOUtils.readByte(byteBuffer))
            ? ReadWriteIOUtils.readInt(byteBuffer)
            : 0;
    final String[] measurement = new String[schemaSize];
    final TsTableColumnCategory[] columnCategories = new TsTableColumnCategory[schemaSize];
    final TSDataType[] dataTypes = new TSDataType[schemaSize];

    // Calculate memory for arrays headers and references during deserialization
    // measurements array: array header + object references
    long measurementMemorySize =
        org.apache.tsfile.utils.RamUsageEstimator.alignObjectSize(
            NUM_BYTES_ARRAY_HEADER + NUM_BYTES_OBJECT_REF * schemaSize);

    // dataTypes array: shallow size (array header + references)
    long dataTypesMemorySize =
        org.apache.tsfile.utils.RamUsageEstimator.alignObjectSize(
            NUM_BYTES_ARRAY_HEADER + NUM_BYTES_OBJECT_REF * schemaSize);

    // columnCategories array: shallow size (array header + references)
    long columnCategoriesMemorySize =
        org.apache.tsfile.utils.RamUsageEstimator.alignObjectSize(
            NUM_BYTES_ARRAY_HEADER + NUM_BYTES_OBJECT_REF * schemaSize);

    // tagColumnIndices (TAG columns): ArrayList base + array header
    long tagColumnIndicesSize = SIZE_OF_ARRAYLIST;
    tagColumnIndicesSize += NUM_BYTES_ARRAY_HEADER;

    // Deserialize and calculate memory in the same loop
    for (int i = 0; i < schemaSize; i++) {
      final boolean hasSchema = BytesUtils.byteToBool(ReadWriteIOUtils.readByte(byteBuffer));
      if (hasSchema) {
        final Pair<String, TSDataType> pair = readMeasurement(byteBuffer);
        measurement[i] = pair.getLeft();
        dataTypes[i] = pair.getRight();
        columnCategories[i] =
            TsTableColumnCategory.fromTsFileColumnCategory(
                ColumnCategory.values()[byteBuffer.get()]);

        // Calculate memory for each measurement string
        if (measurement[i] != null) {
          measurementMemorySize += org.apache.tsfile.utils.RamUsageEstimator.sizeOf(measurement[i]);
        }

        // Calculate memory for TAG column indices
        if (columnCategories[i] != null && columnCategories[i].equals(TsTableColumnCategory.TAG)) {
          tagColumnIndicesSize +=
              org.apache.tsfile.utils.RamUsageEstimator.alignObjectSize(
                      Integer.BYTES + NUM_BYTES_OBJECT_HEADER)
                  + NUM_BYTES_OBJECT_REF;
        }
      }
    }

    // Add all calculated memory to total
    memorySize += measurementMemorySize;
    memorySize += dataTypesMemorySize;

    // deserialize times and calculate memory during deserialization
    final long[] times = new long[rowSize];
    // Calculate memory: array header + long size * rowSize
    final long timesMemorySize =
        org.apache.tsfile.utils.RamUsageEstimator.alignObjectSize(
            NUM_BYTES_ARRAY_HEADER + (long) Long.BYTES * rowSize);

    final boolean isTimesNotNull = BytesUtils.byteToBool(ReadWriteIOUtils.readByte(byteBuffer));
    if (isTimesNotNull) {
      for (int i = 0; i < rowSize; i++) {
        times[i] = ReadWriteIOUtils.readLong(byteBuffer);
      }
    }

    // Add times memory to total
    memorySize += timesMemorySize;

    // deserialize bitmaps and calculate memory during deserialization
    final BitMap[] bitMaps;
    final long bitMapsMemorySize;

    final boolean isBitMapsNotNull = BytesUtils.byteToBool(ReadWriteIOUtils.readByte(byteBuffer));
    if (isBitMapsNotNull) {
      // Use the method that returns both BitMap array and memory size
      final Pair<BitMap[], Long> bitMapsAndMemory =
          readBitMapsFromBufferWithMemory(byteBuffer, schemaSize);
      bitMaps = bitMapsAndMemory.getLeft();
      bitMapsMemorySize = bitMapsAndMemory.getRight();
    } else {
      // Calculate memory for empty BitMap array: array header + references
      bitMaps = new BitMap[schemaSize];
      bitMapsMemorySize =
          org.apache.tsfile.utils.RamUsageEstimator.alignObjectSize(
              NUM_BYTES_ARRAY_HEADER + NUM_BYTES_OBJECT_REF * schemaSize);
    }

    // Add bitMaps memory to total
    memorySize += bitMapsMemorySize;

    // Deserialize values and calculate memory during deserialization
    final Object[] values;
    final long valuesMemorySize;

    final boolean isValuesNotNull = BytesUtils.byteToBool(ReadWriteIOUtils.readByte(byteBuffer));
    if (isValuesNotNull) {
      // Use the method that returns both values array and memory size
      final Pair<Object[], Long> valuesAndMemory =
          readValuesFromBufferWithMemory(byteBuffer, dataTypes, schemaSize, rowSize);
      values = valuesAndMemory.getLeft();
      valuesMemorySize = valuesAndMemory.getRight();
    } else {
      // Calculate memory for empty values array: array header + references
      values = new Object[schemaSize];
      valuesMemorySize =
          org.apache.tsfile.utils.RamUsageEstimator.alignObjectSize(
              NUM_BYTES_ARRAY_HEADER + NUM_BYTES_OBJECT_REF * schemaSize);
    }

    // Add values memory to total
    memorySize += valuesMemorySize;

    final boolean isAligned = ReadWriteIOUtils.readBoolean(byteBuffer);

    statement.setMeasurements(measurement);
    statement.setTimes(times);
    statement.setBitMaps(bitMaps);
    statement.setDataTypes(dataTypes);
    statement.setColumns(values);
    statement.setRowCount(rowSize);
    statement.setAligned(isAligned);

    // Read databaseName if requested (V2 format)
    if (readDatabaseName) {
      final String databaseName = ReadWriteIOUtils.readString(byteBuffer);
      if (databaseName != null) {
        statement.setDatabaseName(databaseName);
        statement.setWriteToTable(true);
        // For table model, insertTargetName is table name, convert to lowercase
        statement.setDevicePath(new PartialPath(insertTargetName.toLowerCase(), false));
        // Calculate memory for databaseName
        memorySize += org.apache.tsfile.utils.RamUsageEstimator.sizeOf(databaseName);

        statement.setColumnCategories(columnCategories);

        memorySize += columnCategoriesMemorySize;
        memorySize += tagColumnIndicesSize;
      } else {
        // For tree model, use DataNodeDevicePathCache
        statement.setDevicePath(
            DataNodeDevicePathCache.getInstance().getPartialPath(insertTargetName));
        statement.setColumnCategories(null);
      }
    } else {
      // V1 format: no databaseName in buffer, always use DataNodeDevicePathCache
      statement.setDevicePath(
          DataNodeDevicePathCache.getInstance().getPartialPath(insertTargetName));
      statement.setColumnCategories(null);
    }

    // Calculate memory for devicePath
    memorySize += InsertNodeMemoryEstimator.sizeOfPartialPath(statement.getDevicePath());

    // Set the pre-calculated memory size to avoid recalculation
    statement.setRamBytesUsed(memorySize);

    return statement;
  }

  /**
   * Deserialize InsertTabletStatement from Tablet format ByteBuffer (V1 format, no databaseName).
   *
   * @param byteBuffer ByteBuffer containing serialized data
   * @return InsertTabletStatement with devicePath set using DataNodeDevicePathCache
   */
  public static InsertTabletStatement deserializeStatementFromTabletFormat(
      final ByteBuffer byteBuffer) throws IllegalPathException {
    return deserializeStatementFromTabletFormat(byteBuffer, false);
  }

  /**
   * Skip a string in ByteBuffer without reading it. This is more efficient than reading and
   * discarding the string.
   *
   * @param buffer ByteBuffer to skip string from
   */
  private static void skipString(final ByteBuffer buffer) {
    final int size = ReadWriteIOUtils.readInt(buffer);
    if (size > 0) {
      buffer.position(buffer.position() + size);
    }
  }

  /**
   * Read measurement name and data type from buffer, skipping other measurement schema fields
   * (encoding, compression, and tags/attributes) that are not needed for InsertTabletStatement.
   *
   * @param buffer ByteBuffer containing serialized measurement schema
   * @return Pair of measurement name and data type
   */
  private static Pair<String, TSDataType> readMeasurement(final ByteBuffer buffer) {
    // Read measurement name and data type
    final Pair<String, TSDataType> pair =
        new Pair<>(ReadWriteIOUtils.readString(buffer), TSDataType.deserializeFrom(buffer));

    // Skip encoding type (byte) and compression type (byte) - 2 bytes total
    buffer.position(buffer.position() + 2);

    // Skip props map (Map<String, String>)
    final int size = ReadWriteIOUtils.readInt(buffer);
    if (size > 0) {
      for (int i = 0; i < size; i++) {
        // Skip key (String) and value (String) without constructing temporary objects
        skipString(buffer);
        skipString(buffer);
      }
    }

    return pair;
  }

  /**
   * Deserialize bitmaps and calculate memory size during deserialization. Returns a Pair of BitMap
   * array and the calculated memory size.
   */
  private static Pair<BitMap[], Long> readBitMapsFromBufferWithMemory(
      final ByteBuffer byteBuffer, final int columns) {
    final BitMap[] bitMaps = new BitMap[columns];

    // Calculate memory: array header + object references
    long memorySize =
        org.apache.tsfile.utils.RamUsageEstimator.alignObjectSize(
            NUM_BYTES_ARRAY_HEADER + NUM_BYTES_OBJECT_REF * columns);

    for (int i = 0; i < columns; i++) {
      final boolean hasBitMap = BytesUtils.byteToBool(ReadWriteIOUtils.readByte(byteBuffer));
      if (hasBitMap) {
        final int size = ReadWriteIOUtils.readInt(byteBuffer);
        final Binary valueBinary = ReadWriteIOUtils.readBinary(byteBuffer);
        final byte[] byteArray = valueBinary.getValues();
        bitMaps[i] = new BitMap(size, byteArray);

        // Calculate memory for this BitMap: BitMap object + byte array
        // BitMap shallow size + byte array (array header + array length)
        memorySize +=
            SIZE_OF_BITMAP
                + org.apache.tsfile.utils.RamUsageEstimator.alignObjectSize(
                    NUM_BYTES_ARRAY_HEADER + byteArray.length);
      }
    }

    return new Pair<>(bitMaps, memorySize);
  }

  /**
   * Deserialize values from buffer and calculate memory size during deserialization. Returns a Pair
   * of values array and the calculated memory size.
   *
   * @param byteBuffer data values
   * @param types data types
   * @param columns column number
   * @param rowSize row number
   * @return Pair of values array and memory size
   */
  @SuppressWarnings("squid:S3776") // Suppress high Cognitive Complexity warning
  private static Pair<Object[], Long> readValuesFromBufferWithMemory(
      final ByteBuffer byteBuffer, final TSDataType[] types, final int columns, final int rowSize) {
    final Object[] values = new Object[columns];

    // Calculate memory: array header + object references
    long memorySize =
        org.apache.tsfile.utils.RamUsageEstimator.alignObjectSize(
            NUM_BYTES_ARRAY_HEADER + NUM_BYTES_OBJECT_REF * columns);

    for (int i = 0; i < columns; i++) {
      final boolean isValueColumnsNotNull =
          BytesUtils.byteToBool(ReadWriteIOUtils.readByte(byteBuffer));
      if (isValueColumnsNotNull && types[i] == null) {
        continue;
      }

      switch (types[i]) {
        case BOOLEAN:
          final boolean[] boolValues = new boolean[rowSize];
          if (isValueColumnsNotNull) {
            for (int index = 0; index < rowSize; index++) {
              boolValues[index] = BytesUtils.byteToBool(ReadWriteIOUtils.readByte(byteBuffer));
            }
          }
          values[i] = boolValues;
          // Calculate memory for boolean array: array header + 1 byte per element (aligned)
          memorySize +=
              org.apache.tsfile.utils.RamUsageEstimator.alignObjectSize(
                  NUM_BYTES_ARRAY_HEADER + rowSize);
          break;
        case INT32:
        case DATE:
          final int[] intValues = new int[rowSize];
          if (isValueColumnsNotNull) {
            for (int index = 0; index < rowSize; index++) {
              intValues[index] = ReadWriteIOUtils.readInt(byteBuffer);
            }
          }
          values[i] = intValues;
          // Calculate memory for int array: array header + 4 bytes per element (aligned)
          memorySize +=
              org.apache.tsfile.utils.RamUsageEstimator.alignObjectSize(
                  NUM_BYTES_ARRAY_HEADER + (long) Integer.BYTES * rowSize);
          break;
        case INT64:
        case TIMESTAMP:
          final long[] longValues = new long[rowSize];
          if (isValueColumnsNotNull) {
            for (int index = 0; index < rowSize; index++) {
              longValues[index] = ReadWriteIOUtils.readLong(byteBuffer);
            }
          }
          values[i] = longValues;
          // Calculate memory for long array: array header + 8 bytes per element (aligned)
          memorySize +=
              org.apache.tsfile.utils.RamUsageEstimator.alignObjectSize(
                  NUM_BYTES_ARRAY_HEADER + (long) Long.BYTES * rowSize);
          break;
        case FLOAT:
          final float[] floatValues = new float[rowSize];
          if (isValueColumnsNotNull) {
            for (int index = 0; index < rowSize; index++) {
              floatValues[index] = ReadWriteIOUtils.readFloat(byteBuffer);
            }
          }
          values[i] = floatValues;
          // Calculate memory for float array: array header + 4 bytes per element (aligned)
          memorySize +=
              org.apache.tsfile.utils.RamUsageEstimator.alignObjectSize(
                  NUM_BYTES_ARRAY_HEADER + (long) Float.BYTES * rowSize);
          break;
        case DOUBLE:
          final double[] doubleValues = new double[rowSize];
          if (isValueColumnsNotNull) {
            for (int index = 0; index < rowSize; index++) {
              doubleValues[index] = ReadWriteIOUtils.readDouble(byteBuffer);
            }
          }
          values[i] = doubleValues;
          // Calculate memory for double array: array header + 8 bytes per element (aligned)
          memorySize +=
              org.apache.tsfile.utils.RamUsageEstimator.alignObjectSize(
                  NUM_BYTES_ARRAY_HEADER + (long) Double.BYTES * rowSize);
          break;
        case TEXT:
        case STRING:
        case BLOB:
        case OBJECT:
          // Handle object array type: Binary[] is an array of objects
          final Binary[] binaryValues = new Binary[rowSize];
          // Calculate memory for Binary array: array header + object references
          long binaryArrayMemory =
              org.apache.tsfile.utils.RamUsageEstimator.alignObjectSize(
                  NUM_BYTES_ARRAY_HEADER + NUM_BYTES_OBJECT_REF * rowSize);

          if (isValueColumnsNotNull) {
            for (int index = 0; index < rowSize; index++) {
              final boolean isNotNull =
                  BytesUtils.byteToBool(ReadWriteIOUtils.readByte(byteBuffer));
              if (isNotNull) {
                binaryValues[index] = ReadWriteIOUtils.readBinary(byteBuffer);
                // Calculate memory for each Binary object during deserialization
                binaryArrayMemory += binaryValues[index].ramBytesUsed();
              } else {
                binaryValues[index] = Binary.EMPTY_VALUE;
                // EMPTY_VALUE also has memory cost
                binaryArrayMemory += Binary.EMPTY_VALUE.ramBytesUsed();
              }
            }
          } else {
            Arrays.fill(binaryValues, Binary.EMPTY_VALUE);
            // Calculate memory for all EMPTY_VALUE
            binaryArrayMemory += (long) rowSize * Binary.EMPTY_VALUE.ramBytesUsed();
          }
          values[i] = binaryValues;
          // Add calculated Binary array memory to total
          memorySize += binaryArrayMemory;
          break;
        default:
          throw new UnSupportedDataTypeException(
              String.format("data type %s is not supported when convert data at client", types[i]));
      }
    }

    return new Pair<>(values, memorySize);
  }
}
