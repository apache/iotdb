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

package org.apache.iotdb.db.queryengine.transformation.datastructure.row;

import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.queryengine.transformation.datastructure.SerializableList;
import org.apache.iotdb.tsfile.exception.write.UnSupportedDataTypeException;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.block.column.Column;
import org.apache.iotdb.tsfile.read.common.block.column.ColumnBuilder;
import org.apache.iotdb.tsfile.utils.Binary;
import org.apache.iotdb.tsfile.utils.PublicBAOS;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import static org.apache.iotdb.commons.conf.IoTDBConstant.MB;
import static org.apache.iotdb.db.queryengine.transformation.datastructure.util.BinaryUtils.MIN_ARRAY_HEADER_SIZE;
import static org.apache.iotdb.db.queryengine.transformation.datastructure.util.BinaryUtils.MIN_OBJECT_HEADER_SIZE;
import static org.apache.iotdb.db.queryengine.transformation.datastructure.util.RowColumnConverter.appendRowInColumnBuilders;
import static org.apache.iotdb.db.queryengine.transformation.datastructure.util.RowColumnConverter.buildColumnsByBuilders;
import static org.apache.iotdb.db.queryengine.transformation.datastructure.util.RowColumnConverter.constructColumnBuilders;

public class SerializableRowRecordList implements SerializableList {
  public static SerializableRowRecordList newSerializableRowRecordList(
      String queryId, TSDataType[] dataTypes, int internalRowRecordListCapacity) {
    SerializationRecorder recorder = new SerializationRecorder(queryId);
    return new SerializableRowRecordList(recorder, dataTypes, internalRowRecordListCapacity);
  }

  /**
   * Calculate the number of rows that can be cached given the memory limit.
   *
   * @param dataTypes Data types of columns.
   * @param memoryLimitInMB Memory limit.
   * @param byteArrayLengthForMemoryControl Max memory usage for a {@link TSDataType#TEXT}.
   * @return Number of rows that can be cached.
   * @throws QueryProcessException if the result capacity <= 0
   * @throws UnSupportedDataTypeException if the input datatype can not be handled by the given
   *     branches.
   */
  protected static int calculateCapacity(
      TSDataType[] dataTypes, float memoryLimitInMB, int byteArrayLengthForMemoryControl)
      throws QueryProcessException {
    int rowLength = ReadWriteIOUtils.LONG_LEN; // timestamp
    for (TSDataType dataType : dataTypes) { // fields
      switch (dataType) {
        case INT32:
          rowLength += ReadWriteIOUtils.INT_LEN;
          break;
        case INT64:
          rowLength += ReadWriteIOUtils.LONG_LEN;
          break;
        case FLOAT:
          rowLength += ReadWriteIOUtils.FLOAT_LEN;
          break;
        case DOUBLE:
          rowLength += ReadWriteIOUtils.DOUBLE_LEN;
          break;
        case BOOLEAN:
          rowLength += ReadWriteIOUtils.BOOLEAN_LEN;
          break;
        case TEXT:
          rowLength +=
              MIN_OBJECT_HEADER_SIZE + MIN_ARRAY_HEADER_SIZE + byteArrayLengthForMemoryControl;
          break;
        default:
          throw new UnSupportedDataTypeException(dataType.toString());
      }
    }
    rowLength += ReadWriteIOUtils.BIT_LEN; // null field

    int capacity = (int) (memoryLimitInMB * MB / 2 / rowLength);
    if (capacity <= 0) {
      throw new QueryProcessException("Memory is not enough for current query.");
    }
    return capacity;
  }

  private final SerializationRecorder serializationRecorder;
  private final TSDataType[] dataTypes;
  private final int internalRowRecordListCapacity;
  private final int valueColumnCount;

  private List<Column[]> blocks;

  private int skipPrefixNullCount;

  private int prefixNullCount;

  private boolean isAllNull;

  private SerializableRowRecordList(
      SerializationRecorder serializationRecorder,
      TSDataType[] dataTypes,
      int internalRowRecordListCapacity) {
    this.serializationRecorder = serializationRecorder;
    this.dataTypes = dataTypes;
    this.internalRowRecordListCapacity = internalRowRecordListCapacity;

    valueColumnCount = dataTypes.length;
    prefixNullCount = 0;
    skipPrefixNullCount = 0;
    isAllNull = true;

    init();
  }

  public int size() {
    return prefixNullCount + skipPrefixNullCount;
  }

  public int getBlockCount() {
    return blocks.size();
  }

  public Object[] getRow(int index) {
    // Fall into prefix nulls
    if (index < prefixNullCount) {
      return null;
    }

    return getRowSkipPrefixNulls(index - prefixNullCount);
  }

  private Object[] getRowSkipPrefixNulls(int index) {
    assert index < skipPrefixNullCount;

    // Value columns + time column
    Object[] row = new Object[valueColumnCount + 1];

    int total = 0;
    for (Column[] block : blocks) {
      // Find position
      int length = block[0].getPositionCount();
      if (index < total + length) {
        int offset = index - total;

        // Fill value columns
        for (int i = 0; i < block.length - 1; i++) {
          switch (dataTypes[i]) {
            case INT32:
              row[i] = block[i].getInt(offset);
              break;
            case INT64:
              row[i] = block[i].getLong(offset);
              break;
            case FLOAT:
              row[i] = block[i].getFloat(offset);
              break;
            case DOUBLE:
              row[i] = block[i].getDouble(offset);
              break;
            case BOOLEAN:
              row[i] = block[i].getBoolean(offset);
              break;
            case TEXT:
              row[i] = block[i].getBinary(offset);
              break;
            default:
              throw new UnSupportedDataTypeException(dataTypes[i].toString());
          }
        }
        // Fill time column
        row[block.length - 1] = block[block.length - 1].getLong(offset);

        break;
      }
      total += length;
    }

    return row;
  }

  public Column[] getColumns(int index) {
    return blocks.get(index);
  }

  public long getTime(int index) {
    // Would never access null row's time
    assert index >= prefixNullCount;

    return getTimeSkipPrefixNulls(index - prefixNullCount);
  }

  private long getTimeSkipPrefixNulls(int index) {
    assert index < skipPrefixNullCount;

    int total = 0;
    long time = -1;
    for (Column[] block : blocks) {
      int length = block[0].getPositionCount();
      if (index < total + length) {
        int offset = index - total;

        // Last column is always time column
        time = block[valueColumnCount].getLong(offset);
        break;
      }
      total += length;
    }

    return time;
  }

  @Deprecated
  public void putRow(Object[] rowRecord) {}

  public void putNulls(int nullCount) {
    assert isAllNull;
    prefixNullCount += nullCount;
  }

  public void putColumns(Column[] columns) {
    isAllNull = false;

    blocks.add(columns);
    skipPrefixNullCount += columns[0].getPositionCount();
  }

  @Override
  public void release() {
    blocks = null;
  }

  @Override
  public void init() {
    blocks = new ArrayList<>();
  }

  @Override
  public void serialize(PublicBAOS outputStream) throws IOException {
    // Write size field
    int total = size();
    serializationRecorder.setSerializedElementSize(total);
    // Write prefix null count field
    int serializedByteLength = 0;
    serializedByteLength += ReadWriteIOUtils.write(prefixNullCount, outputStream);
    // Write subsequent rows
    for (int i = 0; i < skipPrefixNullCount; ++i) {
      Object[] rowRecord = getRowSkipPrefixNulls(i);
      serializedByteLength +=
          ReadWriteIOUtils.write((long) rowRecord[valueColumnCount], outputStream);
      serializedByteLength += writeFields(rowRecord, outputStream);
    }
    serializationRecorder.setSerializedByteLength(serializedByteLength);
  }

  @Override
  public void deserialize(ByteBuffer byteBuffer) {
    // Read total size
    int serializedElementSize = serializationRecorder.getSerializedElementSize();
    // Read prefix null count size and set relevant field
    isAllNull = true;
    prefixNullCount = ReadWriteIOUtils.readInt(byteBuffer);
    skipPrefixNullCount = serializedElementSize - prefixNullCount;
    putNulls(prefixNullCount);
    // Read subsequent rows
    ColumnBuilder[] builders = constructColumnBuilders(dataTypes, skipPrefixNullCount);
    for (int i = 0; i < skipPrefixNullCount; ++i) {
      Object[] rowRecord = new Object[valueColumnCount + 1];
      rowRecord[valueColumnCount] = ReadWriteIOUtils.readLong(byteBuffer); // timestamp
      readFields(byteBuffer, rowRecord);
      appendRowInColumnBuilders(dataTypes, rowRecord, builders);
    }
    // Discard old columns and build a new one
    blocks = new ArrayList<>();
    blocks.add(buildColumnsByBuilders(dataTypes, builders));
  }

  private int writeFields(Object[] rowRecord, PublicBAOS outputStream) throws IOException {
    int serializedByteLength = 0;
    for (int i = 0; i < valueColumnCount; ++i) {
      Object field = rowRecord[i];
      boolean isNull = field == null;
      serializedByteLength += ReadWriteIOUtils.write(isNull, outputStream);
      if (isNull) {
        continue;
      }

      switch (dataTypes[i]) {
        case INT32:
          serializedByteLength += ReadWriteIOUtils.write((int) field, outputStream);
          break;
        case INT64:
          serializedByteLength += ReadWriteIOUtils.write((long) field, outputStream);
          break;
        case FLOAT:
          serializedByteLength += ReadWriteIOUtils.write((float) field, outputStream);
          break;
        case DOUBLE:
          serializedByteLength += ReadWriteIOUtils.write((double) field, outputStream);
          break;
        case BOOLEAN:
          serializedByteLength += ReadWriteIOUtils.write((boolean) field, outputStream);
          break;
        case TEXT:
          serializedByteLength += ReadWriteIOUtils.write((Binary) field, outputStream);
          break;
        default:
          throw new UnSupportedDataTypeException(dataTypes[i].toString());
      }
    }
    return serializedByteLength;
  }

  private void readFields(ByteBuffer byteBuffer, Object[] rowRecord) {
    for (int i = 0; i < valueColumnCount; ++i) {
      boolean isNull = ReadWriteIOUtils.readBool(byteBuffer);
      if (isNull) {
        continue;
      }

      switch (dataTypes[i]) {
        case INT32:
          rowRecord[i] = ReadWriteIOUtils.readInt(byteBuffer);
          break;
        case INT64:
          rowRecord[i] = ReadWriteIOUtils.readLong(byteBuffer);
          break;
        case FLOAT:
          rowRecord[i] = ReadWriteIOUtils.readFloat(byteBuffer);
          break;
        case DOUBLE:
          rowRecord[i] = ReadWriteIOUtils.readDouble(byteBuffer);
          break;
        case BOOLEAN:
          rowRecord[i] = ReadWriteIOUtils.readBool(byteBuffer);
          break;
        case TEXT:
          rowRecord[i] = ReadWriteIOUtils.readBinary(byteBuffer);
          break;
        default:
          throw new UnSupportedDataTypeException(dataTypes[i].toString());
      }
    }
  }

  @Override
  public SerializationRecorder getSerializationRecorder() {
    return serializationRecorder;
  }
}
