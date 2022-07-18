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

package org.apache.iotdb.db.mpp.transformation.datastructure.row;

import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.mpp.transformation.datastructure.SerializableList;
import org.apache.iotdb.tsfile.exception.write.UnSupportedDataTypeException;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.utils.Binary;
import org.apache.iotdb.tsfile.utils.PublicBAOS;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import static org.apache.iotdb.commons.conf.IoTDBConstant.MB;

public class SerializableRowRecordList implements SerializableList {

  protected static final int MIN_OBJECT_HEADER_SIZE = 8;
  protected static final int MIN_ARRAY_HEADER_SIZE = MIN_OBJECT_HEADER_SIZE + 4;

  public static SerializableRowRecordList newSerializableRowRecordList(
      long queryId, TSDataType[] dataTypes, int internalRowRecordListCapacity) {
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

    // 1 extra bit for null fields mark in bitMap
    int size = (int) (memoryLimitInMB * MB / 2 / (rowLength + ReadWriteIOUtils.BIT_LEN));
    if (size <= 0) {
      throw new QueryProcessException("Memory is not enough for current query.");
    }
    return size;
  }

  private final SerializationRecorder serializationRecorder;
  private final TSDataType[] dataTypes;
  private final int internalRowRecordListCapacity;
  private final int seriesNumber;

  private List<Object[]> rowRecords;

  private SerializableRowRecordList(
      SerializationRecorder serializationRecorder,
      TSDataType[] dataTypes,
      int internalRowRecordListCapacity) {
    this.serializationRecorder = serializationRecorder;
    this.dataTypes = dataTypes;
    this.internalRowRecordListCapacity = internalRowRecordListCapacity;
    seriesNumber = dataTypes.length;
    init();
  }

  public int size() {
    return rowRecords.size();
  }

  public Object[] getRowRecord(int index) {
    return rowRecords.get(index);
  }

  public long getTime(int index) {
    return (long) rowRecords.get(index)[seriesNumber];
  }

  public void put(Object[] rowRecord) {
    rowRecords.add(rowRecord);
  }

  @Override
  public void release() {
    rowRecords = null;
  }

  @Override
  public void init() {
    rowRecords = new ArrayList<>(internalRowRecordListCapacity);
  }

  @Override
  public void serialize(PublicBAOS outputStream) throws IOException {
    int size = rowRecords.size();
    serializationRecorder.setSerializedElementSize(size);
    int serializedByteLength = 0;
    int nullCount = 0;
    for (Object[] record : rowRecords) {
      if (record != null) {
        break;
      }
      ++nullCount;
    }
    serializedByteLength += ReadWriteIOUtils.write(nullCount, outputStream);
    for (int i = nullCount; i < size; ++i) {
      Object[] rowRecord = rowRecords.get(i);
      serializedByteLength += ReadWriteIOUtils.write((long) rowRecord[seriesNumber], outputStream);
      serializedByteLength += writeFields(rowRecord, outputStream);
    }
    serializationRecorder.setSerializedByteLength(serializedByteLength);
  }

  @Override
  public void deserialize(ByteBuffer byteBuffer) {
    int serializedElementSize = serializationRecorder.getSerializedElementSize();
    int nullCount = ReadWriteIOUtils.readInt(byteBuffer);
    for (int i = 0; i < nullCount; ++i) {
      put(null);
    }
    for (int i = nullCount; i < serializedElementSize; ++i) {
      Object[] rowRecord = new Object[seriesNumber + 1];
      rowRecord[seriesNumber] = ReadWriteIOUtils.readLong(byteBuffer); // timestamp
      readFields(byteBuffer, rowRecord);
      put(rowRecord);
    }
  }

  private int writeFields(Object[] rowRecord, PublicBAOS outputStream) throws IOException {
    int serializedByteLength = 0;
    for (int i = 0; i < seriesNumber; ++i) {
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
    for (int i = 0; i < seriesNumber; ++i) {
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
