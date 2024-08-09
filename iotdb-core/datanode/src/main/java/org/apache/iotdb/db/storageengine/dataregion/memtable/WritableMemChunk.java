/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iotdb.db.storageengine.dataregion.memtable;

import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.storageengine.dataregion.wal.buffer.IWALByteBufferView;
import org.apache.iotdb.db.utils.datastructure.TVList;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.utils.BitMap;
import org.apache.tsfile.write.UnSupportedDataTypeException;
import org.apache.tsfile.write.chunk.ChunkWriterImpl;
import org.apache.tsfile.write.chunk.IChunkWriter;
import org.apache.tsfile.write.schema.IMeasurementSchema;
import org.apache.tsfile.write.schema.MeasurementSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.Callable;

public class WritableMemChunk implements IWritableMemChunk {

  private IMeasurementSchema schema;
  private TVList list;
  private static final String UNSUPPORTED_TYPE = "Unsupported data type:";
  private static final Logger LOGGER = LoggerFactory.getLogger(WritableMemChunk.class);

  private static final IoTDBConfig CONFIG = IoTDBDescriptor.getInstance().getConfig();

  public WritableMemChunk(IMeasurementSchema schema) {
    this.schema = schema;
    this.list = TVList.newList(schema.getType());
  }

  private WritableMemChunk() {}

  @Override
  public boolean writeWithFlushCheck(long insertTime, Object objectValue) {
    switch (schema.getType()) {
      case BOOLEAN:
        return putBooleanWithFlushCheck(insertTime, (boolean) objectValue);
      case INT32:
      case DATE:
        return putIntWithFlushCheck(insertTime, (int) objectValue);
      case INT64:
      case TIMESTAMP:
        return putLongWithFlushCheck(insertTime, (long) objectValue);
      case FLOAT:
        return putFloatWithFlushCheck(insertTime, (float) objectValue);
      case DOUBLE:
        return putDoubleWithFlushCheck(insertTime, (double) objectValue);
      case TEXT:
      case BLOB:
      case STRING:
        return putBinaryWithFlushCheck(insertTime, (Binary) objectValue);
      default:
        throw new UnSupportedDataTypeException(UNSUPPORTED_TYPE + schema.getType().name());
    }
  }

  @Override
  public boolean writeAlignedValueWithFlushCheck(
      long insertTime, Object[] objectValue, List<IMeasurementSchema> schemaList) {
    throw new UnSupportedDataTypeException(UNSUPPORTED_TYPE + list.getDataType());
  }

  @Override
  public boolean writeWithFlushCheck(
      long[] times, Object valueList, BitMap bitMap, TSDataType dataType, int start, int end) {
    switch (dataType) {
      case BOOLEAN:
        boolean[] boolValues = (boolean[]) valueList;
        return putBooleansWithFlushCheck(times, boolValues, bitMap, start, end);
      case INT32:
      case DATE:
        int[] intValues = (int[]) valueList;
        return putIntsWithFlushCheck(times, intValues, bitMap, start, end);
      case INT64:
      case TIMESTAMP:
        long[] longValues = (long[]) valueList;
        return putLongsWithFlushCheck(times, longValues, bitMap, start, end);
      case FLOAT:
        float[] floatValues = (float[]) valueList;
        return putFloatsWithFlushCheck(times, floatValues, bitMap, start, end);
      case DOUBLE:
        double[] doubleValues = (double[]) valueList;
        return putDoublesWithFlushCheck(times, doubleValues, bitMap, start, end);
      case TEXT:
      case BLOB:
      case STRING:
        Binary[] binaryValues = (Binary[]) valueList;
        return putBinariesWithFlushCheck(times, binaryValues, bitMap, start, end);
      default:
        throw new UnSupportedDataTypeException(UNSUPPORTED_TYPE + dataType.name());
    }
  }

  @Override
  public boolean writeAlignedValuesWithFlushCheck(
      long[] times,
      Object[] valueList,
      BitMap[] bitMaps,
      List<IMeasurementSchema> schemaList,
      int start,
      int end) {
    throw new UnSupportedDataTypeException(UNSUPPORTED_TYPE + list.getDataType());
  }

  @Override
  public boolean putLongWithFlushCheck(long t, long v) {
    list.putLong(t, v);
    return list.reachChunkSizeOrPointNumThreshold();
  }

  @Override
  public boolean putIntWithFlushCheck(long t, int v) {
    list.putInt(t, v);
    return list.reachChunkSizeOrPointNumThreshold();
  }

  @Override
  public boolean putFloatWithFlushCheck(long t, float v) {
    list.putFloat(t, v);
    return list.reachChunkSizeOrPointNumThreshold();
  }

  @Override
  public boolean putDoubleWithFlushCheck(long t, double v) {
    list.putDouble(t, v);
    return list.reachChunkSizeOrPointNumThreshold();
  }

  @Override
  public boolean putBinaryWithFlushCheck(long t, Binary v) {
    list.putBinary(t, v);
    return list.reachChunkSizeOrPointNumThreshold();
  }

  @Override
  public boolean putBooleanWithFlushCheck(long t, boolean v) {
    list.putBoolean(t, v);
    return list.reachChunkSizeOrPointNumThreshold();
  }

  @Override
  public boolean putAlignedValueWithFlushCheck(long t, Object[] v) {
    throw new UnSupportedDataTypeException(UNSUPPORTED_TYPE + schema.getType());
  }

  @Override
  public boolean putLongsWithFlushCheck(long[] t, long[] v, BitMap bitMap, int start, int end) {
    list.putLongs(t, v, bitMap, start, end);
    return list.reachChunkSizeOrPointNumThreshold();
  }

  @Override
  public boolean putIntsWithFlushCheck(long[] t, int[] v, BitMap bitMap, int start, int end) {
    list.putInts(t, v, bitMap, start, end);
    return list.reachChunkSizeOrPointNumThreshold();
  }

  @Override
  public boolean putFloatsWithFlushCheck(long[] t, float[] v, BitMap bitMap, int start, int end) {
    list.putFloats(t, v, bitMap, start, end);
    return list.reachChunkSizeOrPointNumThreshold();
  }

  @Override
  public boolean putDoublesWithFlushCheck(long[] t, double[] v, BitMap bitMap, int start, int end) {
    list.putDoubles(t, v, bitMap, start, end);
    return list.reachChunkSizeOrPointNumThreshold();
  }

  @Override
  public boolean putBinariesWithFlushCheck(
      long[] t, Binary[] v, BitMap bitMap, int start, int end) {
    list.putBinaries(t, v, bitMap, start, end);
    return list.reachChunkSizeOrPointNumThreshold();
  }

  @Override
  public boolean putBooleansWithFlushCheck(
      long[] t, boolean[] v, BitMap bitMap, int start, int end) {
    list.putBooleans(t, v, bitMap, start, end);
    return list.reachChunkSizeOrPointNumThreshold();
  }

  @Override
  public boolean putAlignedValuesWithFlushCheck(
      long[] t, Object[] v, BitMap[] bitMaps, int start, int end) {
    throw new UnSupportedDataTypeException(UNSUPPORTED_TYPE + schema.getType());
  }

  @Override
  public synchronized TVList getSortedTvListForQuery() {
    sortTVList();
    // increase reference count
    list.increaseReferenceCount();
    return list;
  }

  @Override
  public synchronized TVList getSortedTvListForQuery(List<IMeasurementSchema> measurementSchema) {
    throw new UnSupportedDataTypeException(UNSUPPORTED_TYPE + list.getDataType());
  }

  private void sortTVList() {
    // check reference count
    if ((list.getReferenceCount() > 0 && !list.isSorted())) {
      list = list.clone();
    }

    if (!list.isSorted()) {
      list.sort();
    }
  }

  @Override
  public synchronized void sortTvListForFlush() {
    sortTVList();
  }

  @Override
  public TVList getTVList() {
    return list;
  }

  @Override
  public long count() {
    return list.rowCount();
  }

  @Override
  public IMeasurementSchema getSchema() {
    return schema;
  }

  @Override
  public long getMaxTime() {
    return list.getMaxTime();
  }

  @Override
  public long getFirstPoint() {
    if (list.rowCount() == 0) {
      return Long.MAX_VALUE;
    }
    return getSortedTvListForQuery().getTimeValuePair(0).getTimestamp();
  }

  @Override
  public long getLastPoint() {
    if (list.rowCount() == 0) {
      return Long.MIN_VALUE;
    }
    return getSortedTvListForQuery()
        .getTimeValuePair(getSortedTvListForQuery().rowCount() - 1)
        .getTimestamp();
  }

  @Override
  public boolean isEmpty() {
    return list.rowCount() == 0;
  }

  @Override
  public int delete(long lowerBound, long upperBound) {
    return list.delete(lowerBound, upperBound);
  }

  @Override
  public IChunkWriter createIChunkWriter() {
    return new ChunkWriterImpl(schema, list.rowCount());
  }

  @Override
  public String toString() {
    int size = list.rowCount();
    int firstIndex = 0;
    int lastIndex = size - 1;
    long minTime = Long.MAX_VALUE;
    long maxTime = Long.MIN_VALUE;
    for (int i = 0; i < size; i++) {
      long currentTime = list.getTime(i);
      if (currentTime < minTime) {
        firstIndex = i;
        minTime = currentTime;
      }
      if (currentTime >= maxTime) {
        lastIndex = i;
        maxTime = currentTime;
      }
    }

    StringBuilder out = new StringBuilder("MemChunk Size: " + size + System.lineSeparator());
    if (size != 0) {
      out.append("Data type:").append(schema.getType()).append(System.lineSeparator());
      out.append("First point:")
          .append(list.getTimeValuePair(firstIndex))
          .append(System.lineSeparator());
      out.append("Last point:")
          .append(list.getTimeValuePair(lastIndex))
          .append(System.lineSeparator());
    }
    return out.toString();
  }

  public Object newArrayByType(TSDataType type, int length) {
    switch (type) {
      case BOOLEAN:
        return new boolean[length];
      case INT32:
      case DATE:
        return new int[length];
      case INT64:
      case TIMESTAMP:
        return new long[length];
      case FLOAT:
        return new float[length];
      case DOUBLE:
        return new double[length];
      case TEXT:
      case BLOB:
      case STRING:
        return new Binary[length];
      default:
        return null;
    }
  }

  private void flushBatchToWriter(ChunkWriterImpl writer, long[] times, Object values, int length) {
    switch (schema.getType()) {
      case BOOLEAN:
        writer.write(times, (boolean[]) values, length);
        break;
      case INT32:
      case DATE:
        writer.write(times, (int[]) values, length);
        break;
      case INT64:
      case TIMESTAMP:
        writer.write(times, (long[]) values, length);
        break;
      case FLOAT:
        writer.write(times, (float[]) values, length);
        break;
      case DOUBLE:
        writer.write(times, (double[]) values, length);
        break;
      case TEXT:
      case BLOB:
      case STRING:
        writer.write(times, (Binary[]) values, length);
        break;
      default:
        LOGGER.error("WritableMemChunk does not support data type: {}", schema.getType());
        break;
    }
  }

  @FunctionalInterface
  interface Exec<T extends TSDataType, V> {
    void run(T type, Callable<Void> func, V args) throws Exception;
  }

  @Override
  public void encode(IChunkWriter chunkWriter) {

    ChunkWriterImpl chunkWriterImpl = (ChunkWriterImpl) chunkWriter;
    int batchSize = Math.min(512, list.rowCount());
    Object values = newArrayByType(schema.getType(), batchSize);
    long[] times = new long[batchSize];
    int idx = 0;
    TSDataType tsDataType = schema.getType();

    for (int sortedRowIndex = 0; sortedRowIndex < list.rowCount(); sortedRowIndex++) {
      long time = list.getTime(sortedRowIndex);

      // skip duplicated data
      if ((sortedRowIndex + 1 < list.rowCount() && (time == list.getTime(sortedRowIndex + 1)))) {
        continue;
      }

      // store last point for SDT
      if (sortedRowIndex + 1 == list.rowCount()) {
        chunkWriterImpl.setLastPoint(true);
      }
      times[idx] = time;
      switch (tsDataType) {
        case BOOLEAN:
          ((boolean[]) values)[idx++] = list.getBoolean(sortedRowIndex);
          break;
        case INT32:
        case DATE:
          ((int[]) values)[idx++] = list.getInt(sortedRowIndex);
          break;
        case INT64:
        case TIMESTAMP:
          ((long[]) values)[idx++] = list.getLong(sortedRowIndex);
          break;
        case FLOAT:
          ((float[]) values)[idx++] = list.getFloat(sortedRowIndex);
          break;
        case DOUBLE:
          ((double[]) values)[idx++] = list.getDouble(sortedRowIndex);
          break;
        case TEXT:
        case BLOB:
        case STRING:
          ((Binary[]) values)[idx++] = list.getBinary(sortedRowIndex);
          break;
        default:
          LOGGER.error("WritableMemChunk does not support data type: {}", tsDataType);
          break;
      }

      // batch write, the code is redundant, we can use lambda to reduce the redundant codes
      // exec.run(tsDataType, () -> {});
      if (idx == batchSize || sortedRowIndex + 1 == list.rowCount()) {
        flushBatchToWriter(chunkWriterImpl, times, values, idx);
        idx = 0;
      }
    }
    if (idx > 0) {
      flushBatchToWriter(chunkWriterImpl, times, values, idx);
    }
  }

  @Override
  public void release() {
    if (list.getReferenceCount() == 0) {
      list.clear();
    }
  }

  @Override
  public int serializedSize() {
    return schema.serializedSize() + list.serializedSize();
  }

  @Override
  public void serializeToWAL(IWALByteBufferView buffer) {
    byte[] bytes = new byte[schema.serializedSize()];
    schema.serializeTo(ByteBuffer.wrap(bytes));
    buffer.put(bytes);

    list.serializeToWAL(buffer);
  }

  public static WritableMemChunk deserialize(DataInputStream stream) throws IOException {
    WritableMemChunk memChunk = new WritableMemChunk();
    memChunk.schema = MeasurementSchema.deserializeFrom(stream);
    memChunk.list = TVList.deserialize(stream);
    return memChunk;
  }
}
