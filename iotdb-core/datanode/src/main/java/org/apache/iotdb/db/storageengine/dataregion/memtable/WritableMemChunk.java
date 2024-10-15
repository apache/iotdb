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

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.storageengine.dataregion.wal.buffer.IWALByteBufferView;
import org.apache.iotdb.db.utils.datastructure.TVList;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.utils.BitMap;
import org.apache.tsfile.write.UnSupportedDataTypeException;
import org.apache.tsfile.write.chunk.ChunkWriterImpl;
import org.apache.tsfile.write.schema.IMeasurementSchema;
import org.apache.tsfile.write.schema.MeasurementSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.BlockingQueue;

import static org.apache.iotdb.db.utils.MemUtils.getBinarySize;

public class WritableMemChunk implements IWritableMemChunk {

  private IMeasurementSchema schema;
  private TVList list;
  private static final String UNSUPPORTED_TYPE = "Unsupported data type:";

  private static final Logger LOGGER = LoggerFactory.getLogger(WritableMemChunk.class);

  private static final IoTDBConfig CONFIG = IoTDBDescriptor.getInstance().getConfig();
  private final long TARGET_CHUNK_SIZE = CONFIG.getTargetChunkSize();
  private final long MAX_NUMBER_OF_POINTS_IN_CHUNK = CONFIG.getAvgSeriesPointNumberThreshold();

  public WritableMemChunk(IMeasurementSchema schema) {
    this.schema = schema;
    this.list = TVList.newList(schema.getType());
  }

  private WritableMemChunk() {}

  @Override
  public void writeNonAlignedPoint(long insertTime, Object objectValue) {
    switch (schema.getType()) {
      case BOOLEAN:
        putBoolean(insertTime, (boolean) objectValue);
        break;
      case INT32:
      case DATE:
        putInt(insertTime, (int) objectValue);
        break;
      case INT64:
      case TIMESTAMP:
        putLong(insertTime, (long) objectValue);
        break;
      case FLOAT:
        putFloat(insertTime, (float) objectValue);
        break;
      case DOUBLE:
        putDouble(insertTime, (double) objectValue);
        break;
      case TEXT:
      case BLOB:
      case STRING:
        putBinary(insertTime, (Binary) objectValue);
        break;
      default:
        throw new UnSupportedDataTypeException(UNSUPPORTED_TYPE + schema.getType().name());
    }
  }

  @Override
  public void writeAlignedPoints(
      long insertTime, Object[] objectValue, List<IMeasurementSchema> schemaList) {
    throw new UnSupportedDataTypeException(UNSUPPORTED_TYPE + list.getDataType());
  }

  @Override
  public void writeNonAlignedTablet(
      long[] times, Object valueList, BitMap bitMap, TSDataType dataType, int start, int end) {
    switch (dataType) {
      case BOOLEAN:
        boolean[] boolValues = (boolean[]) valueList;
        putBooleans(times, boolValues, bitMap, start, end);
        break;
      case INT32:
      case DATE:
        int[] intValues = (int[]) valueList;
        putInts(times, intValues, bitMap, start, end);
        break;
      case INT64:
      case TIMESTAMP:
        long[] longValues = (long[]) valueList;
        putLongs(times, longValues, bitMap, start, end);
        break;
      case FLOAT:
        float[] floatValues = (float[]) valueList;
        putFloats(times, floatValues, bitMap, start, end);
        break;
      case DOUBLE:
        double[] doubleValues = (double[]) valueList;
        putDoubles(times, doubleValues, bitMap, start, end);
        break;
      case TEXT:
      case BLOB:
      case STRING:
        Binary[] binaryValues = (Binary[]) valueList;
        putBinaries(times, binaryValues, bitMap, start, end);
        break;
      default:
        throw new UnSupportedDataTypeException(UNSUPPORTED_TYPE + dataType.name());
    }
  }

  @Override
  public void writeAlignedTablet(
      long[] times,
      Object[] valueList,
      BitMap[] bitMaps,
      List<IMeasurementSchema> schemaList,
      int start,
      int end,
      TSStatus[] results) {
    throw new UnSupportedDataTypeException(UNSUPPORTED_TYPE + list.getDataType());
  }

  @Override
  public void putLong(long t, long v) {
    list.putLong(t, v);
  }

  @Override
  public void putInt(long t, int v) {
    list.putInt(t, v);
  }

  @Override
  public void putFloat(long t, float v) {
    list.putFloat(t, v);
  }

  @Override
  public void putDouble(long t, double v) {
    list.putDouble(t, v);
  }

  @Override
  public void putBinary(long t, Binary v) {
    list.putBinary(t, v);
  }

  @Override
  public void putBoolean(long t, boolean v) {
    list.putBoolean(t, v);
  }

  @Override
  public void putAlignedRow(long t, Object[] v) {
    throw new UnSupportedDataTypeException(UNSUPPORTED_TYPE + schema.getType());
  }

  @Override
  public void putLongs(long[] t, long[] v, BitMap bitMap, int start, int end) {
    list.putLongs(t, v, bitMap, start, end);
  }

  @Override
  public void putInts(long[] t, int[] v, BitMap bitMap, int start, int end) {
    list.putInts(t, v, bitMap, start, end);
  }

  @Override
  public void putFloats(long[] t, float[] v, BitMap bitMap, int start, int end) {
    list.putFloats(t, v, bitMap, start, end);
  }

  @Override
  public void putDoubles(long[] t, double[] v, BitMap bitMap, int start, int end) {
    list.putDoubles(t, v, bitMap, start, end);
  }

  @Override
  public void putBinaries(long[] t, Binary[] v, BitMap bitMap, int start, int end) {
    list.putBinaries(t, v, bitMap, start, end);
  }

  @Override
  public void putBooleans(long[] t, boolean[] v, BitMap bitMap, int start, int end) {
    list.putBooleans(t, v, bitMap, start, end);
  }

  @Override
  public void putAlignedTablet(
      long[] t, Object[] v, BitMap[] bitMaps, int start, int end, TSStatus[] results) {
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
  public synchronized TVList getSortedTvListForQuery(
      List<IMeasurementSchema> measurementSchema, boolean ignoreAllNullRows) {
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
  public ChunkWriterImpl createIChunkWriter() {
    return new ChunkWriterImpl(schema);
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

  @Override
  public void encode(BlockingQueue<Object> ioTaskQueue) {

    TSDataType tsDataType = schema.getType();
    ChunkWriterImpl chunkWriterImpl = createIChunkWriter();
    long binarySizeInCurrentChunk = 0;
    int pointNumInCurrentChunk = 0;
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

      switch (tsDataType) {
        case BOOLEAN:
          chunkWriterImpl.write(time, list.getBoolean(sortedRowIndex));
          break;
        case INT32:
        case DATE:
          chunkWriterImpl.write(time, list.getInt(sortedRowIndex));
          break;
        case INT64:
        case TIMESTAMP:
          chunkWriterImpl.write(time, list.getLong(sortedRowIndex));
          break;
        case FLOAT:
          chunkWriterImpl.write(time, list.getFloat(sortedRowIndex));
          break;
        case DOUBLE:
          chunkWriterImpl.write(time, list.getDouble(sortedRowIndex));
          break;
        case TEXT:
        case BLOB:
        case STRING:
          Binary value = list.getBinary(sortedRowIndex);
          chunkWriterImpl.write(time, value);
          binarySizeInCurrentChunk += getBinarySize(value);
          break;
        default:
          LOGGER.error("WritableMemChunk does not support data type: {}", tsDataType);
          break;
      }
      pointNumInCurrentChunk++;
      if (pointNumInCurrentChunk > MAX_NUMBER_OF_POINTS_IN_CHUNK
          || binarySizeInCurrentChunk > TARGET_CHUNK_SIZE) {
        chunkWriterImpl.sealCurrentPage();
        chunkWriterImpl.clearPageWriter();
        try {
          ioTaskQueue.put(chunkWriterImpl);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        }
        chunkWriterImpl = createIChunkWriter();
        binarySizeInCurrentChunk = 0;
        pointNumInCurrentChunk = 0;
      }
    }
    if (pointNumInCurrentChunk != 0) {
      chunkWriterImpl.sealCurrentPage();
      chunkWriterImpl.clearPageWriter();
      try {
        ioTaskQueue.put(chunkWriterImpl);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
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
