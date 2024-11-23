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
import org.apache.iotdb.db.queryengine.execution.fragment.FragmentInstanceContext;
import org.apache.iotdb.db.queryengine.execution.fragment.QueryContext;
import org.apache.iotdb.db.queryengine.plan.planner.memory.MemoryReservationManager;
import org.apache.iotdb.db.storageengine.dataregion.wal.buffer.IWALByteBufferView;
import org.apache.iotdb.db.utils.datastructure.MergeSortTvListIterator;
import org.apache.iotdb.db.utils.datastructure.TVList;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.TimeValuePair;
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
import java.util.ArrayList;
import java.util.List;

public class WritableMemChunk implements IWritableMemChunk {

  private IMeasurementSchema schema;
  private TVList list;
  private List<TVList> sortedList;
  private static final String UNSUPPORTED_TYPE = "Unsupported data type:";
  private static final Logger LOGGER = LoggerFactory.getLogger(WritableMemChunk.class);

  private static final IoTDBConfig CONFIG = IoTDBDescriptor.getInstance().getConfig();

  public WritableMemChunk(IMeasurementSchema schema) {
    this.schema = schema;
    this.list = TVList.newList(schema.getType());
    this.sortedList = new ArrayList<>();
  }

  private WritableMemChunk() {}

  protected void handoverTvList() {
    // ensure query contexts won't be removed from list during handover process.
    list.lockQueryList();
    try {
      if (list.isSorted()) {
        sortedList.add(list);
        return;
      }

      if (list.getQueryContextList().isEmpty()) {
        // tvlist could be sorted by query thread in the meanwhile
        list.safelySort();
        sortedList.add(list);
      } else {
        QueryContext firstQuery = list.getQueryContextList().get(0);
        // reserve query memory
        if (firstQuery instanceof FragmentInstanceContext) {
          MemoryReservationManager memoryReservationManager =
              ((FragmentInstanceContext) firstQuery).getMemoryReservationContext();
          memoryReservationManager.reserveMemoryCumulatively(list.calculateRamSize());
        }
        // update current TVList owner to first query in the list
        list.setOwnerQuery(firstQuery);
        // clone tv list
        TVList cloneList = list.clone();
        sortedList.add(cloneList);
      }
    } finally {
      list.unlockQueryList();
    }
    this.list = TVList.newList(schema.getType());
  }

  @Override
  public boolean writeWithFlushCheck(long insertTime, Object objectValue) {
    boolean shouldFlush;
    switch (schema.getType()) {
      case BOOLEAN:
        shouldFlush = putBooleanWithFlushCheck(insertTime, (boolean) objectValue);
        break;
      case INT32:
      case DATE:
        shouldFlush = putIntWithFlushCheck(insertTime, (int) objectValue);
        break;
      case INT64:
      case TIMESTAMP:
        shouldFlush = putLongWithFlushCheck(insertTime, (long) objectValue);
        break;
      case FLOAT:
        shouldFlush = putFloatWithFlushCheck(insertTime, (float) objectValue);
        break;
      case DOUBLE:
        shouldFlush = putDoubleWithFlushCheck(insertTime, (double) objectValue);
        break;
      case TEXT:
      case BLOB:
      case STRING:
        shouldFlush = putBinaryWithFlushCheck(insertTime, (Binary) objectValue);
        break;
      default:
        throw new UnSupportedDataTypeException(UNSUPPORTED_TYPE + schema.getType().name());
    }
    if (shouldFlush) {
      return true;
    }

    if (list.rowCount() >= SORT_THRESHOLD) {
      handoverTvList();
    }
    return false;
  }

  @Override
  public boolean writeAlignedValueWithFlushCheck(
      long insertTime, Object[] objectValue, List<IMeasurementSchema> schemaList) {
    throw new UnSupportedDataTypeException(UNSUPPORTED_TYPE + list.getDataType());
  }

  @Override
  public boolean writeWithFlushCheck(
      long[] times, Object valueList, BitMap bitMap, TSDataType dataType, int start, int end) {
    boolean shouldFlush;
    switch (dataType) {
      case BOOLEAN:
        boolean[] boolValues = (boolean[]) valueList;
        shouldFlush = putBooleansWithFlushCheck(times, boolValues, bitMap, start, end);
        break;
      case INT32:
      case DATE:
        int[] intValues = (int[]) valueList;
        shouldFlush = putIntsWithFlushCheck(times, intValues, bitMap, start, end);
        break;
      case INT64:
      case TIMESTAMP:
        long[] longValues = (long[]) valueList;
        return putLongsWithFlushCheck(times, longValues, bitMap, start, end);
      case FLOAT:
        float[] floatValues = (float[]) valueList;
        shouldFlush = putFloatsWithFlushCheck(times, floatValues, bitMap, start, end);
        break;
      case DOUBLE:
        double[] doubleValues = (double[]) valueList;
        shouldFlush = putDoublesWithFlushCheck(times, doubleValues, bitMap, start, end);
        break;
      case TEXT:
      case BLOB:
      case STRING:
        Binary[] binaryValues = (Binary[]) valueList;
        shouldFlush = putBinariesWithFlushCheck(times, binaryValues, bitMap, start, end);
        break;
      default:
        throw new UnSupportedDataTypeException(UNSUPPORTED_TYPE + dataType.name());
    }
    if (shouldFlush) {
      return true;
    }

    if (list.rowCount() >= SORT_THRESHOLD) {
      handoverTvList();
    }
    return false;
  }

  @Override
  public boolean writeAlignedValuesWithFlushCheck(
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
  public synchronized void sortTvListForFlush(boolean ignoreAllNullRows) {
    if (!list.isSorted()) {
      list.safelySort();
    }
  }

  @Override
  public TVList getTVList() {
    return list;
  }

  public void setTVList(TVList list) {
    this.list = list;
  }

  @Override
  public long count() {
    long count = list.count();
    for (TVList tvList : sortedList) {
      count += tvList.count();
    }
    return count;
  }

  @Override
  public long rowCount() {
    long rowCount = list.rowCount();
    for (TVList tvList : sortedList) {
      rowCount += tvList.rowCount();
    }
    return rowCount;
  }

  @Override
  public IMeasurementSchema getSchema() {
    return schema;
  }

  @Override
  public long getMaxTime() {
    long maxTime = list.getMaxTime();
    for (TVList tvList : sortedList) {
      maxTime = Math.max(maxTime, tvList.getMaxTime());
    }
    return maxTime;
  }

  @Override
  public long getMinTime() {
    long minTime = list.getMinTime();
    for (TVList tvList : sortedList) {
      minTime = Math.min(minTime, tvList.getMinTime());
    }
    return minTime;
  }

  @Override
  public long getFirstPoint() {
    if (count() == 0) {
      return Long.MAX_VALUE;
    }
    return getMinTime();
  }

  @Override
  public long getLastPoint() {
    if (count() == 0) {
      return Long.MIN_VALUE;
    }
    return getMaxTime();
  }

  @Override
  public boolean isEmpty() {
    return count() == 0;
  }

  @Override
  public int delete(long lowerBound, long upperBound) {
    int deletedNumber = list.delete(lowerBound, upperBound);
    for (TVList tvList : sortedList) {
      deletedNumber += tvList.delete(lowerBound, upperBound);
    }
    return deletedNumber;
  }

  @Override
  public IChunkWriter createIChunkWriter() {
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

  private void writeData(ChunkWriterImpl chunkWriterImpl, TimeValuePair tvPair) {
    switch (schema.getType()) {
      case BOOLEAN:
        chunkWriterImpl.write(tvPair.getTimestamp(), tvPair.getValue().getBoolean());
        break;
      case INT32:
      case DATE:
        chunkWriterImpl.write(tvPair.getTimestamp(), tvPair.getValue().getInt());
        break;
      case INT64:
      case TIMESTAMP:
        chunkWriterImpl.write(tvPair.getTimestamp(), tvPair.getValue().getLong());
        break;
      case FLOAT:
        chunkWriterImpl.write(tvPair.getTimestamp(), tvPair.getValue().getFloat());
        break;
      case DOUBLE:
        chunkWriterImpl.write(tvPair.getTimestamp(), tvPair.getValue().getDouble());
        break;
      case TEXT:
      case BLOB:
      case STRING:
        chunkWriterImpl.write(tvPair.getTimestamp(), tvPair.getValue().getBinary());
        break;
      default:
        LOGGER.error("WritableMemChunk does not support data type: {}", schema.getType());
        break;
    }
  }

  @Override
  public void encode(IChunkWriter chunkWriter) {
    ChunkWriterImpl chunkWriterImpl = (ChunkWriterImpl) chunkWriter;

    // create MergeSortTvListIterator. It need not handle float/double precision.
    List<TVList> tvLists = new ArrayList<>(sortedList);
    tvLists.add(list);
    MergeSortTvListIterator iterator = new MergeSortTvListIterator(schema.getType(), tvLists);

    TimeValuePair prevTvPair = null;
    while (iterator.hasNextTimeValuePair()) {
      TimeValuePair currTvPair = iterator.nextTimeValuePair();
      if (prevTvPair == null) {
        prevTvPair = currTvPair;
        continue;
      }
      writeData(chunkWriterImpl, prevTvPair);
      prevTvPair = currTvPair;
    }
    // last point for SDT
    if (prevTvPair != null) {
      chunkWriterImpl.setLastPoint(true);
      writeData(chunkWriterImpl, prevTvPair);
    }
  }

  /**
   * Release process for memtable flush. Release the TVList if there is no query on it, otherwise
   * set query owner and release the TVList until query finishes.
   *
   * @param tvList
   */
  private void maybeReleaseTvList(TVList tvList) {
    tvList.lockQueryList();
    try {
      if (tvList.getQueryContextList().isEmpty()) {
        tvList.clear();
      } else {
        QueryContext firstQuery = tvList.getQueryContextList().get(0);
        // transfer memory from write process to read process. Here it reserves read memory and
        // releaseFlushedMemTable will release write memory.
        if (firstQuery instanceof FragmentInstanceContext) {
          MemoryReservationManager memoryReservationManager =
              ((FragmentInstanceContext) firstQuery).getMemoryReservationContext();
          memoryReservationManager.reserveMemoryCumulatively(tvList.calculateRamSize());
        }
        // update current TVList owner to first query in the list
        tvList.setOwnerQuery(firstQuery);
      }
    } finally {
      tvList.unlockQueryList();
    }
  }

  @Override
  public void release() {
    maybeReleaseTvList(list);
    for (TVList tvList : sortedList) {
      maybeReleaseTvList(tvList);
    }
  }

  @Override
  public int serializedSize() {
    int serializedSize = schema.serializedSize() + list.serializedSize();
    for (TVList tvList : sortedList) {
      serializedSize += tvList.serializedSize();
    }
    return serializedSize;
  }

  @Override
  public void serializeToWAL(IWALByteBufferView buffer) {
    byte[] bytes = new byte[schema.serializedSize()];
    schema.serializeTo(ByteBuffer.wrap(bytes));
    buffer.put(bytes);
    for (TVList tvList : sortedList) {
      tvList.serializeToWAL(buffer);
    }
    list.serializeToWAL(buffer);
  }

  public static WritableMemChunk deserialize(DataInputStream stream) throws IOException {
    WritableMemChunk memChunk = new WritableMemChunk();
    memChunk.schema = MeasurementSchema.deserializeFrom(stream);
    memChunk.list = TVList.deserialize(stream);
    return memChunk;
  }

  public List<TVList> getSortedList() {
    return sortedList;
  }
}
