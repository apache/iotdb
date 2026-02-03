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
import org.apache.iotdb.commons.utils.TestOnly;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.storageengine.dataregion.wal.buffer.IWALByteBufferView;
import org.apache.iotdb.db.storageengine.rescon.memory.PrimitiveArrayManager;
import org.apache.iotdb.db.utils.ModificationUtils;
import org.apache.iotdb.db.utils.datastructure.BatchEncodeInfo;
import org.apache.iotdb.db.utils.datastructure.MemPointIterator;
import org.apache.iotdb.db.utils.datastructure.MemPointIteratorFactory;
import org.apache.iotdb.db.utils.datastructure.TVList;

import org.apache.tsfile.encrypt.EncryptParameter;
import org.apache.tsfile.encrypt.EncryptUtils;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.TimeValuePair;
import org.apache.tsfile.read.common.TimeRange;
import org.apache.tsfile.read.filter.basic.Filter;
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
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.BlockingQueue;

import static org.apache.iotdb.db.utils.MemUtils.getBinarySize;

public class WritableMemChunk extends AbstractWritableMemChunk {

  private IMeasurementSchema schema;
  private TVList list;
  private List<TVList> sortedList;
  private long sortedRowCount = 0;
  private static final String UNSUPPORTED_TYPE = "Unsupported data type:";

  private static final Logger LOGGER = LoggerFactory.getLogger(WritableMemChunk.class);

  private static final IoTDBConfig CONFIG = IoTDBDescriptor.getInstance().getConfig();
  private final int TVLIST_SORT_THRESHOLD = CONFIG.getTvListSortThreshold();

  private EncryptParameter encryptParameter;

  @TestOnly
  public WritableMemChunk(IMeasurementSchema schema) {
    this.schema = schema;
    this.list = TVList.newList(schema.getType());
    this.sortedList = new ArrayList<>();
    this.encryptParameter = EncryptUtils.getEncryptParameter();
  }

  public WritableMemChunk(IMeasurementSchema schema, EncryptParameter encryptParameter) {
    this.schema = schema;
    this.list = TVList.newList(schema.getType());
    this.sortedList = new ArrayList<>();
    this.encryptParameter = encryptParameter;
  }

  private WritableMemChunk() {
    this.encryptParameter = EncryptUtils.getEncryptParameter();
  }

  protected void handoverTvList() {
    if (!list.isSorted()) {
      list.sort();
    }
    sortedList.add(list);
    this.sortedRowCount += list.rowCount();
    this.list = TVList.newList(schema.getType());
  }

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
    if (TVLIST_SORT_THRESHOLD > 0 && list.rowCount() >= TVLIST_SORT_THRESHOLD) {
      handoverTvList();
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
      case OBJECT:
        Binary[] binaryValues = (Binary[]) valueList;
        putBinaries(times, binaryValues, bitMap, start, end);
        break;
      default:
        throw new UnSupportedDataTypeException(UNSUPPORTED_TYPE + dataType.name());
    }
    if (TVLIST_SORT_THRESHOLD > 0 && list.rowCount() >= TVLIST_SORT_THRESHOLD) {
      handoverTvList();
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
  public synchronized void sortTvListForFlush() {
    if (!list.isSorted()) {
      list.sort();
    }
  }

  @Override
  public TVList getWorkingTVList() {
    return list;
  }

  @Override
  public void setWorkingTVList(TVList list) {
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
    return sortedRowCount + list.rowCount();
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
  public ChunkWriterImpl createIChunkWriter() {
    return new ChunkWriterImpl(schema, encryptParameter);
  }

  @Override
  public String toString() {
    TimeValuePair firstTvPair = null;
    TimeValuePair lastTvPair = null;
    int size = 0;
    long minTime = Long.MAX_VALUE;
    long maxTime = Long.MIN_VALUE;

    List<TVList> tvLists = new ArrayList<>(sortedList);
    tvLists.add(list);
    for (TVList tvList : tvLists) {
      for (int i = 0; i < tvList.rowCount(); i++) {
        if (tvList.isNullValue(tvList.getValueIndex(i))) {
          continue;
        }
        size++;
        long currentTime = tvList.getTime(i);
        if (currentTime < minTime) {
          firstTvPair = tvList.getTimeValuePair(i);
          minTime = currentTime;
        }
        if (currentTime >= maxTime) {
          lastTvPair = tvList.getTimeValuePair(i);
          maxTime = currentTime;
        }
      }
    }

    StringBuilder out = new StringBuilder("MemChunk Size: " + size + System.lineSeparator());
    if (size != 0) {
      out.append("Data type:").append(schema.getType()).append(System.lineSeparator());
      out.append("First point:").append(firstTvPair).append(System.lineSeparator());
      out.append("Last point:").append(lastTvPair).append(System.lineSeparator());
    }
    return out.toString();
  }

  public void encodeWorkingTVList(
      BlockingQueue<Object> ioTaskQueue, long maxNumberOfPointsInChunk, long targetChunkSize) {

    TSDataType tsDataType = schema.getType();
    ChunkWriterImpl chunkWriterImpl = createIChunkWriter();
    long dataSizeInCurrentChunk = 0;
    int pointNumInCurrentChunk = 0;
    for (int sortedRowIndex = 0; sortedRowIndex < list.rowCount(); sortedRowIndex++) {
      if (list.isNullValue(list.getValueIndex(sortedRowIndex))) {
        continue;
      }

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
          dataSizeInCurrentChunk += 8L + 1L;
          break;
        case INT32:
        case DATE:
          chunkWriterImpl.write(time, list.getInt(sortedRowIndex));
          dataSizeInCurrentChunk += 8L + 4L;
          break;
        case INT64:
        case TIMESTAMP:
          chunkWriterImpl.write(time, list.getLong(sortedRowIndex));
          dataSizeInCurrentChunk += 8L + 8L;
          break;
        case FLOAT:
          chunkWriterImpl.write(time, list.getFloat(sortedRowIndex));
          dataSizeInCurrentChunk += 8L + 4L;
          break;
        case DOUBLE:
          chunkWriterImpl.write(time, list.getDouble(sortedRowIndex));
          dataSizeInCurrentChunk += 8L + 8L;
          break;
        case TEXT:
        case BLOB:
        case STRING:
          Binary value = list.getBinary(sortedRowIndex);
          chunkWriterImpl.write(time, value);
          dataSizeInCurrentChunk += 8L + getBinarySize(value);
          break;
        default:
          LOGGER.error("WritableMemChunk does not support data type: {}", tsDataType);
          break;
      }
      pointNumInCurrentChunk++;
      if (pointNumInCurrentChunk > maxNumberOfPointsInChunk
          || dataSizeInCurrentChunk > targetChunkSize) {
        chunkWriterImpl.sealCurrentPage();
        chunkWriterImpl.clearPageWriter();
        try {
          ioTaskQueue.put(chunkWriterImpl);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        }
        chunkWriterImpl = createIChunkWriter();
        dataSizeInCurrentChunk = 0;
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
  public synchronized void encode(
      BlockingQueue<Object> ioTaskQueue, BatchEncodeInfo encodeInfo, long[] times) {
    if (TVLIST_SORT_THRESHOLD == 0) {
      encodeWorkingTVList(
          ioTaskQueue, encodeInfo.maxNumberOfPointsInChunk, encodeInfo.targetChunkSize);
      return;
    }

    ChunkWriterImpl chunkWriterImpl = createIChunkWriter();
    if (sortedList.isEmpty()) {
      encodeInfo.lastIterator = true;
    }

    // create MultiTvListIterator. It need not handle float/double precision here.
    List<TVList> tvLists = new ArrayList<>(sortedList);
    tvLists.add(list);
    MemPointIterator timeValuePairIterator =
        MemPointIteratorFactory.create(
            schema.getType(), tvLists, encodeInfo.maxNumberOfPointsInPage);

    while (timeValuePairIterator.hasNextBatch()) {
      timeValuePairIterator.encodeBatch(chunkWriterImpl, encodeInfo, times);
      if (encodeInfo.pointNumInChunk >= encodeInfo.maxNumberOfPointsInChunk
          || encodeInfo.dataSizeInChunk >= encodeInfo.targetChunkSize) {
        chunkWriterImpl.sealCurrentPage();
        chunkWriterImpl.clearPageWriter();
        try {
          ioTaskQueue.put(chunkWriterImpl);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        }
        chunkWriterImpl = createIChunkWriter();
        encodeInfo.resetPointAndSize();
      }
    }
    if (encodeInfo.pointNumInChunk != 0) {
      chunkWriterImpl.sealCurrentPage();
      chunkWriterImpl.clearPageWriter();
      try {
        ioTaskQueue.put(chunkWriterImpl);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
      encodeInfo.reset();
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
    serializedSize += Integer.BYTES;
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
    buffer.putInt(sortedList.size());
    for (TVList tvList : sortedList) {
      tvList.serializeToWAL(buffer);
    }
    list.serializeToWAL(buffer);
  }

  public static WritableMemChunk deserialize(DataInputStream stream) throws IOException {
    WritableMemChunk memChunk = new WritableMemChunk();
    memChunk.schema = MeasurementSchema.deserializeFrom(stream);
    int sortedListSize = stream.readInt();
    memChunk.sortedList = new ArrayList<>();
    for (int i = 0; i < sortedListSize; i++) {
      TVList tvList = TVList.deserialize(stream);
      memChunk.sortedList.add(tvList);
    }
    memChunk.list = TVList.deserialize(stream);
    return memChunk;
  }

  public static WritableMemChunk deserializeSingleTVListMemChunks(DataInputStream stream)
      throws IOException {
    WritableMemChunk memChunk = new WritableMemChunk();
    memChunk.schema = MeasurementSchema.deserializeFrom(stream);
    memChunk.list = TVList.deserialize(stream);
    return memChunk;
  }

  @Override
  public List<TVList> getSortedList() {
    return sortedList;
  }

  public Optional<Long> getAnySatisfiedTimestamp(
      List<TimeRange> deletionList, Filter globalTimeFilter) {
    Optional<Long> anySatisfiedTimestamp =
        getAnySatisfiedTimestamp(list, deletionList, globalTimeFilter);
    if (anySatisfiedTimestamp.isPresent()) {
      return anySatisfiedTimestamp;
    }
    for (TVList tvList : sortedList) {
      anySatisfiedTimestamp = getAnySatisfiedTimestamp(tvList, deletionList, globalTimeFilter);
      if (anySatisfiedTimestamp.isPresent()) {
        break;
      }
    }
    return anySatisfiedTimestamp;
  }

  private Optional<Long> getAnySatisfiedTimestamp(
      TVList tvlist, List<TimeRange> deletionList, Filter globalTimeFilter) {
    int[] deletionCursor = {0};
    int rowCount = tvlist.rowCount();
    if (globalTimeFilter != null
        && !globalTimeFilter.satisfyStartEndTime(tvlist.getMinTime(), tvlist.getMaxTime())) {
      return Optional.empty();
    }

    List<long[]> timestampsList = tvlist.getTimestamps();
    List<BitMap> bitMaps = tvlist.getBitMap();
    List<int[]> indicesList = tvlist.getIndices();
    for (int i = 0; i < timestampsList.size(); i++) {
      long[] timestamps = timestampsList.get(i);
      BitMap bitMap = bitMaps == null ? null : bitMaps.get(i);
      int[] indices = indicesList == null ? null : indicesList.get(i);
      int limit =
          (i == timestampsList.size() - 1)
              ? rowCount - i * PrimitiveArrayManager.ARRAY_SIZE
              : PrimitiveArrayManager.ARRAY_SIZE;
      for (int j = 0; j < limit; j++) {
        if (bitMap != null
            && (indices == null ? bitMap.isMarked(j) : tvlist.isNullValue(indices[j]))) {
          continue;
        }
        long curTime = timestamps[j];
        if (deletionList != null && !deletionList.isEmpty()) {
          if (!tvlist.isSorted()) {
            deletionCursor[0] = 0;
          }
          if (ModificationUtils.isPointDeleted(curTime, deletionList, deletionCursor)) {
            continue;
          }
        }
        if (globalTimeFilter != null && !globalTimeFilter.satisfy(curTime, null)) {
          continue;
        }

        return Optional.of(curTime);
      }
    }
    return Optional.empty();
  }

  @Override
  public void setEncryptParameter(EncryptParameter encryptParameter) {
    this.encryptParameter = encryptParameter;
  }
}
