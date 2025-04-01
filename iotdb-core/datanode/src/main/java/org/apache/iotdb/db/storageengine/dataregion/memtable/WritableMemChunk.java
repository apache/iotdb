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
import org.apache.iotdb.db.utils.ModificationUtils;
import org.apache.iotdb.db.utils.datastructure.MemPointIterator;
import org.apache.iotdb.db.utils.datastructure.MemPointIteratorFactory;
import org.apache.iotdb.db.utils.datastructure.TVList;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.TimeValuePair;
import org.apache.tsfile.read.common.TimeRange;
import org.apache.tsfile.read.common.block.TsBlock;
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
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.stream.Collectors;

import static org.apache.iotdb.db.utils.MemUtils.getBinarySize;

public class WritableMemChunk extends AbstractWritableMemChunk {

  private IMeasurementSchema schema;
  private TVList list;
  private List<TVList> sortedList;
  private static final String UNSUPPORTED_TYPE = "Unsupported data type:";

  private static final Logger LOGGER = LoggerFactory.getLogger(WritableMemChunk.class);

  private static final IoTDBConfig CONFIG = IoTDBDescriptor.getInstance().getConfig();
  private final long TARGET_CHUNK_SIZE = CONFIG.getTargetChunkSize();
  private final long MAX_NUMBER_OF_POINTS_IN_CHUNK = CONFIG.getTargetChunkPointNum();
  private final int TVLIST_SORT_THRESHOLD = CONFIG.getTvListSortThreshold();

  public WritableMemChunk(IMeasurementSchema schema) {
    this.schema = schema;
    this.list = TVList.newList(schema.getType());
    this.sortedList = new ArrayList<>();
  }

  private WritableMemChunk() {}

  protected void handoverTvList() {
    if (!list.isSorted()) {
      list.sort();
    }
    sortedList.add(list);
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
  public ChunkWriterImpl createIChunkWriter() {
    return new ChunkWriterImpl(schema);
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

  public void encodeWorkingTVList(BlockingQueue<Object> ioTaskQueue) {

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
      if (pointNumInCurrentChunk > MAX_NUMBER_OF_POINTS_IN_CHUNK
          || dataSizeInCurrentChunk > TARGET_CHUNK_SIZE) {
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
  public synchronized void encode(BlockingQueue<Object> ioTaskQueue) {
    if (TVLIST_SORT_THRESHOLD == 0) {
      encodeWorkingTVList(ioTaskQueue);
      return;
    }

    TSDataType tsDataType = schema.getType();
    ChunkWriterImpl chunkWriterImpl = createIChunkWriter();
    long dataSizeInCurrentChunk = 0;
    int pointNumInCurrentChunk = 0;

    // create MultiTvListIterator. It need not handle float/double precision here.
    List<TVList> tvLists = new ArrayList<>(sortedList);
    tvLists.add(list);
    MemPointIterator timeValuePairIterator =
        MemPointIteratorFactory.create(schema.getType(), tvLists);

    while (timeValuePairIterator.hasNextBatch()) {
      TsBlock tsBlock = timeValuePairIterator.nextBatch();
      if (tsBlock == null) {
        continue;
      }

      for (int rowIndex = 0; rowIndex < tsBlock.getPositionCount(); rowIndex++) {
        long time = tsBlock.getTimeByIndex(rowIndex);
        // store last point for SDT
        if (rowIndex + 1 == tsBlock.getPositionCount() && !timeValuePairIterator.hasNextBatch()) {
          chunkWriterImpl.setLastPoint(true);
        }

        switch (tsDataType) {
          case BOOLEAN:
            chunkWriterImpl.write(time, tsBlock.getColumn(0).getBoolean(rowIndex));
            dataSizeInCurrentChunk += 8L + 1L;
            break;
          case INT32:
          case DATE:
            chunkWriterImpl.write(time, tsBlock.getColumn(0).getInt(rowIndex));
            dataSizeInCurrentChunk += 8L + 4L;
            break;
          case INT64:
          case TIMESTAMP:
            chunkWriterImpl.write(time, tsBlock.getColumn(0).getLong(rowIndex));
            dataSizeInCurrentChunk += 8L + 8L;
            break;
          case FLOAT:
            chunkWriterImpl.write(time, tsBlock.getColumn(0).getFloat(rowIndex));
            dataSizeInCurrentChunk += 8L + 4L;
            break;
          case DOUBLE:
            chunkWriterImpl.write(time, tsBlock.getColumn(0).getDouble(rowIndex));
            dataSizeInCurrentChunk += 8L + 8L;
            break;
          case TEXT:
          case BLOB:
          case STRING:
            Binary value = tsBlock.getColumn(0).getBinary(rowIndex);
            chunkWriterImpl.write(time, value);
            dataSizeInCurrentChunk += 8L + getBinarySize(value);
            break;
          default:
            LOGGER.error("WritableMemChunk does not support data type: {}", tsDataType);
            break;
        }

        pointNumInCurrentChunk++;
        if (pointNumInCurrentChunk > MAX_NUMBER_OF_POINTS_IN_CHUNK
            || dataSizeInCurrentChunk > TARGET_CHUNK_SIZE) {
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

  private void filterDeletedTimestamp(
      TVList tvlist, List<TimeRange> deletionList, List<Long> timestampList) {
    long lastTime = Long.MIN_VALUE;
    int[] deletionCursor = {0};
    int rowCount = tvlist.rowCount();
    for (int i = 0; i < rowCount; i++) {
      if (tvlist.getBitMap() != null && tvlist.isNullValue(tvlist.getValueIndex(i))) {
        continue;
      }
      long curTime = tvlist.getTime(i);
      if (deletionList != null
          && ModificationUtils.isPointDeleted(curTime, deletionList, deletionCursor)) {
        continue;
      }

      if (i == rowCount - 1 || curTime != lastTime) {
        timestampList.add(curTime);
      }
      lastTime = curTime;
    }
  }

  public long[] getFilteredTimestamp(List<TimeRange> deletionList) {
    List<Long> timestampList = new ArrayList<>();
    filterDeletedTimestamp(list, deletionList, timestampList);
    for (TVList tvList : sortedList) {
      filterDeletedTimestamp(tvList, deletionList, timestampList);
    }

    // remove duplicated time
    List<Long> distinctTimestamps = timestampList.stream().distinct().collect(Collectors.toList());
    // sort timestamps
    long[] filteredTimestamps = distinctTimestamps.stream().mapToLong(Long::longValue).toArray();
    Arrays.sort(filteredTimestamps);
    return filteredTimestamps;
  }
}
