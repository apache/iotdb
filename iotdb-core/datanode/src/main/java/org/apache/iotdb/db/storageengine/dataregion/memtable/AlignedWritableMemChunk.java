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

package org.apache.iotdb.db.storageengine.dataregion.memtable;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.queryengine.plan.statement.crud.InsertTabletStatement.TimeView;
import org.apache.iotdb.db.queryengine.plan.statement.crud.InsertTabletStatement.ValueView;
import org.apache.iotdb.db.storageengine.dataregion.wal.buffer.IWALByteBufferView;
import org.apache.iotdb.db.storageengine.dataregion.wal.utils.WALWriteUtils;
import org.apache.iotdb.db.utils.datastructure.AlignedTVList;
import org.apache.iotdb.db.utils.datastructure.MemPointIterator;
import org.apache.iotdb.db.utils.datastructure.MemPointIteratorFactory;
import org.apache.iotdb.db.utils.datastructure.TVList;

import org.apache.tsfile.common.conf.TSFileDescriptor;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.TimeRange;
import org.apache.tsfile.read.common.block.TsBlock;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.utils.BitMap;
import org.apache.tsfile.utils.Pair;
import org.apache.tsfile.write.UnSupportedDataTypeException;
import org.apache.tsfile.write.chunk.AlignedChunkWriterImpl;
import org.apache.tsfile.write.chunk.IChunkWriter;
import org.apache.tsfile.write.chunk.ValueChunkWriter;
import org.apache.tsfile.write.schema.IMeasurementSchema;
import org.apache.tsfile.write.schema.MeasurementSchema;

import java.io.DataInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.BlockingQueue;

import static org.apache.iotdb.db.utils.ModificationUtils.isPointDeleted;

public class AlignedWritableMemChunk extends AbstractWritableMemChunk {

  private final Map<String, Integer> measurementIndexMap;
  private final List<TSDataType> dataTypes;
  private final List<IMeasurementSchema> schemaList;
  private AlignedTVList list;
  private List<AlignedTVList> sortedList;
  private final boolean ignoreAllNullRows;

  private static final IoTDBConfig CONFIG = IoTDBDescriptor.getInstance().getConfig();
  private final long TARGET_CHUNK_SIZE = CONFIG.getTargetChunkSize();
  private long maxNumberOfPointsInChunk = CONFIG.getTargetChunkPointNum();
  private final int TVLIST_SORT_THRESHOLD = CONFIG.getTvListSortThreshold();
  private final int MAX_NUMBER_OF_POINTS_IN_PAGE =
      TSFileDescriptor.getInstance().getConfig().getMaxNumberOfPointsInPage();

  private static final String UNSUPPORTED_TYPE = "Unsupported data type:";

  public AlignedWritableMemChunk(List<IMeasurementSchema> schemaList, boolean isTableModel) {
    this.measurementIndexMap = new LinkedHashMap<>();
    this.dataTypes = new ArrayList<>();
    this.schemaList = schemaList;
    for (int i = 0; i < schemaList.size(); i++) {
      measurementIndexMap.put(schemaList.get(i).getMeasurementName(), i);
      dataTypes.add(schemaList.get(i).getType());
    }
    this.list = AlignedTVList.newAlignedList(dataTypes);
    this.sortedList = new ArrayList<>();
    this.ignoreAllNullRows = !isTableModel;
  }

  private AlignedWritableMemChunk(
      List<IMeasurementSchema> schemaList, AlignedTVList list, boolean isTableModel) {
    this.measurementIndexMap = new LinkedHashMap<>();
    this.schemaList = schemaList;
    for (int i = 0; i < schemaList.size(); i++) {
      measurementIndexMap.put(schemaList.get(i).getMeasurementName(), i);
    }
    this.list = list;
    this.dataTypes = list.getTsDataTypes();
    this.sortedList = new ArrayList<>();
    this.ignoreAllNullRows = !isTableModel;
  }

  public Set<String> getAllMeasurements() {
    return measurementIndexMap.keySet();
  }

  public boolean containsMeasurement(String measurementId) {
    return measurementIndexMap.containsKey(measurementId);
  }

  @Override
  public void putLong(long t, long v) {
    throw new UnSupportedDataTypeException(UNSUPPORTED_TYPE + TSDataType.VECTOR);
  }

  @Override
  public void putInt(long t, int v) {
    throw new UnSupportedDataTypeException(UNSUPPORTED_TYPE + TSDataType.VECTOR);
  }

  @Override
  public void putFloat(long t, float v) {
    throw new UnSupportedDataTypeException(UNSUPPORTED_TYPE + TSDataType.VECTOR);
  }

  @Override
  public void putDouble(long t, double v) {
    throw new UnSupportedDataTypeException(UNSUPPORTED_TYPE + TSDataType.VECTOR);
  }

  @Override
  public void putBinary(long t, Binary v) {
    throw new UnSupportedDataTypeException(UNSUPPORTED_TYPE + TSDataType.VECTOR);
  }

  @Override
  public void putBoolean(long t, boolean v) {
    throw new UnSupportedDataTypeException(UNSUPPORTED_TYPE + TSDataType.VECTOR);
  }

  @Override
  public void putAlignedRow(long t, Object[] v, List<Integer> columnIndices) {
    list.putAlignedValue(t, v, columnIndices);
  }

  @Override
  public void putLongs(long[] t, long[] v, BitMap bitMap, int start, int end) {
    throw new UnSupportedDataTypeException(UNSUPPORTED_TYPE + TSDataType.VECTOR);
  }

  @Override
  public void putInts(long[] t, int[] v, BitMap bitMap, int start, int end) {
    throw new UnSupportedDataTypeException(UNSUPPORTED_TYPE + TSDataType.VECTOR);
  }

  @Override
  public void putFloats(long[] t, float[] v, BitMap bitMap, int start, int end) {
    throw new UnSupportedDataTypeException(UNSUPPORTED_TYPE + TSDataType.VECTOR);
  }

  @Override
  public void putDoubles(long[] t, double[] v, BitMap bitMap, int start, int end) {
    throw new UnSupportedDataTypeException(UNSUPPORTED_TYPE + TSDataType.VECTOR);
  }

  @Override
  public void putBinaries(long[] t, Binary[] v, BitMap bitMap, int start, int end) {
    throw new UnSupportedDataTypeException(UNSUPPORTED_TYPE + TSDataType.VECTOR);
  }

  @Override
  public void putBooleans(long[] t, boolean[] v, BitMap bitMap, int start, int end) {
    throw new UnSupportedDataTypeException(UNSUPPORTED_TYPE + TSDataType.VECTOR);
  }

  @Override
  public void putAlignedTablet(
      TimeView t,
      ValueView v,
      List<Integer> columnIndices,
      BitMap[] bitMaps,
      int start,
      int end,
      TSStatus[] results) {
    int currentRowCount = list.rowCount();
    t.putTo(list, null, start, end);
    v.putTo(list, columnIndices, bitMaps, start, end, results, currentRowCount);
  }

  @Override
  public void writeNonAlignedPoint(long insertTime, Object objectValue) {
    throw new UnSupportedDataTypeException(UNSUPPORTED_TYPE + TSDataType.VECTOR);
  }

  @Override
  public void writeNonAlignedTablet(
      TimeView times,
      ValueView values,
      int columnIndex,
      BitMap bitMap,
      TSDataType dataType,
      int start,
      int end) {
    throw new UnSupportedDataTypeException(UNSUPPORTED_TYPE + TSDataType.VECTOR);
  }

  protected void handoverAlignedTvList() {
    if (!list.isSorted()) {
      list.sort();
    }
    sortedList.add(list);
    this.list = AlignedTVList.newAlignedList(dataTypes);
  }

  @Override
  public void writeAlignedPoints(
      long insertTime, Object[] objectValue, List<IMeasurementSchema> schemaList) {
    List<Integer> columnIndices = calculateColumnIndices(schemaList);
    putAlignedRow(insertTime, objectValue, columnIndices);
    if (TVLIST_SORT_THRESHOLD > 0 && list.rowCount() >= TVLIST_SORT_THRESHOLD) {
      handoverAlignedTvList();
    }
  }

  @Override
  public void writeAlignedTablet(
      TimeView times,
      ValueView valueList,
      BitMap[] bitMaps,
      List<IMeasurementSchema> schemaList,
      int start,
      int end,
      TSStatus[] results) {
    List<Integer> columnIndices = calculateColumnIndices(schemaList);
    putAlignedTablet(times, valueList, columnIndices, bitMaps, start, end, results);
    if (TVLIST_SORT_THRESHOLD > 0 && list.rowCount() >= TVLIST_SORT_THRESHOLD) {
      handoverAlignedTvList();
    }
  }

  @Override
  public AlignedTVList getWorkingTVList() {
    return list;
  }

  @Override
  public void setWorkingTVList(TVList list) {
    this.list = (AlignedTVList) list;
  }

  @Override
  public long count() {
    if (!ignoreAllNullRows && measurementIndexMap.isEmpty()) {
      return rowCount();
    }
    return rowCount() * measurementIndexMap.size();
  }

  @Override
  public long rowCount() {
    return alignedListSize();
  }

  public int alignedListSize() {
    int rowCount = list.rowCount();
    for (AlignedTVList alignedTvList : sortedList) {
      rowCount += alignedTvList.rowCount();
    }
    return rowCount;
  }

  @Override
  public IMeasurementSchema getSchema() {
    return null;
  }

  @Override
  public long getMaxTime() {
    if (isEmpty()) {
      return Long.MIN_VALUE;
    }
    long maxTime = list.getMaxTime();
    for (AlignedTVList alignedTvList : sortedList) {
      maxTime = Math.max(maxTime, alignedTvList.getMaxTime());
    }
    return maxTime;
  }

  @Override
  public long getMinTime() {
    long minTime = list.getMinTime();
    for (AlignedTVList alignedTvList : sortedList) {
      minTime = Math.min(minTime, alignedTvList.getMinTime());
    }
    return minTime;
  }

  @Override
  public synchronized void sortTvListForFlush() {
    if (!list.isSorted()) {
      list.sort();
    }
  }

  @Override
  public int delete(long lowerBound, long upperBound) {
    int deletedNumber = list.delete(lowerBound, upperBound);
    for (AlignedTVList alignedTvList : sortedList) {
      deletedNumber += alignedTvList.delete(lowerBound, upperBound);
    }
    return deletedNumber;
  }

  public int deleteTime(long lowerBound, long upperBound) {
    int deletedNumber = list.deleteTime(lowerBound, upperBound);
    for (AlignedTVList alignedTvList : sortedList) {
      deletedNumber += alignedTvList.deleteTime(lowerBound, upperBound);
    }
    return deletedNumber;
  }

  public Pair<Integer, Boolean> deleteDataFromAColumn(
      long lowerBound, long upperBound, String measurementId) {
    Pair<Integer, Boolean> deletePair =
        list.delete(lowerBound, upperBound, measurementIndexMap.get(measurementId));
    for (AlignedTVList alignedTvList : sortedList) {
      Pair<Integer, Boolean> p =
          alignedTvList.delete(lowerBound, upperBound, measurementIndexMap.get(measurementId));
      deletePair.left += p.left;
      deletePair.right = deletePair.right && p.right;
    }
    return deletePair;
  }

  public void removeColumn(String measurementId) {
    int columnIndex = measurementIndexMap.get(measurementId);
    list.deleteColumn(columnIndex);
    for (AlignedTVList alignedTvList : sortedList) {
      alignedTvList.deleteColumn(columnIndex);
    }
    IMeasurementSchema schemaToBeRemoved = schemaList.get(columnIndex);
    measurementIndexMap.remove(schemaToBeRemoved.getMeasurementName());
  }

  @Override
  public IChunkWriter createIChunkWriter() {
    return new AlignedChunkWriterImpl(schemaList);
  }

  @SuppressWarnings({"squid:S6541", "squid:S3776"})
  public void encodeWorkingAlignedTVList(BlockingQueue<Object> ioTaskQueue) {
    BitMap allValueColDeletedMap;
    allValueColDeletedMap = ignoreAllNullRows ? list.getAllValueColDeletedMap() : null;
    int avgPointSizeOfLargestColumn = list.getAvgPointSizeOfLargestColumn();
    maxNumberOfPointsInChunk =
        Math.min(maxNumberOfPointsInChunk, (TARGET_CHUNK_SIZE / avgPointSizeOfLargestColumn));

    boolean[] timeDuplicateInfo = null;

    List<List<Integer>> chunkRange = new ArrayList<>();
    // Eg. chunkRange: ((0,9,10,12),(13,15)) means this TVList contains 2 chunks,
    // 1st chunk contains 2 pages, 2nd chunk contains 1 page.
    List<Integer> pageRange = new ArrayList<>();

    int pointNumInPage = 0;
    int pointNumInChunk = 0;

    for (int sortedRowIndex = 0; sortedRowIndex < list.rowCount(); sortedRowIndex++) {
      long time = list.getTime(sortedRowIndex);
      if (pointNumInPage == 0) {
        pageRange.add(sortedRowIndex);
      }
      pointNumInPage++;
      pointNumInChunk++;
      if (pointNumInPage == MAX_NUMBER_OF_POINTS_IN_PAGE) {
        pageRange.add(sortedRowIndex);
        pointNumInPage = 0;
      }
      if (pointNumInChunk >= maxNumberOfPointsInChunk) {
        if (pointNumInPage != 0) {
          pageRange.add(sortedRowIndex);
          pointNumInPage = 0;
        }
        chunkRange.add(pageRange);
        pageRange = new ArrayList<>();
        pointNumInChunk = 0;
      }

      int nextRowIndex = sortedRowIndex + 1;
      while (nextRowIndex < list.rowCount()
          && ((allValueColDeletedMap != null
                  && allValueColDeletedMap.isMarked(list.getValueIndex(nextRowIndex)))
              || list.isTimeDeleted(nextRowIndex))) {
        nextRowIndex++;
      }
      if (nextRowIndex != list.rowCount() && time == list.getTime(nextRowIndex)) {
        if (Objects.isNull(timeDuplicateInfo)) {
          timeDuplicateInfo = new boolean[list.rowCount()];
        }
        timeDuplicateInfo[sortedRowIndex] = true;
      }
      sortedRowIndex = nextRowIndex - 1;
    }

    if (pointNumInPage != 0) {
      pageRange.add(list.rowCount() - 1);
    }
    if (pointNumInChunk != 0) {
      chunkRange.add(pageRange);
    }

    handleEncoding(ioTaskQueue, chunkRange, timeDuplicateInfo, allValueColDeletedMap);
  }

  private void handleEncoding(
      BlockingQueue<Object> ioTaskQueue,
      List<List<Integer>> chunkRange,
      boolean[] timeDuplicateInfo,
      BitMap allValueColDeletedMap) {
    List<TSDataType> dataTypes = list.getTsDataTypes();
    Pair<Long, Integer>[] lastValidPointIndexForTimeDupCheck = new Pair[dataTypes.size()];
    for (List<Integer> pageRange : chunkRange) {
      AlignedChunkWriterImpl alignedChunkWriter = new AlignedChunkWriterImpl(schemaList);
      for (int pageNum = 0; pageNum < pageRange.size() / 2; pageNum += 1) {
        for (int columnIndex = 0; columnIndex < dataTypes.size(); columnIndex++) {
          // Pair of Time and Index
          if (Objects.nonNull(timeDuplicateInfo)
              && lastValidPointIndexForTimeDupCheck[columnIndex] == null) {
            lastValidPointIndexForTimeDupCheck[columnIndex] = new Pair<>(Long.MIN_VALUE, null);
          }
          TSDataType tsDataType = dataTypes.get(columnIndex);
          for (int sortedRowIndex = pageRange.get(pageNum * 2);
              sortedRowIndex <= pageRange.get(pageNum * 2 + 1);
              sortedRowIndex++) {
            // skip empty row
            if (allValueColDeletedMap != null
                && allValueColDeletedMap.isMarked(list.getValueIndex(sortedRowIndex))) {
              continue;
            }
            // skip time duplicated rows
            long time = list.getTime(sortedRowIndex);
            if (Objects.nonNull(timeDuplicateInfo)) {
              if (!list.isNullValue(list.getValueIndex(sortedRowIndex), columnIndex)) {
                lastValidPointIndexForTimeDupCheck[columnIndex].left = time;
                lastValidPointIndexForTimeDupCheck[columnIndex].right =
                    list.getValueIndex(sortedRowIndex);
              }
              if (timeDuplicateInfo[sortedRowIndex]) {
                continue;
              }
            }

            // The part of code solves the following problem:
            // Time: 1,2,2,3
            // Value: 1,2,null,null
            // When rowIndex:1, pair(min,null), timeDuplicateInfo:false, write(T:1,V:1)
            // When rowIndex:2, pair(2,2), timeDuplicateInfo:true, skip writing value
            // When rowIndex:3, pair(2,2), timeDuplicateInfo:false, T:2==pair.left:2, write(T:2,V:2)
            // When rowIndex:4, pair(2,2), timeDuplicateInfo:false, T:3!=pair.left:2,
            // write(T:3,V:null)

            int originRowIndex;
            if (Objects.nonNull(lastValidPointIndexForTimeDupCheck[columnIndex])
                && (time == lastValidPointIndexForTimeDupCheck[columnIndex].left)) {
              originRowIndex = lastValidPointIndexForTimeDupCheck[columnIndex].right;
            } else {
              originRowIndex = list.getValueIndex(sortedRowIndex);
            }

            boolean isNull = list.isNullValue(originRowIndex, columnIndex);
            switch (tsDataType) {
              case BOOLEAN:
                alignedChunkWriter.writeByColumn(
                    time,
                    !isNull && list.getBooleanByValueIndex(originRowIndex, columnIndex),
                    isNull);
                break;
              case INT32:
              case DATE:
                alignedChunkWriter.writeByColumn(
                    time,
                    isNull ? 0 : list.getIntByValueIndex(originRowIndex, columnIndex),
                    isNull);
                break;
              case INT64:
              case TIMESTAMP:
                alignedChunkWriter.writeByColumn(
                    time,
                    isNull ? 0 : list.getLongByValueIndex(originRowIndex, columnIndex),
                    isNull);
                break;
              case FLOAT:
                alignedChunkWriter.writeByColumn(
                    time,
                    isNull ? 0 : list.getFloatByValueIndex(originRowIndex, columnIndex),
                    isNull);
                break;
              case DOUBLE:
                alignedChunkWriter.writeByColumn(
                    time,
                    isNull ? 0 : list.getDoubleByValueIndex(originRowIndex, columnIndex),
                    isNull);
                break;
              case TEXT:
              case STRING:
              case BLOB:
                alignedChunkWriter.writeByColumn(
                    time,
                    isNull ? null : list.getBinaryByValueIndex(originRowIndex, columnIndex),
                    isNull);
                break;
              default:
                break;
            }
          }
          alignedChunkWriter.nextColumn();
        }

        long[] times = new long[Math.min(MAX_NUMBER_OF_POINTS_IN_PAGE, list.rowCount())];
        int pointsInPage = 0;
        for (int sortedRowIndex = pageRange.get(pageNum * 2);
            sortedRowIndex <= pageRange.get(pageNum * 2 + 1);
            sortedRowIndex++) {
          // skip empty row
          if (((allValueColDeletedMap != null
                  && allValueColDeletedMap.isMarked(list.getValueIndex(sortedRowIndex)))
              || (list.isTimeDeleted(sortedRowIndex)))) {
            continue;
          }
          if (Objects.isNull(timeDuplicateInfo) || !timeDuplicateInfo[sortedRowIndex]) {
            times[pointsInPage++] = list.getTime(sortedRowIndex);
          }
        }
        alignedChunkWriter.write(times, pointsInPage, 0);
      }
      alignedChunkWriter.sealCurrentPage();
      alignedChunkWriter.clearPageWriter();
      try {
        ioTaskQueue.put(alignedChunkWriter);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }
  }

  @Override
  public synchronized void encode(BlockingQueue<Object> ioTaskQueue) {
    if (TVLIST_SORT_THRESHOLD == 0) {
      encodeWorkingAlignedTVList(ioTaskQueue);
      return;
    }

    AlignedChunkWriterImpl alignedChunkWriter = new AlignedChunkWriterImpl(schemaList);
    // create MergeSortAlignedTVListIterator.
    List<AlignedTVList> alignedTvLists = new ArrayList<>(sortedList);
    alignedTvLists.add(list);
    MemPointIterator timeValuePairIterator =
        MemPointIteratorFactory.create(dataTypes, null, alignedTvLists, ignoreAllNullRows);

    int pointNumInPage = 0;
    int pointNumInChunk = 0;
    long[] times = new long[MAX_NUMBER_OF_POINTS_IN_PAGE];

    while (timeValuePairIterator.hasNextBatch()) {
      TsBlock tsBlock = timeValuePairIterator.nextBatch();
      if (tsBlock == null) {
        continue;
      }
      for (int rowIndex = 0; rowIndex < tsBlock.getPositionCount(); rowIndex++) {
        long time = tsBlock.getTimeByIndex(rowIndex);
        times[pointNumInPage] = time;

        for (int columnIndex = 0; columnIndex < dataTypes.size(); columnIndex++) {
          ValueChunkWriter valueChunkWriter =
              alignedChunkWriter.getValueChunkWriterByIndex(columnIndex);
          if (tsBlock.getColumn(columnIndex).isNull(rowIndex)) {
            valueChunkWriter.write(time, null, true);
            continue;
          }
          switch (schemaList.get(columnIndex).getType()) {
            case BOOLEAN:
              valueChunkWriter.write(
                  time, tsBlock.getColumn(columnIndex).getBoolean(rowIndex), false);
              break;
            case INT32:
            case DATE:
              valueChunkWriter.write(time, tsBlock.getColumn(columnIndex).getInt(rowIndex), false);
              break;
            case INT64:
            case TIMESTAMP:
              valueChunkWriter.write(time, tsBlock.getColumn(columnIndex).getLong(rowIndex), false);
              break;
            case FLOAT:
              valueChunkWriter.write(
                  time, tsBlock.getColumn(columnIndex).getFloat(rowIndex), false);
              break;
            case DOUBLE:
              valueChunkWriter.write(
                  time, tsBlock.getColumn(columnIndex).getDouble(rowIndex), false);
              break;
            case TEXT:
            case BLOB:
            case STRING:
              valueChunkWriter.write(
                  time, tsBlock.getColumn(columnIndex).getBinary(rowIndex), false);
              break;
            default:
              break;
          }
        }
        pointNumInPage++;
        pointNumInChunk++;

        // new page
        if (pointNumInPage == MAX_NUMBER_OF_POINTS_IN_PAGE
            || pointNumInChunk >= maxNumberOfPointsInChunk) {
          alignedChunkWriter.write(times, pointNumInPage, 0);
          pointNumInPage = 0;
        }

        // new chunk
        if (pointNumInChunk >= maxNumberOfPointsInChunk) {
          alignedChunkWriter.sealCurrentPage();
          alignedChunkWriter.clearPageWriter();
          try {
            ioTaskQueue.put(alignedChunkWriter);
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
          }
          alignedChunkWriter = new AlignedChunkWriterImpl(schemaList);
          pointNumInChunk = 0;
        }
      }
    }

    // last batch of points
    if (pointNumInChunk > 0) {
      if (pointNumInPage > 0) {
        alignedChunkWriter.write(times, pointNumInPage, 0);
        alignedChunkWriter.sealCurrentPage();
        alignedChunkWriter.clearPageWriter();
      }
      try {
        ioTaskQueue.put(alignedChunkWriter);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }
  }

  @Override
  public void release() {
    maybeReleaseTvList(list);
    for (AlignedTVList alignedTvList : sortedList) {
      maybeReleaseTvList(alignedTvList);
    }
  }

  @Override
  public long getFirstPoint() {
    if (rowCount() == 0) {
      return Long.MAX_VALUE;
    }
    return getMinTime();
  }

  @Override
  public long getLastPoint() {
    if (rowCount() == 0) {
      return Long.MIN_VALUE;
    }
    return getMaxTime();
  }

  @Override
  public boolean isEmpty() {
    if (rowCount() == 0) {
      return true;
    }
    if (ignoreAllNullRows) {
      if (measurementIndexMap.isEmpty()) {
        return true;
      }

      if (list.rowCount() > 0) {
        BitMap allValueColDeletedMap = list.getAllValueColDeletedMap();
        if (allValueColDeletedMap == null || !allValueColDeletedMap.isAllMarked()) {
          return false;
        }
      }
      for (AlignedTVList alignedTvList : sortedList) {
        if (alignedTvList.rowCount() > 0) {
          BitMap allValueColDeletedMap = alignedTvList.getAllValueColDeletedMap();
          if (allValueColDeletedMap == null || !allValueColDeletedMap.isAllMarked()) {
            return false;
          }
        }
      }
      return true;
    }
    return false;
  }

  @Override
  public int serializedSize() {
    int size = 0;
    size += Integer.BYTES;
    for (IMeasurementSchema schema : schemaList) {
      size += schema.serializedSize();
    }
    size += Integer.BYTES;
    for (AlignedTVList alignedTvList : sortedList) {
      size += alignedTvList.serializedSize();
    }
    size += list.serializedSize();
    return size;
  }

  @Override
  public void serializeToWAL(IWALByteBufferView buffer) {
    WALWriteUtils.write(schemaList.size(), buffer);
    for (IMeasurementSchema schema : schemaList) {
      byte[] bytes = new byte[schema.serializedSize()];
      schema.serializeTo(ByteBuffer.wrap(bytes));
      buffer.put(bytes);
    }
    buffer.putInt(sortedList.size());
    for (AlignedTVList alignedTvList : sortedList) {
      alignedTvList.serializeToWAL(buffer);
    }
    list.serializeToWAL(buffer);
  }

  public static AlignedWritableMemChunk deserialize(DataInputStream stream, boolean isTableModel)
      throws IOException {
    int schemaListSize = stream.readInt();
    List<IMeasurementSchema> schemaList = new ArrayList<>(schemaListSize);
    for (int i = 0; i < schemaListSize; i++) {
      IMeasurementSchema schema = MeasurementSchema.deserializeFrom(stream);
      schemaList.add(schema);
    }
    int sortedListSize = stream.readInt();
    List<AlignedTVList> sortedList = new ArrayList<>();
    for (int i = 0; i < sortedListSize; i++) {
      AlignedTVList tvList = AlignedTVList.deserialize(stream);
      sortedList.add(tvList);
    }
    AlignedTVList list = AlignedTVList.deserialize(stream);
    AlignedWritableMemChunk chunk = new AlignedWritableMemChunk(schemaList, list, isTableModel);
    chunk.sortedList = sortedList;
    return chunk;
  }

  public static AlignedWritableMemChunk deserializeSingleTVListMemChunks(
      DataInputStream stream, boolean isTableModel) throws IOException {
    int schemaListSize = stream.readInt();
    List<IMeasurementSchema> schemaList = new ArrayList<>(schemaListSize);
    for (int i = 0; i < schemaListSize; i++) {
      IMeasurementSchema schema = MeasurementSchema.deserializeFrom(stream);
      schemaList.add(schema);
    }

    AlignedTVList list = AlignedTVList.deserialize(stream);
    return new AlignedWritableMemChunk(schemaList, list, isTableModel);
  }

  public List<IMeasurementSchema> getSchemaList() {
    return schemaList;
  }

  public boolean isAllDeleted() {
    if (!list.isAllDeleted()) {
      return false;
    }
    for (AlignedTVList alignedTvList : sortedList) {
      if (!alignedTvList.isAllDeleted()) {
        return false;
      }
    }
    return true;
  }

  @Override
  public List<AlignedTVList> getSortedList() {
    return sortedList;
  }

  public List<Integer> buildColumnIndexList(List<IMeasurementSchema> schemaList) {
    List<Integer> columnIndexList = new ArrayList<>();
    for (IMeasurementSchema measurementSchema : schemaList) {
      columnIndexList.add(
          measurementIndexMap.getOrDefault(measurementSchema.getMeasurementName(), -1));
    }
    return columnIndexList;
  }

  private List<Integer> calculateColumnIndices(List<IMeasurementSchema> schemaListInInsertPlan) {
    List<Integer> columnIndexList = new ArrayList<>();
    for (IMeasurementSchema measurementSchema : schemaListInInsertPlan) {
      if (measurementSchema != null) {
        Integer index = this.measurementIndexMap.get(measurementSchema.getMeasurementName());
        // Index is null means this measurement was not in this AlignedTVList before.
        // We need to extend a new column in AlignedMemChunk and AlignedTVList.
        // And the reorderedColumnValues should extend one more column for the new measurement
        if (index == null) {
          index = this.list.getColumnSize();
          this.measurementIndexMap.put(measurementSchema.getMeasurementName(), index);
          this.schemaList.add(measurementSchema);
          this.list.extendColumn(measurementSchema.getType());
        }
        columnIndexList.add(index);
      } else {
        columnIndexList.add(-1);
      }
    }
    return columnIndexList;
  }

  private void filterDeletedTimeStamp(
      AlignedTVList alignedTVList,
      List<List<TimeRange>> valueColumnsDeletionList,
      boolean ignoreAllNullRows,
      Map<Long, BitMap> timestampWithBitmap) {
    BitMap allValueColDeletedMap = alignedTVList.getAllValueColDeletedMap();

    int rowCount = alignedTVList.rowCount();
    List<int[]> valueColumnDeleteCursor = new ArrayList<>();
    if (valueColumnsDeletionList != null) {
      valueColumnsDeletionList.forEach(x -> valueColumnDeleteCursor.add(new int[] {0}));
    }

    for (int row = 0; row < rowCount; row++) {
      // the row is deleted
      if (allValueColDeletedMap != null && allValueColDeletedMap.isMarked(row)) {
        continue;
      }
      long timestamp = alignedTVList.getTime(row);

      BitMap bitMap = new BitMap(schemaList.size());
      for (int column = 0; column < schemaList.size(); column++) {
        if (alignedTVList.isNullValue(alignedTVList.getValueIndex(row), column)) {
          bitMap.mark(column);
        }

        // skip deleted row
        if (valueColumnsDeletionList != null
            && !valueColumnsDeletionList.isEmpty()
            && isPointDeleted(
                timestamp,
                valueColumnsDeletionList.get(column),
                valueColumnDeleteCursor.get(column))) {
          bitMap.mark(column);
        }

        // skip all-null row
        if (ignoreAllNullRows && bitMap.isAllMarked()) {
          continue;
        }
        timestampWithBitmap.put(timestamp, bitMap);
      }
    }
  }

  public long[] getFilteredTimestamp(
      List<List<TimeRange>> deletionList, List<BitMap> bitMaps, boolean ignoreAllNullRows) {
    Map<Long, BitMap> timestampWithBitmap = new TreeMap<>();

    filterDeletedTimeStamp(list, deletionList, ignoreAllNullRows, timestampWithBitmap);
    for (AlignedTVList alignedTVList : sortedList) {
      filterDeletedTimeStamp(alignedTVList, deletionList, ignoreAllNullRows, timestampWithBitmap);
    }

    List<Long> filteredTimestamps = new ArrayList<>();
    for (Map.Entry<Long, BitMap> entry : timestampWithBitmap.entrySet()) {
      filteredTimestamps.add(entry.getKey());
      bitMaps.add(entry.getValue());
    }
    return filteredTimestamps.stream().mapToLong(Long::valueOf).toArray();
  }
}
