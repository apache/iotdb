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

package org.apache.iotdb.db.utils.datastructure;

import org.apache.iotdb.db.queryengine.plan.statement.component.Ordering;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.read.common.TimeRange;
import org.apache.tsfile.read.filter.basic.Filter;
import org.apache.tsfile.utils.BitMap;
import org.apache.tsfile.utils.Pair;
import org.apache.tsfile.write.UnSupportedDataTypeException;
import org.apache.tsfile.write.chunk.AlignedChunkWriterImpl;
import org.apache.tsfile.write.chunk.IChunkWriter;
import org.apache.tsfile.write.chunk.ValueChunkWriter;

import java.util.Iterator;
import java.util.List;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.apache.iotdb.db.utils.ModificationUtils.isPointDeleted;

public class MergeSortMultiAlignedTVListIterator extends MultiAlignedTVListIterator {
  private final Set<Integer> probeIterators;
  private final int[] iteratorIndices;
  private final int[] rowIndices;

  private final BitMap bitMap;
  // Min-Heap: minimal timestamp; if same timestamp, maximum TVList index
  private final PriorityQueue<Pair<Long, Integer>> heap;

  public MergeSortMultiAlignedTVListIterator(
      List<TSDataType> tsDataTypes,
      List<Integer> columnIndexList,
      List<AlignedTVList> alignedTvLists,
      List<Integer> tvListRowCounts,
      Ordering scanOrder,
      Filter globalTimeFilter,
      List<TimeRange> timeColumnDeletion,
      List<List<TimeRange>> valueColumnsDeletionList,
      Integer floatPrecision,
      List<TSEncoding> encodingList,
      boolean ignoreAllNullRows,
      int maxNumberOfPointsInPage) {
    super(
        tsDataTypes,
        columnIndexList,
        alignedTvLists,
        tvListRowCounts,
        scanOrder,
        globalTimeFilter,
        timeColumnDeletion,
        valueColumnsDeletionList,
        floatPrecision,
        encodingList,
        ignoreAllNullRows,
        maxNumberOfPointsInPage);
    this.probeIterators =
        IntStream.range(0, alignedTvListIterators.size()).boxed().collect(Collectors.toSet());
    this.bitMap = new BitMap(tsDataTypeList.size());
    this.iteratorIndices = new int[tsDataTypeList.size()];
    this.rowIndices = new int[tsDataTypeList.size()];
    this.ignoreAllNullRows = ignoreAllNullRows;
    this.heap =
        new PriorityQueue<>(
            scanOrder.isAscending()
                ? (a, b) ->
                    a.left.equals(b.left) ? b.right.compareTo(a.right) : a.left.compareTo(b.left)
                : (a, b) ->
                    a.left.equals(b.left) ? a.right.compareTo(b.right) : b.left.compareTo(a.left));
  }

  @Override
  protected void skipToCurrentTimeRangeStartPosition() {
    hasNext = false;
    probeIterators.clear();
    for (int i = 0; i < alignedTvListIterators.size(); i++) {
      AlignedTVList.AlignedTVListIterator iterator = alignedTvListIterators.get(i);
      iterator.skipToCurrentTimeRangeStartPosition();
      if (iterator.hasNextTimeValuePair()) {
        probeIterators.add(i);
      }
    }
    probeNext = false;
  }

  @Override
  protected void prepareNext() {
    hasNext = false;
    for (int i : probeIterators) {
      AlignedTVList.AlignedTVListIterator iterator = alignedTvListIterators.get(i);
      if (iterator.hasNextTimeValuePair()) {
        heap.add(new Pair<>(iterator.currentTime(), i));
      }
    }
    probeIterators.clear();

    while (!heap.isEmpty() && !hasNext) {
      bitMap.reset();
      Pair<Long, Integer> top = heap.poll();
      currentTime = top.left;
      probeIterators.add(top.right);

      for (int columnIndex = 0; columnIndex < tsDataTypeList.size(); columnIndex++) {
        iteratorIndices[columnIndex] = top.right;
        rowIndices[columnIndex] =
            alignedTvListIterators.get(top.right).getSelectedIndex(columnIndex);
        if (alignedTvListIterators
            .get(top.right)
            .isNullValue(rowIndices[columnIndex], columnIndex)) {
          bitMap.mark(columnIndex);
        }
        // check valueColumnsDeletionList
        if (valueColumnsDeletionList != null
            && isPointDeleted(
                currentTime,
                valueColumnsDeletionList.get(columnIndex),
                valueColumnDeleteCursor.get(columnIndex),
                scanOrder)) {
          iteratorIndices[columnIndex] = -1;
          bitMap.mark(columnIndex);
        }
      }
      hasNext = true;

      // duplicated timestamps
      while (!heap.isEmpty() && heap.peek().left == currentTime) {
        Pair<Long, Integer> element = heap.poll();
        probeIterators.add(element.right);

        for (int columnIndex = 0; columnIndex < tsDataTypeList.size(); columnIndex++) {
          // if current column null, it needs update
          int iteratorIndex = currentIteratorIndex(columnIndex);
          if (iteratorIndex == -1) {
            // -1 means all point of this timestamp was deleted by Deletion and no further
            // processing is required.
            continue;
          }
          if (alignedTvListIterators
              .get(iteratorIndex)
              .isNullValue(rowIndices[columnIndex], columnIndex)) {
            iteratorIndices[columnIndex] = element.right;
            rowIndices[columnIndex] =
                alignedTvListIterators.get(element.right).getSelectedIndex(columnIndex);
            if (!alignedTvListIterators
                .get(element.right)
                .isNullValue(rowIndices[columnIndex], columnIndex)) {
              bitMap.unmark(columnIndex);
            }
          }

          // check valueColumnsDeletionList
          if (valueColumnsDeletionList != null
              && isPointDeleted(
                  currentTime,
                  valueColumnsDeletionList.get(columnIndex),
                  valueColumnDeleteCursor.get(columnIndex),
                  scanOrder)) {
            iteratorIndices[columnIndex] = -1;
            bitMap.mark(columnIndex);
          }
        }
      }

      if (ignoreAllNullRows && bitMap.isAllMarked()) {
        Iterator<Integer> it = probeIterators.iterator();
        while (it.hasNext()) {
          int idx = it.next();
          AlignedTVList.AlignedTVListIterator iterator = alignedTvListIterators.get(idx);
          iterator.next();
          if (iterator.hasNextTimeValuePair()) {
            heap.add(new Pair<>(iterator.currentTime(), idx));
          } else {
            it.remove();
          }
        }
        hasNext = false;
      }
    }
    probeNext = true;
  }

  @Override
  protected void next() {
    for (int index : probeIterators) {
      alignedTvListIterators.get(index).next();
    }
    probeNext = false;
  }

  @Override
  public void encodeBatch(IChunkWriter chunkWriter, BatchEncodeInfo encodeInfo, long[] times) {
    AlignedChunkWriterImpl alignedChunkWriterImpl = (AlignedChunkWriterImpl) chunkWriter;
    while (hasNextTimeValuePair()) {
      times[encodeInfo.pointNumInPage] = currentTime;
      for (int columnIndex = 0; columnIndex < tsDataTypeList.size(); columnIndex++) {
        ValueChunkWriter valueChunkWriter =
            alignedChunkWriterImpl.getValueChunkWriterByIndex(columnIndex);
        int iteratorIndex = currentIteratorIndex(columnIndex);
        if (iteratorIndex == -1) {
          valueChunkWriter.write(currentTime, null, true);
          continue;
        }
        AlignedTVList alignedTVList = alignedTvListIterators.get(iteratorIndex).getAlignedTVList();

        // sanity check
        int validColumnIndex =
            columnIndexList != null ? columnIndexList.get(columnIndex) : columnIndex;
        if (validColumnIndex < 0 || validColumnIndex >= alignedTVList.dataTypes.size()) {
          valueChunkWriter.write(currentTime, null, true);
          continue;
        }
        int valueIndex = alignedTVList.getValueIndex(currentRowIndex(columnIndex));

        // null value
        if (alignedTVList.isNullValue(valueIndex, validColumnIndex)) {
          valueChunkWriter.write(currentTime, null, true);
          continue;
        }

        switch (tsDataTypeList.get(columnIndex)) {
          case BOOLEAN:
            valueChunkWriter.write(
                currentTime,
                alignedTVList.getBooleanByValueIndex(valueIndex, validColumnIndex),
                false);
            break;
          case INT32:
          case DATE:
            valueChunkWriter.write(
                currentTime, alignedTVList.getIntByValueIndex(valueIndex, validColumnIndex), false);
            break;
          case INT64:
          case TIMESTAMP:
            valueChunkWriter.write(
                currentTime,
                alignedTVList.getLongByValueIndex(valueIndex, validColumnIndex),
                false);
            break;
          case FLOAT:
            valueChunkWriter.write(
                currentTime,
                alignedTVList.getFloatByValueIndex(valueIndex, validColumnIndex),
                false);
            break;
          case DOUBLE:
            valueChunkWriter.write(
                currentTime,
                alignedTVList.getDoubleByValueIndex(valueIndex, validColumnIndex),
                false);
            break;
          case TEXT:
          case BLOB:
          case OBJECT:
          case STRING:
            valueChunkWriter.write(
                currentTime,
                alignedTVList.getBinaryByValueIndex(valueIndex, validColumnIndex),
                false);
            break;
          default:
            throw new UnSupportedDataTypeException(
                String.format("Data type %s is not supported.", tsDataTypeList.get(columnIndex)));
        }
      }
      next();
      encodeInfo.pointNumInPage++;
      encodeInfo.pointNumInChunk++;

      // new page
      if (encodeInfo.pointNumInPage >= encodeInfo.maxNumberOfPointsInPage
          || encodeInfo.pointNumInChunk >= encodeInfo.maxNumberOfPointsInChunk) {
        alignedChunkWriterImpl.write(times, encodeInfo.pointNumInPage, 0);
        encodeInfo.pointNumInPage = 0;
        break;
      }
    }
  }

  @Override
  protected int currentIteratorIndex(int columnIndex) {
    return iteratorIndices[columnIndex];
  }

  @Override
  protected int currentRowIndex(int columnIndex) {
    return rowIndices[columnIndex];
  }

  @Override
  public void setCurrentPageTimeRange(TimeRange timeRange) {
    for (TVList.TVListIterator tvListIterator : this.alignedTvListIterators) {
      tvListIterator.timeRange = timeRange;
    }
    super.setCurrentPageTimeRange(timeRange);
  }
}
