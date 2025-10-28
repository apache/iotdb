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
import org.apache.tsfile.write.chunk.IChunkWriter;

import java.util.List;

import static org.apache.iotdb.db.utils.ModificationUtils.isPointDeleted;

public class OrderedMultiAlignedTVListIterator extends MultiAlignedTVListIterator {
  private final BitMap bitMap;
  private int iteratorIndex = 0;
  private int[] rowIndices;

  public OrderedMultiAlignedTVListIterator(
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
    this.bitMap = new BitMap(tsDataTypeList.size());
    this.ignoreAllNullRows = ignoreAllNullRows;
  }

  @Override
  protected void prepareNext() {
    hasNext = false;

    while (iteratorIndex < alignedTvListIterators.size() && !hasNext) {
      AlignedTVList.AlignedTVListIterator iterator = alignedTvListIterators.get(iteratorIndex);
      if (!iterator.hasNextTimeValuePair()) {
        iteratorIndex++;
        continue;
      }

      bitMap.reset();
      rowIndices = iterator.getSelectedIndices();
      currentTime = iterator.currentTime();
      hasNext = true;

      // check valueColumnsDeletionList
      for (int columnIndex = 0; columnIndex < tsDataTypeList.size(); columnIndex++) {
        if ((valueColumnsDeletionList != null
                && isPointDeleted(
                    currentTime,
                    valueColumnsDeletionList.get(columnIndex),
                    valueColumnDeleteCursor.get(columnIndex),
                    scanOrder))
            || iterator.isNullValue(rowIndices[columnIndex], columnIndex)) {
          bitMap.mark(columnIndex);
        }
      }
      if (ignoreAllNullRows && bitMap.isAllMarked()) {
        iterator.next();
        hasNext = false;
      }
    }
    probeNext = true;
  }

  @Override
  protected void skipToCurrentTimeRangeStartPosition() {
    hasNext = false;
    iteratorIndex = 0;
    while (iteratorIndex < alignedTvListIterators.size() && !hasNext) {
      AlignedTVList.AlignedTVListIterator iterator = alignedTvListIterators.get(iteratorIndex);
      iterator.skipToCurrentTimeRangeStartPosition();
      if (!iterator.hasNextTimeValuePair()) {
        iteratorIndex++;
        continue;
      }
      hasNext = iterator.hasNextTimeValuePair();
    }
    probeNext = false;
  }

  @Override
  protected void next() {
    TVList.TVListIterator iterator = alignedTvListIterators.get(iteratorIndex);
    iterator.next();
    rowIndices = null;
    probeNext = false;
  }

  @Override
  public void encodeBatch(IChunkWriter chunkWriter, BatchEncodeInfo encodeInfo, long[] times) {
    while (iteratorIndex < alignedTvListIterators.size()) {
      TVList.TVListIterator iterator = alignedTvListIterators.get(iteratorIndex);
      if (!iterator.hasNextBatch()) {
        iteratorIndex++;
        continue;
      }
      iterator.encodeBatch(chunkWriter, encodeInfo, times);
      break;
    }
    probeNext = false;
  }

  @Override
  protected int currentIteratorIndex(int columnIndex) {
    return iteratorIndex;
  }

  @Override
  protected int currentRowIndex(int columnIndex) {
    return rowIndices[columnIndex];
  }

  @Override
  public void setCurrentPageTimeRange(TimeRange timeRange) {
    for (AlignedTVList.AlignedTVListIterator iterator : alignedTvListIterators) {
      iterator.timeRange = timeRange;
    }
    super.setCurrentPageTimeRange(timeRange);
  }
}
