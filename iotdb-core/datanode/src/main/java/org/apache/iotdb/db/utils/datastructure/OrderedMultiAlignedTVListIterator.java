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

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.read.common.TimeRange;
import org.apache.tsfile.utils.BitMap;

import java.util.ArrayList;
import java.util.List;

import static org.apache.iotdb.db.utils.ModificationUtils.isPointDeleted;

public class OrderedMultiAlignedTVListIterator extends MultiAlignedTVListIterator {
  private final BitMap bitMap;
  private final List<int[]> valueColumnDeleteCursor;
  private int iteratorIndex = 0;
  private int[] rowIndices;

  public OrderedMultiAlignedTVListIterator(
      List<TSDataType> tsDataTypes,
      List<Integer> columnIndexList,
      List<AlignedTVList> alignedTvLists,
      List<TimeRange> timeColumnDeletion,
      List<List<TimeRange>> valueColumnsDeletionList,
      Integer floatPrecision,
      List<TSEncoding> encodingList,
      boolean ignoreAllNullRows) {
    super(
        tsDataTypes,
        columnIndexList,
        alignedTvLists,
        timeColumnDeletion,
        valueColumnsDeletionList,
        floatPrecision,
        encodingList,
        ignoreAllNullRows);
    this.bitMap = new BitMap(tsDataTypeList.size());
    this.valueColumnDeleteCursor = new ArrayList<>();
    for (int i = 0; i < tsDataTypeList.size(); i++) {
      valueColumnDeleteCursor.add(new int[] {0});
    }
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
                    valueColumnDeleteCursor.get(columnIndex)))
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
  protected void next() {
    TVList.TVListIterator iterator = alignedTvListIterators.get(iteratorIndex);
    iterator.next();
    rowIndices = null;
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
}
