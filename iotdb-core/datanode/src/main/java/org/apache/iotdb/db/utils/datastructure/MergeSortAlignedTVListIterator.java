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
import org.apache.tsfile.read.TimeValuePair;
import org.apache.tsfile.read.reader.IPointReader;
import org.apache.tsfile.utils.BitMap;
import org.apache.tsfile.utils.TsPrimitiveType;

import java.io.IOException;
import java.util.List;

public class MergeSortAlignedTVListIterator implements IPointReader {
  private final AlignedTVList.AlignedTVListIterator[] alignedTvListIterators;
  private final int columnNum;

  private boolean probeNext = false;
  private boolean hasNext = false;

  private final int[] alignedTvListOffsets;

  // Remember the selected columns for prepareNext
  // column index -> [selectedTVList, selectedIndex]
  private final int[][] columnAccessInfo;

  private long time;
  private final BitMap bitMap;

  public MergeSortAlignedTVListIterator(
      List<AlignedTVList> alignedTvLists,
      List<TSDataType> tsDataTypes,
      List<Integer> columnIndexList,
      Integer floatPrecision,
      List<TSEncoding> encodingList,
      boolean ignoreAllNullRows) {
    this.alignedTvListIterators = new AlignedTVList.AlignedTVListIterator[alignedTvLists.size()];
    for (int i = 0; i < alignedTvLists.size(); i++) {
      alignedTvListIterators[i] =
          alignedTvLists
              .get(i)
              .iterator(
                  tsDataTypes, columnIndexList, ignoreAllNullRows, floatPrecision, encodingList);
    }
    this.alignedTvListOffsets = new int[alignedTvLists.size()];
    this.columnNum = tsDataTypes.size();
    this.columnAccessInfo = new int[columnNum][];
    for (int i = 0; i < columnAccessInfo.length; i++) {
      columnAccessInfo[i] = new int[2];
    }
    this.bitMap = new BitMap(columnNum);
  }

  public MergeSortAlignedTVListIterator(
      AlignedTVList.AlignedTVListIterator[] alignedTvListIterators, int columnNum) {
    this.alignedTvListIterators =
        new AlignedTVList.AlignedTVListIterator[alignedTvListIterators.length];
    for (int i = 0; i < alignedTvListIterators.length; i++) {
      this.alignedTvListIterators[i] = alignedTvListIterators[i].clone();
    }
    this.alignedTvListOffsets = new int[alignedTvListIterators.length];
    this.columnNum = columnNum;
    this.columnAccessInfo = new int[columnNum][];
    for (int i = 0; i < columnAccessInfo.length; i++) {
      columnAccessInfo[i] = new int[2];
    }
    this.bitMap = new BitMap(columnNum);
  }

  private void prepareNextRow() {
    time = Long.MAX_VALUE;
    for (int i = 0; i < alignedTvListIterators.length; i++) {
      AlignedTVList.AlignedTVListIterator iterator = alignedTvListIterators[i];
      if (iterator.hasNext() && iterator.currentTime() <= time) {
        if (i == 0 || iterator.currentTime() < time) {
          for (int columnIndex = 0; columnIndex < columnNum; columnIndex++) {
            int rowIndex = iterator.getSelectedIndex(columnIndex);
            columnAccessInfo[columnIndex][0] = i;
            columnAccessInfo[columnIndex][1] = rowIndex;
            if (iterator.isNull(rowIndex, columnIndex)) {
              bitMap.mark(columnIndex);
            }
          }
          time = iterator.currentTime();
        } else {
          for (int columnIndex = 0; columnIndex < columnNum; columnIndex++) {
            int rowIndex = iterator.getSelectedIndex(columnIndex);
            // update if the column is not null
            if (!iterator.isNull(rowIndex, columnIndex)) {
              columnAccessInfo[columnIndex][0] = i;
              columnAccessInfo[columnIndex][1] = rowIndex;
              bitMap.unmark(columnIndex);
            }
          }
        }
        hasNext = true;
      }
    }
    probeNext = true;
  }

  @Override
  public boolean hasNextTimeValuePair() {
    if (!probeNext) {
      prepareNextRow();
    }
    return hasNext;
  }

  @Override
  public TimeValuePair nextTimeValuePair() {
    if (!hasNextTimeValuePair()) {
      return null;
    }
    TimeValuePair tvPair = buildTimeValuePair();
    step();
    return tvPair;
  }

  @Override
  public TimeValuePair currentTimeValuePair() {
    if (!hasNextTimeValuePair()) {
      return null;
    }
    return buildTimeValuePair();
  }

  private TimeValuePair buildTimeValuePair() {
    TsPrimitiveType[] vector = new TsPrimitiveType[columnNum];
    for (int columnIndex = 0; columnIndex < vector.length; columnIndex++) {
      int[] accessInfo = columnAccessInfo[columnIndex];
      AlignedTVList.AlignedTVListIterator iterator = alignedTvListIterators[accessInfo[0]];
      vector[columnIndex] = iterator.getPrimitiveObject(accessInfo[1], columnIndex);
    }
    return new TimeValuePair(time, TsPrimitiveType.getByType(TSDataType.VECTOR, vector));
  }

  public void step() {
    for (int i = 0; i < alignedTvListIterators.length; i++) {
      AlignedTVList.AlignedTVListIterator iterator = alignedTvListIterators[i];
      if (iterator.hasCurrent() && iterator.currentTime() == time) {
        alignedTvListIterators[i].step();
        alignedTvListOffsets[i] = alignedTvListIterators[i].getIndex();
      }
    }
    probeNext = false;
    hasNext = false;
    bitMap.reset();
  }

  @Override
  public long getUsedMemorySize() {
    // not used
    return 0;
  }

  @Override
  public void close() throws IOException {}

  public int[] getAlignedTVListOffsets() {
    return alignedTvListOffsets;
  }

  public void setAlignedTVListOffsets(int[] alignedTvListOffsets) {
    for (int i = 0; i < alignedTvListIterators.length; i++) {
      alignedTvListIterators[i].setIndex(alignedTvListOffsets[i]);
      this.alignedTvListOffsets[i] = alignedTvListOffsets[i];
    }
    probeNext = false;
    hasNext = false;
    bitMap.reset();
  }

  public int[][] getColumnAccessInfo() {
    return columnAccessInfo;
  }

  public long getTime() {
    return time;
  }

  public TsPrimitiveType getPrimitiveObject(int[] accessInfo, int columnIndex) {
    if (columnIndex >= columnAccessInfo.length) {
      return null;
    }
    AlignedTVList.AlignedTVListIterator iterator = alignedTvListIterators[accessInfo[0]];
    return iterator.getPrimitiveObject(accessInfo[1], columnIndex);
  }

  public BitMap getBitmap() {
    return bitMap;
  }

  @Override
  public MergeSortAlignedTVListIterator clone() {
    return new MergeSortAlignedTVListIterator(alignedTvListIterators, columnNum);
  }
}
