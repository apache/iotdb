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
import org.apache.tsfile.utils.Pair;
import org.apache.tsfile.utils.TsPrimitiveType;

import java.io.IOException;
import java.util.List;
import java.util.PriorityQueue;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class MergeSortAlignedTVListIterator implements IPointReader {
  private final AlignedTVList.AlignedTVListIterator[] alignedTvListIterators;
  private final int columnNum;

  private boolean probeNext = false;
  private boolean hasNext = false;
  private final List<Integer> probeIterators;

  // Min-Heap: minimal timestamp; if same timestamp, maximum TVList index
  private final PriorityQueue<Pair<Long, Integer>> minHeap =
      new PriorityQueue<>(
          (a, b) -> a.left.equals(b.left) ? b.right.compareTo(a.right) : a.left.compareTo(b.left));

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
      List<TSEncoding> encodingList) {
    this.alignedTvListIterators = new AlignedTVList.AlignedTVListIterator[alignedTvLists.size()];
    for (int i = 0; i < alignedTvLists.size(); i++) {
      alignedTvListIterators[i] =
          alignedTvLists
              .get(i)
              .iterator(tsDataTypes, columnIndexList, floatPrecision, encodingList);
    }
    this.alignedTvListOffsets = new int[alignedTvLists.size()];
    this.columnNum = tsDataTypes.size();
    this.columnAccessInfo = new int[columnNum][];
    for (int i = 0; i < columnAccessInfo.length; i++) {
      columnAccessInfo[i] = new int[2];
    }
    this.bitMap = new BitMap(columnNum);
    this.probeIterators =
        IntStream.range(0, alignedTvListIterators.length).boxed().collect(Collectors.toList());
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
    this.probeIterators =
        IntStream.range(0, alignedTvListIterators.length).boxed().collect(Collectors.toList());
  }

  private void prepareNextRow() {
    for (int i : probeIterators) {
      TVList.TVListIterator iterator = alignedTvListIterators[i];
      if (iterator.hasNext()) {
        minHeap.add(new Pair<>(iterator.currentTime(), i));
      }
    }
    probeIterators.clear();

    if (!minHeap.isEmpty()) {
      Pair<Long, Integer> top = minHeap.poll();
      time = top.left;
      probeIterators.add(top.right);
      for (int columnIndex = 0; columnIndex < columnNum; columnIndex++) {
        int rowIndex = alignedTvListIterators[top.right].getSelectedIndex(columnIndex);
        columnAccessInfo[columnIndex][0] = top.right;
        columnAccessInfo[columnIndex][1] = rowIndex;
        if (alignedTvListIterators[top.right].isNull(rowIndex, columnIndex)) {
          bitMap.mark(columnIndex);
        }
      }
      while (!minHeap.isEmpty() && minHeap.peek().left == time) {
        Pair<Long, Integer> element = minHeap.poll();
        probeIterators.add(element.right);
        for (int columnIndex = 0; columnIndex < columnNum; columnIndex++) {
          // if the column is currently not null, it needs not update
          if (bitMap.isMarked(columnIndex)) {
            int rowIndex = alignedTvListIterators[element.right].getSelectedIndex(columnIndex);
            if (!alignedTvListIterators[element.right].isNull(rowIndex, columnIndex)) {
              columnAccessInfo[columnIndex][0] = element.right;
              columnAccessInfo[columnIndex][1] = rowIndex;
              bitMap.unmark(columnIndex);
            }
          }
        }
      }
      hasNext = true;
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
    for (int index : probeIterators) {
      TVList.TVListIterator iterator = alignedTvListIterators[index];
      iterator.step();
      alignedTvListOffsets[index] = iterator.getIndex();
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
    minHeap.clear();
    probeIterators.clear();
    for (int i = 0; i < alignedTvListIterators.length; i++) {
      probeIterators.add(i);
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
