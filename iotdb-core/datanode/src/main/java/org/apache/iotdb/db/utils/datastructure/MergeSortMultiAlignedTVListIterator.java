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
import org.apache.tsfile.utils.Pair;

import java.util.ArrayList;
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
  private final List<int[]> valueColumnDeleteCursor;
  // Min-Heap: minimal timestamp; if same timestamp, maximum TVList index
  private final PriorityQueue<Pair<Long, Integer>> minHeap =
      new PriorityQueue<>(
          (a, b) -> a.left.equals(b.left) ? b.right.compareTo(a.right) : a.left.compareTo(b.left));

  public MergeSortMultiAlignedTVListIterator(
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
    this.probeIterators =
        IntStream.range(0, alignedTvListIterators.size()).boxed().collect(Collectors.toSet());
    this.bitMap = new BitMap(tsDataTypeList.size());
    this.iteratorIndices = new int[tsDataTypeList.size()];
    this.rowIndices = new int[tsDataTypeList.size()];
    this.valueColumnDeleteCursor = new ArrayList<>();
    for (int i = 0; i < tsDataTypeList.size(); i++) {
      valueColumnDeleteCursor.add(new int[] {0});
    }
    this.ignoreAllNullRows = ignoreAllNullRows;
  }

  @Override
  protected void prepareNext() {
    hasNext = false;
    for (int i : probeIterators) {
      AlignedTVList.AlignedTVListIterator iterator = alignedTvListIterators.get(i);
      if (iterator.hasNextTimeValuePair()) {
        minHeap.add(new Pair<>(iterator.currentTime(), i));
      }
    }
    probeIterators.clear();

    while (!minHeap.isEmpty() && !hasNext) {
      bitMap.reset();
      Pair<Long, Integer> top = minHeap.poll();
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
      }
      hasNext = true;

      // duplicated timestamps
      while (!minHeap.isEmpty() && minHeap.peek().left == currentTime) {
        Pair<Long, Integer> element = minHeap.poll();
        probeIterators.add(element.right);

        for (int columnIndex = 0; columnIndex < tsDataTypeList.size(); columnIndex++) {
          // if current column null, it needs update
          if (alignedTvListIterators
              .get(iteratorIndices[columnIndex])
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
        }
      }

      // check valueColumnsDeletionList
      for (int columnIndex = 0; columnIndex < tsDataTypeList.size(); columnIndex++) {
        if (valueColumnsDeletionList != null
            && isPointDeleted(
                currentTime,
                valueColumnsDeletionList.get(columnIndex),
                valueColumnDeleteCursor.get(columnIndex))) {
          iteratorIndices[columnIndex] = -1;
          bitMap.mark(columnIndex);
        }
      }
      if (ignoreAllNullRows && bitMap.isAllMarked()) {
        Iterator<Integer> it = probeIterators.iterator();
        while (it.hasNext()) {
          int idx = it.next();
          AlignedTVList.AlignedTVListIterator iterator = alignedTvListIterators.get(idx);
          iterator.next();
          if (iterator.hasNextTimeValuePair()) {
            minHeap.add(new Pair<>(iterator.currentTime(), idx));
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
  protected int currentIteratorIndex(int columnIndex) {
    return iteratorIndices[columnIndex];
  }

  @Override
  protected int currentRowIndex(int columnIndex) {
    return rowIndices[columnIndex];
  }
}
