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
import org.apache.tsfile.utils.Pair;
import org.apache.tsfile.utils.TsPrimitiveType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.PriorityQueue;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class MergeSortAlignedTVListIterator implements IPointReader {
  private List<AlignedTVList.AlignedTVListIterator> alignedTvListIterators;

  private boolean probeNext = false;
  private TimeValuePair currentTvPair;

  private List<Integer> probeIterators;

  // Min-Heap: minimal timestamp; if same timestamp, maximum TVList index
  private final PriorityQueue<Pair<Long, Integer>> minHeap =
      new PriorityQueue<>(
          (a, b) -> a.left.equals(b.left) ? b.right.compareTo(a.right) : a.left.compareTo(b.left));

  private int[] alignedTvListOffsets;

  public MergeSortAlignedTVListIterator(
      List<AlignedTVList> alignedTvLists,
      List<TSDataType> tsDataTypes,
      List<Integer> columnIndexList,
      Integer floatPrecision,
      List<TSEncoding> encodingList,
      boolean ignoreAllNullRows) {
    this.alignedTvListIterators = new ArrayList<>(alignedTvLists.size());
    for (AlignedTVList alignedTvList : alignedTvLists) {
      alignedTvListIterators.add(
          alignedTvList.iterator(
              tsDataTypes, columnIndexList, ignoreAllNullRows, floatPrecision, encodingList));
    }
    this.alignedTvListOffsets = new int[alignedTvLists.size()];
    this.probeIterators =
        IntStream.range(0, alignedTvListIterators.size()).boxed().collect(Collectors.toList());
  }

  private MergeSortAlignedTVListIterator() {}

  private void prepareNextRow() {
    currentTvPair = null;
    for (int i : probeIterators) {
      TVList.TVListIterator iterator = alignedTvListIterators.get(i);
      if (iterator.hasNext()) {
        minHeap.add(new Pair<>(iterator.currentTime(), i));
      }
    }
    probeIterators.clear();

    if (!minHeap.isEmpty()) {
      Pair<Long, Integer> top = minHeap.poll();
      long time = top.left;
      probeIterators.add(top.right);
      currentTvPair = alignedTvListIterators.get(top.right).current();
      TsPrimitiveType[] currentValues = currentTvPair.getValue().getVector();

      while (!minHeap.isEmpty() && minHeap.peek().left == time) {
        Pair<Long, Integer> element = minHeap.poll();
        probeIterators.add(element.right);

        TimeValuePair tvPair = alignedTvListIterators.get(element.right).current();
        TsPrimitiveType[] values = tvPair.getValue().getVector();
        for (int columnIndex = 0; columnIndex < values.length; columnIndex++) {
          // if the column is currently not null, it needs not update
          if (currentValues[columnIndex] == null) {
            currentValues[columnIndex] = values[columnIndex];
          }
        }
      }
    }
    probeNext = true;
  }

  @Override
  public boolean hasNextTimeValuePair() {
    if (!probeNext) {
      prepareNextRow();
    }
    return currentTvPair != null;
  }

  @Override
  public TimeValuePair nextTimeValuePair() {
    if (!hasNextTimeValuePair()) {
      return null;
    }
    step();
    return currentTvPair;
  }

  @Override
  public TimeValuePair currentTimeValuePair() {
    if (!hasNextTimeValuePair()) {
      return null;
    }
    return currentTvPair;
  }

  public void step() {
    for (int index : probeIterators) {
      TVList.TVListIterator iterator = alignedTvListIterators.get(index);
      iterator.step();
      alignedTvListOffsets[index] = iterator.getIndex();
    }
    probeNext = false;
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
    for (int i = 0; i < alignedTvListIterators.size(); i++) {
      alignedTvListIterators.get(i).setIndex(alignedTvListOffsets[i]);
      this.alignedTvListOffsets[i] = alignedTvListOffsets[i];
    }
    minHeap.clear();
    probeIterators.clear();
    for (int i = 0; i < alignedTvListIterators.size(); i++) {
      probeIterators.add(i);
    }
    probeNext = false;
  }

  @Override
  public MergeSortAlignedTVListIterator clone() {
    MergeSortAlignedTVListIterator cloneIterator = new MergeSortAlignedTVListIterator();
    cloneIterator.alignedTvListIterators = new ArrayList<>(alignedTvListIterators.size());
    for (int i = 0; i < alignedTvListIterators.size(); i++) {
      cloneIterator.alignedTvListIterators.add(alignedTvListIterators.get(i).clone());
    }
    cloneIterator.alignedTvListOffsets = new int[alignedTvListIterators.size()];
    cloneIterator.probeIterators =
        IntStream.range(0, alignedTvListIterators.size()).boxed().collect(Collectors.toList());
    return cloneIterator;
  }
}
