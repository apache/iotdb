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

import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.read.TimeValuePair;
import org.apache.tsfile.read.reader.IPointReader;
import org.apache.tsfile.utils.Pair;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.PriorityQueue;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class MergeSortTVListIterator implements IPointReader {
  private List<TVList.TVListIterator> tvListIterators;
  private int[] tvListOffsets;

  private boolean probeNext = false;
  private TimeValuePair currentTvPair;

  private List<Integer> probeIterators;
  private final PriorityQueue<Pair<Long, Integer>> minHeap =
      new PriorityQueue<>(
          (a, b) -> a.left.equals(b.left) ? b.right.compareTo(a.right) : a.left.compareTo(b.left));

  public MergeSortTVListIterator(List<TVList> tvLists) {
    tvListIterators = new ArrayList<>(tvLists.size());
    for (TVList tvList : tvLists) {
      tvListIterators.add(tvList.iterator(null, null));
    }
    this.tvListOffsets = new int[tvLists.size()];
    this.probeIterators =
        IntStream.range(0, tvListIterators.size()).boxed().collect(Collectors.toList());
  }

  public MergeSortTVListIterator(
      List<TVList> tvLists, Integer floatPrecision, TSEncoding encoding) {
    tvListIterators = new ArrayList<>(tvLists.size());
    for (TVList tvList : tvLists) {
      tvListIterators.add(tvList.iterator(floatPrecision, encoding));
    }
    this.tvListOffsets = new int[tvLists.size()];
    this.probeIterators =
        IntStream.range(0, tvListIterators.size()).boxed().collect(Collectors.toList());
  }

  private MergeSortTVListIterator() {}

  private void prepareNext() {
    currentTvPair = null;
    for (int i : probeIterators) {
      TVList.TVListIterator iterator = tvListIterators.get(i);
      if (iterator.hasNext()) {
        minHeap.add(new Pair<>(iterator.currentTime(), i));
      }
    }
    probeIterators.clear();

    if (!minHeap.isEmpty()) {
      Pair<Long, Integer> top = minHeap.poll();
      probeIterators.add(top.right);
      currentTvPair = tvListIterators.get(top.right).current();
      while (!minHeap.isEmpty() && minHeap.peek().left.longValue() == top.left.longValue()) {
        Pair<Long, Integer> element = minHeap.poll();
        probeIterators.add(element.right);
      }
    }
    probeNext = true;
  }

  @Override
  public boolean hasNextTimeValuePair() {
    if (!probeNext) {
      prepareNext();
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
      TVList.TVListIterator iterator = tvListIterators.get(index);
      iterator.step();
      tvListOffsets[index] = iterator.getIndex();
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

  public int[] getTVListOffsets() {
    return tvListOffsets;
  }

  public void setTVListOffsets(int[] tvListOffsets) {
    for (int i = 0; i < tvListIterators.size(); i++) {
      tvListIterators.get(i).setIndex(tvListOffsets[i]);
      this.tvListOffsets[i] = tvListOffsets[i];
    }
    minHeap.clear();
    probeIterators.clear();
    for (int i = 0; i < tvListIterators.size(); i++) {
      probeIterators.add(i);
    }
    probeNext = false;
  }

  @Override
  public MergeSortTVListIterator clone() {
    MergeSortTVListIterator cloneIterator = new MergeSortTVListIterator();
    cloneIterator.tvListIterators = new ArrayList<>(tvListIterators.size());
    for (int i = 0; i < tvListIterators.size(); i++) {
      cloneIterator.tvListIterators.add(tvListIterators.get(i).clone());
    }
    cloneIterator.tvListOffsets = new int[tvListIterators.size()];
    cloneIterator.probeIterators =
        IntStream.range(0, tvListIterators.size()).boxed().collect(Collectors.toList());
    return cloneIterator;
  }
}
