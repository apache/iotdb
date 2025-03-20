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
import org.apache.tsfile.utils.Pair;

import java.util.List;
import java.util.PriorityQueue;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class MergeSortMultiTVListIterator extends MultiTVListIterator {
  private final List<Integer> probeIterators;
  private final PriorityQueue<Pair<Long, Integer>> minHeap =
      new PriorityQueue<>(
          (a, b) -> a.left.equals(b.left) ? b.right.compareTo(a.right) : a.left.compareTo(b.left));

  public MergeSortMultiTVListIterator(
      TSDataType tsDataType,
      List<TVList> tvLists,
      List<TimeRange> deletionList,
      Integer floatPrecision,
      TSEncoding encoding) {
    super(tsDataType, tvLists, deletionList, floatPrecision, encoding);
    this.probeIterators =
        IntStream.range(0, tvListIterators.size()).boxed().collect(Collectors.toList());
  }

  @Override
  protected void prepareNext() {
    hasNext = false;
    for (int i : probeIterators) {
      TVList.TVListIterator iterator = tvListIterators.get(i);
      if (iterator.hasNextTimeValuePair()) {
        minHeap.add(new Pair<>(iterator.currentTime(), i));
      }
    }
    probeIterators.clear();

    if (!minHeap.isEmpty()) {
      Pair<Long, Integer> top = minHeap.poll();
      iteratorIndex = top.right;
      probeIterators.add(iteratorIndex);
      rowIndex = tvListIterators.get(iteratorIndex).getIndex();
      hasNext = true;

      // duplicated timestamps
      while (!minHeap.isEmpty() && minHeap.peek().left.longValue() == top.left.longValue()) {
        Pair<Long, Integer> element = minHeap.poll();
        probeIterators.add(element.right);
      }
    }
    probeNext = true;
  }

  @Override
  protected void next() {
    for (int index : probeIterators) {
      tvListIterators.get(index).next();
    }
    probeNext = false;
  }
}
