/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements.  See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership.  The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with the License.  You may obtain
 * a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied.  See the License for the specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iotdb.db.query.reader.merge;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.PriorityQueue;
import org.apache.iotdb.tsfile.utils.Pair;

/**
 * <p>
 * Usage: Get value in timestamp by sorting time-value pair in multiple readers with time and
 * priority. (1) merge multiple chunk group readers in the unsequence file (2）merge sequence reader,
 * unsequence reader and mem reader
 * </p>
 */
public class PriorityMergeReaderByTimestamp implements EngineReaderByTimeStamp {

  private boolean hasCachedTimeValuePair;
  private Pair<Long, Object> cachedTimeValuePair;

  private List<EngineReaderByTimeStamp> readerList = new ArrayList<>();
  private List<Integer> priorityList = new ArrayList<>();
  private PriorityQueue<MergeElement> heap = new PriorityQueue<>();

  /**
   * The bigger the priority value is, the higher the priority of this reader is
   */
  public void addReaderWithPriority(EngineReaderByTimeStamp reader, int priority)
      throws IOException {
    Pair<Long, Object> timeValuePair = reader.getValueGtEqTimestamp(Long.MIN_VALUE);

    if (timeValuePair != null) {
      heap.add(
          new MergeElement(readerList.size(), timeValuePair.left, timeValuePair.right, priority));
    }
    readerList.add(reader);
    priorityList.add(priority);
  }

  @Override
  public Object getValueInTimestamp(long timestamp) throws IOException {

    if (hasCachedTimeValuePair) {
      if (cachedTimeValuePair.left == timestamp) {
        hasCachedTimeValuePair = false;
        return cachedTimeValuePair.right;
      } else if (cachedTimeValuePair.left > timestamp) {
        return null;
      }
    }

    while (hasNext()) {
      cachedTimeValuePair = next(timestamp);
      if (cachedTimeValuePair.left == timestamp) {
        hasCachedTimeValuePair = false;
        return cachedTimeValuePair.right;
      } else if (cachedTimeValuePair.left > timestamp) {
        hasCachedTimeValuePair = true;
        return null;
      }
    }

    return null;
  }

  @Override
  public Pair<Long, Object> getValueGtEqTimestamp(long timestamp) throws IOException {

    if (hasCachedTimeValuePair) {
      if (cachedTimeValuePair.left >= timestamp) {
        hasCachedTimeValuePair = false;
        return cachedTimeValuePair;
      }
    }

    while (hasNext()) {
      cachedTimeValuePair = next(timestamp);
      if (cachedTimeValuePair.left >= timestamp) {
        hasCachedTimeValuePair = false;
        return cachedTimeValuePair;
      }
    }

    return null;
  }

  @Override
  public void close() throws IOException {
    for (EngineReaderByTimeStamp reader : readerList) {
      reader.close();
    }
  }

  public boolean hasNext() {
    return !heap.isEmpty();
  }

  /**
   * Get the top element of the heap and update the element of the stack with a value whose time is greater than
   * that timestamp
   */
  private Pair<Long, Object> next(long timestamp) throws IOException {
    MergeElement top = heap.peek();
    updateHeap(top, timestamp);
    return new Pair<>(top.time, top.value);
  }

  /**
   * This method is only used in test.
   */
  public Pair<Long, Object> next() throws IOException {
    MergeElement top = heap.peek();
    updateHeap(top, top.time);
    return new Pair<>(top.time, top.value);
  }

  private void updateHeap(MergeElement top, long timestamp) throws IOException {
    while (!heap.isEmpty() && heap.peek().time == top.time) {
      MergeElement e = heap.poll();
      EngineReaderByTimeStamp reader = readerList.get(e.index);
      Pair<Long, Object> timeValuePair = reader.getValueGtEqTimestamp(timestamp);
      if (timeValuePair != null) {
        heap.add(new MergeElement(e.index, timeValuePair.left, timeValuePair.right,
            priorityList.get(e.index)));
      }
    }
  }
}
