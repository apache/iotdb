/**
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
package org.apache.iotdb.db.query.reader.merge;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.PriorityQueue;
import org.apache.iotdb.db.query.reader.IReader;
import org.apache.iotdb.db.utils.TimeValuePair;
import org.apache.iotdb.tsfile.read.common.BatchData;

/**
 * Usage:
 *
 * (1) merge multiple chunk group readers in the unsequence file (2）merge sequence reader , unsequence reader and mem
 * reader
 *
 * Notice that: the bigger the priority value is, the higher the priority of this reader is
 */
public class PriorityMergeReader implements IReader {

  private List<IReader> readerList = new ArrayList<>();
  private List<Integer> priorityList = new ArrayList<>();
  private PriorityQueue<Element> heap = new PriorityQueue<>();

  public void addReaderWithPriority(IReader reader, int priority) throws IOException {
    if (reader.hasNext()) {
      heap.add(new Element(readerList.size(), reader.next(), priority));
    }
    readerList.add(reader);
    priorityList.add(priority);
  }

  @Override
  public boolean hasNext() {
    return !heap.isEmpty();
  }

  @Override
  public TimeValuePair next() throws IOException {
    Element top = heap.peek();
    updateHeap(top);
    return top.timeValuePair;
  }

  private void updateHeap(Element top) throws IOException {
    while (!heap.isEmpty() && heap.peek().timeValuePair.getTimestamp() == top.timeValuePair
        .getTimestamp()) {
      Element e = heap.poll();
      IReader reader = readerList.get(e.index);
      if (reader.hasNext()) {
        heap.add(new Element(e.index, reader.next(), priorityList.get(e.index)));
      }
    }
  }

  @Override
  public void skipCurrentTimeValuePair() throws IOException {
    if (hasNext()) {
      next();
    }
  }

  @Override
  public void close() throws IOException {
    for (IReader reader : readerList) {
      reader.close();
    }
  }

  @Override
  public boolean hasNextBatch() {
    // TODO
    return false;
  }

  @Override
  public BatchData nextBatch() {
    return null;
  }

  @Override
  public BatchData currentBatch() {
    return null;
  }

  protected class Element implements Comparable<Element> {

    int index;
    TimeValuePair timeValuePair;
    Integer priority;

    public Element(int index, TimeValuePair timeValuePair, int priority) {
      this.index = index;
      this.timeValuePair = timeValuePair;
      this.priority = priority;
    }

    @Override
    public int compareTo(Element o) {

      if (this.timeValuePair.getTimestamp() > o.timeValuePair.getTimestamp()) {
        return 1;
      }

      if (this.timeValuePair.getTimestamp() < o.timeValuePair.getTimestamp()) {
        return -1;
      }

      return o.priority.compareTo(this.priority);
    }
  }
}
