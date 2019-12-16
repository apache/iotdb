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
package org.apache.iotdb.db.query.reader.universal;

import java.io.IOException;
import java.util.List;
import java.util.PriorityQueue;
import org.apache.iotdb.db.query.reader.IPointReader;
import org.apache.iotdb.db.utils.TimeValuePair;
import org.apache.iotdb.db.utils.TsPrimitiveType;

/**
 * This class is for data sources with different priorities. It used to implement {@link
 * IPointReader}, but now, instead of returning TimeValuePair, it returns Element directly
 */
public class PriorityMergeReader {

  PriorityQueue<Element> heap = new PriorityQueue<>((o1, o2) -> {
    int timeCompare = Long.compare(o1.time, o2.time);
    return timeCompare != 0 ? timeCompare : Integer.compare(o2.priority, o1.priority);
  });

  public PriorityMergeReader() {
  }

  public PriorityMergeReader(List<IPointReader> prioritySeriesReaders, int startPriority)
      throws IOException {
    for (IPointReader reader : prioritySeriesReaders) {
      addReaderWithPriority(reader, startPriority++);
    }
  }

  public void addReaderWithPriority(IPointReader reader, int priority) throws IOException {
    if (reader.hasNext()) {
      TimeValuePair pair = reader.next();
      heap.add(
          new Element(reader, pair.getTimestamp(), pair.getValue(), priority));
    } else {
      reader.close();
    }
  }

  public boolean hasNext() {
    return !heap.isEmpty();
  }

  public Element next() throws IOException {
    Element top = heap.poll();
    long ret = top.time;
    long topNextTime = Long.MAX_VALUE;
    TsPrimitiveType topNextValue = null;
    if (top.hasNext()) {
      top.next();
      topNextTime = top.currTime();
      topNextValue = top.currValue();
    }
    updateHeap(ret, topNextTime);
    if (topNextValue != null) {
      top.time = topNextTime;
      top.value = topNextValue;
      heap.add(top);
    }
    return top;
  }

  public Element current() throws IOException {
    return heap.peek();
  }

  private void updateHeap(long topTime, long topNextTime) throws IOException {
    while (!heap.isEmpty() && heap.peek().currTime() == topTime) {
      Element e = heap.poll();
      if (!e.hasNext()) {
        e.reader.close();
        continue;
      }

      e.next();
      if (e.currTime() == topNextTime) {
        // if the next value of the peek will be overwritten by the next of the top, skip it
        if (e.hasNext()) {
          e.next();
          heap.add(e);
        } else {
          // the chunk is end
          e.close();
        }
      } else {
        heap.add(e);
      }
    }
  }

  public void close() throws IOException {
    while (!heap.isEmpty()) {
      Element e = heap.poll();
      e.close();
    }
  }

  /**
   * This class is static because it is used in "TimeValuePairUtils"
   */
  public static class Element {

    IPointReader reader;
    long time;
    TsPrimitiveType value;
    int priority;

    /**
     * This constructor is only used in "TimeValuePairUtils" to get empty Element
     */
    public Element(long time, TsPrimitiveType value) {
      this.time = time;
      this.value = value;
    }

    Element(IPointReader reader, long time, TsPrimitiveType value, int priority) {
      this.reader = reader;
      this.time = time;
      this.value = value;
      this.priority = priority;
    }

    long currTime() {
      return time;
    }

    TsPrimitiveType currValue() {
      return value;
    }

    boolean hasNext() throws IOException {
      return reader.hasNext();
    }

    void next() throws IOException {
      TimeValuePair timeValuePair = reader.next();
      time = timeValuePair.getTimestamp();
      value = timeValuePair.getValue();
    }

    void close() throws IOException {
      reader.close();
    }

    public long getTime() {
      return time;
    }

    public TsPrimitiveType getValue() {
      return value;
    }

    public void setTime(long time) {
      this.time = time;
    }

    public void setValue(TsPrimitiveType value) {
      this.value = value;
    }
  }
}