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

import org.apache.iotdb.tsfile.read.reader.IPointReader;
import org.apache.iotdb.tsfile.read.TimeValuePair;
import java.io.IOException;
import java.util.List;
import java.util.PriorityQueue;

/**
 * This class implements {@link IPointReader} for data sources with different priorities.
 */
public class PriorityMergeReader implements IPointReader {

  // largest end time of all added readers
  private long currentLargestEndTime;

  PriorityQueue<Element> heap = new PriorityQueue<>((o1, o2) -> {
    int timeCompare = Long.compare(o1.timeValuePair.getTimestamp(),
        o2.timeValuePair.getTimestamp());
    return timeCompare != 0 ? timeCompare : Long.compare(o2.priority, o1.priority);
  });

  public PriorityMergeReader() {
  }

  public PriorityMergeReader(List<IPointReader> prioritySeriesReaders, int startPriority)
      throws IOException {
    for (IPointReader reader : prioritySeriesReaders) {
      addReader(reader, startPriority++);
    }
  }

  public void addReader(IPointReader reader, long priority) throws IOException {
    if (reader.hasNextTimeValuePair()) {
      heap.add(new Element(reader, reader.nextTimeValuePair(), priority));
    } else {
      reader.close();
    }
  }

  public void addReader(IPointReader reader, long priority, long endTime) throws IOException {
    if (reader.hasNextTimeValuePair()) {
      heap.add(new Element(reader, reader.nextTimeValuePair(), priority));
      currentLargestEndTime = Math.max(currentLargestEndTime, endTime);
    } else {
      reader.close();
    }
  }

  public long getCurrentLargestEndTime() {
    return currentLargestEndTime;
  }

  @Override
  public boolean hasNextTimeValuePair() {
    return !heap.isEmpty();
  }

  @Override
  public TimeValuePair nextTimeValuePair() throws IOException {
    Element top = heap.poll();
    TimeValuePair ret = top.timeValuePair;
    TimeValuePair topNext = null;
    if (top.hasNext()) {
      top.next();
      topNext = top.currPair();
    }
    long topNextTime = topNext == null ? Long.MAX_VALUE : topNext.getTimestamp();
    updateHeap(ret.getTimestamp(), topNextTime);
    if (topNext != null) {
      top.timeValuePair = topNext;
      heap.add(top);
    }
    return ret;
  }

  @Override
  public TimeValuePair currentTimeValuePair() throws IOException {
    return heap.peek().timeValuePair;
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

  @Override
  public void close() throws IOException {
    while (!heap.isEmpty()) {
      Element e = heap.poll();
      e.close();
    }
  }

  static class Element {

    IPointReader reader;
    TimeValuePair timeValuePair;
    long priority;

    Element(IPointReader reader, TimeValuePair timeValuePair, long priority) {
      this.reader = reader;
      this.timeValuePair = timeValuePair;
      this.priority = priority;
    }

    long currTime() {
      return timeValuePair.getTimestamp();
    }

    TimeValuePair currPair() {
      return timeValuePair;
    }

    boolean hasNext() throws IOException {
      return reader.hasNextTimeValuePair();
    }

    void next() throws IOException {
      timeValuePair = reader.nextTimeValuePair();
    }

    void close() throws IOException {
      reader.close();
    }
  }
}