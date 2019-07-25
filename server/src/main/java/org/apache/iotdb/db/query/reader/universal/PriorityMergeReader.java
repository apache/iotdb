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
package org.apache.iotdb.db.query.reader.universal;

import java.io.IOException;
import java.util.PriorityQueue;
import org.apache.iotdb.db.query.reader.IPointReader;
import org.apache.iotdb.db.utils.TimeValuePair;

/**
 * This class implements {@link IPointReader} for data sources with different priorities.
 */
public class PriorityMergeReader implements IPointReader {

  PriorityQueue<Element> heap = new PriorityQueue<>((o1, o2) -> {
    int timeCompare = Long.compare(o1.timeValuePair.getTimestamp(),
        o2.timeValuePair.getTimestamp());
    return timeCompare != 0 ? timeCompare : Integer.compare(o2.priority, o1.priority);
  });

  public void addReaderWithPriority(IPointReader reader, int priority) throws IOException {
    if (reader.hasNext()) {
      heap.add(new Element(reader, reader.next(), priority));
    } else {
      reader.close();
    }
  }

  @Override
  public boolean hasNext() {
    return !heap.isEmpty();
  }

  @Override
  public TimeValuePair next() throws IOException {
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
  public TimeValuePair current() {
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

  class Element {

    IPointReader reader;
    TimeValuePair timeValuePair;
    int priority;

    Element(IPointReader reader, TimeValuePair timeValuePair, int priority) {
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
      return reader.hasNext();
    }

    void next() throws IOException {
      timeValuePair = reader.next();
    }

    void close() throws IOException {
      reader.close();
    }
  }
}
