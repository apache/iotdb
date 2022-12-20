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

import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.tsfile.read.TimeValuePair;
import org.apache.iotdb.tsfile.read.reader.IPointReader;

import java.io.IOException;
import java.util.List;
import java.util.Objects;
import java.util.PriorityQueue;

/** This class implements {@link IPointReader} for data sources with different priorities. */
@SuppressWarnings("ConstantConditions") // heap is ensured by hasNext non-empty
public class PriorityMergeReader implements IPointReader {

  // max time of all added readers in PriorityMergeReader
  // or min time of all added readers in DescPriorityMergeReader
  protected long currentReadStopTime;

  protected PriorityQueue<Element> heap;

  public PriorityMergeReader() {
    heap =
        new PriorityQueue<>(
            (o1, o2) -> {
              int timeCompare =
                  Long.compare(o1.timeValuePair.getTimestamp(), o2.timeValuePair.getTimestamp());
              return timeCompare != 0 ? timeCompare : o2.priority.compareTo(o1.priority);
            });
  }

  // only used in external sort, need to refactor later
  public PriorityMergeReader(List<IPointReader> prioritySeriesReaders, int startPriority)
      throws IOException {
    heap =
        new PriorityQueue<>(
            (o1, o2) -> {
              int timeCompare =
                  Long.compare(o1.timeValuePair.getTimestamp(), o2.timeValuePair.getTimestamp());
              return timeCompare != 0 ? timeCompare : o2.priority.compareTo(o1.priority);
            });
    for (IPointReader reader : prioritySeriesReaders) {
      addReader(reader, startPriority++);
    }
  }

  public void addReader(IPointReader reader, long priority) throws IOException {
    if (reader.hasNextTimeValuePair()) {
      heap.add(
          new Element(reader, reader.nextTimeValuePair(), new MergeReaderPriority(priority, 0)));
    } else {
      reader.close();
    }
  }

  public void addReader(
      IPointReader reader, MergeReaderPriority priority, long endTime, QueryContext context)
      throws IOException {
    if (reader.hasNextTimeValuePair()) {
      heap.add(new Element(reader, reader.nextTimeValuePair(), priority));
      currentReadStopTime = Math.max(currentReadStopTime, endTime);
    } else {
      reader.close();
    }
  }

  public long getCurrentReadStopTime() {
    return currentReadStopTime;
  }

  @Override
  public boolean hasNextTimeValuePair() {
    return !heap.isEmpty();
  }

  @Override
  public TimeValuePair nextTimeValuePair() throws IOException {
    Element top = heap.poll();
    TimeValuePair ret = top.getTimeValuePair();
    TimeValuePair topNext = null;
    if (top.hasNext()) {
      top.next();
      topNext = top.currPair();
    }
    updateHeap(ret, topNext);
    if (topNext != null) {
      top.timeValuePair = topNext;
      heap.add(top);
    }
    return ret;
  }

  @Override
  public TimeValuePair currentTimeValuePair() throws IOException {
    return heap.peek().getTimeValuePair();
  }

  /**
   * remove all the TimeValuePair that shares the same timestamp if it's an aligned path we may need
   * to use those records that share the same timestamp to fill the null sub sensor value in current
   * TimeValuePair
   */
  protected void updateHeap(TimeValuePair ret, TimeValuePair topNext) throws IOException {
    long topTime = ret.getTimestamp();
    long topNextTime = (topNext == null ? Long.MAX_VALUE : topNext.getTimestamp());
    while (!heap.isEmpty() && heap.peek().currTime() == topTime) {
      Element e = heap.poll();
      fillNullValue(ret, e.getTimeValuePair());
      if (!e.hasNext()) {
        e.reader.close();
        continue;
      }
      e.next();
      if (e.currTime() == topNextTime) {
        // if the next value of the peek will be overwritten by the next of the top, skip it
        fillNullValue(topNext, e.getTimeValuePair());
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

  /** this method only take effect for aligned time series, so the override version */
  protected void fillNullValue(TimeValuePair v, TimeValuePair c) {
    // do nothing for non-aligned time series
  }

  @Override
  public void close() throws IOException {
    while (!heap.isEmpty()) {
      Element e = heap.poll();
      e.close();
    }
  }

  public static class MergeReaderPriority implements Comparable<MergeReaderPriority> {
    long version;
    long offset;

    public MergeReaderPriority(long version, long offset) {
      this.version = version;
      this.offset = offset;
    }

    @Override
    public int compareTo(MergeReaderPriority o) {
      if (version < o.version) {
        return -1;
      }
      return ((version > o.version) ? 1 : (Long.compare(offset, o.offset)));
    }

    @Override
    public boolean equals(Object object) {
      if (this == object) {
        return true;
      }
      if (object == null || getClass() != object.getClass()) {
        return false;
      }
      MergeReaderPriority that = (MergeReaderPriority) object;
      return (this.version == that.version && this.offset == that.offset);
    }

    @Override
    public int hashCode() {
      return Objects.hash(version, offset);
    }
  }
}
