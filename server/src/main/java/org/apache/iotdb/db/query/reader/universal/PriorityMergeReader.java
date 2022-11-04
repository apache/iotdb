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
import org.apache.iotdb.db.query.control.tracing.TracingManager;
import org.apache.iotdb.tsfile.file.metadata.statistics.Statistics;
import org.apache.iotdb.tsfile.read.TimeValuePair;
import org.apache.iotdb.tsfile.read.reader.IPointReader;
import org.apache.iotdb.tsfile.utils.TsPrimitiveType;

import org.eclipse.collections.api.map.MutableMap;
import org.eclipse.collections.impl.factory.Maps;
import org.eclipse.collections.impl.list.mutable.primitive.LongArrayList;

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
  public MutableMap<IPointReader, LongArrayList[]> pageData;
  public MutableMap<IPointReader, Statistics> pageStat;

  public PriorityMergeReader() {
    heap =
        new PriorityQueue<>(
            (o1, o2) -> {
              int timeCompare =
                  Long.compare(o1.timeValuePair.getTimestamp(), o2.timeValuePair.getTimestamp());
              return timeCompare != 0 ? timeCompare : o2.priority.compareTo(o1.priority);
            });
    pageData = Maps.mutable.empty();
    pageStat = Maps.mutable.empty();
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
      // for tracing: try to calculate the number of overlapped pages
      if (context.isEnableTracing()) {
        addOverlappedPageNum(context.getQueryId());
      }
    } else {
      reader.close();
    }
  }

  public void addReader(
      IPointReader reader,
      Statistics stat,
      MergeReaderPriority priority,
      long endTime,
      QueryContext context)
      throws IOException {
    if (reader.hasNextTimeValuePair()) {
      TimeValuePair tmp = reader.nextTimeValuePair();
      heap.add(new Element(reader, tmp, priority));
      currentReadStopTime = Math.max(currentReadStopTime, endTime);

      //      System.out.println(
      //          "\t\t[PriMergeReader Debug]\t"
      //              + "addReader ["
      //              + tmp
      //              + "]\tversion:"
      //              + priority.version
      //              + "\thashCode:"
      //              + reader.hashCode());
      pageData.put(reader, new LongArrayList[] {new LongArrayList(8000), new LongArrayList(8000)});
      pageStat.put(reader, stat);
      // for tracing: try to calculate the number of overlapped pages
      if (context.isEnableTracing()) {
        addOverlappedPageNum(context.getQueryId());
      }
    } else {
      reader.close();
    }
  }

  private void addOverlappedPageNum(long queryId) {
    TracingManager.getInstance().addOverlappedPageNum(queryId);
  }

  public long getCurrentReadStopTime() {
    return currentReadStopTime;
  }

  @Override
  public boolean hasNextTimeValuePair() {
    return !heap.isEmpty();
  }

  private long dataToLong(TsPrimitiveType data) {
    long result;
    switch (data.getDataType()) {
      case INT32:
        return (int) data.getValue();
      case FLOAT:
        result = Float.floatToIntBits((float) data.getValue());
        return (float) data.getValue() >= 0f ? result : result ^ Long.MAX_VALUE;
      case INT64:
        return (long) data.getValue();
      case DOUBLE:
        result = Double.doubleToLongBits((double) data.getValue());
        return (double) data.getValue() >= 0d ? result : result ^ Long.MAX_VALUE;
    }
    return -233;
  }

  @Override
  public TimeValuePair nextTimeValuePair() throws IOException {
    Element top = heap.poll();
    TimeValuePair ret = top.getTimeValuePair();
    pageData.get(top.reader)[0].add(dataToLong(ret.getValue()));
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
      pageData.get(e.reader)[1].add(dataToLong(e.currPair().getValue()));
      //      System.out.println(
      //          "\t\t[PriMergeReader Debug]\t"
      //              + "remove invalid data ["
      //              + e.currPair()
      //              + "]\telement_hashCode:"
      //              + e.hashCode());

      if (!e.hasNext()) {
        e.reader.close();
        continue;
      }

      e.next();
      if (e.currTime() == topNextTime) {
        // if the next value of the peek will be overwritten by the next of the top, skip it
        fillNullValue(topNext, e.getTimeValuePair());
        pageData.get(e.reader)[1].add(dataToLong(e.currPair().getValue()));
        //        System.out.println(
        //            "\t\t[PriMergeReader Debug]\t"
        //                + "remove invalid data ["
        //                + e.currPair()
        //                + "]\telement_hashCode:"
        //                + e.hashCode());
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
