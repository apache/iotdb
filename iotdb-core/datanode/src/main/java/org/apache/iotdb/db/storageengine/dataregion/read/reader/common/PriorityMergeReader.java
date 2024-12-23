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

package org.apache.iotdb.db.storageengine.dataregion.read.reader.common;

import org.apache.iotdb.commons.utils.TestOnly;
import org.apache.iotdb.db.queryengine.plan.planner.memory.MemoryReservationManager;

import org.apache.tsfile.read.TimeValuePair;
import org.apache.tsfile.read.reader.IPointReader;

import java.io.IOException;
import java.util.PriorityQueue;

/** This class implements {@link IPointReader} for data sources with different priorities. */
@SuppressWarnings("ConstantConditions") // heap is ensured by hasNext non-empty
public class PriorityMergeReader implements IPointReader {

  // max time of all added readers in PriorityMergeReader
  // or min time of all added readers in DescPriorityMergeReader
  protected long currentReadStopTime;

  protected PriorityQueue<Element> heap;

  protected long usedMemorySize = 0;

  protected MemoryReservationManager memoryReservationManager;

  public PriorityMergeReader() {
    heap =
        new PriorityQueue<>(
            (o1, o2) -> {
              int timeCompare =
                  Long.compare(
                      o1.getTimeValuePair().getTimestamp(), o2.getTimeValuePair().getTimestamp());
              return timeCompare != 0 ? timeCompare : o2.getPriority().compareTo(o1.getPriority());
            });
  }

  public void setMemoryReservationManager(MemoryReservationManager memoryReservationManager) {
    this.memoryReservationManager = memoryReservationManager;
  }

  @TestOnly
  public void addReader(IPointReader reader, long priority) throws IOException {
    if (reader.hasNextTimeValuePair()) {
      heap.add(
          new Element(
              reader,
              reader.nextTimeValuePair(),
              new MergeReaderPriority(Long.MAX_VALUE, priority, 0, false)));
    } else {
      reader.close();
    }
  }

  public void addReader(IPointReader reader, MergeReaderPriority priority, long endTime)
      throws IOException {
    if (reader.hasNextTimeValuePair()) {
      Element element = new Element(reader, reader.nextTimeValuePair(), priority);
      heap.add(element);
      currentReadStopTime = Math.max(currentReadStopTime, endTime);
      long size = element.getReader().getUsedMemorySize();
      usedMemorySize += size;
      if (memoryReservationManager != null) {
        memoryReservationManager.reserveMemoryCumulatively(size);
      }
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
      top.setTimeValuePair(topNext);
      heap.add(top);
    } else {
      long size = top.getReader().getUsedMemorySize();
      usedMemorySize -= size;
      if (memoryReservationManager != null) {
        memoryReservationManager.releaseMemoryCumulatively(size);
      }
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
   * TimeValuePair.
   *
   * @throws IOException while reading next value and close the Element while there is no value,
   *     there may throw IOException
   */
  protected void updateHeap(TimeValuePair ret, TimeValuePair topNext) throws IOException {
    long topTime = ret.getTimestamp();
    long topNextTime = (topNext == null ? Long.MAX_VALUE : topNext.getTimestamp());
    while (!heap.isEmpty() && heap.peek().currTime() == topTime) {
      Element e = heap.poll();
      fillNullValue(ret, e.getTimeValuePair());
      if (!e.hasNext()) {
        long size = e.getReader().getUsedMemorySize();
        usedMemorySize -= size;
        if (memoryReservationManager != null) {
          memoryReservationManager.releaseMemoryCumulatively(size);
        }
        e.getReader().close();
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
          long size = e.getReader().getUsedMemorySize();
          usedMemorySize -= size;
          if (memoryReservationManager != null) {
            memoryReservationManager.releaseMemoryCumulatively(size);
          }
          // the chunk is end
          e.close();
        }
      } else {
        heap.add(e);
      }
    }
  }

  /** this method only take effect for aligned time series, so the override version. */
  protected void fillNullValue(TimeValuePair v, TimeValuePair c) {
    // do nothing for non-aligned time series
  }

  @Override
  public long getUsedMemorySize() {
    return usedMemorySize;
  }

  @Override
  public void close() throws IOException {
    while (!heap.isEmpty()) {
      Element e = heap.poll();
      e.close();
    }
    if (memoryReservationManager != null) {
      memoryReservationManager.releaseMemoryCumulatively(usedMemorySize);
    }
    usedMemorySize = 0;
  }
}
