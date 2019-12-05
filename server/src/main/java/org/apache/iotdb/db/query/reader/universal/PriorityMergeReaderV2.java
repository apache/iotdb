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
import org.apache.iotdb.db.query.reader.IAggregateReader;
import org.apache.iotdb.db.query.reader.IPointReader;
import org.apache.iotdb.db.utils.TimeValuePair;
import org.apache.iotdb.db.utils.TsPrimitiveType;
import org.apache.iotdb.tsfile.file.header.PageHeader;
import org.apache.iotdb.tsfile.read.common.BatchData;

/**
 * This class implements {@link IPointReader} for data sources with different priorities.
 */
public class PriorityMergeReaderV2 implements IAggregateReader, IPointReader {

  private PriorityQueue<Element> heap = new PriorityQueue<>((o1, o2) -> {
    int timeCompare = Long.compare(o1.currBatchData().currentTime(),
        o2.currBatchData().currentTime());
    return timeCompare != 0 ? timeCompare : Integer.compare(o2.priority, o1.priority);
  });

  public PriorityMergeReaderV2() {
  }

  public PriorityMergeReaderV2(List<IAggregateReader> prioritySeriesReaders, int startPriority)
      throws IOException {
    for (IAggregateReader reader : prioritySeriesReaders) {
      addReaderWithPriority(reader, startPriority++);
    }
  }

  public void addReaderWithPriority(IAggregateReader reader, int priority) throws IOException {
    if (reader.hasNext()) {
      heap.add(new Element(reader, reader.nextBatch(), priority));
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
    // FIXME To be removed
    BatchData next = nextBatch();
    return new TimeValuePair(next.currentTime(),
        TsPrimitiveType.getByType(next.getDataType(), next.currentValue()));
  }

  @Override
  public TimeValuePair current() throws IOException {
    BatchData current = heap.peek().batchData;
    return new TimeValuePair(current.currentTime(),
        TsPrimitiveType.getByType(current.getDataType(), current.currentValue()));
  }

  @Override
  public BatchData nextBatch() throws IOException {
    Element top = heap.poll();
    BatchData ret = top.batchData;
    BatchData topNext = null;
    if (top.hasNext()) {
      top.nextBatch();
      topNext = top.currBatchData();
    }
    long topNextTime = topNext == null ? Long.MAX_VALUE : topNext.currentTime();
    updateHeap(ret.currentTime(), topNextTime);
    if (topNext != null) {
      top.batchData = topNext;
      heap.add(top);
    }
    return ret;
  }

  private void updateHeap(long topTime, long topNextTime) throws IOException {
    while (!heap.isEmpty() && heap.peek().currTime() == topTime) {
      Element e = heap.poll();
      if (!e.hasNext()) {
        e.reader.close();
        continue;
      }

      e.nextBatch();
      if (e.currTime() == topNextTime) {
        // if the next value of the peek will be overwritten by the next of the top, skip it
        if (e.hasNext()) {
          e.nextBatch();
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

  @Override
  public PageHeader nextPageHeader() throws IOException {
    return null;
  }

  @Override
  public void skipPageData() throws IOException {
    nextBatch();
  }

  class Element {

    IAggregateReader reader;
    BatchData batchData;
    int priority;

    Element(IAggregateReader reader, BatchData batchData, int priority) {
      this.reader = reader;
      this.batchData = batchData;
      this.priority = priority;
    }

    long currTime() {
      return batchData.currentTime();
    }

    BatchData currBatchData() {
      return batchData;
    }

    boolean hasNext() throws IOException {
      return reader.hasNext();
    }

    void nextBatch() throws IOException {
      batchData = reader.nextBatch();
    }

    void close() throws IOException {
      reader.close();
    }
  }
}