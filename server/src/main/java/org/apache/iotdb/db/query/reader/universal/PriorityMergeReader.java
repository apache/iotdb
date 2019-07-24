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
import java.util.ArrayList;
import java.util.List;
import java.util.PriorityQueue;
import org.apache.iotdb.db.query.reader.IPointReader;
import org.apache.iotdb.db.utils.TimeValuePair;

/**
 * This class implements {@link IPointReader} for data sources with different priorities.
 */
public class PriorityMergeReader implements IPointReader {

  private List<IPointReader> readerList = new ArrayList<>();
  private PriorityQueue<Element> heap = new PriorityQueue<>((o1, o2) -> {
    int timeCompare = Long.compare(o1.timeValuePair.getTimestamp(),
        o2.timeValuePair.getTimestamp());
    return timeCompare != 0 ? timeCompare : Integer.compare(o2.priority, o1.priority);
  });

  public void addReaderWithPriority(IPointReader reader, int priority) throws IOException {
    if (reader.hasNext()) {
      heap.add(new Element(readerList.size(), reader.next(), priority));
    }
    readerList.add(reader);
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

  @Override
  public TimeValuePair current() {
    return heap.peek().timeValuePair;
  }

  private void updateHeap(Element top) throws IOException {
    while (!heap.isEmpty() && heap.peek().timeValuePair.getTimestamp() == top.timeValuePair
        .getTimestamp()) {
      Element e = heap.poll();
      IPointReader reader = readerList.get(e.index);
      if (reader.hasNext()) {
        e.timeValuePair = reader.next();
        heap.add(e);
      }
    }
  }

  @Override
  public void close() throws IOException {
    for (IPointReader reader : readerList) {
      reader.close();
    }
  }

  protected class Element {

    int index;
    TimeValuePair timeValuePair;
    int priority;

    Element(int index, TimeValuePair timeValuePair, int priority) {
      this.index = index;
      this.timeValuePair = timeValuePair;
      this.priority = priority;
    }
  }
}
