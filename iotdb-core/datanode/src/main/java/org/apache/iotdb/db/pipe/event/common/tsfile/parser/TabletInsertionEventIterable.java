/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.db.pipe.event.common.tsfile.parser;

import org.apache.iotdb.db.pipe.metric.processor.PipeTsFileToTabletsMetrics;
import org.apache.iotdb.pipe.api.event.dml.insertion.TabletInsertionEvent;

import java.util.Iterator;

public class TabletInsertionEventIterable implements Iterable<TabletInsertionEvent> {
  private final Iterable<TabletInsertionEvent> originalIterable;
  private int count = 0;
  private boolean isMarked = false;
  private final PipeTsFileToTabletsMetrics.PipeCallerID pipeCallerID;

  public TabletInsertionEventIterable(
      Iterable<TabletInsertionEvent> originalIterable,
      PipeTsFileToTabletsMetrics.PipeCallerID pipeCallerID) {
    this.originalIterable = originalIterable;
    this.pipeCallerID = pipeCallerID;
  }

  @Override
  public Iterator<TabletInsertionEvent> iterator() {
    return new Iterator<TabletInsertionEvent>() {
      private final Iterator<TabletInsertionEvent> originalIterator = originalIterable.iterator();

      @Override
      public boolean hasNext() {
        boolean hasNext = originalIterator.hasNext();
        if (!hasNext && !isMarked) {
          isMarked = true;
          if (pipeCallerID != null) {
            PipeTsFileToTabletsMetrics.getInstance().markTabletCount(pipeCallerID, count);
          }
        }
        return hasNext;
      }

      @Override
      public TabletInsertionEvent next() {
        count++;
        return originalIterator.next();
      }
    };
  }
}
