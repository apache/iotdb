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

package org.apache.iotdb.db.storageengine.dataregion.compaction.execute.utils.reader;

import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.db.exception.WriteProcessException;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.utils.executor.fast.SeriesCompactionExecutor;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.utils.executor.fast.element.PageElement;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.utils.executor.fast.element.PointElement;

import org.apache.tsfile.read.TimeValuePair;
import org.apache.tsfile.utils.TsPrimitiveType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.PriorityQueue;

/**
 * This reader is used to deduplicate and organize overlapping pages, and read out points in order.
 * It is used for compaction.
 */
public class PointPriorityReader {
  private long lastTime;

  private final PriorityQueue<PointElement> pointQueue;

  private final SeriesCompactionExecutor.RemovePage removePage;

  private TimeValuePair currentPoint;

  private long nextPointInOtherPage = Long.MAX_VALUE;

  private PointElement currentPointElement;

  private final boolean isAligned;

  public PointPriorityReader(SeriesCompactionExecutor.RemovePage removePage, boolean isAligned) {
    this.removePage = removePage;
    pointQueue =
        new PriorityQueue<>(
            (o1, o2) -> {
              int timeCompare = Long.compare(o1.timestamp, o2.timestamp);
              if (timeCompare != 0) {
                return timeCompare;
              }
              boolean o1IsSeq =
                  o1.pageElement.getChunkMetadataElement().fileElement.resource.isSeq();
              boolean o2IsSeq =
                  o2.pageElement.getChunkMetadataElement().fileElement.resource.isSeq();
              if (o1IsSeq != o2IsSeq) {
                return o1IsSeq ? 1 : -1;
              }
              return Long.compare(o2.priority, o1.priority);
            });
    this.isAligned = isAligned;
  }

  public TimeValuePair currentPoint() {
    if (currentPointElement == null) {
      // if the current point is overlapped with other pages, then get the highest priority point
      // from queue
      currentPoint = pointQueue.peek().timeValuePair;
      lastTime = currentPoint.getTimestamp();

      // fill aligned null value with the same timestamp
      if (isAligned) {
        fillAlignedNullValue();
      }
    }
    return currentPoint;
  }

  /**
   * Use those records that share the same timestamp to fill the null sub sensor value in current
   * TimeValuePair.
   */
  @SuppressWarnings({"squid:S3776", "squid:S135"})
  private void fillAlignedNullValue() {
    List<PointElement> pointElementsWithSameTimestamp = new ArrayList<>();
    // remove the current point element
    pointElementsWithSameTimestamp.add(pointQueue.poll());

    TsPrimitiveType[] currentValues = currentPoint.getValue().getVector();
    while (!pointQueue.isEmpty()) {
      int nullValueNum = currentValues.length;
      if (pointQueue.peek().timestamp > lastTime) {
        // the smallest time of all pages is later than the last time, then break the loop
        break;
      } else {
        // find the data points in other pages that has the same timestamp
        PointElement pointElement = pointQueue.poll();
        pointElementsWithSameTimestamp.add(pointElement);
        TsPrimitiveType[] values = pointElement.timeValuePair.getValue().getVector();
        for (int i = 0; i < values.length; i++) {
          if (currentValues[i] == null) {
            if (values[i] != null) {
              // if current page of aligned value is null while other page of this aligned value
              // with same timestamp is not null, then fill it.
              currentValues[i] = values[i];
              nullValueNum--;
            }
          } else {
            nullValueNum--;
          }
        }
      }
      if (nullValueNum == 0) {
        // if there is no sub sensor with null value, then break the loop
        break;
      }
    }

    // add point elements into queue
    pointQueue.addAll(pointElementsWithSameTimestamp);
  }

  @SuppressWarnings("squid:S3776")
  public void next() throws IllegalPathException, IOException, WriteProcessException {
    if (currentPointElement != null) {

      if (currentPointElement.hasNext()) {
        // get the point directly if it is not overlapped with other points
        currentPoint = currentPointElement.next();
        if (currentPoint.getTimestamp() >= nextPointInOtherPage) {
          // if the point is overlapped with other points, then add it into priority queue
          pointQueue.add(currentPointElement);
          currentPointElement = null;
        }
      } else {
        // end page
        PageElement pageElement = currentPointElement.pageElement;
        currentPointElement = null;
        removePage.call(pageElement);
      }
    } else {
      // remove data points with the same timestamp as the last point
      while (!pointQueue.isEmpty() && pointQueue.peek().timestamp == lastTime) {
        // find the data points in other pages that has the same timestamp
        PointElement pointElement = pointQueue.poll();
        if (pointElement.hasNext()) {
          pointElement.next();
          nextPointInOtherPage =
              !pointQueue.isEmpty() ? pointQueue.peek().timestamp : Long.MAX_VALUE;
          if (pointElement.timestamp < nextPointInOtherPage) {
            currentPointElement = pointElement;
            currentPoint = currentPointElement.timeValuePair;
          } else {
            pointQueue.add(pointElement);
          }
        } else {
          // end page
          removePage.call(pointElement.pageElement);
        }
      }
    }
  }

  public boolean hasNext() {
    return currentPointElement != null || !pointQueue.isEmpty();
  }

  /**
   * Add a new overlapped page.
   *
   * @throws IOException if io errors occurred
   * @return whether page is added into the queue
   */
  public boolean addNewPageIfPageNotEmpty(PageElement pageElement)
      throws IOException, IllegalPathException, WriteProcessException {
    if (currentPointElement != null) {
      nextPointInOtherPage = Math.min(nextPointInOtherPage, pageElement.getStartTime());
      if (currentPoint.getTimestamp() >= nextPointInOtherPage) {
        pointQueue.add(currentPointElement);
        currentPointElement = null;
      }
    }
    PointElement pointElement = new PointElement(pageElement);
    boolean pageIsNotEmpty = pointElement.timeValuePair != null;
    if (pageIsNotEmpty) {
      pointQueue.add(pointElement);
    } else {
      removePage.call(pageElement);
    }

    return pageIsNotEmpty;
  }
}
