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
package org.apache.iotdb.db.engine.compaction.reader;

import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.db.engine.compaction.cross.utils.PageElement;
import org.apache.iotdb.db.engine.compaction.cross.utils.PointElement;
import org.apache.iotdb.db.engine.compaction.cross.utils.SeriesCompactionExecutor;
import org.apache.iotdb.db.exception.WriteProcessException;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.TimeValuePair;
import org.apache.iotdb.tsfile.read.reader.IPointReader;
import org.apache.iotdb.tsfile.utils.TsPrimitiveType;

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

  private boolean shouldReadNextPoint = true;

  private long nextPageStartTime = Long.MAX_VALUE;

  private PointElement currentPointElement;

  public PointPriorityReader(SeriesCompactionExecutor.RemovePage removePage) {
    this.removePage = removePage;
    pointQueue =
        new PriorityQueue<>(
            (o1, o2) -> {
              int timeCompare = Long.compare(o1.timestamp, o2.timestamp);
              return timeCompare != 0 ? timeCompare : Long.compare(o2.priority, o1.priority);
            });
  }

  public TimeValuePair currentPoint() {
    if (shouldReadNextPoint) {
      // get the highest priority point
      if (currentPointElement == null) {
        // the current point is overlapped with other pages
        currentPoint = pointQueue.peek().timeValuePair;

        lastTime = currentPoint.getTimestamp();

        // fill aligned null value with the same timestamp
        if (currentPoint.getValue().getDataType().equals(TSDataType.VECTOR)) {
          fillAlignedNullValue();
        }
      }
      shouldReadNextPoint = false;
    }
    return currentPoint;
  }

  /**
   * Use those records that share the same timestamp to fill the null sub sensor value in current
   * TimeValuePair.
   */
  private void fillAlignedNullValue() {
    List<PointElement> pointElementsWithSameTimestamp = new ArrayList<>();
    // remove the current point element
    pointElementsWithSameTimestamp.add(pointQueue.poll());

    TsPrimitiveType[] currentValues = currentPoint.getValue().getVector();
    int nullValueNum = currentValues.length;
    while (!pointQueue.isEmpty()) {
      if (pointQueue.peek().timestamp > lastTime) {
        // the smallest time of all pages is later then the last time, then break the loop
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

  public void next() throws IllegalPathException, IOException, WriteProcessException {
    if (currentPointElement != null) {
      IPointReader pointReader = currentPointElement.pointReader;
      if (pointReader.hasNextTimeValuePair()) {
        // get the point directly if it is not overlapped with other points
        currentPoint = pointReader.nextTimeValuePair();
        if (currentPoint.getTimestamp() >= nextPageStartTime) {
          // if the point is overlapped with other points, then add it into priority queue
          currentPointElement.setPoint(currentPoint);
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
      while (!pointQueue.isEmpty()) {
        if (pointQueue.peek().timestamp > lastTime) {
          // the smallest time of all pages is later than the last time, then break the loop
          break;
        } else {
          // find the data points in other pages that has the same timestamp
          PointElement pointElement = pointQueue.poll();
          IPointReader pointReader = pointElement.pointReader;
          if (pointReader.hasNextTimeValuePair()) {
            pointElement.setPoint(pointReader.nextTimeValuePair());
            nextPageStartTime =
                pointQueue.size() > 0 ? pointQueue.peek().pageElement.startTime : Long.MAX_VALUE;
            if (pointElement.timestamp > lastTime && pointElement.timestamp < nextPageStartTime) {
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
    shouldReadNextPoint = true;
  }

  public boolean hasNext() {
    return currentPointElement != null || !pointQueue.isEmpty();
  }

  /** Add a new overlapped page. */
  public void addNewPage(PageElement pageElement) throws IOException {
    if (currentPointElement != null) {
      nextPageStartTime = Math.min(nextPageStartTime, pageElement.startTime);
      if (currentPoint.getTimestamp() >= nextPageStartTime) {
        currentPointElement.setPoint(currentPoint);
        pointQueue.add(currentPointElement);
        currentPointElement = null;
      }
    }
    pageElement.deserializePage();
    pointQueue.add(new PointElement(pageElement));
    shouldReadNextPoint = true;
  }
}
