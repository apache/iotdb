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

package org.apache.iotdb.commons.udf.builtin;

import org.apache.iotdb.udf.api.access.Row;
import org.apache.iotdb.udf.api.access.RowIterator;
import org.apache.iotdb.udf.api.access.RowWindow;
import org.apache.iotdb.udf.api.collector.PointCollector;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.PriorityQueue;

public class UDTFTopKDTWSlidingWindow extends UDTFTopKDTW {

  private static class DTWPoint {

    private final long time;
    private final double value;

    private DTWPoint(long time, double value) {
      this.time = time;
      this.value = value;
    }
  }

  private static class DTWPath {

    private final long startTime;
    private long endTime;
    private double distance;

    private DTWPath(long startTime, long endTime, double distance) {
      this.startTime = startTime;
      this.endTime = endTime;
      this.distance = distance;
    }

    private DTWPath copy() {
      return new DTWPath(startTime, endTime, distance);
    }

    @Override
    public String toString() {
      return "DTWPath{"
          + "startTime="
          + startTime
          + ", endTime="
          + endTime
          + ", distance="
          + distance
          + '}';
    }
  }

  private DTWPoint[] pattern;
  private DTWPath[] dtwBefore;
  private DTWPath[] dtwCurrent;

  private final PriorityQueue<DTWPath> topK =
      new PriorityQueue<>((o1, o2) -> Double.compare(o2.distance, o1.distance));

  @Override
  public void transform(RowWindow rowWindow, PointCollector collector) throws Exception {
    if (pattern == null) {
      // Read pattern
      RowIterator iterator = rowWindow.getRowIterator();
      List<DTWPoint> patternList = new ArrayList<>();
      while (iterator.hasNextRow()) {
        Row row = iterator.next();
        if (row.isNull(COLUMN_P)) {
          break;
        }
        patternList.add(new DTWPoint(row.getTime(), row.getDouble(COLUMN_P)));
      }
      pattern = patternList.toArray(new DTWPoint[0]);
    }

    RowIterator iterator = rowWindow.getRowIterator();
    while (iterator.hasNextRow()) {
      Row row = iterator.next();
      if (row.isNull(COLUMN_S)) {
        continue;
      }

      double value = row.getDouble(COLUMN_S);
      dtwCurrent = new DTWPath[pattern.length];
      for (int i = 0; i < pattern.length; i++) {
        double currentDistance = Math.abs(value - pattern[i].value);
        if (i == 0) {
          // Start a new DTW path
          dtwCurrent[i] = new DTWPath(pattern[i].time, pattern[i].time, currentDistance);
          continue;
        }

        // Find the optimal DTW path from previous
        dtwCurrent[i] = dtwCurrent[i - 1].copy();
        if (dtwBefore != null) {
          if (dtwBefore[i].distance < dtwCurrent[i].distance) {
            dtwCurrent[i] = dtwBefore[i].copy();
          }
          if (dtwBefore[i - 1].distance < dtwCurrent[i].distance) {
            dtwCurrent[i] = dtwBefore[i - 1].copy();
          }
        }
        dtwCurrent[i].endTime = pattern[i].time;
        dtwCurrent[i].distance += currentDistance;
      }
      dtwBefore = Arrays.copyOf(dtwCurrent, dtwCurrent.length);
      DTWPath currentPath = dtwCurrent[dtwCurrent.length - 1];
      if (topK.size() < k) {
        topK.offer(currentPath);
      } else if (topK.peek().distance > currentPath.distance) {
        topK.poll();
        topK.offer(currentPath);
      }
    }
  }

  @Override
  public void terminate(PointCollector collector) throws Exception {
    while (!topK.isEmpty()) {
      DTWPath path = topK.poll();
      collector.putString(path.startTime, path.toString());
    }
  }
}
