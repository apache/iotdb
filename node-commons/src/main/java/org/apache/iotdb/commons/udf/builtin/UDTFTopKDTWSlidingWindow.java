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
    private double distance;
    private int length;

    private DTWPath(long startTime, double distance, int length) {
      this.startTime = startTime;
      this.distance = distance;
      this.length = length;
    }

    private DTWPath copy() {
      return new DTWPath(startTime, distance, length);
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
        if (row.isNull(1)) {
          break;
        }
        patternList.add(new DTWPoint(row.getTime(), row.getDouble(1)));
      }
      pattern = patternList.toArray(new DTWPoint[0]);
    }

    RowIterator iterator = rowWindow.getRowIterator();
    while (iterator.hasNextRow()) {
      Row row = iterator.next();
      if (row.isNull(0)) {
        continue;
      }

      double value = row.getDouble(0);
      dtwCurrent = new DTWPath[pattern.length];
      for (int i = 0; i < pattern.length; i++) {
        double currentDistance = Math.abs(value - pattern[i].value);
        if (i == 0) {
          // Start a new DTW path
          dtwCurrent[i] = new DTWPath(pattern[i].time, currentDistance, 1);
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
        dtwCurrent[i].distance += currentDistance;
        dtwCurrent[i].length++;
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
      collector.putDouble(path.startTime, path.distance);
      collector.putInt(path.startTime, path.length);
    }
  }
}
