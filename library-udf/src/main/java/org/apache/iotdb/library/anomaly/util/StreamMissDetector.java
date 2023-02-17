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

package org.apache.iotdb.library.anomaly.util;

import org.apache.iotdb.library.util.BooleanCircularQueue;
import org.apache.iotdb.library.util.DoubleCircularQueue;
import org.apache.iotdb.library.util.LongCircularQueue;

import org.apache.commons.math3.stat.regression.SimpleRegression;

/** Streaming anomaly detection for anomalies of data missing. */
public class StreamMissDetector {

  private final LongCircularQueue timeWindow = new LongCircularQueue();
  private final DoubleCircularQueue valueWindow = new DoubleCircularQueue();
  private final LongCircularQueue timeBuffer = new LongCircularQueue();
  private final BooleanCircularQueue labelBuffer = new BooleanCircularQueue();
  private final SimpleRegression regression = new SimpleRegression();
  private final double threshold = 0.9999;
  private int minLength;
  private int state;
  private long startTime;
  private int missingStartIndex;
  private boolean horizon;
  private double standard;

  public StreamMissDetector(int minLength) {
    this.state = 0;
    this.startTime = -1;
    this.minLength = minLength;
  }

  public void insert(long time, double value) {
    timeWindow.push(time);
    valueWindow.push(value);
    if (startTime < 0) {
      startTime = time;
    }
    switch (state) {
      case 0:
        if (timeWindow.getSize() >= getWindowSize()) {
          linearRegress(0, getWindowSize());
          double alpha = regression.getRSquare();
          if (Double.isNaN(alpha) || alpha > threshold) {
            missingStartIndex = 0;
            state = 2;
          } else {
            regression.clear();
            state = 1;
          }
        }
        break;
      case 1:
        if (timeWindow.getSize() >= getWindowSize() * 2) {
          linearRegress(getWindowSize(), getWindowSize() * 2);
          double alpha = regression.getRSquare();
          if (Double.isNaN(alpha) || alpha > threshold) {
            missingStartIndex = backExtend();
            state = 2;
          } else {
            for (int i = 0; i < getWindowSize(); i++) {
              timeBuffer.push(timeWindow.pop());
              labelBuffer.push(false);
              valueWindow.pop();
            }
            regression.clear();
            state = 1;
          }
        }
        break;
      case 2:
        regression.addData(time - startTime, value);
        double alpha = regression.getRSquare();
        if ((horizon && value != standard) || (!horizon && alpha < threshold)) {
          int missingEndIndex = timeWindow.getSize() - 1;
          for (int i = 0; i < missingStartIndex; i++) {
            timeBuffer.push(timeWindow.pop());
            labelBuffer.push(false);
            valueWindow.pop();
          }
          boolean label = missingEndIndex - missingStartIndex > minLength;
          for (int i = missingStartIndex; i < missingEndIndex; i++) {
            timeBuffer.push(timeWindow.pop());
            labelBuffer.push(label);
            valueWindow.pop();
          }
          regression.clear();
          state = 0;
        }
    }
  }

  public void flush() {
    switch (state) {
      case 0:
      case 1:
        while (!timeWindow.isEmpty()) {
          timeBuffer.push(timeWindow.pop());
          labelBuffer.push(false);
          valueWindow.pop();
        }
        break;
      case 2:
        boolean label = timeWindow.getSize() - missingStartIndex > minLength;
        for (int i = 0; i < missingStartIndex; i++) {
          timeBuffer.push(timeWindow.pop());
          labelBuffer.push(false);
          valueWindow.pop();
        }
        while (!timeWindow.isEmpty()) {
          timeBuffer.push(timeWindow.pop());
          labelBuffer.push(label);
          valueWindow.pop();
        }
    }
  }

  private int backExtend() {
    horizon = Double.isNaN(regression.getRSquare());
    standard = regression.getIntercept();
    int bindex = getWindowSize();
    while (bindex > 0) {
      bindex--;
      regression.addData(timeWindow.get(bindex) - startTime, valueWindow.get(bindex));
      double alpha = regression.getRSquare();
      if ((horizon && valueWindow.get(bindex) != standard) || (!horizon && alpha < threshold)) {
        break;
      }
    }
    regression.removeData(timeWindow.get(bindex) - startTime, valueWindow.get(bindex));
    return bindex + 1;
  }

  private void linearRegress(int start, int end) {
    double[][] data = new double[getWindowSize()][2];
    for (int i = start; i < end; i++) {
      data[i - start][0] = timeWindow.get(i) - startTime;
      data[i - start][1] = valueWindow.get(i);
    }
    regression.addData(data);
  }

  public boolean hasNext() {
    return !timeBuffer.isEmpty();
  }

  public long getOutTime() {
    return timeBuffer.getHead();
  }

  public boolean getOutValue() {
    return labelBuffer.getHead();
  }

  public void next() {
    timeBuffer.pop();
    labelBuffer.pop();
  }

  private int getWindowSize() {
    return minLength / 2;
  }

  public int getMinLength() {
    return minLength;
  }

  /** @param minLength the minLength to set */
  public void setMinLength(int minLength) {
    this.minLength = minLength;
  }
}
