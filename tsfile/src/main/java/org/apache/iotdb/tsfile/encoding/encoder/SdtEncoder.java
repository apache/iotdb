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

package org.apache.iotdb.tsfile.encoding.encoder;

import java.nio.FloatBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.iotdb.tsfile.utils.Pair;

public class SdtEncoder {

  /**
   * the last read pair <time, value></>
   * if upperDoor >= lowerDoor meaning out of compDeviation range, will store lastReadPair
   */
  private Pair<Long, Double> lastReadPair;

  /**
   * the last stored pair <time, value></>
   * we compare current point against lastStoredPair
   */
  private Pair<Long, Double> lastStoredPair;

  /**
   * the maximum curUpperSlope between the lastStoredPoint to the current point
   * upperDoor can only open up
   */
  private double upperDoor;

  /**
   * the minimum curLowerSlope between the lastStoredPoint to the current point
   * lowerDoor can only open downard
   */
  private double lowerDoor;

  private List<Long> timestamps;
  private List<Double> values;
  private List<Float> floatValues;

  /**
   * the maximum absolute difference the user set
   * if the data's value is within compDeviation, it will be compressed and discarded
   * after compression, it will only store out of range <time, data></> to form the trend
   */
  private double compDeviation;

  /**
   * the minimum time distance between two stored data points
   * if current point time to the last stored point time distance <= compMin,
   * current point will NOT be stored regardless of compression deviation
   */
  private double compMin;

  /**
   * the maximum time distance between two stored data points
   * if current point time to the last stored point time distance >= compMax,
   * current point will be stored regardless of compression deviation
   */
  private double compMax;

  public SdtEncoder () {
    lastReadPair = new Pair<>(null, null);
    lastStoredPair = new Pair<>(null, null);
    upperDoor = Integer.MIN_VALUE;
    lowerDoor = Integer.MAX_VALUE;
    compDeviation = -1;
    compMin = Integer.MIN_VALUE;
    compMax = Integer.MAX_VALUE;
    timestamps = new ArrayList<>();
    values = new ArrayList<>();
    floatValues = new ArrayList<>();
  }


  public boolean encode(long time, double value) {
    // store the first time and value pair
    if (firstPair(time, value)) {
      return true;
    }

    // if current point to the last stored point's time distance is within compMin,
    // will not check two doors nor store any point within the compMin time range
    if (time - lastStoredPair.left <= compMin) {
      return false;
    }

    // if current point to the last stored point's time distance is larger than compMax,
    // will reset two doors, and store current point;
    if (time - lastStoredPair.left >= compMax) {
      reset(time, value);
      return true;
    }

    double curUpperSlope = (value - lastStoredPair.right - compDeviation) / (time - lastStoredPair.left);
    if (curUpperSlope > upperDoor) {
      upperDoor = curUpperSlope;
    }

    double curLowerSlope = (value - lastStoredPair.right + compDeviation) / (time - lastStoredPair.left);
    if (curLowerSlope < lowerDoor) {
      lowerDoor = curLowerSlope;
    }

    // current point to the lastStoredPair's value exceeds compDev, will store lastReadPair and update two doors
    if (upperDoor >= lowerDoor) {
      lastStoredPair = lastReadPair;
      upperDoor = (value - lastStoredPair.right - compDeviation) / (time - lastStoredPair.left);
      lowerDoor = (value - lastStoredPair.right + compDeviation) / (time - lastStoredPair.left);
      lastReadPair = new Pair<>(time, value);
      return true;
    }

    lastReadPair = new Pair<>(time, value);
    return false;
  }


  public boolean encode(long time, long value) {
    return encode(time, (double) value);
  }

  public boolean encode(long time, int value) {
    return encode(time, (double) value);
  }

  public boolean encode(long time, float value) {
    return encode(time, (double) value);
  }

  public void encode(long[] timestamps, double[] values) {
    for (int i = 0; i < timestamps.length; i++) {
      if (encode(timestamps[i], values[i])) {
        this.timestamps.add(getTime());
        this.values.add(getValue());
      }
    }
  }

  public void encode(long[] timestamps, int[] values) {
    encode(timestamps, Arrays.stream(values).mapToDouble(i -> i).toArray());
  }

  public void encode(long[] timestamps, long[] values) {
    encode(timestamps, Arrays.stream(values).mapToDouble(i -> i).toArray());
  }

  public void encode(long[] timestamps, float[] values) {
    for (int i = 0; i < timestamps.length; i++) {
      if (encode(timestamps[i], values[i])) {
        this.timestamps.add(getTime());
        this.floatValues.add((float) getValue());
      }
    }
  }

  private boolean firstPair(long time, double value) {
    if (lastReadPair.left == null && lastReadPair.right == null) {
      lastReadPair = new Pair<>(time, value);
      lastStoredPair = lastReadPair;
      return true;
    }
    return false;
  }

  /**
   * if current point to the last stored point's time distance >= compMax, will store current point
   * and reset two doors
   * @param time current time
   * @param value current value
   */
  private void reset(long time, double value) {
    // lastStoredPair is set to current time, value for getValue(), getTime()
    // will be reset when next time using encode()
    lastStoredPair = new Pair<>(time, value);
    upperDoor = Integer.MIN_VALUE;
    lowerDoor = Integer.MAX_VALUE;
    lastReadPair = new Pair<>(null, null);
  }

  public void setCompDeviation(double compDeviation) {
    this.compDeviation = compDeviation;
  }

  public double getCompDeviation() {
    return compDeviation;
  }

  public void setCompMin(double compMin) {
    this.compMin = compMin;
  }

  public double getCompMin() {
    return compMin;
  }

  public void setCompMax(double compMax) {
    this.compMax = compMax;
  }

  public double getCompMax() {
    return compMax;
  }

  public long getTime() {
    return lastStoredPair.left;
  }

  public double getValue() {
    return lastStoredPair.right;
  }

  public long[] getTimestamps() {
    return timestamps.stream().mapToLong(i -> i).toArray();
  }

  public double[] getValues() {
    return values.stream().mapToDouble(i -> i).toArray();
  }

  public float[] getFloatValues() {
    return floatValues.stream().collect(
        ()-> FloatBuffer.allocate(floatValues.size()),
        FloatBuffer::put,
        (left, right) -> {
          throw new UnsupportedOperationException();
        }
    ).array();
  }
}
