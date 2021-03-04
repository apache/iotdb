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

public class SDTEncoder {

  /**
   * the last read time and value if upperDoor >= lowerDoor meaning out of compDeviation range, will
   * store lastReadPair
   */
  private long lastReadTimestamp;

  private long lastReadLong;
  private double lastReadDouble;
  private int lastReadInt;
  private float lastReadFloat;

  /** the last stored time and vlaue we compare current point against lastStoredPair */
  private long lastStoredTimestamp;

  private long lastStoredLong;
  private double lastStoredDouble;
  private int lastStoredInt;
  private float lastStoredFloat;

  /**
   * the maximum curUpperSlope between the lastStoredPoint to the current point upperDoor can only
   * open up
   */
  private double upperDoor;

  /**
   * the minimum curLowerSlope between the lastStoredPoint to the current point lowerDoor can only
   * open downard
   */
  private double lowerDoor;

  /**
   * the maximum absolute difference the user set if the data's value is within compDeviation, it
   * will be compressed and discarded after compression, it will only store out of range <time,
   * data></> to form the trend
   */
  private double compDeviation;

  /**
   * the minimum time distance between two stored data points if current point time to the last
   * stored point time distance <= compMinTime, current point will NOT be stored regardless of
   * compression deviation
   */
  private long compMinTime;

  /**
   * the maximum time distance between two stored data points if current point time to the last
   * stored point time distance >= compMaxTime, current point will be stored regardless of
   * compression deviation
   */
  private long compMaxTime;

  /**
   * isFirstValue is true when the encoder takes the first point or reset() when cur point's
   * distance to the last stored point's distance exceeds compMaxTime
   */
  private boolean isFirstValue;

  public SDTEncoder() {
    upperDoor = Integer.MIN_VALUE;
    lowerDoor = Integer.MAX_VALUE;
    compDeviation = -1;
    compMinTime = 0;
    compMaxTime = Long.MAX_VALUE;
    isFirstValue = true;
  }

  public boolean encodeFloat(long time, float value) {
    // store the first time and value pair
    if (isFirstValue(time, value)) {
      return true;
    }

    // if current point to the last stored point's time distance is within compMinTime,
    // will not check two doors nor store any point within the compMinTime time range
    if (time - lastStoredTimestamp <= compMinTime) {
      return false;
    }

    // if current point to the last stored point's time distance is larger than compMaxTime,
    // will reset two doors, and store current point;
    if (time - lastStoredTimestamp >= compMaxTime) {
      reset(time, value);
      return true;
    }

    double curUpperSlope = (value - lastStoredFloat - compDeviation) / (time - lastStoredTimestamp);
    if (curUpperSlope > upperDoor) {
      upperDoor = curUpperSlope;
    }

    double curLowerSlope = (value - lastStoredFloat + compDeviation) / (time - lastStoredTimestamp);
    if (curLowerSlope < lowerDoor) {
      lowerDoor = curLowerSlope;
    }

    // current point to the lastStoredPair's value exceeds compDev, will store lastReadPair and
    // update two doors
    if (upperDoor >= lowerDoor) {
      lastStoredTimestamp = lastReadTimestamp;
      lastStoredFloat = lastReadFloat;
      upperDoor = (value - lastStoredFloat - compDeviation) / (time - lastStoredTimestamp);
      lowerDoor = (value - lastStoredFloat + compDeviation) / (time - lastStoredTimestamp);
      lastReadFloat = value;
      lastReadTimestamp = time;
      return true;
    }

    lastReadFloat = value;
    lastReadTimestamp = time;
    return false;
  }

  public boolean encodeLong(long time, long value) {
    // store the first time and value pair
    if (isFirstValue(time, value)) {
      return true;
    }

    // if current point to the last stored point's time distance is within compMinTime,
    // will not check two doors nor store any point within the compMinTime time range
    if (time - lastStoredTimestamp <= compMinTime) {
      return false;
    }

    // if current point to the last stored point's time distance is larger than compMaxTime,
    // will reset two doors, and store current point;
    if (time - lastStoredTimestamp >= compMaxTime) {
      reset(time, value);
      return true;
    }

    double curUpperSlope = (value - lastStoredLong - compDeviation) / (time - lastStoredTimestamp);
    if (curUpperSlope > upperDoor) {
      upperDoor = curUpperSlope;
    }

    double curLowerSlope = (value - lastStoredLong + compDeviation) / (time - lastStoredTimestamp);
    if (curLowerSlope < lowerDoor) {
      lowerDoor = curLowerSlope;
    }

    // current point to the lastStoredPair's value exceeds compDev, will store lastReadPair and
    // update two doors
    if (upperDoor >= lowerDoor) {
      lastStoredLong = lastReadLong;
      lastStoredTimestamp = lastReadTimestamp;
      upperDoor = (value - lastStoredLong - compDeviation) / (time - lastStoredTimestamp);
      lowerDoor = (value - lastStoredLong + compDeviation) / (time - lastStoredTimestamp);
      lastReadLong = value;
      lastReadTimestamp = time;
      return true;
    }

    lastReadLong = value;
    lastReadTimestamp = time;
    return false;
  }

  public boolean encodeInt(long time, int value) {
    // store the first time and value pair
    if (isFirstValue(time, value)) {
      return true;
    }

    // if current point to the last stored point's time distance is within compMinTime,
    // will not check two doors nor store any point within the compMinTime time range
    if (time - lastStoredTimestamp <= compMinTime) {
      return false;
    }

    // if current point to the last stored point's time distance is larger than compMaxTime,
    // will reset two doors, and store current point;
    if (time - lastStoredTimestamp >= compMaxTime) {
      reset(time, value);
      return true;
    }

    double curUpperSlope = (value - lastStoredInt - compDeviation) / (time - lastStoredTimestamp);
    if (curUpperSlope > upperDoor) {
      upperDoor = curUpperSlope;
    }

    double curLowerSlope = (value - lastStoredInt + compDeviation) / (time - lastStoredTimestamp);
    if (curLowerSlope < lowerDoor) {
      lowerDoor = curLowerSlope;
    }

    // current point to the lastStoredPair's value exceeds compDev, will store lastReadPair and
    // update two doors
    if (upperDoor >= lowerDoor) {
      lastStoredTimestamp = lastReadTimestamp;
      lastStoredInt = lastReadInt;
      upperDoor = (value - lastStoredInt - compDeviation) / (time - lastStoredTimestamp);
      lowerDoor = (value - lastStoredInt + compDeviation) / (time - lastStoredTimestamp);
      lastReadInt = value;
      lastReadTimestamp = time;
      return true;
    }

    lastReadInt = value;
    lastReadTimestamp = time;
    return false;
  }

  public boolean encodeDouble(long time, double value) {
    // store the first time and value pair
    if (isFirstValue(time, value)) {
      return true;
    }

    // if current point to the last stored point's time distance is within compMinTime,
    // will not check two doors nor store any point within the compMinTime time range
    if (time - lastStoredTimestamp <= compMinTime) {
      return false;
    }

    // if current point to the last stored point's time distance is larger than compMaxTime,
    // will reset two doors, and store current point;
    if (time - lastStoredTimestamp >= compMaxTime) {
      reset(time, value);
      return true;
    }

    double curUpperSlope =
        (value - lastStoredDouble - compDeviation) / (time - lastStoredTimestamp);
    if (curUpperSlope > upperDoor) {
      upperDoor = curUpperSlope;
    }

    double curLowerSlope =
        (value - lastStoredDouble + compDeviation) / (time - lastStoredTimestamp);
    if (curLowerSlope < lowerDoor) {
      lowerDoor = curLowerSlope;
    }

    // current point to the lastStoredPair's value exceeds compDev, will store lastReadPair and
    // update two doors
    if (upperDoor >= lowerDoor) {
      lastStoredTimestamp = lastReadTimestamp;
      lastStoredDouble = lastReadDouble;
      upperDoor = (value - lastStoredDouble - compDeviation) / (time - lastStoredTimestamp);
      lowerDoor = (value - lastStoredDouble + compDeviation) / (time - lastStoredTimestamp);
      lastReadDouble = value;
      lastReadTimestamp = time;
      return true;
    }

    lastReadDouble = value;
    lastReadTimestamp = time;
    return false;
  }

  public int encode(long[] timestamps, double[] values, int batchSize) {
    int index = 0;
    for (int i = 0; i < batchSize; i++) {
      if (encodeDouble(timestamps[i], values[i])) {
        timestamps[index] = lastStoredTimestamp;
        values[index] = lastStoredDouble;
        index++;
      }
    }
    return index;
  }

  public int encode(long[] timestamps, int[] values, int batchSize) {
    int index = 0;
    for (int i = 0; i < batchSize; i++) {
      if (encodeInt(timestamps[i], values[i])) {
        timestamps[index] = lastStoredTimestamp;
        values[index] = lastStoredInt;
        index++;
      }
    }
    return index;
  }

  public int encode(long[] timestamps, long[] values, int batchSize) {
    int index = 0;
    for (int i = 0; i < batchSize; i++) {
      if (encodeLong(timestamps[i], values[i])) {
        timestamps[index] = lastStoredTimestamp;
        values[index] = lastStoredLong;
        index++;
      }
    }
    return index;
  }

  public int encode(long[] timestamps, float[] values, int batchSize) {
    int index = 0;
    for (int i = 0; i < batchSize; i++) {
      if (encodeFloat(timestamps[i], values[i])) {
        timestamps[index] = lastStoredTimestamp;
        values[index] = lastStoredFloat;
        index++;
      }
    }
    return index;
  }

  private boolean isFirstValue(long time, float value) {
    if (isFirstValue) {
      isFirstValue = false;
      lastReadTimestamp = time;
      lastReadFloat = value;
      lastStoredTimestamp = time;
      lastStoredFloat = value;
      return true;
    }
    return false;
  }

  private boolean isFirstValue(long time, long value) {
    if (isFirstValue) {
      isFirstValue = false;
      lastReadTimestamp = time;
      lastReadLong = value;
      lastStoredTimestamp = time;
      lastStoredLong = value;
      return true;
    }
    return false;
  }

  private boolean isFirstValue(long time, int value) {
    if (isFirstValue) {
      isFirstValue = false;
      lastReadTimestamp = time;
      lastReadInt = value;
      lastStoredTimestamp = time;
      lastStoredInt = value;
      return true;
    }
    return false;
  }

  private boolean isFirstValue(long time, double value) {
    if (isFirstValue) {
      isFirstValue = false;
      lastReadTimestamp = time;
      lastReadDouble = value;
      lastStoredTimestamp = time;
      lastStoredDouble = value;
      return true;
    }
    return false;
  }

  private void reset() {
    upperDoor = Integer.MIN_VALUE;
    lowerDoor = Integer.MAX_VALUE;
  }

  /**
   * if current point to the last stored point's time distance >= compMaxTime, will store current
   * point and reset upperDoor and lowerDoor
   *
   * @param time current time
   * @param value current value
   */
  private void reset(long time, long value) {
    reset();
    lastStoredTimestamp = time;
    lastStoredLong = value;
  }

  private void reset(long time, double value) {
    reset();
    lastStoredTimestamp = time;
    lastStoredDouble = value;
  }

  private void reset(long time, int value) {
    reset();
    lastStoredTimestamp = time;
    lastStoredInt = value;
  }

  private void reset(long time, float value) {
    reset();
    lastStoredTimestamp = time;
    lastStoredFloat = value;
  }

  public void setCompDeviation(double compDeviation) {
    this.compDeviation = compDeviation;
  }

  public double getCompDeviation() {
    return compDeviation;
  }

  public void setCompMinTime(long compMinTime) {
    this.compMinTime = compMinTime;
  }

  public long getCompMinTime() {
    return compMinTime;
  }

  public void setCompMaxTime(long compMaxTime) {
    this.compMaxTime = compMaxTime;
  }

  public long getCompMaxTime() {
    return compMaxTime;
  }

  public long getTime() {
    return lastStoredTimestamp;
  }

  public int getIntValue() {
    return lastStoredInt;
  }

  public double getDoubleValue() {
    return lastStoredDouble;
  }

  public long getLongValue() {
    return lastStoredLong;
  }

  public float getFloatValue() {
    return lastStoredFloat;
  }
}
