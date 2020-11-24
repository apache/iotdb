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
   * the last read time and value
   * if upperDoor >= lowerDoor meaning out of compDeviation range, will store lastReadPair
   */
  private long lastReadTimestamp;
  private long lastReadLong;
  private double lastReadDouble;
  private int lastReadInt;
  private float lastReadFloat;

  /**
   * the last stored time and vlaue
   * we compare current point against lastStoredPair
   */
  private long lastStoredTimestamp;
  private long lastStoredLong;
  private double lastStoredDouble;
  private int lastStoredInt;
  private float lastStoredFloat;

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

  /**
   * isFirstValue is true when the encoder takes the first point or reset() when cur point's
   * distance to the last stored point's distance exceeds compMax
   */
  private boolean isFirstValue;

  public SDTEncoder() {
    upperDoor = Integer.MIN_VALUE;
    lowerDoor = Integer.MAX_VALUE;
    compDeviation = -1;
    compMin = Integer.MIN_VALUE;
    compMax = Integer.MAX_VALUE;
    isFirstValue = true;
  }

  public boolean encodeFloat(long time, float value) {
    // store the first time and value pair
    if (isFirstValue(time, value)) {
      return true;
    }

    // if current point to the last stored point's time distance is within compMin,
    // will not check two doors nor store any point within the compMin time range
    if (time - lastStoredTimestamp <= compMin) {
      return false;
    }

    // if current point to the last stored point's time distance is larger than compMax,
    // will reset two doors, and store current point;
    if (time - lastStoredTimestamp >= compMax) {
      reset(time, value);
      return true;
    }

    double curUpperSlope = (value - lastStoredFloat - compDeviation) / (time - lastStoredTimestamp);
    if (curUpperSlope > upperDoor) {
      upperDoor = curUpperSlope;
      if (upperDoor > lowerDoor) {
        // slope between curr point and last read point
        double slope = (value - lastReadFloat) / (time - lastReadTimestamp);
        // start point of the next segment
        long timestamp = (long) ((lastStoredFloat + compDeviation - lastReadFloat + slope * lastReadTimestamp -
            lowerDoor * lastStoredTimestamp) / (slope - lowerDoor));
        lastStoredFloat = (float) (lastStoredFloat + compDeviation + lowerDoor * (timestamp - lastStoredTimestamp)
            - compDeviation / 2);
        lastStoredTimestamp = timestamp;
        // recalculate upperDoor and lowerDoor
        upperDoor = (value - lastStoredFloat - compDeviation) / (time - lastStoredTimestamp);
        lowerDoor = (value - lastStoredFloat + compDeviation) / (time - lastStoredTimestamp);
        // update last read point to current point
        lastReadFloat = value;
        lastReadTimestamp = time;
        return true;
      }
    }

    double curLowerSlope = (value - lastStoredFloat + compDeviation) / (time - lastStoredTimestamp);
    if (curLowerSlope < lowerDoor) {
      lowerDoor = curLowerSlope;
      if (upperDoor > lowerDoor) {
        // slope between curr point and last read point
        double slope = (value - lastReadFloat) / (time - lastReadTimestamp);
        // start point of the next segment
        long timestamp = (long) ((lastStoredFloat - compDeviation - lastReadFloat + slope * lastReadTimestamp -
            upperDoor * lastStoredTimestamp) / (slope - upperDoor));
        lastStoredFloat = (float) (lastStoredFloat - compDeviation + upperDoor * (timestamp - lastStoredTimestamp)
            + compDeviation / 2);
        lastStoredTimestamp = timestamp;
        // recalculate upperDoor and lowerDoor
        upperDoor = (value - lastStoredFloat - compDeviation) / (time - lastStoredTimestamp);
        lowerDoor = (value - lastStoredFloat + compDeviation) / (time - lastStoredTimestamp);
        // update last read point to current point
        lastReadFloat = value;
        lastReadTimestamp = time;
        return true;
      }
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

    // if current point to the last stored point's time distance is within compMin,
    // will not check two doors nor store any point within the compMin time range
    if (time - lastStoredTimestamp <= compMin) {
      return false;
    }

    // if current point to the last stored point's time distance is larger than compMax,
    // will reset two doors, and store current point;
    if (time - lastStoredTimestamp >= compMax) {
      reset(time, value);
      return true;
    }

    double curUpperSlope = (value - lastStoredLong - compDeviation) / (time - lastStoredTimestamp);
    if (curUpperSlope > upperDoor) {
      upperDoor = curUpperSlope;
      if (upperDoor > lowerDoor) {
        // slope between curr point and last read point
        double slope = ((double) value - lastReadLong) / (time - lastReadTimestamp);
        // start point of the next segment
        long timestamp = (long) ((lastStoredLong + compDeviation - lastReadLong + slope * lastReadTimestamp -
            lowerDoor * lastStoredTimestamp) / (slope - lowerDoor));
        lastStoredLong = Math.round((lastStoredLong + compDeviation + lowerDoor * (timestamp - lastStoredTimestamp)
            - compDeviation / 2));
        lastStoredTimestamp = timestamp;
        // recalculate upperDoor and lowerDoor
        upperDoor = (value - lastStoredLong - compDeviation) / (time - lastStoredTimestamp);
        lowerDoor = (value - lastStoredLong + compDeviation) / (time - lastStoredTimestamp);
        // update last read point to current point
        lastReadLong = value;
        lastReadTimestamp = time;
        return true;
      }
    }

    double curLowerSlope = (value - lastStoredLong + compDeviation) / (time - lastStoredTimestamp);
    if (curLowerSlope < lowerDoor) {
      lowerDoor = curLowerSlope;
      if (upperDoor > lowerDoor) {
        // slope between curr point and last read point
        double slope = ((double) value - lastReadLong) / (time - lastReadTimestamp);
        // start point of the next segment
        long timestamp = (long) ((lastStoredLong - compDeviation - lastReadLong + slope * lastReadTimestamp -
            upperDoor * lastStoredTimestamp) / (slope - upperDoor));
        lastStoredLong = Math.round((lastStoredLong - compDeviation + upperDoor * (timestamp - lastStoredTimestamp)
            + compDeviation / 2));
        lastStoredTimestamp = timestamp;

        // recalculate upperDoor and lowerDoor
        upperDoor = (value - lastStoredLong - compDeviation) / (time - lastStoredTimestamp);
        lowerDoor = (value - lastStoredLong + compDeviation) / (time - lastStoredTimestamp);
        // update last read point to current point
        lastReadLong = value;
        lastReadTimestamp = time;
        return true;
      }
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

    // if current point to the last stored point's time distance is within compMin,
    // will not check two doors nor store any point within the compMin time range
    if (time - lastStoredTimestamp <= compMin) {
      return false;
    }

    // if current point to the last stored point's time distance is larger than compMax,
    // will reset two doors, and store current point;
    if (time - lastStoredTimestamp >= compMax) {
      reset(time, value);
      return true;
    }

    double curUpperSlope = (value - lastStoredInt - compDeviation) / (time - lastStoredTimestamp);
    if (curUpperSlope > upperDoor) {
      upperDoor = curUpperSlope;
      if (upperDoor > lowerDoor) {
        // slope between curr point and last read point
        double slope = ((double) value - lastReadInt) / (time - lastReadTimestamp);
        // start point of the next segment
        long timestamp = (long) ((lastStoredInt + compDeviation - lastReadInt + slope * lastReadTimestamp -
            lowerDoor * lastStoredTimestamp) / (slope - lowerDoor));
        lastStoredInt = (int) Math.round((lastStoredInt + compDeviation + lowerDoor * (timestamp - lastStoredTimestamp)
            - compDeviation / 2));
        lastStoredTimestamp = timestamp;
        // recalculate upperDoor and lowerDoor
        upperDoor = (value - lastStoredInt - compDeviation) / (time - lastStoredTimestamp);
        lowerDoor = (value - lastStoredInt + compDeviation) / (time - lastStoredTimestamp);
        // update last read point to current point
        lastReadInt = value;
        lastReadTimestamp = time;
        return true;
      }
    }

    double curLowerSlope = (value - lastStoredInt + compDeviation) / (time - lastStoredTimestamp);
    if (curLowerSlope < lowerDoor) {
      lowerDoor = curLowerSlope;
      if (upperDoor > lowerDoor) {
        // slope between curr point and last read point
        double slope = ((double) value - lastReadInt) / (time - lastReadTimestamp);
        // start point of the next segment
        long timestamp = (long) ((lastStoredInt - compDeviation - lastReadInt + slope * lastReadTimestamp -
            upperDoor * lastStoredTimestamp) / (slope - upperDoor));
        lastStoredInt = (int) Math.round((lastStoredInt - compDeviation + upperDoor * (timestamp - lastStoredTimestamp)
            + compDeviation / 2));
        lastStoredTimestamp = timestamp;
        // recalculate upperDoor and lowerDoor
        upperDoor = (value - lastStoredInt - compDeviation) / (time - lastStoredTimestamp);
        lowerDoor = (value - lastStoredInt + compDeviation) / (time - lastStoredTimestamp);
        // update last read point to current point
        lastReadInt = value;
        lastReadTimestamp = time;
        return true;
      }
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

    // if current point to the last stored point's time distance is within compMin,
    // will not check two doors nor store any point within the compMin time range
    if (time - lastStoredTimestamp <= compMin) {
      return false;
    }

    // if current point to the last stored point's time distance is larger than compMax,
    // will reset two doors, and store current point;
    if (time - lastStoredTimestamp >= compMax) {
      reset(time, value);
      return true;
    }

    double curUpperSlope = (value - lastStoredDouble - compDeviation) / (time - lastStoredTimestamp);
    if (curUpperSlope > upperDoor) {
      upperDoor = curUpperSlope;
      if (upperDoor > lowerDoor) {
        // slope between curr point and last read point
        double slope = (value - lastReadDouble) / (time - lastReadTimestamp);
        // start point of the next segment
        long timestamp = (long) ((lastStoredDouble + compDeviation - lastReadDouble + slope * lastReadTimestamp -
            lowerDoor * lastStoredTimestamp) / (slope - lowerDoor));
        lastStoredDouble = (lastStoredDouble + compDeviation + lowerDoor * (timestamp - lastStoredTimestamp)
            - compDeviation / 2);
        lastStoredTimestamp = timestamp;
        // recalculate upperDoor and lowerDoor
        upperDoor = (value - lastStoredDouble - compDeviation) / (time - lastStoredTimestamp);
        lowerDoor = (value - lastStoredDouble + compDeviation) / (time - lastStoredTimestamp);
        // update last read point to current point
        lastReadDouble = value;
        lastReadTimestamp = time;
        return true;
      }
    }

    double curLowerSlope = (value - lastStoredDouble + compDeviation) / (time - lastStoredTimestamp);
    if (curLowerSlope < lowerDoor) {
      lowerDoor = curLowerSlope;
      if (upperDoor > lowerDoor) {
        // slope between curr point and last read point
        double slope = (value - lastReadDouble) / (time - lastReadTimestamp);
        // start point of the next segment
        long timestamp = (long) ((lastStoredDouble - compDeviation - lastReadDouble + slope * lastReadTimestamp -
            upperDoor * lastStoredTimestamp) / (slope - upperDoor));
        lastStoredDouble = (lastStoredDouble - compDeviation + upperDoor * (timestamp - lastStoredTimestamp)
            + compDeviation / 2);
        lastStoredTimestamp = timestamp;
        // recalculate upperDoor and lowerDoor
        upperDoor = (value - lastStoredDouble - compDeviation) / (time - lastStoredTimestamp);
        lowerDoor = (value - lastStoredDouble + compDeviation) / (time - lastStoredTimestamp);
        // update last read point to current point
        lastReadDouble = value;
        lastReadTimestamp = time;
        return true;
      }
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
    isFirstValue = true;
  }

  /**
   * if current point to the last stored point's time distance >= compMax, will store current point
   * and reset upperDoor and lowerDoor
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