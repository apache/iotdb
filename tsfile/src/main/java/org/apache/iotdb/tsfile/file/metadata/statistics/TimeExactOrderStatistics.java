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
package org.apache.iotdb.tsfile.file.metadata.statistics;

import org.eclipse.collections.impl.list.mutable.primitive.DoubleArrayList;
import org.eclipse.collections.impl.list.mutable.primitive.FloatArrayList;
import org.eclipse.collections.impl.list.mutable.primitive.IntArrayList;
import org.eclipse.collections.impl.list.mutable.primitive.LongArrayList;

import java.util.NoSuchElementException;

/** Util for computing median, MAD, percentile */
public class TimeExactOrderStatistics {

  private LongArrayList longArrayList;

  public TimeExactOrderStatistics() {
    longArrayList = new LongArrayList();
  }

  public void insert(long timestamp) {
    longArrayList.add(timestamp);
  }

  public double getMedian() {
    return getMedian(longArrayList);
  }

  public double getMad() {
    return getMad(longArrayList);
  }

  public String getPercentile(double phi) {
    return Long.toString(getPercentile(longArrayList, phi));
  }

  public static double getMedian(FloatArrayList nums) {
    if (nums.isEmpty()) {
      throw new NoSuchElementException();
    } else {
      nums.sortThis();
      if (nums.size() % 2 == 0) {
        return ((nums.get(nums.size() / 2) + nums.get(nums.size() / 2 - 1)) / 2.0);
      } else {
        return nums.get((nums.size() - 1) / 2);
      }
    }
  }

  public static double getMad(FloatArrayList nums) {
    if (nums.isEmpty()) {
      throw new NoSuchElementException();
    } else {
      double median = getMedian(nums);
      DoubleArrayList dal = new DoubleArrayList();
      for (int i = 0; i < nums.size(); ++i) {
        dal.set(i, Math.abs(nums.get(i) - median));
      }
      return getMedian(dal);
    }
  }

  public static float getPercentile(FloatArrayList nums, double phi) {
    if (nums.isEmpty()) {
      throw new NoSuchElementException();
    } else {
      nums.sortThis();
      return nums.get((int) Math.ceil(nums.size() * phi));
    }
  }

  public static double getMedian(DoubleArrayList nums) {
    if (nums.isEmpty()) {
      throw new NoSuchElementException();
    } else {
      nums.sortThis();
      if (nums.size() % 2 == 0) {
        return (nums.get(nums.size() / 2) + nums.get(nums.size() / 2 - 1)) / 2.0;
      } else {
        return nums.get((nums.size() - 1) / 2);
      }
    }
  }

  public static double getMad(DoubleArrayList nums) {
    if (nums.isEmpty()) {
      throw new NoSuchElementException();
    } else {
      double median = getMedian(nums);
      DoubleArrayList dal = new DoubleArrayList();
      for (int i = 0; i < nums.size(); ++i) {
        dal.set(i, Math.abs(nums.get(i) - median));
      }
      return getMedian(dal);
    }
  }

  public static double getPercentile(DoubleArrayList nums, double phi) {
    if (nums.isEmpty()) {
      throw new NoSuchElementException();
    } else {
      nums.sortThis();
      return nums.get((int) Math.ceil(nums.size() * phi));
    }
  }

  public static double getMedian(IntArrayList nums) {
    if (nums.isEmpty()) {
      throw new NoSuchElementException();
    } else {
      nums.sortThis();
      if (nums.size() % 2 == 0) {
        return (nums.get(nums.size() / 2) + nums.get(nums.size() / 2 - 1)) / 2.0;
      } else {
        return nums.get((nums.size() - 1) / 2);
      }
    }
  }

  public static double getMad(IntArrayList nums) {
    if (nums.isEmpty()) {
      throw new NoSuchElementException();
    } else {
      double median = getMedian(nums);
      DoubleArrayList dal = new DoubleArrayList();
      for (int i = 0; i < nums.size(); ++i) {
        dal.set(i, Math.abs(nums.get(i) - median));
      }
      return getMedian(dal);
    }
  }

  public static int getPercentile(IntArrayList nums, double phi) {
    if (nums.isEmpty()) {
      throw new NoSuchElementException();
    } else {
      nums.sortThis();
      return nums.get((int) Math.ceil(nums.size() * phi));
    }
  }

  public static double getMedian(LongArrayList nums) {
    if (nums.isEmpty()) {
      throw new NoSuchElementException();
    } else {
      nums.sortThis();
      if (nums.size() % 2 == 0) {
        return (nums.get(nums.size() / 2) + nums.get(nums.size() / 2 - 1)) / 2.0;
      } else {
        return nums.get((nums.size() - 1) / 2);
      }
    }
  }

  public static double getMad(LongArrayList nums) {
    if (nums.isEmpty()) {
      throw new NoSuchElementException();
    } else {
      double median = getMedian(nums);
      DoubleArrayList dal = DoubleArrayList.newWithNValues(nums.size(), 0);
      for (int i = 0; i < nums.size(); ++i) {
        dal.set(i, Math.abs(nums.get(i) - median));
      }
      return getMedian(dal);
    }
  }

  public static long getPercentile(LongArrayList nums, double phi) {
    if (nums.isEmpty()) {
      throw new NoSuchElementException();
    } else {
      nums.sortThis();
      return nums.get((int) Math.ceil(nums.size() * phi));
    }
  }
}
