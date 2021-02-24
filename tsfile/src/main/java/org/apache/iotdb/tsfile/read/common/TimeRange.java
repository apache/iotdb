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
package org.apache.iotdb.tsfile.read.common;

import org.apache.iotdb.tsfile.read.expression.IExpression;
import org.apache.iotdb.tsfile.read.expression.impl.BinaryExpression;
import org.apache.iotdb.tsfile.read.expression.impl.GlobalTimeExpression;
import org.apache.iotdb.tsfile.read.filter.TimeFilter;

import java.util.*;

/**
 * interval [min,max] of long data type
 *
 * <p>Reference: http://www.java2s.com/Code/Java/Collections-Data-Structure/Anumericalinterval.htm
 */
public class TimeRange implements Comparable<TimeRange> {

  /** The lower value */
  private long min = 0;

  /** The upper value */
  private long max = 0;

  /**
   * Initialize a closed interval [min,max].
   *
   * @param min the left endpoint of the closed interval
   * @param max the right endpoint of the closed interval
   */
  public TimeRange(long min, long max) {
    set(min, max);
  }

  @Override
  public int compareTo(TimeRange r) {
    if (r == null) {
      throw new NullPointerException("The input cannot be null!");
    }
    long res1 = this.min - r.min;
    if (res1 > 0) {
      return 1;
    } else if (res1 < 0) {
      return -1;
    } else {
      long res2 = this.max - r.max;
      if (res2 > 0) {
        return 1;
      } else if (res2 < 0) {
        return -1;
      } else {
        return 0;
      }
    }
  }

  public void setMin(long min) {
    if (min < 0 || min > this.max) {
      throw new IllegalArgumentException("Invalid input!");
    }
    this.min = min;
  }

  public void setMax(long max) {
    if (max < 0 || max < this.min) {
      throw new IllegalArgumentException("Invalid input!");
    }
    this.max = max;
  }

  /** @return true if the given range lies in this range, inclusively */
  public boolean contains(TimeRange r) {
    return min <= r.min && max >= r.max;
  }

  public boolean contains(long min, long max) {
    return this.min <= min && this.max >= max;
  }

  public boolean contains(long time) {
    return this.min <= time && time <= this.max;
  }

  /**
   * Set a closed interval [min,max].
   *
   * @param min the left endpoint of the closed interval
   * @param max the right endpoint of the closed interval
   */
  public void set(long min, long max) {
    if (min > max) {
      throw new IllegalArgumentException("min should not be larger than max.");
    }
    this.min = min;
    this.max = max;
  }

  /** @return The lower range boundary */
  public long getMin() {
    return min;
  }

  /** @return The upper range boundary */
  public long getMax() {
    return max;
  }

  /**
   * Here are some examples.
   *
   * <p>[1,3] does not intersect with (4,5].
   *
   * <p>[1,3) does not intersect with (3,5].
   *
   * <p>[1,3] does not intersect with [5,6].
   *
   * <p>[1,3] intersects with [2,5].
   *
   * <p>[1,3] intersects with (3,5].
   *
   * <p>[1,3) intersects with (2,5].
   *
   * <p>Note: this method treats [1,3] and [4,5] as two "intersected" interval even if they are not
   * truly intersected.
   *
   * @param r the given time range
   * @return true if the current time range "intersects" with the given time range r
   */
  public boolean intersects(TimeRange r) {
    if ((!leftClose || !r.rightClose) && (r.max < min)) {
      // e.g., [1,3] does not intersect with (4,5].
      return false;
    } else if (!leftClose && !r.rightClose && r.max <= min) {
      // e.g.,[1,3) does not intersect with (3,5]
      return false;
    } else if (leftClose && r.rightClose && r.max <= min - 2) {
      // e.g.,[1,3] does not intersect with [5,6].
      // take care of overflow. e.g., Long.MIN_VALUE
      return false;
    } else if ((!rightClose || !r.leftClose) && (r.min > max)) {
      return false;
    } else if (!rightClose && !r.leftClose && r.min >= max) {
      return false;
    } else if (rightClose && r.leftClose && r.min >= max + 2) {
      // take care of overflow. e.g., Long.MAX_VALUE
      return false;
    } else {
      return true;
    }
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    TimeRange that = (TimeRange) o;
    return (this.min == that.min && this.max == that.max);
  }

  @Override
  public int hashCode() {
    return Objects.hash(min, max);
  }

  /**
   * Check if two TimeRanges overlap
   *
   * @param rhs the given time range
   * @return true if the current time range overlaps with the given time range rhs
   */
  public boolean overlaps(TimeRange rhs) {
    if ((!this.leftClose || !rhs.rightClose) && (rhs.max <= this.min)) {
      // e.g., rhs:[1,3] does not overlap with this:(3,5].
      return false;
    } else if (!this.leftClose && !rhs.rightClose && rhs.max <= this.min + 1) {
      // e.g., rhs:[1,4) does not overlap with this:(3,5]
      return false;
    } else if (this.leftClose && rhs.rightClose && rhs.max < this.min) {
      // e.g., rhs:[1,4] does not overlap with this:[5,6]
      return false;
    } else if ((!this.rightClose || !rhs.leftClose) && (rhs.min >= this.max)) {
      // e.g., this:[1,5) does not overlap with rhs:[5,6]
      return false;
    } else if (!this.rightClose && !rhs.leftClose && rhs.min + 1 >= this.max) {
      // e.g., this:[1,5) does not overlap with rhs:(4,6]
      return false;
    } else if (this.rightClose && rhs.leftClose && rhs.min > this.max) {
      // e.g., this:[1,5] does not overlap with rhs:[6,8]
      return false;
    } else {
      return true;
    }
  }

  @Override
  public String toString() {
    StringBuilder res = new StringBuilder();
    if (leftClose) {
      res.append("[ ");
    } else {
      res.append("( ");
    }
    res.append(min).append(" : ").append(max);
    if (rightClose) {
      res.append(" ]");
    } else {
      res.append(" )");
    }
    return res.toString();
  }

  // NOTE the primitive timeRange is always a closed interval [min,max] and
  // only in getRemains functions are leftClose and rightClose considered.
  private boolean leftClose = true; // default true
  private boolean rightClose = true; // default true

  public void setLeftClose(boolean leftClose) {
    this.leftClose = leftClose;
  }

  public void setRightClose(boolean rightClose) {
    this.rightClose = rightClose;
  }

  public boolean getLeftClose() {
    return leftClose;
  }

  public boolean getRightClose() {
    return rightClose;
  }

  /**
   * Return the union of the given time ranges.
   *
   * @param unionCandidates time ranges to be merged
   * @return the union of time ranges
   */
  public static List<TimeRange> sortAndMerge(List<TimeRange> unionCandidates) {
    // sort the time ranges in ascending order of the start time
    Collections.sort(unionCandidates);

    ArrayList<TimeRange> unionResult = new ArrayList<>();
    Iterator<TimeRange> iterator = unionCandidates.iterator();
    TimeRange rangeCurr;

    if (!iterator.hasNext()) {
      return unionResult;
    } else {
      rangeCurr = iterator.next();
    }

    while (iterator.hasNext()) {
      TimeRange rangeNext = iterator.next();
      if (rangeCurr.intersects(rangeNext)) {
        rangeCurr.set(
            Math.min(rangeCurr.getMin(), rangeNext.getMin()),
            Math.max(rangeCurr.getMax(), rangeNext.getMax()));
      } else {
        unionResult.add(rangeCurr);
        rangeCurr = rangeNext;
      }
    }
    unionResult.add(rangeCurr);
    return unionResult;
  }

  /**
   * Get the remaining time ranges in the current ranges but not in timeRangesPrev.
   *
   * <p>NOTE the primitive timeRange is always a closed interval [min,max] and only in this function
   * are leftClose and rightClose changed.
   *
   * @param timeRangesPrev time ranges union in ascending order of the start time
   * @return the remaining time ranges
   */
  @SuppressWarnings("squid:S3776") // Suppress high Cognitive Complexity warning
  public List<TimeRange> getRemains(List<TimeRange> timeRangesPrev) {
    List<TimeRange> remains = new ArrayList<>();

    for (TimeRange prev : timeRangesPrev) {
      // +2 is to keep consistent with the definition of `intersects` of two closed intervals
      if (prev.min >= max + 2) {
        // break early since timeRangesPrev is sorted
        break;
      }

      if (intersects(prev)) {
        if (prev.contains(this)) {
          // e.g., this=[3,5], prev=[1,10]
          // e.g., this=[3,5], prev=[3,5] Note that in this case, prev contains this and vice versa.
          return remains;
        } else if (this.contains(prev)) {
          if (prev.min > this.min && prev.max == this.max) {
            // e.g., this=[1,6], prev=[3,6]
            this.setMax(prev.min);
            this.setRightClose(false);
            remains.add(this);
            // return the final result because timeRangesPrev is sorted
            return remains;
          } else if (prev.min == this.min) {
            // Note prev.max < this.max e.g., this=[1,10], prev=[1,4]
            min = prev.max;
            leftClose = false;
          } else {
            // e.g., prev=[3,6], this=[1,10]
            TimeRange r = new TimeRange(this.min, prev.min);
            r.setLeftClose(this.leftClose);
            r.setRightClose(false);
            remains.add(r);
            min = prev.max;
            leftClose = false;
          }
        } else {
          // intersect without one containing the other
          if (prev.min < this.min) {
            // e.g., this=[3,10], prev=[1,6]
            min = prev.max;
            leftClose = false;
          } else {
            // e.g., this=[1,8], prev=[5,12]
            this.setMax(prev.min);
            this.setRightClose(false);
            remains.add(this);
            // return the final result because timeRangesPrev is sorted
            return remains;
          }
        }
      }
    }

    remains.add(this);
    return remains;
  }

  public IExpression getExpression() {
    IExpression left;
    IExpression right;
    if (leftClose) {
      left = new GlobalTimeExpression(TimeFilter.gtEq(min));
    } else {
      left = new GlobalTimeExpression(TimeFilter.gt(min));
    }

    if (rightClose) {
      right = new GlobalTimeExpression(TimeFilter.ltEq(max));
    } else {
      right = new GlobalTimeExpression(TimeFilter.lt(max));
    }

    return BinaryExpression.and(left, right);
  }
}
