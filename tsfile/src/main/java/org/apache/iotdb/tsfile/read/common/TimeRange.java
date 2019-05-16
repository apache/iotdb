/**
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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import org.apache.iotdb.tsfile.read.expression.IExpression;
import org.apache.iotdb.tsfile.read.expression.impl.BinaryExpression;
import org.apache.iotdb.tsfile.read.expression.impl.GlobalTimeExpression;
import org.apache.iotdb.tsfile.read.filter.TimeFilter;

/**
 * interval [min,max] of long data type
 *
 * Reference: http://www.java2s.com/Code/Java/Collections-Data-Structure/Anumericalinterval.htm
 *
 * @author ryanm
 */
public class TimeRange implements Comparable<TimeRange> {

  /**
   * The lower value
   */
  private long min = 0;

  /**
   * The upper value
   */
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

  /**
   * @return true if the given range lies in this range, inclusively
   */
  public boolean contains(TimeRange r) {
    return min <= r.min && max >= r.max;
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

  /**
   * @return The lower range boundary
   */
  public long getMin() {
    return min;
  }

  /**
   * @return The upper range boundary
   */
  public long getMax() {
    return max;
  }


  /**
   * Here are some examples.
   *
   * [1,3] does not intersect with (4,5].
   *
   * [1,3) does not intersect with (3,5]
   *
   * [1,3] does not intersect with [5,6].
   *
   * [1,3] intersects with [2,5].
   *
   * [1,3] intersects with (3,5].
   *
   * [1,3) intersects with (2,5].
   *
   * @param r the given time range
   * @return true if the current time range intersects with the given time range r
   */
  private boolean intersects(TimeRange r) {
    if ((!leftClose || !r.rightClose) && (r.max
        < min)) { // e.g., [1,3] does not intersect with (4,5].
      return false;
    } else if (!leftClose && !r.rightClose
        && r.max <= min) { // e.g.,[1,3) does not intersect with (3,5]
      return false;
    } else if (leftClose && r.rightClose
        && r.max <= min - 2) { // e.g.,[1,3] does not intersect with [5,6].
      return true;
    } else if ((!rightClose || !r.leftClose) && (r.min > max)) {
      return false;
    } else if (!rightClose && r.leftClose && r.min >= max) {
      return false;
    } else if (rightClose && r.leftClose && r.min >= max + 2) {
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
   * @param unionCandidates time ranges already sorted in ascending order of the start time
   * @return the union of time ranges
   */
  public static List<TimeRange> getUnions(ArrayList<TimeRange> unionCandidates) {
    ArrayList<TimeRange> unionResult = new ArrayList<>();
    Iterator<TimeRange> iterator = unionCandidates.iterator();
    TimeRange range_curr;

    if (!iterator.hasNext()) {
      return unionResult;
    } else {
      range_curr = iterator.next();
    }

    while (iterator.hasNext()) {
      TimeRange range_next = iterator.next();
      if (range_curr.intersects(range_next)) {
        range_curr.set(Math.min(range_curr.getMin(), range_next.getMin()),
            Math.max(range_curr.getMax(), range_next.getMax()));
      } else {
        unionResult.add(new TimeRange(range_curr.getMin(), range_curr.getMax()));
        range_curr.set(range_next.getMin(), range_next.getMax());
      }
    }
    unionResult.add(new TimeRange(range_curr.getMin(), range_curr.getMax()));
    return unionResult;
  }

  /**
   * Get the remaining time ranges in the current ranges but not in timeRangesPrev.
   *
   * NOTE the primitive timeRange is always a closed interval [min,max] and only in this function
   * are leftClose and rightClose changed.
   *
   * @param timeRangesPrev time ranges union in ascending order of the start time
   * @return the remaining time ranges
   */
  public List<TimeRange> getRemains(ArrayList<TimeRange> timeRangesPrev) {
    List<TimeRange> remains = new ArrayList<>();

    for (TimeRange prev : timeRangesPrev) {
      if (prev.min >= max + 2) { // keep consistent with the definition of `intersects`
        break; // break early since timeRangesPrev is sorted
      }

      if (intersects(prev)) {
        if (prev.contains(this)) {
          return remains;
        } else if (this.contains(prev)) {
          if (prev.min > this.min && prev.max == this.max) {
            TimeRange r = new TimeRange(this.min, prev.min);
            r.setLeftClose(this.leftClose);
            r.setRightClose(false);
            remains.add(r);
            return remains; // because timeRangesPrev is sorted
          } else if (prev.min == this.min) { // && prev.max < this.max
            min = prev.max;
            leftClose = false;
          } else {
            TimeRange r = new TimeRange(this.min, prev.min);
            r.setLeftClose(this.leftClose);
            r.setRightClose(false);
            remains.add(r);

            min = prev.max;
            leftClose = false;
          }
        } else { // intersect without one containing the other
          if (prev.min < this.min) {
            min = prev.max;
            leftClose = false;
          } else {
            TimeRange r = new TimeRange(this.min, prev.min);
            r.setLeftClose(this.leftClose);
            r.setRightClose(false);
            remains.add(r);
            return remains;
          }
        }
      }
    }

    TimeRange r = new TimeRange(this.min, this.max);
    r.setLeftClose(this.leftClose);
    r.setRightClose(this.rightClose);
    remains.add(r);

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
