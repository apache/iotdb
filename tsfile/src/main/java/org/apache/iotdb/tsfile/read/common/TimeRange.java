package org.apache.iotdb.tsfile.read.common;

import java.util.ArrayList;
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
   * @param min
   * @param max
   */
  public TimeRange(long min, long max) {
    set(min, max);
  }

  public int compareTo(TimeRange r) {
    long res = this.min - r.min;
    if (res > 0) {
      return 1;
    } else if (res == -1) {
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
   * @return true if the value lies between the min and max values, inclusively
   */
  public boolean contains(long value) {
    return min <= value && max >= value;
  }

  /**
   * @return true if the given range lies in this range, inclusively
   */
  public boolean contains(TimeRange r) {
    return min <= r.min && max >= r.max;
  }

  /**
   * @param min
   * @param max
   */
  public void set(long min, long max) {
    this.min = min;
    this.max = max;

    sort();
  }

  /**
   * @param r
   */
  public void set(TimeRange r) {
    set(r.getMin(), r.getMax());
  }

  /**
   * @return The difference between min and max
   */
  public float getSpan() {
    return max - min;
  }

  /**
   * @return The lower range boundary
   */
  public long getMin() {
    return min;
  }

  /**
   * @param min
   */
  public void setMin(long min) {
    this.min = min;

    sort();
  }

  /**
   * @return The upper range boundary
   */
  public long getMax() {
    return max;
  }

  /**
   * @param max
   */
  public void setMax(long max) {
    this.max = max;

    sort();
  }

  private void sort() {
    if (min > max) {
      long t = min;
      min = max;
      max = t;
    }
  }

  /**
   * @return <code>true</code> if the intersection exists
   */
  public boolean intersection(TimeRange r, TimeRange dest) {
    if (intersects(r)) {
      dest.set(Math.max(min, r.min), Math.min(max, r.max));
      return true;
    }

    return false;
  }

  /**
   * @return The size of the intersection of the two ranges
   */
  public static long intersection(long minA, long maxA, long minB,
                                  long maxB) {
    long highMin = Math.max(minA, minB);
    long lowMax = Math.min(maxA, maxB);

    if (lowMax > highMin) {
      return lowMax - highMin;
    }

    return 0;
  }

  /**
   * @return <code>true</code> if the ranges have values in common
   */
  public boolean intersects(TimeRange r) {
    return overlaps(min, max, r.min, r.max);
  }


  @Override
  public String toString() {
    return "[ " + min + " : " + max + " ]";
  }

  /**
   * Limits a value to lie within some range
   *
   * @return min if v < min, max if v > max, otherwise v
   */
  public static long limit(long v, long min, long max) {
    if (v < min) {
      v = min;
    } else if (v > max) {
      v = max;
    }

    return v;
  }


  /**
   * @return <code>true</code> if the ranges overlap
   */
  public static boolean overlaps(long minA, long maxA, long minB, long maxB) {
    assert minA <= maxA;
    assert minB <= maxB;

    return !(minA > maxB || maxA < minB);
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
   * Get the remaining time ranges in the current ranges but not in timeRangesPrev.
   *
   * NOTE the primitive timeRange is always a closed interval [min,max] and only in this function
   * are leftClose and rightClose changed.
   *
   * @param timeRangesPrev time ranges union in ascending order
   * @return the remaining time ranges
   */
  public ArrayList<TimeRange> getRemains(ArrayList<TimeRange> timeRangesPrev) {
    ArrayList<TimeRange> remains = new ArrayList<>();

    for (TimeRange prev : timeRangesPrev) {
      if (prev.min > max) {
        break; // because timeRangesPrev is sorted
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
    IExpression left, right;
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

    IExpression expression = BinaryExpression.and(left, right);
    return expression;
  }
}
