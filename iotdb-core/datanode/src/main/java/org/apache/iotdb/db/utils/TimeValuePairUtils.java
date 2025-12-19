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
package org.apache.iotdb.db.utils;

import org.apache.tsfile.common.conf.TSFileConfig;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.TimeValuePair;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.utils.TsPrimitiveType.TsBinary;
import org.apache.tsfile.utils.TsPrimitiveType.TsBoolean;
import org.apache.tsfile.utils.TsPrimitiveType.TsDouble;
import org.apache.tsfile.utils.TsPrimitiveType.TsFloat;
import org.apache.tsfile.utils.TsPrimitiveType.TsInt;
import org.apache.tsfile.utils.TsPrimitiveType.TsLong;
import org.apache.tsfile.write.UnSupportedDataTypeException;

import java.util.ArrayList;

public class TimeValuePairUtils {

  private TimeValuePairUtils() {}

  /**
   * get given data's current (time,value) pair.
   *
   * @param data -batch data
   * @return -given data's (time,value) pair
   */
  public static void setTimeValuePair(TimeValuePair from, TimeValuePair to) {
    to.setTimestamp(from.getTimestamp());
    switch (from.getValue().getDataType()) {
      case INT32:
      case DATE:
        to.getValue().setInt(from.getValue().getInt());
        break;
      case INT64:
      case TIMESTAMP:
        to.getValue().setLong(from.getValue().getLong());
        break;
      case FLOAT:
        to.getValue().setFloat(from.getValue().getFloat());
        break;
      case DOUBLE:
        to.getValue().setDouble(from.getValue().getDouble());
        break;
      case TEXT:
      case BLOB:
      case STRING:
      case OBJECT:
        to.getValue().setBinary(from.getValue().getBinary());
        break;
      case BOOLEAN:
        to.getValue().setBoolean(from.getValue().getBoolean());
        break;
      default:
        throw new UnSupportedDataTypeException(String.valueOf(from.getValue().getDataType()));
    }
  }

  public static TimeValuePair getEmptyTimeValuePair(TSDataType dataType) {
    switch (dataType) {
      case FLOAT:
        return new TimeValuePair(0, new TsFloat(0.0f));
      case INT32:
      case DATE:
        return new TimeValuePair(0, new TsInt(0));
      case INT64:
      case TIMESTAMP:
        return new TimeValuePair(0, new TsLong(0));
      case BOOLEAN:
        return new TimeValuePair(0, new TsBoolean(false));
      case DOUBLE:
        return new TimeValuePair(0, new TsDouble(0.0));
      case TEXT:
      case BLOB:
      case STRING:
      case OBJECT:
        return new TimeValuePair(0, new TsBinary(new Binary("", TSFileConfig.STRING_CHARSET)));
      default:
        throw new UnsupportedOperationException("Unrecognized datatype: " + dataType);
    }
  }

  /** All intervals are closed. */
  public static class Intervals extends ArrayList<Long> {

    static final Intervals ALL_INTERVAL = new Intervals(Long.MIN_VALUE, Long.MAX_VALUE);

    public Intervals() {
      super();
    }

    Intervals(long lowerBound, long upperBound) {
      super();
      addInterval(lowerBound, upperBound);
    }

    public int getIntervalSize() {
      return size() / 2;
    }

    public long getLowerBound(int index) {
      return get(index * 2);
    }

    public long getUpperBound(int index) {
      return get(index * 2 + 1);
    }

    void setLowerBound(int index, long lb) {
      set(index * 2, lb);
    }

    void setUpperBound(int index, long ub) {
      set(index * 2 + 1, ub);
    }

    public void addInterval(long lowerBound, long upperBound) {
      add(lowerBound);
      add(upperBound);
    }

    Intervals intersection(Intervals that) {
      Intervals result = new Intervals();
      int thisSize = this.getIntervalSize();
      int thatSize = that.getIntervalSize();
      for (int i = 0; i < thisSize; i++) {
        for (int j = 0; j < thatSize; j++) {
          long thisLB = this.getLowerBound(i);
          long thisUB = this.getUpperBound(i);
          long thatLB = that.getLowerBound(i);
          long thatUB = that.getUpperBound(i);
          if (thisUB >= thatLB) {
            if (thisUB <= thatUB) {
              result.addInterval(Math.max(thisLB, thatLB), thisUB);
            } else if (thisLB <= thatUB) {
              result.addInterval(Math.max(thisLB, thatLB), thatUB);
            }
          }
        }
      }
      return result;
    }

    /**
     * The union is implemented by merge, so the two intervals must be ordered.
     *
     * @param that
     * @return
     */
    Intervals union(Intervals that) {
      if (this.isEmpty()) {
        return that;
      } else if (that.isEmpty()) {
        return this;
      }
      Intervals result = new Intervals();

      int thisSize = this.getIntervalSize();
      int thatSize = that.getIntervalSize();
      int thisIndex = 0;
      int thatIndex = 0;
      // merge the heads of the two intervals
      while (thisIndex < thisSize && thatIndex < thatSize) {
        long thisLB = this.getLowerBound(thisIndex);
        long thisUB = this.getUpperBound(thisIndex);
        long thatLB = that.getLowerBound(thatIndex);
        long thatUB = that.getUpperBound(thatIndex);
        if (thisLB <= thatLB) {
          result.mergeLast(thisLB, thisUB);
          thisIndex++;
        } else {
          result.mergeLast(thatLB, thatUB);
          thatIndex++;
        }
      }
      // merge the remaining intervals
      Intervals remainingIntervals = thisIndex < thisSize ? this : that;
      int remainingIndex = thisIndex < thisSize ? thisIndex : thatIndex;
      mergeRemainingIntervals(remainingIndex, remainingIntervals, result);

      return result;
    }

    private void mergeRemainingIntervals(
        int remainingIndex, Intervals remainingIntervals, Intervals result) {
      for (int i = remainingIndex; i < remainingIntervals.getIntervalSize(); i++) {
        long lb = remainingIntervals.getLowerBound(i);
        long ub = remainingIntervals.getUpperBound(i);
        result.mergeLast(lb, ub);
      }
    }

    /**
     * Merge an interval of [lowerBound, upperBound] with the last interval if they can be merged,
     * or just add it as the last interval if its lowerBound is larger than the upperBound of the
     * last interval. If the upperBound of the new interval is less than the lowerBound of the last
     * interval, nothing will be done.
     *
     * @param lowerBound
     * @param upperBound
     */
    private void mergeLast(long lowerBound, long upperBound) {
      if (getIntervalSize() == 0) {
        addInterval(lowerBound, upperBound);
        return;
      }
      int lastIndex = getIntervalSize() - 1;
      long lastLB = getLowerBound(lastIndex);
      long lastUB = getUpperBound(lastIndex);
      if (lowerBound > lastUB + 1) {
        // e.g., last [3, 5], new [7, 10], just add the new interval
        addInterval(lowerBound, upperBound);
        return;
      }
      if (upperBound < lastLB - 1) {
        // e.g., last [7, 10], new [3, 5], do nothing
        return;
      }
      // merge the new interval into the last one
      setLowerBound(lastIndex, Math.min(lastLB, lowerBound));
      setUpperBound(lastIndex, Math.max(lastUB, upperBound));
    }

    public Intervals not() {
      if (isEmpty()) {
        return ALL_INTERVAL;
      }
      Intervals result = new Intervals();
      long firstLB = getLowerBound(0);
      if (firstLB != Long.MIN_VALUE) {
        result.addInterval(Long.MIN_VALUE, firstLB - 1);
      }

      int intervalSize = getIntervalSize();
      for (int i = 0; i < intervalSize - 1; i++) {
        long currentUB = getUpperBound(i);
        long nextLB = getLowerBound(i + 1);
        if (currentUB + 1 <= nextLB - 1) {
          result.addInterval(currentUB + 1, nextLB - 1);
        }
      }

      long lastUB = getUpperBound(result.getIntervalSize() - 1);
      if (lastUB != Long.MAX_VALUE) {
        result.addInterval(lastUB + 1, Long.MAX_VALUE);
      }
      return result;
    }
  }
}
