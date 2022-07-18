/*
 * Copyright © 2021 iotdb-quality developer group (iotdb-quality@protonmail.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.iotdb.library.series.util;

import org.apache.iotdb.library.util.Util;
import org.apache.iotdb.udf.api.access.Row;
import org.apache.iotdb.udf.api.collector.PointCollector;

import org.apache.commons.lang3.tuple.Pair;

import java.io.IOException;
import java.util.ArrayList;

/** Util for ConsecutiveSequences and ConsecutiveWindows */
public class ConsecutiveUtil {
  private static final int maxLen = 128;
  private long first;
  private long last;
  private long gap;
  private int count = 0;
  private final ArrayList<Pair<Long, Boolean>> window = new ArrayList<>(maxLen);

  public ConsecutiveUtil(long first, long last, long gap) {
    this.first = first;
    this.last = last;
    this.gap = gap;
  }

  public ArrayList<Pair<Long, Boolean>> getWindow() {
    return window;
  }

  public long getGap() {
    return gap;
  }

  public void setGap(long gap) {
    this.gap = gap;
  }

  public int getMaxLen() {
    return maxLen;
  }

  public int getCount() {
    return count;
  }

  public void setCount(int count) {
    this.count = count;
  }

  public long getFirst() {
    return first;
  }

  public void setFirst(long first) {
    this.first = first;
  }

  public long getLast() {
    return last;
  }

  /** check Null values */
  public boolean check(Row row) {
    for (int i = 0; i < row.size(); i++) {
      if (row.isNull(i)) {
        return true;
      }
    }
    return false;
  }

  /** calculate standard timestamp gap in given window. */
  public void calculateGap() {
    long[] time = new long[window.size() - 1];
    for (int i = 0; i < time.length; i++) {
      time[i] = window.get(i + 1).getLeft() - window.get(i).getLeft();
    }
    gap = Util.mode(time);
  }

  /** clear data points in the window */
  public void cleanWindow(PointCollector collector) throws IOException {
    if (window.isEmpty()) {
      return;
    }
    first = last = -gap;
    for (Pair<Long, Boolean> p : window) {
      process(p.getLeft(), p.getRight(), collector);
    }
  }

  /** process one row */
  public void process(long time, boolean nullExist, PointCollector collector) throws IOException {
    if (nullExist) { // consecutive subsequence ends with null
      if (count > 1) {
        collector.putInt(first, count);
      }
      first = last = -gap;
      count = 0;
    } else {
      if (time == last + gap) { // correct gap and not null value, subsequence grows
        last = time;
        count++;
      } else { // incorrect gap and not null value, subsequence ends, and new subsequence starts
        if (count > 1) {
          collector.putInt(first, count);
        }
        first = last = time;
        count = 1;
      }
    }
  }
}
