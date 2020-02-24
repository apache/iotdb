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


public class TimeColumn {

  private static final int DEFAULT_INIT_SIZE = 1000;


  private long[] times;

  private int size;

  private int cur;

  public TimeColumn() {
    this(DEFAULT_INIT_SIZE);
  }

  public TimeColumn(int initSize) {
    times = new long[initSize];
  }


  public TimeColumn(long[] times) {
    this.times = times;
  }

  public void add(long time) {
    if (size == times.length) {
      long[] newArray = new long[times.length * 2];
      System.arraycopy(times, 0, newArray, 0, times.length);
      times = newArray;
    }
    times[size++] = time;
  }

  public long[] getTimes() {
    return times;
  }

  public boolean hasCurrent() {
    return size > 0 && cur < size;
  }

  public long currentTime() {
    return times[cur];
  }

  public void next() {
    cur++;
  }

  public long getLastTime() {
    return times[size - 1];
  }

  public int size() {
    return size;
  }
}
