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


import java.nio.ReadOnlyBufferException;
import java.util.ArrayList;
import org.apache.iotdb.tsfile.common.conf.TSFileConfig;

public class TimeColumn {

  private static final int capacityThreshold = TSFileConfig.DYNAMIC_DATA_SIZE;
  private int capacity = 16;

  // outer list index for read
  private int readCurListIndex;
  // inner array index for read
  private int readCurArrayIndex;

  // outer list index for write
  private int writeCurListIndex;
  // inner array index for write
  private int writeCurArrayIndex;

  // the insert timestamp number of timeRet
  private int count;

  private ArrayList<long[]> timeRet;

  public TimeColumn() {
    this.readCurListIndex = 0;
    this.readCurArrayIndex = 0;
    this.writeCurListIndex = 0;
    this.writeCurArrayIndex = 0;
    timeRet = new ArrayList<>();
    timeRet.add(new long[capacity]);
    count = 0;
  }

  public TimeColumn(ArrayList<long[]> timeRet, int count, int capacity) {
    this.count = count;
    this.capacity = capacity;
    this.readCurListIndex = 0;
    this.readCurArrayIndex = 0;

    this.writeCurListIndex = count / capacity;
    this.writeCurArrayIndex = count % capacity;
    this.timeRet = timeRet;
  }

  public void add(long time) {
    if (writeCurArrayIndex == capacity) {
      if (capacity >= capacityThreshold) {
        timeRet.add(new long[capacity]);
        writeCurListIndex++;
        writeCurArrayIndex = 0;
      } else {
        int newCapacity = capacity << 1;

        long[] newTimeData = new long[newCapacity];
        System.arraycopy(timeRet.get(0), 0, newTimeData, 0, capacity);
        timeRet.set(0, newTimeData);

        capacity = newCapacity;
      }
    }
    timeRet.get(writeCurListIndex)[writeCurArrayIndex] = time;
    writeCurArrayIndex++;
    count++;
  }

  public boolean hasCurrent() {
    if (readCurListIndex < writeCurListIndex) {
      return readCurArrayIndex < capacity;
    } else if (readCurListIndex == writeCurListIndex) {
      return readCurArrayIndex < writeCurArrayIndex;
    } else {
      return false;
    }
  }

  public long currentTime() {
    return this.timeRet.get(readCurListIndex)[readCurArrayIndex];
  }

  public void next() {
    readCurArrayIndex++;
    if (readCurArrayIndex == capacity) {
      readCurArrayIndex = 0;
      readCurListIndex++;
    }
  }

  public int size() {
    return this.count;
  }

  public TimeColumnR asReadOnlyTimeColumn() {
    return new TimeColumnR(timeRet, count, capacity);
  }

  private class TimeColumnR extends TimeColumn {

    public TimeColumnR(ArrayList<long[]> timeRet, int count, int capacity) {
      super(timeRet, count, capacity);
    }

    @Override
    public void add(long time) {
      throw new ReadOnlyBufferException();
    }
  }
}
