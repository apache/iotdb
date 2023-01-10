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
package org.apache.iotdb.db.utils.datastructure;

import org.apache.iotdb.db.rescon.PrimitiveArrayManager;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

import java.util.ArrayList;
import java.util.List;

import static org.apache.iotdb.db.rescon.PrimitiveArrayManager.ARRAY_SIZE;

public class BackIntTVList extends QuickIntTVList implements BackwardSort {

  private final List<long[]> tmpTimestamps = new ArrayList<>();
  private final List<int[]> tmpValues = new ArrayList<>();
  private int tmpLength = 0;

  @Override
  public void sort() {
    if (!sorted) {
      backwardSort(timestamps, rowCount);
      clearTmp();
    }
    sorted = true;
  }

  @Override
  public void setFromTmp(int src, int dest) {
    set(
        dest,
        tmpTimestamps.get(src / ARRAY_SIZE)[src % ARRAY_SIZE],
        tmpValues.get(src / ARRAY_SIZE)[src % ARRAY_SIZE]);
  }

  @Override
  public void setToTmp(int src, int dest) {
    tmpTimestamps.get(dest / ARRAY_SIZE)[dest % ARRAY_SIZE] = getTime(src);
    tmpValues.get(dest / ARRAY_SIZE)[dest % ARRAY_SIZE] = getInt(src);
  }

  @Override
  public void backward_set(int src, int dest) {
    set(src, dest);
  }

  @Override
  public int compareTmp(int idx, int tmpIdx) {
    long t1 = getTime(idx);
    long t2 = tmpTimestamps.get(tmpIdx / ARRAY_SIZE)[tmpIdx % ARRAY_SIZE];
    return Long.compare(t1, t2);
  }

  @Override
  public void checkTmpLength(int len) {
    while (len > tmpLength) {
      tmpTimestamps.add((long[]) getPrimitiveArraysByType(TSDataType.INT64));
      tmpValues.add((int[]) getPrimitiveArraysByType(TSDataType.INT32));
      tmpLength += ARRAY_SIZE;
    }
  }

  @Override
  public void clearTmp() {
    for (long[] dataArray : tmpTimestamps) {
      PrimitiveArrayManager.release(dataArray);
    }
    tmpTimestamps.clear();
    for (int[] dataArray : tmpValues) {
      PrimitiveArrayManager.release(dataArray);
    }
    tmpValues.clear();
  }
}
