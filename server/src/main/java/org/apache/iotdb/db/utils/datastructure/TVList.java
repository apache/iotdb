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

import static org.apache.iotdb.db.rescon.PrimitiveArrayPool.ARRAY_SIZE;

import java.util.ArrayList;
import java.util.List;
import org.apache.iotdb.db.rescon.PrimitiveArrayPool;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

@SuppressWarnings("unused")
public abstract class TVList extends AbstractTVList {

  protected List<long[]> timestamps;

  public TVList() {
    timestamps = new ArrayList<>();
    size = 0;
    minTime = Long.MIN_VALUE;
  }

  @Override
  public long getTime(int index) {
    if (index >= size) {
      throw new ArrayIndexOutOfBoundsException(index);
    }
    int arrayIndex = index / ARRAY_SIZE;
    int elementIndex = index % ARRAY_SIZE;
    return timestamps.get(arrayIndex)[elementIndex];
  }

  @Override
  protected long getTimeForSort(int index) {
    return getTime(index);
  }

  @Override
  protected void releaseLastTimeArray() {
    PrimitiveArrayPool.getInstance().release(timestamps.remove(timestamps.size() - 1));
  }

  @Override
  public void delete(long upperBound) {
    int newSize = 0;
    minTime = Long.MAX_VALUE;
    for (int i = 0; i < size; i++) {
      long time = getTime(i);
      if (time > upperBound) {
        set(i, newSize++);
        minTime = time < minTime ? time : minTime;
      }
    }
    size = newSize;
    // release primitive arrays that are empty
    int newArrayNum = newSize / ARRAY_SIZE;
    if (newSize % ARRAY_SIZE != 0) {
      newArrayNum ++;
    }
    for (int releaseIdx = newArrayNum; releaseIdx < timestamps.size(); releaseIdx++) {
      releaseLastTimeArray();
      releaseLastValueArray();
    }
  }

  @Override
  protected void cloneAs(AbstractTVList abstractCloneList) {
    TVList cloneList = (TVList) abstractCloneList;
    for (long[] timestampArray : timestamps) {
      cloneList.timestamps.add((long[]) cloneTime(timestampArray));
    }
    cloneList.size = size;
    cloneList.sorted = sorted;
    cloneList.minTime = minTime;
  }

  @Override
  protected void clearTime() {
    if (timestamps != null) {
      for (long[] dataArray : timestamps) {
        PrimitiveArrayPool.getInstance().release(dataArray);
      }
      timestamps.clear();
    }
  }

  @Override
  protected void clearSortedTime() {
    if (sortedTimestamps != null) {
      for (long[] dataArray : sortedTimestamps) {
        PrimitiveArrayPool.getInstance().release(dataArray);
      }
      sortedTimestamps = null;
    }
  }

  @Override
  protected void checkExpansion() {
    if ((size % ARRAY_SIZE) == 0) {
      expandValues();
      timestamps.add((long[]) PrimitiveArrayPool.getInstance().getPrimitiveDataListByType(
          TSDataType.INT64));
    }
  }

  @Override
  protected Object cloneTime(Object object) {
    long[] array = (long[]) object;
    long[] cloneArray = new long[array.length];
    System.arraycopy(array, 0, cloneArray, 0, array.length);
    return cloneArray;
  }

  public static TVList newList(TSDataType dataType) {
    switch (dataType) {
      case TEXT:
        return new BinaryTVList();
      case FLOAT:
        return new FloatTVList();
      case INT32:
        return new IntTVList();
      case INT64:
        return new LongTVList();
      case DOUBLE:
        return new DoubleTVList();
      case BOOLEAN:
        return new BooleanTVList();
    }
    return null;
  }
}
