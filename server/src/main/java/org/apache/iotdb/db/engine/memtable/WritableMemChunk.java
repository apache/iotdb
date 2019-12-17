/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iotdb.db.engine.memtable;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ConcurrentNavigableMap;
import org.apache.iotdb.db.utils.TimeValuePair;
import org.apache.iotdb.db.utils.TsPrimitiveType;
import org.apache.iotdb.db.utils.datastructure.TVSkipListMap;
import org.apache.iotdb.tsfile.exception.write.UnSupportedDataTypeException;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.utils.Binary;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WritableMemChunk implements IWritableMemChunk {

  private static final Logger logger = LoggerFactory.getLogger(WritableMemChunk.class);
  private TSDataType dataType;
  private TVSkipListMap<Long, TimeValuePair> list;

  public WritableMemChunk(TSDataType dataType, TVSkipListMap<Long, TimeValuePair> list) {
    this.dataType = dataType;
    this.list = list;
  }

  @Override
  public void write(long insertTime, Object objectValue) {
    put(insertTime, objectValue);
  }

  @Override
  public void write(long[] times, Object valueList, TSDataType dataType, List<Integer> indexes) {
    switch (dataType) {
      case BOOLEAN:
        boolean[] boolValues = (boolean[]) valueList;
        if (times.length == indexes.size()) {
          putBooleans(times, boolValues);
          break;
        }
        for (Integer index : indexes) {
          put(times[index], boolValues[index]);
        }
        break;
      case INT32:
        int[] intValues = (int[]) valueList;
        if (times.length == indexes.size()) {
          putInts(times, intValues);
          break;
        }
        for (Integer index : indexes) {
          put(times[index], intValues[index]);
        }
        break;
      case INT64:
        long[] longValues = (long[]) valueList;
        if (times.length == indexes.size()) {
          putLongs(times, longValues);
          break;
        }
        for (Integer index : indexes) {
          put(times[index], longValues[index]);
        }
        break;
      case FLOAT:
        float[] floatValues = (float[]) valueList;
        if (times.length == indexes.size()) {
          putFloats(times, floatValues);
          break;
        }
        for (Integer index : indexes) {
          put(times[index], floatValues[index]);
        }
        break;
      case DOUBLE:
        double[] doubleValues = (double[]) valueList;
        if (times.length == indexes.size()) {
          putDoubles(times, doubleValues);
          break;
        }
        for (Integer index : indexes) {
          put(times[index], doubleValues[index]);
        }
        break;
      case TEXT:
        Binary[] binaryValues = (Binary[]) valueList;
        if (times.length == indexes.size()) {
          putBinaries(times, binaryValues);
          break;
        }
        for (Integer index : indexes) {
          put(times[index], binaryValues[index]);
        }
        break;
      default:
        throw new UnSupportedDataTypeException("Unsupported data type:" + dataType);
    }
  }


  @Override
  public void put(long t, Object v) {
    list.put(t, new TimeValuePair(t, TsPrimitiveType.getByType(dataType, v)));
  }


  @Override
  public void putLongs(long[] t, long[] v) {
    for (int i = 0; i < t.length; i++) {
      put(t[i], v[i]);
    }
  }

  @Override
  public void putInts(long[] t, int[] v) {
    for (int i = 0; i < t.length; i++) {
      put(t[i], v[i]);
    }
  }

  @Override
  public void putFloats(long[] t, float[] v) {
    for (int i = 0; i < t.length; i++) {
      put(t[i], v[i]);
    }
  }

  @Override
  public void putDoubles(long[] t, double[] v) {
    for (int i = 0; i < t.length; i++) {
      put(t[i], v[i]);
    }
  }

  @Override
  public void putBinaries(long[] t, Binary[] v) {
    for (int i = 0; i < t.length; i++) {
      put(t[i], v[i]);
    }
  }

  @Override
  public void putBooleans(long[] t, boolean[] v) {
    for (int i = 0; i < t.length; i++) {
      put(t[i], v[i]);
    }
  }

  @Override
  public long count() {
    return list.size();
  }

  @Override
  public TSDataType getType() {
    return dataType;
  }

  @Override
  public void setTimeOffset(long offset) {
    list.setTimeOffset(offset);
  }

  @Override
  public TVSkipListMap<Long, TimeValuePair> getDataList() {
    return list;
  }

  @Override
  public TVSkipListMap<Long, TimeValuePair> getCloneList() {
    TVSkipListMap<Long, TimeValuePair> clone = new TVSkipListMap<>(
        list.comparator());
    clone.putAll(list);
    return clone;
  }

  @Override
  public synchronized List<TimeValuePair> getSortedTimeValuePairList() {
    List<TimeValuePair> sortedList = new ArrayList();
    Iterator<Long> iterator = list.keySet().iterator();
    while (iterator.hasNext()) {
      long time = iterator.next();
      if (time < list.getTimeOffset()) {
        continue;
      }
      sortedList.add(list.get(time));
    }
    return sortedList;
  }

  @Override
  public boolean isEmpty() {
    return list.size() == 0;
  }

  @Override
  public long getMinTime() {
    return list.firstKey();
  }

  @Override
  public void delete(long upperBound) {
    ConcurrentNavigableMap<Long, TimeValuePair> values = list
        .headMap(upperBound, true);

    Iterator<Long> iterator = values.keySet().iterator();
    while (iterator.hasNext()) {
      list.remove(iterator.next());
    }
  }
}
