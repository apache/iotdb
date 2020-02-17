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
import java.util.List;
import org.apache.iotdb.db.utils.TimeValuePair;
import org.apache.iotdb.tsfile.utils.TsPrimitiveType.TsBinary;
import org.apache.iotdb.tsfile.utils.TsPrimitiveType.TsBoolean;
import org.apache.iotdb.tsfile.utils.TsPrimitiveType.TsDouble;
import org.apache.iotdb.tsfile.utils.TsPrimitiveType.TsFloat;
import org.apache.iotdb.tsfile.utils.TsPrimitiveType.TsInt;
import org.apache.iotdb.tsfile.utils.TsPrimitiveType.TsLong;
import org.apache.iotdb.db.utils.datastructure.TVList;
import org.apache.iotdb.tsfile.exception.write.UnSupportedDataTypeException;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.utils.Binary;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WritableMemChunk implements IWritableMemChunk {

  private static final Logger logger = LoggerFactory.getLogger(WritableMemChunk.class);
  private TSDataType dataType;
  private TVList list;
  private List<TimeValuePair> sortedList;

  public WritableMemChunk(TSDataType dataType, TVList list) {
    this.dataType = dataType;
    this.list = list;
  }

  @Override
  public void write(long insertTime, Object objectValue) {
    switch (dataType) {
      case BOOLEAN:
        putBoolean(insertTime, (boolean) objectValue);
        break;
      case INT32:
        putInt(insertTime, (int) objectValue);
        break;
      case INT64:
        putLong(insertTime, (long) objectValue);
        break;
      case FLOAT:
        putFloat(insertTime, (float) objectValue);
        break;
      case DOUBLE:
        putDouble(insertTime, (double) objectValue);
        break;
      case TEXT:
        putBinary(insertTime, (Binary) objectValue);
        break;
      default:
        throw new UnSupportedDataTypeException("Unsupported data type:" + dataType);
    }
    sortedList = null;
  }

  @Override
  public void write(long[] times, Object valueList, TSDataType dataType, int start, int end) {
    switch (dataType) {
      case BOOLEAN:
        boolean[] boolValues = (boolean[]) valueList;
        putBooleans(times, boolValues, start, end);
        break;
      case INT32:
        int[] intValues = (int[]) valueList;
        putInts(times, intValues, start, end);
        break;
      case INT64:
        long[] longValues = (long[]) valueList;
        putLongs(times, longValues, start, end);
        break;
      case FLOAT:
        float[] floatValues = (float[]) valueList;
        putFloats(times, floatValues, start, end);
        break;
      case DOUBLE:
        double[] doubleValues = (double[]) valueList;
        putDoubles(times, doubleValues, start, end);
        break;
      case TEXT:
        Binary[] binaryValues = (Binary[]) valueList;
        putBinaries(times, binaryValues, start, end);
        break;
      default:
        throw new UnSupportedDataTypeException("Unsupported data type:" + dataType);
    }
    sortedList = null;
  }


  @Override
  public void putLong(long t, long v) {
    list.putLong(t, v);
  }

  @Override
  public void putInt(long t, int v) {
    list.putInt(t, v);
  }

  @Override
  public void putFloat(long t, float v) {
    list.putFloat(t, v);
  }

  @Override
  public void putDouble(long t, double v) {
    list.putDouble(t, v);
  }

  @Override
  public void putBinary(long t, Binary v) {
    list.putBinary(t, v);
  }

  @Override
  public void putBoolean(long t, boolean v) {
    list.putBoolean(t, v);
  }

  @Override
  public void putLongs(long[] t, long[] v) {
    list.putLongs(t, v);
  }

  @Override
  public void putInts(long[] t, int[] v) {
    list.putInts(t, v);
  }

  @Override
  public void putFloats(long[] t, float[] v) {
    list.putFloats(t, v);
  }

  @Override
  public void putDoubles(long[] t, double[] v) {
    list.putDoubles(t, v);
  }

  @Override
  public void putBinaries(long[] t, Binary[] v) {
    list.putBinaries(t, v);
  }

  @Override
  public void putBooleans(long[] t, boolean[] v) {
    list.putBooleans(t, v);
  }

  @Override
  public void putLongs(long[] t, long[] v, int start, int end) {
    list.putLongs(t, v, start, end);
  }

  @Override
  public void putInts(long[] t, int[] v, int start, int end) {
    list.putInts(t, v, start, end);
  }

  @Override
  public void putFloats(long[] t, float[] v, int start, int end) {
    list.putFloats(t, v, start, end);
  }

  @Override
  public void putDoubles(long[] t, double[] v, int start, int end) {
    list.putDoubles(t, v, start, end);
  }

  @Override
  public void putBinaries(long[] t, Binary[] v, int start, int end) {
    list.putBinaries(t, v, start, end);
  }

  @Override
  public void putBooleans(long[] t, boolean[] v, int start, int end) {
    list.putBooleans(t, v, start, end);
  }

  @Override
  public synchronized TVList getSortedTVList() {
    list.sort();
    return list;
  }

  @Override
  public TVList getTVList() {
    return list;
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
  public synchronized List<TimeValuePair> getSortedTimeValuePairList() {
    if (sortedList != null) {
      return sortedList;
    }
    sortedList = new ArrayList<>();
    list.sort();
    for (int i = 0; i < list.size(); i++) {
      long time = list.getTime(i);
      if (time < list.getTimeOffset() ||
          (i + 1 < list.size() && (time == list.getTime(i + 1)))) {
        continue;
      }
      switch (dataType) {
        case BOOLEAN:
          sortedList.add(new TimeValuePair(time, new TsBoolean(list.getBoolean(i))));
          break;
        case INT32:
          sortedList.add(new TimeValuePair(time, new TsInt(list.getInt(i))));
          break;
        case INT64:
          sortedList.add(new TimeValuePair(time, new TsLong(list.getLong(i))));
          break;
        case FLOAT:
          sortedList.add(new TimeValuePair(time, new TsFloat(list.getFloat(i))));
          break;
        case DOUBLE:
          sortedList.add(new TimeValuePair(time, new TsDouble(list.getDouble(i))));
          break;
        case TEXT:
          sortedList.add(new TimeValuePair(time, new TsBinary(list.getBinary(i))));
          break;
        default:
          logger.error("Unsupported data type: {}", dataType);
          break;
      }
    }
    return this.sortedList;
  }

  @Override
  public boolean isEmpty() {
    return list.size() == 0;
  }

  @Override
  public long getMinTime() {
    return list.getMinTime();
  }

  @Override
  public void delete(long upperBound) {
    list.delete(upperBound);
  }

  @Override
  public String toString() {
    int size = getSortedTimeValuePairList().size();
    StringBuilder out = new StringBuilder("MemChunk Size: " + size + System.lineSeparator());
    if (size != 0) {
      out.append("Data type:").append(dataType).append(System.lineSeparator());
      out.append("First value:").append(getSortedTimeValuePairList().get(0))
          .append(System.lineSeparator());
      out.append("Last value:").append(getSortedTimeValuePairList().get(size - 1))
          .append(System.lineSeparator());
      ;
    }
    return out.toString();
  }
}
