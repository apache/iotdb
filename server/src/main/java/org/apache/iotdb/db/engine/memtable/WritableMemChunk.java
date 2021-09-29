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

import org.apache.iotdb.db.utils.datastructure.TVList;
import org.apache.iotdb.tsfile.exception.write.UnSupportedDataTypeException;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.utils.Binary;
import org.apache.iotdb.tsfile.utils.BitMap;
import org.apache.iotdb.tsfile.write.schema.IMeasurementSchema;

import java.util.List;

public class WritableMemChunk implements IWritableMemChunk {

  private IMeasurementSchema schema;
  private TVList list;
  private static final String UNSUPPORTED_TYPE = "Unsupported data type:";

  public WritableMemChunk(IMeasurementSchema schema, TVList list) {
    this.schema = schema;
    this.list = list;
  }

  @Override
  public void write(long insertTime, Object objectValue) {
    switch (schema.getType()) {
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
      case VECTOR:
        putVector(insertTime, (Object[]) objectValue);
        break;
      default:
        throw new UnSupportedDataTypeException(UNSUPPORTED_TYPE + schema.getType());
    }
  }

  @Override
  public void write(
      long[] times, Object bitMap, Object valueList, TSDataType dataType, int start, int end) {
    switch (dataType) {
      case BOOLEAN:
        boolean[] boolValues = (boolean[]) valueList;
        putBooleans(times, (BitMap) bitMap, boolValues, start, end);
        break;
      case INT32:
        int[] intValues = (int[]) valueList;
        putInts(times, (BitMap) bitMap, intValues, start, end);
        break;
      case INT64:
        long[] longValues = (long[]) valueList;
        putLongs(times, (BitMap) bitMap, longValues, start, end);
        break;
      case FLOAT:
        float[] floatValues = (float[]) valueList;
        putFloats(times, (BitMap) bitMap, floatValues, start, end);
        break;
      case DOUBLE:
        double[] doubleValues = (double[]) valueList;
        putDoubles(times, (BitMap) bitMap, doubleValues, start, end);
        break;
      case TEXT:
        Binary[] binaryValues = (Binary[]) valueList;
        putBinaries(times, (BitMap) bitMap, binaryValues, start, end);
        break;
      case VECTOR:
        Object[] vectorValues = (Object[]) valueList;
        putVectors(times, (BitMap[]) bitMap, vectorValues, start, end);
        break;
      default:
        throw new UnSupportedDataTypeException(UNSUPPORTED_TYPE + dataType);
    }
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
  public void putVector(long t, Object[] v) {
    list.putVector(t, v);
  }

  @Override
  public void putLongs(long[] t, BitMap bitMap, long[] v, int start, int end) {
    list.putLongs(t, v, start, end);
  }

  @Override
  public void putInts(long[] t, BitMap bitMap, int[] v, int start, int end) {
    list.putInts(t, v, start, end);
  }

  @Override
  public void putFloats(long[] t, BitMap bitMap, float[] v, int start, int end) {
    list.putFloats(t, v, start, end);
  }

  @Override
  public void putDoubles(long[] t, BitMap bitMap, double[] v, int start, int end) {
    list.putDoubles(t, v, start, end);
  }

  @Override
  public void putBinaries(long[] t, BitMap bitMap, Binary[] v, int start, int end) {
    list.putBinaries(t, v, start, end);
  }

  @Override
  public void putBooleans(long[] t, BitMap bitMap, boolean[] v, int start, int end) {
    list.putBooleans(t, v, start, end);
  }

  @Override
  public void putVectors(long[] t, BitMap[] bitMaps, Object[] v, int start, int end) {
    list.putVectors(t, bitMaps, v, start, end);
  }

  @Override
  public synchronized TVList getSortedTvListForQuery() {
    sortTVList();
    // increase reference count
    list.increaseReferenceCount();
    return list;
  }

  @Override
  public synchronized TVList getSortedTvListForQuery(List<Integer> columnIndexList) {
    if (list.getDataType() != TSDataType.VECTOR) {
      throw new UnSupportedDataTypeException(UNSUPPORTED_TYPE + list.getDataType());
    }
    sortTVList();
    // increase reference count
    list.increaseReferenceCount();
    return list.getTvListByColumnIndex(columnIndexList);
  }

  private void sortTVList() {
    // check reference count
    if ((list.getReferenceCount() > 0 && !list.isSorted())) {
      list = list.clone();
    }

    if (!list.isSorted()) {
      list.sort();
    }
  }

  @Override
  public synchronized TVList getSortedTvListForFlush() {
    sortTVList();
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
  public IMeasurementSchema getSchema() {
    return schema;
  }

  @Override
  public long getMinTime() {
    return list.getMinTime();
  }

  public Long getFirstPoint() {
    if (list.size() == 0) return Long.MAX_VALUE;
    return getSortedTvListForQuery().getTimeValuePair(0).getTimestamp();
  }

  public Long getLastPoint() {
    if (list.size() == 0) return Long.MIN_VALUE;
    return getSortedTvListForQuery()
        .getTimeValuePair(getSortedTvListForQuery().size() - 1)
        .getTimestamp();
  }

  @Override
  public int delete(long lowerBound, long upperBound) {
    return list.delete(lowerBound, upperBound);
  }

  // TODO: THIS METHOLD IS FOR DELETING ONE COLUMN OF A VECTOR
  @Override
  public int delete(long lowerBound, long upperBound, int columnIndex) {
    return list.delete(lowerBound, upperBound, columnIndex);
  }

  @Override
  public String toString() {
    int size = list.size();
    int firstIndex = 0;
    int lastIndex = size - 1;
    long minTime = Long.MAX_VALUE;
    long maxTime = Long.MIN_VALUE;
    for (int i = 0; i < size; i++) {
      long currentTime = list.getTime(i);
      if (currentTime < minTime) {
        firstIndex = i;
        minTime = currentTime;
      }
      if (currentTime >= maxTime) {
        lastIndex = i;
        maxTime = currentTime;
      }
    }

    StringBuilder out = new StringBuilder("MemChunk Size: " + size + System.lineSeparator());
    if (size != 0) {
      out.append("Data type:").append(schema.getType()).append(System.lineSeparator());
      out.append("First point:")
          .append(list.getTimeValuePair(firstIndex))
          .append(System.lineSeparator());
      out.append("Last point:")
          .append(list.getTimeValuePair(lastIndex))
          .append(System.lineSeparator());
    }
    return out.toString();
  }
}
