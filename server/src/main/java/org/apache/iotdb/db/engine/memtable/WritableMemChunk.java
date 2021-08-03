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
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;

public class WritableMemChunk implements IWritableMemChunk {

  private MeasurementSchema schema;
  private TVList list;

  public WritableMemChunk(MeasurementSchema schema, TVList list) {
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
      default:
        throw new UnSupportedDataTypeException("Unsupported data type:" + schema.getType());
    }
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
  public synchronized TVList getSortedTVListForQuery() {
    sortTVList();
    // increase reference count
    list.increaseReferenceCount();
    return list;
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
  public synchronized TVList getSortedTVListForFlush() {
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
  public MeasurementSchema getSchema() {
    return schema;
  }

  @Override
  public long getMinTime() {
    return list.getMinTime();
  }

  @Override
  public int delete(long lowerBound, long upperBound) {
    return list.delete(lowerBound, upperBound);
  }

  @Override
  public String toString() {
    int size = getSortedTVListForQuery().size();
    StringBuilder out = new StringBuilder("MemChunk Size: " + size + System.lineSeparator());
    if (size != 0) {
      out.append("Data type:").append(schema.getType()).append(System.lineSeparator());
      out.append("First point:")
          .append(getSortedTVListForQuery().getTimeValuePair(0))
          .append(System.lineSeparator());
      out.append("Last point:")
          .append(getSortedTVListForQuery().getTimeValuePair(size - 1))
          .append(System.lineSeparator());
    }
    return out.toString();
  }
}
