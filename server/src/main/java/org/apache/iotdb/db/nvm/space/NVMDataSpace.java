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
package org.apache.iotdb.db.nvm.space;

import java.nio.ByteBuffer;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

public class NVMDataSpace extends NVMSpace {

  private final long INVALID_VALUE = 0;

  private int index;
  private TSDataType dataType;
  private int unitSize;
  private int unitNum;

  NVMDataSpace(long offset, long size, ByteBuffer byteBuffer, int index, TSDataType dataType, boolean refresh) {
    super(offset, size, byteBuffer);
    this.index = index;
    this.dataType = dataType;
    unitSize = NVMSpaceManager.getPrimitiveTypeByteSize(dataType);
    unitNum = (int) (size / unitSize);

    if (refresh) {
      refreshData();
    }
  }

  public int getUnitNum() {
    return unitNum;
  }
  
  private void refreshData() {
    for (int i = 0; i < size / NVMSpaceManager.getPrimitiveTypeByteSize(TSDataType.INT64); i++) {
      setData(i, INVALID_VALUE);
    }
  }

  public int getValidUnitNum() {
    // only for time space
    int count = 0;
    while (count < unitNum) {
      long v = (long) getData(count);
      if (v == INVALID_VALUE) {
        break;
      }
      count++;
    }
    return count;
  }

  public Object getData(int index) {
    index *= unitSize;
    Object object = null;
    switch (dataType) {
      case BOOLEAN:
        object = byteToBoolean(byteBuffer.get(index));
        break;
      case INT32:
        object = byteBuffer.getInt(index);
        break;
      case INT64:
        object = byteBuffer.getLong(index);
        break;
      case FLOAT:
        object = byteBuffer.getFloat(index);
        break;
      case DOUBLE:
        object = byteBuffer.getDouble(index);
        break;
      case TEXT:
        // override in NVMBinaryDataSpace
        break;
    }
    return object;
  }

  private boolean byteToBoolean(byte v) {
    return v == 1;
  }

  public void setData(int index, Object object) {
    index *= unitSize;
    switch (dataType) {
      case BOOLEAN:
        byteBuffer.put(index, booleanToByte((boolean) object));
        break;
      case INT32:
        byteBuffer.putInt(index, (int) object);
        break;
      case INT64:
        byteBuffer.putLong(index, (long) object);
        break;
      case FLOAT:
        byteBuffer.putFloat(index, (float) object);
        break;
      case DOUBLE:
        byteBuffer.putDouble(index, (double) object);
        break;
      case TEXT:
        // Override in class NVMBinaryDataSpace, no need to implement here
        break;
    }
  }

  private byte booleanToByte(boolean v) {
    return v ? (byte) 1 : (byte) 0;
  }

  public int getIndex() {
    return index;
  }

  public TSDataType getDataType() {
    return dataType;
  }

  public Object toArray() {
    int arraySize = (int) (size / NVMSpaceManager.getPrimitiveTypeByteSize(dataType));
    switch (dataType) {
      case BOOLEAN:
        boolean[] boolArray = new boolean[arraySize];
        for (int i = 0; i < arraySize; i++) {
          boolArray[i] = (boolean) getData(i);
        }
        return boolArray;
      case INT32:
        int[] intArray = new int[arraySize];
        for (int i = 0; i < arraySize; i++) {
          intArray[i] = (int) getData(i);
        }
        return intArray;
      case INT64:
        long[] longArray = new long[arraySize];
        for (int i = 0; i < arraySize; i++) {
          longArray[i] = (long) getData(i);
        }
        return longArray;
      case FLOAT:
        float[] floatArray = new float[arraySize];
        for (int i = 0; i < arraySize; i++) {
          floatArray[i] = (float) getData(i);
        }
        return floatArray;
      case DOUBLE:
        double[] doubleArray = new double[arraySize];
        for (int i = 0; i < arraySize; i++) {
          doubleArray[i] = (double) getData(i);
        }
        return doubleArray;
      case TEXT:
        // Override in class NVMBinaryDataSpace, no need to implement here
        break;
    }
    return null;
  }
}
