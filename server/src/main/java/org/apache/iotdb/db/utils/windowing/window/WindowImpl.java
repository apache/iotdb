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

package org.apache.iotdb.db.utils.windowing.window;

import org.apache.iotdb.db.utils.windowing.api.Window;
import org.apache.iotdb.tsfile.exception.write.UnSupportedDataTypeException;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.utils.Binary;

public class WindowImpl implements Window {

  private final int size;
  private final TSDataType dataType;

  private long[] timestamps = null;

  private int[] intValues = null;
  private long[] longValues = null;
  private float[] floatValues = null;
  private double[] doubleValues = null;
  private boolean[] booleanValues = null;
  private Binary[] binaryValues = null;

  public WindowImpl(EvictableBatchList list, int begin, int size) {
    this.size = size;
    dataType = list.getDataType();
    init(list, begin);
  }

  private void init(EvictableBatchList list, int begin) {
    timestamps = new long[size];
    for (int i = 0; i < size; ++i) {
      timestamps[i] = list.getTimeByIndex(begin + i);
    }

    switch (dataType) {
      case INT32:
        intValues = new int[size];
        for (int i = 0; i < size; ++i) {
          intValues[i] = list.getIntByIndex(begin + i);
        }
        break;
      case INT64:
        longValues = new long[size];
        for (int i = 0; i < size; ++i) {
          longValues[i] = list.getLongByIndex(begin + i);
        }
        break;
      case FLOAT:
        floatValues = new float[size];
        for (int i = 0; i < size; ++i) {
          floatValues[i] = list.getFloatByIndex(begin + i);
        }
        break;
      case DOUBLE:
        doubleValues = new double[size];
        for (int i = 0; i < size; ++i) {
          doubleValues[i] = list.getDoubleByIndex(begin + i);
        }
        break;
      case BOOLEAN:
        booleanValues = new boolean[size];
        for (int i = 0; i < size; ++i) {
          booleanValues[i] = list.getBooleanByIndex(begin + i);
        }
        break;
      case TEXT:
        binaryValues = new Binary[size];
        for (int i = 0; i < size; ++i) {
          binaryValues[i] = list.getBinaryByIndex(begin + i);
        }
        break;
      default:
        throw new UnSupportedDataTypeException(dataType.toString());
    }
  }

  @Override
  public int size() {
    return size;
  }

  @Override
  public TSDataType getDataType() {
    return dataType;
  }

  @Override
  public long getTime(int index) {
    return timestamps[index];
  }

  @Override
  public int getInt(int index) {
    return intValues[index];
  }

  @Override
  public long getLong(int index) {
    return longValues[index];
  }

  @Override
  public float getFloat(int index) {
    return floatValues[index];
  }

  @Override
  public double getDouble(int index) {
    return doubleValues[index];
  }

  @Override
  public boolean getBoolean(int index) {
    return booleanValues[index];
  }

  @Override
  public Binary getBinary(int index) {
    return binaryValues[index];
  }

  @Override
  public long[] getTimeArray() {
    return timestamps;
  }

  @Override
  public int[] getIntArray() {
    return intValues;
  }

  @Override
  public long[] getLongArray() {
    return longValues;
  }

  @Override
  public float[] getFloatArray() {
    return floatValues;
  }

  @Override
  public double[] getDoubleArray() {
    return doubleValues;
  }

  @Override
  public boolean[] getBooleanArray() {
    return booleanValues;
  }

  @Override
  public Binary[] getBinaryArray() {
    return binaryValues;
  }

  @Override
  public void setInt(int index, int value) {
    intValues[index] = value;
  }

  @Override
  public void setLong(int index, long value) {
    longValues[index] = value;
  }

  @Override
  public void setFloat(int index, float value) {
    floatValues[index] = value;
  }

  @Override
  public void setDouble(int index, double value) {
    doubleValues[index] = value;
  }

  @Override
  public void setBoolean(int index, boolean value) {
    booleanValues[index] = value;
  }

  @Override
  public void setBinary(int index, Binary value) {
    binaryValues[index] = value;
  }
}
