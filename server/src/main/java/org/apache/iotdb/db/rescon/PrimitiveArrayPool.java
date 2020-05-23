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
package org.apache.iotdb.db.rescon;

import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.EnumMap;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.tsfile.exception.write.UnSupportedDataTypeException;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.utils.Binary;

/**
 * Manage all primitive data list in memory, including get and release operation.
 */
public class PrimitiveArrayPool {

  /**
   * data type -> Array<PrimitiveArray>
   */
  private static final EnumMap<TSDataType, ArrayDeque<Object>> primitiveArraysMap = new EnumMap<>(TSDataType.class);

  public static final int ARRAY_SIZE =
      IoTDBDescriptor.getInstance().getConfig().getPrimitiveArraySize();

  static {
    primitiveArraysMap.put(TSDataType.BOOLEAN, new ArrayDeque<>());
    primitiveArraysMap.put(TSDataType.INT32, new ArrayDeque<>());
    primitiveArraysMap.put(TSDataType.INT64, new ArrayDeque<>());
    primitiveArraysMap.put(TSDataType.FLOAT, new ArrayDeque<>());
    primitiveArraysMap.put(TSDataType.DOUBLE, new ArrayDeque<>());
    primitiveArraysMap.put(TSDataType.TEXT, new ArrayDeque<>());
  }

  public static PrimitiveArrayPool getInstance() {
    return INSTANCE;
  }

  private static final PrimitiveArrayPool INSTANCE = new PrimitiveArrayPool();


  private PrimitiveArrayPool() {}

  public synchronized Object getPrimitiveDataListByType(TSDataType dataType) {
    ArrayDeque<Object> dataListQueue = primitiveArraysMap.computeIfAbsent(dataType, k ->new ArrayDeque<>());
    Object dataArray = dataListQueue.poll();
    switch (dataType) {
      case BOOLEAN:
        if (dataArray == null) {
          dataArray = new boolean[ARRAY_SIZE];
        }
        break;
      case INT32:
        if (dataArray == null) {
          dataArray = new int[ARRAY_SIZE];
        }
        break;
      case INT64:
        if (dataArray == null) {
          dataArray = new long[ARRAY_SIZE];
        }
        break;
      case FLOAT:
        if (dataArray == null) {
          dataArray = new float[ARRAY_SIZE];
        }
        break;
      case DOUBLE:
        if (dataArray == null) {
          dataArray = new double[ARRAY_SIZE];
        }
        break;
      case TEXT:
        if (dataArray == null) {
          dataArray = new Binary[ARRAY_SIZE];
        }
        break;
      default:
        throw new UnSupportedDataTypeException("DataType: " + dataType);
    }
    return dataArray;
  }


  public synchronized void release(Object dataArray) {
    if (dataArray instanceof boolean[]) {
      primitiveArraysMap.get(TSDataType.BOOLEAN).add(dataArray);
    } else if (dataArray instanceof int[]) {
      primitiveArraysMap.get(TSDataType.INT32).add(dataArray);
    } else if (dataArray instanceof long[]){
      primitiveArraysMap.get(TSDataType.INT64).add(dataArray);
    } else if (dataArray instanceof float[]) {
      primitiveArraysMap.get(TSDataType.FLOAT).add(dataArray);
    } else if (dataArray instanceof double[]) {
      primitiveArraysMap.get(TSDataType.DOUBLE).add(dataArray);
    } else if (dataArray instanceof Binary[]) {
      Arrays.fill((Binary[]) dataArray, null);
      primitiveArraysMap.get(TSDataType.TEXT).add(dataArray);
    }
  }

  /**
   * @param size needed capacity
   * @return an array of primitive data arrays
   */
  public synchronized Object getDataListsByType(TSDataType dataType, int size) {
    int arrayNumber = (int) Math.ceil((float) size / (float)ARRAY_SIZE);
    switch (dataType) {
      case BOOLEAN:
        boolean[][] booleans = new boolean[arrayNumber][];
        for (int i = 0; i < arrayNumber; i++) {
          booleans[i] = (boolean[]) getPrimitiveDataListByType(dataType);
        }
        return booleans;
      case INT32:
        int[][] ints = new int[arrayNumber][];
        for (int i = 0; i < arrayNumber; i++) {
          ints[i] = (int[]) getPrimitiveDataListByType(dataType);
        }
        return ints;
      case INT64:
        long[][] longs = new long[arrayNumber][];
        for (int i = 0; i < arrayNumber; i++) {
          longs[i] = (long[]) getPrimitiveDataListByType(dataType);
        }
        return longs;
      case FLOAT:
        float[][] floats = new float[arrayNumber][];
        for (int i = 0; i < arrayNumber; i++) {
          floats[i] = (float[]) getPrimitiveDataListByType(dataType);
        }
        return floats;
      case DOUBLE:
        double[][] doubles = new double[arrayNumber][];
        for (int i = 0; i < arrayNumber; i++) {
          doubles[i] = (double[]) getPrimitiveDataListByType(dataType);
        }
        return doubles;
      case TEXT:
        Binary[][] binaries = new Binary[arrayNumber][];
        for (int i = 0; i < arrayNumber; i++) {
          binaries[i] = (Binary[]) getPrimitiveDataListByType(dataType);
        }
        return binaries;
      default:
        return null;
    }
  }

}
