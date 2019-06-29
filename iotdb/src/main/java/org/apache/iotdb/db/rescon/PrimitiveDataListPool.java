/**
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
import java.util.EnumMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import org.apache.iotdb.db.utils.PrimitiveArrayListV2;
import org.apache.iotdb.tsfile.exception.write.UnSupportedDataTypeException;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.utils.Binary;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Manage all primitive data list in memory, including get and release operation.
 */
public class PrimitiveDataListPool {

  private static final Logger LOGGER = LoggerFactory.getLogger(PrimitiveDataListPool.class);

  private static final EnumMap<TSDataType, ArrayDeque> primitiveDataListsMap = new EnumMap<>(TSDataType.class);

  public static final int ARRAY_SIZE = 128;

  static {
    primitiveDataListsMap.put(TSDataType.BOOLEAN, new ArrayDeque());
    primitiveDataListsMap.put(TSDataType.INT32, new ArrayDeque());
    primitiveDataListsMap.put(TSDataType.INT64, new ArrayDeque());
    primitiveDataListsMap.put(TSDataType.FLOAT, new ArrayDeque());
    primitiveDataListsMap.put(TSDataType.DOUBLE, new ArrayDeque());
    primitiveDataListsMap.put(TSDataType.TEXT, new ArrayDeque());
  }

  public static PrimitiveDataListPool getInstance() {
    return INSTANCE;
  }

  private static final PrimitiveDataListPool INSTANCE = new PrimitiveDataListPool();


  private PrimitiveDataListPool() {}

  public synchronized Object getPrimitiveDataListByType(TSDataType dataType) {
    ArrayDeque dataListQueue = primitiveDataListsMap.computeIfAbsent(dataType, (k)->new ArrayDeque<>());
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
      primitiveDataListsMap.get(TSDataType.BOOLEAN).add(dataArray);
    } else if (dataArray instanceof int[]) {
      primitiveDataListsMap.get(TSDataType.INT32).add(dataArray);
    } else if (dataArray instanceof long[]){
      primitiveDataListsMap.get(TSDataType.INT64).add(dataArray);
    } else if (dataArray instanceof float[]) {
      primitiveDataListsMap.get(TSDataType.FLOAT).add(dataArray);
    } else if (dataArray instanceof double[]) {
      primitiveDataListsMap.get(TSDataType.DOUBLE).add(dataArray);
    } else if (dataArray instanceof Binary[]) {
      primitiveDataListsMap.get(TSDataType.TEXT).add(dataArray);
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
