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
import org.apache.iotdb.db.utils.FileUtils;
import org.apache.iotdb.tsfile.exception.write.UnSupportedDataTypeException;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.utils.Binary;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Manage all primitive data list in memory, including get and release operation.
 */
public class PrimitiveArrayManager {

  /**
   * data type -> <current number of buffered arrays, ArrayDeque<Array>>
   */
  private static final EnumMap<TSDataType, ArrayDeque<Object>> bufferedArraysMap = new EnumMap<>(
      TSDataType.class);
  private static final EnumMap<TSDataType, Integer> bufferedArraysNumMap = new EnumMap<>(
      TSDataType.class);
  private static final EnumMap<TSDataType, Integer> outOfBufferArraysNumMap = new EnumMap<>(
      TSDataType.class);

  private static final Logger logger = LoggerFactory.getLogger(PrimitiveArrayManager.class);

  public static final int ARRAY_SIZE =
      IoTDBDescriptor.getInstance().getConfig().getPrimitiveArraySize();

  /**
   * theshold number of arrays for one data type TODO modified as a config
   */
  private static final int ARRAY_NUM_THRESHOLD = 1024 * 1024;

  static {
    bufferedArraysMap.put(TSDataType.BOOLEAN, new ArrayDeque<>());
    bufferedArraysMap.put(TSDataType.INT32, new ArrayDeque<>());
    bufferedArraysMap.put(TSDataType.INT64, new ArrayDeque<>());
    bufferedArraysMap.put(TSDataType.FLOAT, new ArrayDeque<>());
    bufferedArraysMap.put(TSDataType.DOUBLE, new ArrayDeque<>());
    bufferedArraysMap.put(TSDataType.TEXT, new ArrayDeque<>());
  }

  public static PrimitiveArrayManager getInstance() {
    return INSTANCE;
  }

  private static final PrimitiveArrayManager INSTANCE = new PrimitiveArrayManager();

  private PrimitiveArrayManager() {

  }

  /**
   * Get primitive data lists according to type
   *
   * @param dataType data type
   * @return an array, or null if the system module refuse to offer an out of buffer array
   */
  public synchronized Object getDataListByType(TSDataType dataType) {
    // check buffered array num
    if (bufferedArraysNumMap.containsKey(dataType)
        && bufferedArraysNumMap.get(dataType) >= ARRAY_NUM_THRESHOLD) {

      if(logger.isDebugEnabled()) {
        logger.debug("Apply out of buffer array from system module...");
      }
      boolean applyResult = applyOOBArray(dataType, ARRAY_SIZE);
      if (!applyResult) {
        if(logger.isDebugEnabled()) {
          logger.debug("System module refuse to offer an out of buffer array, return null");
        }
        return null;
      } else {
        outOfBufferArraysNumMap
            .put(dataType, outOfBufferArraysNumMap.getOrDefault(dataType, 0) + 1);
        return getDataList(dataType);
      }
    }
    ArrayDeque<Object> dataListQueue = bufferedArraysMap
        .computeIfAbsent(dataType, k -> new ArrayDeque<>());
    Object dataArray = dataListQueue.poll();
    if (dataArray != null) {
      return dataArray;
    }
    return getDataList(dataType);
  }

  private Object getDataList(TSDataType dataType) {
    Object dataArray;
    switch (dataType) {
      case BOOLEAN:
        dataArray = new boolean[ARRAY_SIZE];
        break;
      case INT32:
        dataArray = new int[ARRAY_SIZE];
        break;
      case INT64:
        dataArray = new long[ARRAY_SIZE];
        break;
      case FLOAT:
        dataArray = new float[ARRAY_SIZE];
        break;
      case DOUBLE:
        dataArray = new double[ARRAY_SIZE];
        break;
      case TEXT:
        dataArray = new Binary[ARRAY_SIZE];
        break;
      default:
        throw new UnSupportedDataTypeException("DataType: " + dataType);
    }
    return dataArray;
  }

  /**
   * Get primitive data lists according to data type and size
   *
   * @param dataType data type
   * @param size needed capacity
   * @return an array of primitive data arrays
   */
  public synchronized Object getDataListsByType(TSDataType dataType, int size) {
    int arrayNumber = (int) Math.ceil((float) size / (float) ARRAY_SIZE);
    switch (dataType) {
      case BOOLEAN:
        boolean[][] booleans = new boolean[arrayNumber][];
        for (int i = 0; i < arrayNumber; i++) {
          booleans[i] = (boolean[]) getDataListByType(dataType);
        }
        return booleans;
      case INT32:
        int[][] ints = new int[arrayNumber][];
        for (int i = 0; i < arrayNumber; i++) {
          ints[i] = (int[]) getDataListByType(dataType);
        }
        return ints;
      case INT64:
        long[][] longs = new long[arrayNumber][];
        for (int i = 0; i < arrayNumber; i++) {
          longs[i] = (long[]) getDataListByType(dataType);
        }
        return longs;
      case FLOAT:
        float[][] floats = new float[arrayNumber][];
        for (int i = 0; i < arrayNumber; i++) {
          floats[i] = (float[]) getDataListByType(dataType);
        }
        return floats;
      case DOUBLE:
        double[][] doubles = new double[arrayNumber][];
        for (int i = 0; i < arrayNumber; i++) {
          doubles[i] = (double[]) getDataListByType(dataType);
        }
        return doubles;
      case TEXT:
        Binary[][] binaries = new Binary[arrayNumber][];
        for (int i = 0; i < arrayNumber; i++) {
          binaries[i] = (Binary[]) getDataListByType(dataType);
        }
        return binaries;
      default:
        return null;
    }
  }

  /**
   * This method is called when bringing back data array
   *
   * @param dataArray data array
   */
  public synchronized void release(Object dataArray) {
    TSDataType dataType;
    if (dataArray instanceof boolean[]) {
      dataType = TSDataType.BOOLEAN;
    } else if (dataArray instanceof int[]) {
      dataType = TSDataType.INT32;
    } else if (dataArray instanceof long[]) {
      dataType = TSDataType.INT64;
    } else if (dataArray instanceof float[]) {
      dataType = TSDataType.FLOAT;
    } else if (dataArray instanceof double[]) {
      dataType = TSDataType.DOUBLE;
    } else if (dataArray instanceof Binary[]) {
      Arrays.fill((Binary[]) dataArray, null);
      dataType = TSDataType.TEXT;
    } else {
      throw new UnSupportedDataTypeException("Unknown data array type");
    }
    bufferedArraysMap.get(dataType).add(dataArray);
    bufferedArraysNumMap.put(dataType, bufferedArraysNumMap.get(dataType) + 1);

    bringBackOOBArray(dataType, ARRAY_SIZE);
  }

  /**
   * Apply out of buffer array from system module according to data type and size
   *
   * @param dataType data type
   * @param size needed capacity
   * @return true if successfully applied and false if rejected
   */
  private boolean applyOOBArray(TSDataType dataType, int size) {
    return System.getInstance().applyNewOOBArray(dataType, size);
  }

  /**
   * Return out of buffered array to system module
   *
   * @param dataType data type
   * @param size needed capacity
   */
  private void bringBackOOBArray(TSDataType dataType, int size) {
    System.getInstance().releaseOOBArray(dataType, size);
  }
}
