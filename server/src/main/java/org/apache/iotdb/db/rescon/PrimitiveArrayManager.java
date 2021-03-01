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

import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.tsfile.exception.write.UnSupportedDataTypeException;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.utils.Binary;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.EnumMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/** Manage all primitive data list in memory, including get and release operation. */
public class PrimitiveArrayManager {

  /** data type -> ArrayDeque<Array> */
  private static final Map<TSDataType, ArrayDeque<Object>> bufferedArraysMap =
      new EnumMap<>(TSDataType.class);

  /** data type -> current number of buffered arrays */
  private static final Map<TSDataType, Integer> bufferedArraysNumMap =
      new EnumMap<>(TSDataType.class);

  /** data type -> ratio of data type in schema, which could be seen as recommended ratio */
  private static final Map<TSDataType, Double> bufferedArraysNumRatio =
      new EnumMap<>(TSDataType.class);

  private static final Logger logger = LoggerFactory.getLogger(PrimitiveArrayManager.class);

  private static final IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();

  public static final int ARRAY_SIZE = config.getPrimitiveArraySize();

  /** threshold total size of arrays for all data types */
  private static final double BUFFERED_ARRAY_SIZE_THRESHOLD =
      config.getAllocateMemoryForWrite() * config.getBufferedArraysMemoryProportion();

  /** total size of buffered arrays */
  private static AtomicLong bufferedArraysRamSize = new AtomicLong();

  /** total size of out of buffer arrays */
  private static AtomicLong outOfBufferArraysRamSize = new AtomicLong();

  static {
    bufferedArraysMap.put(TSDataType.BOOLEAN, new ArrayDeque<>());
    bufferedArraysMap.put(TSDataType.INT32, new ArrayDeque<>());
    bufferedArraysMap.put(TSDataType.INT64, new ArrayDeque<>());
    bufferedArraysMap.put(TSDataType.FLOAT, new ArrayDeque<>());
    bufferedArraysMap.put(TSDataType.DOUBLE, new ArrayDeque<>());
    bufferedArraysMap.put(TSDataType.TEXT, new ArrayDeque<>());
  }

  private PrimitiveArrayManager() {
    logger.info("BufferedArraySizeThreshold is {}", BUFFERED_ARRAY_SIZE_THRESHOLD);
  }

  /**
   * Get primitive data lists according to type
   *
   * @param dataType data type
   * @return an array
   */
  public static Object getPrimitiveArraysByType(TSDataType dataType) {
    // check memory of buffered array, if already full, generate OOB
    if (bufferedArraysRamSize.get() + ARRAY_SIZE * dataType.getDataTypeSize()
        > BUFFERED_ARRAY_SIZE_THRESHOLD) {
      // return an out of buffer array
      outOfBufferArraysRamSize.addAndGet((long) ARRAY_SIZE * dataType.getDataTypeSize());
      return createPrimitiveArray(dataType);
    }

    synchronized (bufferedArraysMap.get(dataType)) {
      // try to get a buffered array
      Object dataArray = bufferedArraysMap.get(dataType).poll();
      if (dataArray != null) {
        return dataArray;
      }
      // no buffered array, create one
      bufferedArraysNumMap.put(dataType, bufferedArraysNumMap.getOrDefault(dataType, 0) + 1);
      bufferedArraysRamSize.addAndGet((long) ARRAY_SIZE * dataType.getDataTypeSize());
    }

    return createPrimitiveArray(dataType);
  }

  private static Object createPrimitiveArray(TSDataType dataType) {
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
        throw new UnSupportedDataTypeException(dataType.toString());
    }

    return dataArray;
  }

  /**
   * Get primitive data lists according to data type and size, only for TVList's sorting
   *
   * @param dataType data type
   * @param size needed capacity
   * @return an array of primitive data arrays
   */
  public static Object createDataListsByType(TSDataType dataType, int size) {
    int arrayNumber = (int) Math.ceil((float) size / (float) ARRAY_SIZE);
    switch (dataType) {
      case BOOLEAN:
        boolean[][] booleans = new boolean[arrayNumber][];
        for (int i = 0; i < arrayNumber; i++) {
          booleans[i] = new boolean[ARRAY_SIZE];
        }
        return booleans;
      case INT32:
        int[][] ints = new int[arrayNumber][];
        for (int i = 0; i < arrayNumber; i++) {
          ints[i] = new int[ARRAY_SIZE];
        }
        return ints;
      case INT64:
        long[][] longs = new long[arrayNumber][];
        for (int i = 0; i < arrayNumber; i++) {
          longs[i] = new long[ARRAY_SIZE];
        }
        return longs;
      case FLOAT:
        float[][] floats = new float[arrayNumber][];
        for (int i = 0; i < arrayNumber; i++) {
          floats[i] = new float[ARRAY_SIZE];
        }
        return floats;
      case DOUBLE:
        double[][] doubles = new double[arrayNumber][];
        for (int i = 0; i < arrayNumber; i++) {
          doubles[i] = new double[ARRAY_SIZE];
        }
        return doubles;
      case TEXT:
        Binary[][] binaries = new Binary[arrayNumber][];
        for (int i = 0; i < arrayNumber; i++) {
          binaries[i] = new Binary[ARRAY_SIZE];
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
  public static void release(Object dataArray) {
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

    // Check out of buffer array num
    if (outOfBufferArraysRamSize.get() > 0 && isCurrentDataTypeExceeded(dataType)) {
      // release an out of buffer array
      bringBackOOBArray(dataType, ARRAY_SIZE);
    } else if (outOfBufferArraysRamSize.get() > 0 && !isCurrentDataTypeExceeded(dataType)) {
      // if the ratio of buffered arrays of this data type does not exceed the schema ratio,
      // choose one replaced array who has larger ratio than schema recommended ratio
      TSDataType replacedDataType = null;
      for (Map.Entry<TSDataType, Integer> entry : bufferedArraysNumMap.entrySet()) {
        if (isCurrentDataTypeExceeded(entry.getKey())) {
          replacedDataType = entry.getKey();
          // bring back the replaced array as OOB array
          bringBackOOBArray(replacedDataType, ARRAY_SIZE);
          break;
        }
      }
      if (replacedDataType != null) {
        // if we find a replaced array, bring back the original array as a buffered array
        if (logger.isDebugEnabled()) {
          logger.debug(
              "The ratio of {} in buffered array has not reached the schema ratio. Replaced by {}",
              dataType,
              replacedDataType);
        }
        bringBackBufferedArray(dataType, dataArray);
      } else {
        // or else bring back the original array as OOB array
        bringBackOOBArray(dataType, ARRAY_SIZE);
      }
    } else {
      // if there is no out of buffer array, bring back as buffered array directly
      bringBackBufferedArray(dataType, dataArray);
    }
  }

  /**
   * Bring back a buffered array
   *
   * @param dataType data type
   * @param dataArray data array
   */
  private static void bringBackBufferedArray(TSDataType dataType, Object dataArray) {
    synchronized (bufferedArraysMap.get(dataType)) {
      bufferedArraysMap.get(dataType).add(dataArray);
      bufferedArraysNumMap.put(dataType, bufferedArraysNumMap.getOrDefault(dataType, 0) + 1);
    }
    bufferedArraysRamSize.addAndGet((long) -ARRAY_SIZE * dataType.getDataTypeSize());
  }

  /**
   * Bring back out of buffered array
   *
   * @param dataType data type
   * @param size capacity
   */
  private static void bringBackOOBArray(TSDataType dataType, int size) {
    outOfBufferArraysRamSize.addAndGet((long) -size * dataType.getDataTypeSize());
  }

  /**
   * @param schemaDataTypeNumMap schema DataType Num Map (for each series, increase a long and a
   *     specific type)
   * @param total current DataType Total Num (twice of number of time series)
   */
  public static void updateSchemaDataTypeNum(
      Map<TSDataType, Integer> schemaDataTypeNumMap, long total) {
    for (Map.Entry<TSDataType, Integer> entry : schemaDataTypeNumMap.entrySet()) {
      TSDataType dataType = entry.getKey();
      bufferedArraysNumRatio.put(dataType, (double) schemaDataTypeNumMap.get(dataType) / total);
    }
  }

  /**
   * check whether the ratio of buffered array of specific data type reaches the ratio in schema (as
   * recommended ratio)
   *
   * @param dataType data type
   * @return true if the buffered array ratio exceeds the recommend ratio
   */
  private static boolean isCurrentDataTypeExceeded(TSDataType dataType) {
    int total = 0;
    for (int num : bufferedArraysNumMap.values()) {
      total += num;
    }
    return total != 0
        && ((double) bufferedArraysNumMap.getOrDefault(dataType, 0) / total
            > bufferedArraysNumRatio.getOrDefault(dataType, 0.0));
  }

  public static void close() {
    for (ArrayDeque<Object> dataListQueue : bufferedArraysMap.values()) {
      dataListQueue.clear();
    }

    bufferedArraysNumMap.clear();
    bufferedArraysNumRatio.clear();

    bufferedArraysRamSize.set(0);
    outOfBufferArraysRamSize.set(0);
  }
}
