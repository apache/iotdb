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
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicLong;

/** Manage all primitive data list in memory, including get and release operation. */
public class PrimitiveArrayManager {

  /** data type -> ArrayDeque<Array> */
  private static final Map<TSDataType, ArrayDeque<Object>> bufferedArraysMap =
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
  private static final AtomicLong bufferedArraysRamSize = new AtomicLong();

  /** total size of out of buffer arrays */
  private static final AtomicLong outOfBufferArraysRamSize = new AtomicLong();

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
    long delta = (long) ARRAY_SIZE * dataType.getDataTypeSize();

    // check memory of buffered array, if already full, generate OOB
    if (bufferedArraysRamSize.get() + delta > BUFFERED_ARRAY_SIZE_THRESHOLD) {
      // return an out of buffer array
      outOfBufferArraysRamSize.addAndGet(delta);
      return createPrimitiveArray(dataType);
    }

    synchronized (bufferedArraysMap.get(dataType)) {
      // try to get a buffered array
      Object dataArray = bufferedArraysMap.get(dataType).poll();
      if (dataArray != null) {
        return dataArray;
      }
    }

    // no buffered array, create one
    bufferedArraysRamSize.addAndGet(delta);
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
   * @param releasingArray data array to be released
   */
  public static void release(Object releasingArray) {
    TSDataType releasingType;
    if (releasingArray instanceof boolean[]) {
      releasingType = TSDataType.BOOLEAN;
    } else if (releasingArray instanceof int[]) {
      releasingType = TSDataType.INT32;
    } else if (releasingArray instanceof long[]) {
      releasingType = TSDataType.INT64;
    } else if (releasingArray instanceof float[]) {
      releasingType = TSDataType.FLOAT;
    } else if (releasingArray instanceof double[]) {
      releasingType = TSDataType.DOUBLE;
    } else if (releasingArray instanceof Binary[]) {
      Arrays.fill((Binary[]) releasingArray, null);
      releasingType = TSDataType.TEXT;
    } else {
      throw new UnSupportedDataTypeException("Unknown data array type");
    }

    if (outOfBufferArraysRamSize.get() <= 0) {
      // if there is no out of buffer array, bring back as buffered array directly
      putBackBufferedArray(releasingType, releasingArray);
    } else {
      // if the system has out of buffer array, we need to release some memory
      if (!isCurrentDataTypeExceeded(releasingType)) {
        // if the buffered array of the releasingType is less than expected
        // choose an array of redundantDataType to release and try to buffer the array of
        // releasingType
        for (Entry<TSDataType, ArrayDeque<Object>> entry : bufferedArraysMap.entrySet()) {
          TSDataType dataType = entry.getKey();
          if (isCurrentDataTypeExceeded(dataType)) {
            // if we find a replaced array, bring back the original array as a buffered array
            if (logger.isDebugEnabled()) {
              logger.debug(
                  "The ratio of {} in buffered array has not reached the schema ratio. discard a redundant array of {}",
                  releasingType,
                  dataType);
            }
            // bring back the replaced array as OOB array
            replaceBufferedArray(releasingType, releasingArray, dataType);
            break;
          }
        }
      }

      releaseOutOfBuffer(releasingType);
    }
  }

  /**
   * Bring back a buffered array
   *
   * @param dataType data type
   * @param dataArray data array
   */
  private static void putBackBufferedArray(TSDataType dataType, Object dataArray) {
    synchronized (bufferedArraysMap.get(dataType)) {
      bufferedArraysMap.get(dataType).add(dataArray);
    }
  }

  private static void replaceBufferedArray(
      TSDataType releasingType, Object releasingArray, TSDataType redundantType) {
    synchronized (bufferedArraysMap.get(redundantType)) {
      if (bufferedArraysMap.get(redundantType).poll() != null) {
        bufferedArraysRamSize.addAndGet((long) -ARRAY_SIZE * redundantType.getDataTypeSize());
      }
    }

    if (bufferedArraysRamSize.get() + (long) ARRAY_SIZE * releasingType.getDataTypeSize()
        < BUFFERED_ARRAY_SIZE_THRESHOLD) {
      ArrayDeque<Object> releasingArrays = bufferedArraysMap.get(releasingType);
      synchronized (releasingArrays) {
        releasingArrays.add(releasingArray);
      }
      bufferedArraysRamSize.addAndGet((long) ARRAY_SIZE * releasingType.getDataTypeSize());
    }
  }

  private static void releaseOutOfBuffer(TSDataType dataType) {
    outOfBufferArraysRamSize.getAndUpdate(
        l -> Math.max(0, l - (long) ARRAY_SIZE * dataType.getDataTypeSize()));
  }

  /**
   * @param schemaDataTypeNumMap schema DataType Num Map (for each series, increase a long and a
   *     specific type)
   * @param totalSeries total time series number
   */
  public static void updateSchemaDataTypeNum(
      Map<TSDataType, Integer> schemaDataTypeNumMap, long totalSeries) {
    for (Map.Entry<TSDataType, Integer> entry : schemaDataTypeNumMap.entrySet()) {
      TSDataType dataType = entry.getKey();
      // one time series has 2 columns (time column + value column)
      bufferedArraysNumRatio.put(
          dataType, (double) schemaDataTypeNumMap.get(dataType) / (totalSeries * 2));
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
    long total = 0;
    for (ArrayDeque<Object> value : bufferedArraysMap.values()) {
      total += value.size();
    }
    long arrayNumInBuffer =
        bufferedArraysMap.containsKey(dataType) ? bufferedArraysMap.get(dataType).size() : 0;
    return total != 0
        && ((double) arrayNumInBuffer / total > bufferedArraysNumRatio.getOrDefault(dataType, 0.0));
  }

  public static void close() {
    for (ArrayDeque<Object> dataListQueue : bufferedArraysMap.values()) {
      dataListQueue.clear();
    }

    bufferedArraysNumRatio.clear();

    bufferedArraysRamSize.set(0);
    outOfBufferArraysRamSize.set(0);
  }
}