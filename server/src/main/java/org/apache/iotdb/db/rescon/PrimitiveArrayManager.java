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
import org.apache.iotdb.db.utils.datastructure.TVListSortAlgorithm;
import org.apache.iotdb.tsfile.exception.write.UnSupportedDataTypeException;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.utils.Binary;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicLong;

/** Manage all primitive data lists in memory, including get and release operations. */
public class PrimitiveArrayManager {

  private static final Logger LOGGER = LoggerFactory.getLogger(PrimitiveArrayManager.class);

  private static final IoTDBConfig CONFIG = IoTDBDescriptor.getInstance().getConfig();

  public static final int ARRAY_SIZE = CONFIG.getPrimitiveArraySize();

  public static final TVListSortAlgorithm TVLIST_SORT_ALGORITHM = CONFIG.getTvListSortAlgorithm();

  /**
   * The actual used memory will be 50% larger than the statistic, so we need to limit the size of
   * POOLED_ARRAYS_MEMORY_THRESHOLD, make it smaller than its actual allowed value.
   */
  private static final double AMPLIFICATION_FACTOR = 1.5;

  /** threshold total size of arrays for all data types */
  private static final double POOLED_ARRAYS_MEMORY_THRESHOLD =
      CONFIG.getAllocateMemoryForStorageEngine()
          * CONFIG.getBufferedArraysMemoryProportion()
          / AMPLIFICATION_FACTOR;

  /** TSDataType#serialize() -> ArrayDeque<Array>, VECTOR is ignored */
  private static final ArrayDeque[] POOLED_ARRAYS = new ArrayDeque[TSDataType.values().length - 1];

  /** TSDataType#serialize() -> max size of ArrayDeque<Array>, VECTOR is ignored */
  private static final int[] LIMITS = new int[TSDataType.values().length - 1];

  /** LIMITS should be updated if (TOTAL_ALLOCATION_REQUEST_COUNT.get() > limitUpdateThreshold) */
  private static long limitUpdateThreshold;

  /** TSDataType#serialize() -> count of allocation requests, VECTOR is ignored */
  private static final AtomicLong[] ALLOCATION_REQUEST_COUNTS =
      new AtomicLong[] {
        new AtomicLong(0),
        new AtomicLong(0),
        new AtomicLong(0),
        new AtomicLong(0),
        new AtomicLong(0),
        new AtomicLong(0)
      };

  private static final AtomicLong TOTAL_ALLOCATION_REQUEST_COUNT = new AtomicLong(0);

  static {
    init();
  }

  private static void init() {
    LOGGER.info("BufferedArraySizeThreshold is {}", POOLED_ARRAYS_MEMORY_THRESHOLD);

    // POOLED_ARRAYS_MEMORY_THRESHOLD = ∑(datatype[i].getDataTypeSize() * ARRAY_SIZE * LIMITS[i])
    // we init all LIMITS[i] with the same value, so we have
    // => LIMITS[i] = POOLED_ARRAYS_MEMORY_THRESHOLD / ARRAY_SIZE / ∑(datatype[i].getDataTypeSize())
    int totalDataTypeSize = 0;
    for (TSDataType dataType : TSDataType.values()) {
      // VECTOR is ignored
      if (dataType.equals(TSDataType.VECTOR)) {
        continue;
      }
      totalDataTypeSize += dataType.getDataTypeSize();
    }
    @SuppressWarnings("squid:S3518") // totalDataTypeSize can not be zero
    double limit = POOLED_ARRAYS_MEMORY_THRESHOLD / ARRAY_SIZE / totalDataTypeSize;
    Arrays.fill(LIMITS, (int) limit);

    // limitUpdateThreshold = ∑(LIMITS[i])
    limitUpdateThreshold = (long) ((TSDataType.values().length - 1) * limit);

    for (int i = 0; i < POOLED_ARRAYS.length; ++i) {
      POOLED_ARRAYS[i] = new ArrayDeque<>((int) limit);
    }

    for (AtomicLong allocationRequestCount : ALLOCATION_REQUEST_COUNTS) {
      allocationRequestCount.set(0);
    }

    TOTAL_ALLOCATION_REQUEST_COUNT.set(0);
  }

  private PrimitiveArrayManager() {}

  /**
   * Get or allocate primitive data lists according to type.
   *
   * @return an array
   */
  public static Object allocate(TSDataType dataType) {
    if (dataType.equals(TSDataType.VECTOR)) {
      throw new UnSupportedDataTypeException(TSDataType.VECTOR.name());
    }

    if (TOTAL_ALLOCATION_REQUEST_COUNT.get() > limitUpdateThreshold) {
      synchronized (TOTAL_ALLOCATION_REQUEST_COUNT) {
        if (TOTAL_ALLOCATION_REQUEST_COUNT.get() > limitUpdateThreshold) {
          updateLimits();
        }
      }
    }

    int order = dataType.serialize();

    ALLOCATION_REQUEST_COUNTS[order].incrementAndGet();
    TOTAL_ALLOCATION_REQUEST_COUNT.incrementAndGet();

    Object array;
    synchronized (POOLED_ARRAYS[order]) {
      array = POOLED_ARRAYS[order].poll();
    }
    if (array == null) {
      array = createPrimitiveArray(dataType);
    }
    return array;
  }

  private static void updateLimits() {
    // we want to update LIMITS[i] according to ratios[i]
    double[] ratios = new double[ALLOCATION_REQUEST_COUNTS.length];
    for (int i = 0; i < ALLOCATION_REQUEST_COUNTS.length; ++i) {
      ratios[i] =
          ALLOCATION_REQUEST_COUNTS[i].get() / (double) TOTAL_ALLOCATION_REQUEST_COUNT.get();
    }

    // initially we have:
    //   POOLED_ARRAYS_MEMORY_THRESHOLD = ∑(datatype[i].getDataTypeSize() * LIMITS[i]) * ARRAY_SIZE
    // we can find a number called limitBase which satisfies:
    //   LIMITS[i] = limitBase * ratios[i]

    // => POOLED_ARRAYS_MEMORY_THRESHOLD =
    //     limitBase * ∑(datatype[i].getDataTypeSize() * ratios[i]) * ARRAY_SIZE
    // => limitBase = POOLED_ARRAYS_MEMORY_THRESHOLD / ARRAY_SIZE
    //     / ∑(datatype[i].getDataTypeSize() * ratios[i])
    double weightedSumOfRatios = 0;
    for (TSDataType dataType : TSDataType.values()) {
      // VECTOR is ignored
      if (dataType.equals(TSDataType.VECTOR)) {
        continue;
      }
      weightedSumOfRatios += dataType.getDataTypeSize() * ratios[dataType.serialize()];
    }
    @SuppressWarnings("squid:S3518") // weightedSumOfRatios can not be zero
    double limitBase = POOLED_ARRAYS_MEMORY_THRESHOLD / ARRAY_SIZE / weightedSumOfRatios;

    // LIMITS[i] = limitBase * ratios[i]
    for (int i = 0; i < LIMITS.length; ++i) {
      int oldLimit = LIMITS[i];
      int newLimit = (int) (limitBase * ratios[i]);
      LIMITS[i] = newLimit;

      if (LOGGER.isDebugEnabled() && oldLimit != newLimit) {
        LOGGER.debug(
            "limit of {} array deque size updated: {} -> {}",
            TSDataType.deserialize((byte) i).name(),
            oldLimit,
            newLimit);
      }
    }

    long oldLimitUpdateThreshold = limitUpdateThreshold;
    // limitUpdateThreshold = ∑(LIMITS[i])
    limitUpdateThreshold = 0;
    for (int limit : LIMITS) {
      limitUpdateThreshold += limit;
    }
    if (LOGGER.isDebugEnabled() && oldLimitUpdateThreshold != limitUpdateThreshold) {
      LOGGER.debug(
          "limitUpdateThreshold of PrimitiveArrayManager updated: {} -> {}",
          oldLimitUpdateThreshold,
          limitUpdateThreshold);
    }

    for (AtomicLong allocationRequestCount : ALLOCATION_REQUEST_COUNTS) {
      allocationRequestCount.set(0);
    }

    TOTAL_ALLOCATION_REQUEST_COUNT.set(0);
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
        throw new UnSupportedDataTypeException(dataType.name());
    }

    return dataArray;
  }

  /**
   * This method is called when bringing back data array
   *
   * @param array data array to be released
   */
  public static void release(Object array) {
    int order;
    if (array instanceof boolean[]) {
      order = TSDataType.BOOLEAN.serialize();
    } else if (array instanceof int[]) {
      order = TSDataType.INT32.serialize();
    } else if (array instanceof long[]) {
      order = TSDataType.INT64.serialize();
    } else if (array instanceof float[]) {
      order = TSDataType.FLOAT.serialize();
    } else if (array instanceof double[]) {
      order = TSDataType.DOUBLE.serialize();
    } else if (array instanceof Binary[]) {
      Arrays.fill((Binary[]) array, null);
      order = TSDataType.TEXT.serialize();
    } else {
      throw new UnSupportedDataTypeException(array.getClass().toString());
    }

    synchronized (POOLED_ARRAYS[order]) {
      ArrayDeque<Object> arrays = POOLED_ARRAYS[order];
      if (arrays.size() < LIMITS[order]) {
        arrays.add(array);
      }
    }
  }

  public static void close() {
    init();
  }

  /**
   * Get primitive data lists according to data type and size, only for TVList's sorting
   *
   * @param dataType data type
   * @param size needed capacity
   * @return an array of primitive data arrays
   */
  public static Object createDataListsByType(TSDataType dataType, int size) {
    int arrayNumber = getArrayRowCount(size);
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
        throw new UnSupportedDataTypeException(dataType.name());
    }
  }

  public static int getArrayRowCount(int size) {
    return size / ARRAY_SIZE + (size % ARRAY_SIZE == 0 ? 0 : 1);
  }
}
