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
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.service.IoTDB;
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
   * data type -> ArrayDeque<Array>
   */
  private static final Map<TSDataType, ArrayDeque<Object>> bufferedArraysMap = new EnumMap<>(
      TSDataType.class);

  /**
   * data type -> current number of buffered arrays
   */
  private static final Map<TSDataType, Integer> bufferedArraysNumMap = new EnumMap<>(
      TSDataType.class);

  /**
   * data type -> ratio of data type in schema, which could be seen as recommended ratio
   */
  private static final Map<TSDataType, Double> bufferedArraysNumRatio = new EnumMap<>(
      TSDataType.class);

  private static final Logger logger = LoggerFactory.getLogger(PrimitiveArrayManager.class);

  public static final int ARRAY_SIZE =
      IoTDBDescriptor.getInstance().getConfig().getPrimitiveArraySize();

  /**
   * threshold total size of arrays for all data types TODO modified as a config
   */
  private static final int BUFFERED_ARRAY_SIZE_THRESHOLD = 1024 * 1024;

  /**
   * total size of buffered arrays
   */
  private int bufferedArraysSize;

  private int lastReportArraySize = 0;
  /**
   * total size of out of buffer arrays
   */
  private int outOfBufferArraysSize;

  /**
   * collect schema data type number in specific interval time
   */
  private ScheduledExecutorService timedCollectSchemaDataTypeNumThread;

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
    timedCollectSchemaDataTypeNumThread = Executors
        .newSingleThreadScheduledExecutor(
            r -> new Thread(r, "timedCollectSchemaDataTypeNumThread"));
    timedCollectSchemaDataTypeNumThread.scheduleAtFixedRate(this::collectSchemaDataTypeNum, 0,
        3600, TimeUnit.SECONDS); // TODO modified as a config

    bufferedArraysSize = 0;
    outOfBufferArraysSize = 0;
  }

  /**
   * Get primitive data lists according to type
   *
   * @param dataType data type
   * @return an array, or null if the system module refuse to offer an out of buffer array
   */
  public synchronized Object getDataListByType(TSDataType dataType) {
    // check buffered array num
    if (bufferedArraysSize + ARRAY_SIZE * dataType.getDataTypeSize()
        > BUFFERED_ARRAY_SIZE_THRESHOLD) {
      if (logger.isDebugEnabled()) {
        logger.debug("Apply out of buffer array from system module...");
      }
      boolean applyResult = applyOOBArray(dataType, ARRAY_SIZE);
      if (!applyResult) {
        if (logger.isDebugEnabled()) {
          logger.debug("System module REFUSE to offer an out of buffer array");
        }
        return null;
      } else {
        // return an out of buffer array
        outOfBufferArraysSize += ARRAY_SIZE * dataType.getDataTypeSize();
        return getDataList(dataType);
      }
    }

    // return a buffered array
    bufferedArraysNumMap.put(dataType, bufferedArraysNumMap.getOrDefault(dataType, 0) + 1);
    ArrayDeque<Object> dataListQueue = bufferedArraysMap
        .computeIfAbsent(dataType, k -> new ArrayDeque<>());
    bufferedArraysSize += ARRAY_SIZE * dataType.getDataTypeSize();
    if (bufferedArraysSize - lastReportArraySize >= BUFFERED_ARRAY_SIZE_THRESHOLD / 8) {
      // report current buffered array size to system
      SystemInfo.getInstance().reportIncreasingArraySize(bufferedArraysSize - lastReportArraySize);
      lastReportArraySize = bufferedArraysSize;
    }
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
        throw new UnSupportedDataTypeException(dataType.toString());
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
          booleans[i] = (boolean[]) waitAndGetDataListByType(dataType);
        }
        return booleans;
      case INT32:
        int[][] ints = new int[arrayNumber][];
        for (int i = 0; i < arrayNumber; i++) {
          ints[i] = (int[]) waitAndGetDataListByType(dataType);
        }
        return ints;
      case INT64:
        long[][] longs = new long[arrayNumber][];
        for (int i = 0; i < arrayNumber; i++) {
          longs[i] = (long[]) waitAndGetDataListByType(dataType);
        }
        return longs;
      case FLOAT:
        float[][] floats = new float[arrayNumber][];
        for (int i = 0; i < arrayNumber; i++) {
          floats[i] = (float[]) waitAndGetDataListByType(dataType);
        }
        return floats;
      case DOUBLE:
        double[][] doubles = new double[arrayNumber][];
        for (int i = 0; i < arrayNumber; i++) {
          doubles[i] = (double[]) waitAndGetDataListByType(dataType);
        }
        return doubles;
      case TEXT:
        Binary[][] binaries = new Binary[arrayNumber][];
        for (int i = 0; i < arrayNumber; i++) {
          binaries[i] = (Binary[]) waitAndGetDataListByType(dataType);
        }
        return binaries;
      default:
        return null;
    }
  }

  private Object waitAndGetDataListByType(TSDataType dataType) {
    Object res = getDataListByType(dataType);
    while (res == null) {
      try {
        TimeUnit.MILLISECONDS.sleep(100);
      } catch (InterruptedException e) {
        logger.error("Failed when waiting for getting an out of buffer array from system. ", e);
        Thread.currentThread().interrupt();
      }
      res = getDataListByType(dataType);
    }
    return res;
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

    // Check out of buffer array num
    if (outOfBufferArraysSize > 0 && checkBufferedDataTypeNum(dataType)) {
      // bring back an out of buffer array
      bringBackOOBArray(dataType, ARRAY_SIZE);
    } else if (outOfBufferArraysSize > 0 && !checkBufferedDataTypeNum(dataType)) {
      // if the ratio of buffered arrays of this data type has not reached the schema ratio,
      // choose one replaced array who has larger ratio than schema recommended ratio
      TSDataType replacedDataType = null;
      for (Map.Entry<TSDataType, Integer> entry : bufferedArraysNumMap.entrySet()) {
        replacedDataType = entry.getKey();
        if (checkBufferedDataTypeNum(replacedDataType)) {
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
              dataType, replacedDataType);
        }
        bringBackBufferedArray(dataType, dataArray);
        bufferedArraysSize +=
            ARRAY_SIZE * (dataType.getDataTypeSize() - replacedDataType.getDataTypeSize());
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
   * Apply out of buffer array from system module according to data type and size
   *
   * @param dataType data type
   * @param size needed capacity
   * @return true if successfully applied and false if rejected
   */
  private boolean applyOOBArray(TSDataType dataType, int size) {
    return SystemInfo.getInstance().applyNewOOBArray(dataType, size);
  }

  /**
   * Bring back a buffered array
   *
   * @param dataType data type
   * @param dataArray data array
   */
  private void bringBackBufferedArray(TSDataType dataType, Object dataArray) {
    bufferedArraysMap.get(dataType).add(dataArray);
    bufferedArraysNumMap.put(dataType, bufferedArraysNumMap.get(dataType) - 1);
  }

  /**
   * Return out of buffered array to system module
   *
   * @param dataType data type
   * @param size capacity
   */
  private void bringBackOOBArray(TSDataType dataType, int size) {
    if (logger.isDebugEnabled()) {
      logger.debug("Bring back out of buffer array of {} to system module...", dataType);
    }
    outOfBufferArraysSize -= ARRAY_SIZE * dataType.getDataTypeSize();
    SystemInfo.getInstance().reportReleaseOOBArray(dataType, size);
  }

  private void collectSchemaDataTypeNum() {
    try {
      Map<TSDataType, Integer> schemaDataTypeNumMap = IoTDB.metaManager
          .collectSchemaDataTypeNum("root");
      int total = 0;
      for (int num : schemaDataTypeNumMap.values()) {
        total += num;
      }
      if (total == 0) {
        return;
      }
      for (Map.Entry<TSDataType, Integer> entry : schemaDataTypeNumMap.entrySet()) {
        TSDataType dataType = entry.getKey();
        bufferedArraysNumRatio
            .put(dataType, (double) schemaDataTypeNumMap.get(dataType) / total);
      }
    } catch (MetadataException e) {
      logger.error("Failed to get schema data type num map. ", e);
    }
  }

  /**
   * check whether the ratio of buffered array of specific data type reaches the ratio in schema (as
   * recommended ratio)
   *
   * @param dataType data type
   * @return true if the buffered array ratio reaches the recommend ratio
   */
  private boolean checkBufferedDataTypeNum(TSDataType dataType) {
    int total = 0;
    for (int num : bufferedArraysNumMap.values()) {
      total += num;
    }
    return total != 0 && bufferedArraysNumMap.get(dataType) / total > bufferedArraysNumRatio
        .get(dataType);
  }

  public void close() {
    if (timedCollectSchemaDataTypeNumThread != null) {
      timedCollectSchemaDataTypeNumThread.shutdownNow();
      timedCollectSchemaDataTypeNumThread = null;
    }
    bufferedArraysMap.clear();
    bufferedArraysNumMap.clear();
    bufferedArraysNumRatio.clear();

    bufferedArraysSize = 0;
    outOfBufferArraysSize = 0;
  }
}
