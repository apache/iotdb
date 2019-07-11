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
package org.apache.iotdb.db.conf.adapter;

import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.StorageEngine;
import org.apache.iotdb.db.exception.ConfigAdjusterException;
import org.apache.iotdb.db.metadata.MManager;
import org.apache.iotdb.db.rescon.PrimitiveArrayPool;

public class IoTDBConfigDynamicAdapter implements IDynamicAdapter {


  private static final IoTDBConfig CONFIG = IoTDBDescriptor.getInstance().getConfig();

  // static parameter section

  private static final double FLUSH_THRESHOLD = 0.2;

  /**
   * Maximum amount of memory allocated for write process.
   */
  private static final long ALLOCATE_MEMORY_FOR_WRITE = CONFIG.getAllocateMemoryForWrite();

  /**
   * Metadata size of per timeseries, the default value is 2KB.
   */
  private static final long TIMESERIES_METADATA_SIZE_IN_BYTE = 2L * 1024;

  /**
   * Metadata size of per chunk, the default value is 1.5 KB.
   */
  private static final long CHUNK_METADATA_SIZE_IN_BYTE = 1536L;

  /**
   * Average queue length in memtable pool
   */
  static final int MEM_TABLE_AVERAGE_QUEUE_LEN = 5;

  // static memory section

  /**
   * Static memory, includes all timeseries metadata, which equals to TIMESERIES_METADATA_SIZE_IN_BYTE *
   * totalTimeseriesNum, the unit is byte
   */
  private long staticMemory;

  private int totalTimeseries;

  // MemTable section

  private int maxMemTableNum = MEM_TABLE_AVERAGE_QUEUE_LEN;

  private int currentMemTableSize;

  // Adapter section

  private boolean initialized = false;

  @Override
  public synchronized boolean tryToAdaptParameters() {
    boolean shouldAdjust = true;
    int memtableSizeInByte = calcMemTableSize();
    int memTableSizeFloorThreshold = getMemTableSizeFloorThreshold();
    boolean shouldClose = false;
    long tsFileSize = CONFIG.getTsFileSizeThreshold();
    if (memtableSizeInByte < memTableSizeFloorThreshold) {
      shouldClose = true;
      tsFileSize = calcTsFileSize(memTableSizeFloorThreshold);
      memtableSizeInByte = (int) tsFileSize;
      if (tsFileSize < memTableSizeFloorThreshold) {
        shouldAdjust = false;
      }
    }

    if (shouldAdjust) {
      CONFIG.setMaxMemtableNumber(maxMemTableNum);
      CONFIG.setTsFileSizeThreshold(tsFileSize);
      CONFIG.setMemtableSizeThreshold(memtableSizeInByte);
      if(initialized) {
        if (shouldClose) {
          StorageEngine.getInstance().asyncFlushAndSealAllFiles();
        } else if (memtableSizeInByte < currentMemTableSize
            && currentMemTableSize - memtableSizeInByte > currentMemTableSize * FLUSH_THRESHOLD) {
          StorageEngine.getInstance().asyncFlushAllProcessor();
        }
      }
      currentMemTableSize = memtableSizeInByte;
    }
    if (!initialized) {
      CONFIG.setMaxMemtableNumber(maxMemTableNum);
      return true;
    }
    return shouldAdjust;
  }

  /**
   * Calculate appropriate MemTable size
   *
   * @return MemTable size. If the value is -1, there is no valid solution.
   */
  private int calcMemTableSize() {
    double ratio = CompressionRatio.getInstance().getRatio();
    // when unit is byte, it's likely to cause Long type overflow. so use the unit KB.
    double a = (long) (ratio * maxMemTableNum);
    double b = (long) ((ALLOCATE_MEMORY_FOR_WRITE - staticMemory) * ratio);
    int times = b > Integer.MAX_VALUE ? 1024 : 1;
    b /= times;
    double c = (double) CONFIG.getTsFileSizeThreshold() * maxMemTableNum * CHUNK_METADATA_SIZE_IN_BYTE
        * MManager
        .getInstance().getMaximalSeriesNumberAmongStorageGroups() / times / times;
    double tempValue = b * b - 4 * a * c;
    double memTableSize = ((b + Math.sqrt(tempValue)) / (2 * a));
    return tempValue < 0 ? -1 : (int) (memTableSize * times);
  }

  /**
   * Calculate appropriate Tsfile size based on MemTable size
   *
   * @param memTableSize MemTable size
   * @return Tsfile threshold
   */
  private int calcTsFileSize(int memTableSize) {
    return (int) ((ALLOCATE_MEMORY_FOR_WRITE - maxMemTableNum * memTableSize - staticMemory) * CompressionRatio
        .getInstance().getRatio()
        * memTableSize / (maxMemTableNum * CHUNK_METADATA_SIZE_IN_BYTE * MManager.getInstance()
        .getMaximalSeriesNumberAmongStorageGroups()));
  }

  /**
   * Get the floor threshold MemTable size
   */
  private int getMemTableSizeFloorThreshold() {
    return MManager.getInstance().getMaximalSeriesNumberAmongStorageGroups()
        * PrimitiveArrayPool.ARRAY_SIZE * Long.BYTES * 2;
  }

  /**
   * TODO: Currently IoTDB only supports to add a storage group.
   */
  @Override
  public void addOrDeleteStorageGroup(int diff) throws ConfigAdjusterException {
    maxMemTableNum += 2 * diff;
    if(!CONFIG.isEnableParameterAdapter()){
      CONFIG.setMaxMemtableNumber(maxMemTableNum);
      return;
    }
    if (!tryToAdaptParameters()) {
      maxMemTableNum -= 2 * diff;
      throw new ConfigAdjusterException(
          "The IoTDB system load is too large to create storage group.");
    }
  }

  @Override
  public void addOrDeleteTimeSeries(int diff) throws ConfigAdjusterException {
    if(!CONFIG.isEnableParameterAdapter()){
      return;
    }
    totalTimeseries += diff;
    staticMemory += diff * TIMESERIES_METADATA_SIZE_IN_BYTE;
    if (!tryToAdaptParameters()) {
      totalTimeseries -= diff;
      staticMemory -= diff * TIMESERIES_METADATA_SIZE_IN_BYTE;
      throw new ConfigAdjusterException("The IoTDB system load is too large to add timeseries.");
    }
  }

  public void setInitialized(boolean initialized) {
    this.initialized = initialized;
  }

  int getCurrentMemTableSize() {
    return currentMemTableSize;
  }

  int getTotalTimeseries() {
    return totalTimeseries;
  }

  /**
   * Only for test
   */
  public void reset() {
    totalTimeseries = 0;
    staticMemory = 0;
    maxMemTableNum = MEM_TABLE_AVERAGE_QUEUE_LEN;
    initialized = false;
  }

  private IoTDBConfigDynamicAdapter() {
  }

  public static IoTDBConfigDynamicAdapter getInstance() {
    return IoTDBConfigAdapterHolder.INSTANCE;
  }

  private static class IoTDBConfigAdapterHolder {

    private static final IoTDBConfigDynamicAdapter INSTANCE = new IoTDBConfigDynamicAdapter();

    private IoTDBConfigAdapterHolder() {

    }

  }
}
