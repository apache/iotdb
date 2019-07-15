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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class is to dynamically adjust some important parameters of the system, determine the speed
 * of MenTable brushing disk, the speed of file sealing and so on, with the continuous change of
 * load in the process of system operation.
 *
 * There are three dynamically adjustable parameters: maxMemTableNum, memtableSize and
 * tsFileSizeThreshold.
 *
 * 1. maxMemTableNum. This parameter represents the size of the MemTable available in the MemTable
 * pool, which is closely related to the number of storage groups. When adding or deleting a storage
 * group, the parameter also adds or deletes two MemTables. The reason why adding or deleting two
 * MemTables is that when the system is running stably, the speed of the flush operation is faster
 * than that of data writing, so one is used for the Flush process and the other is used for data
 * writing. Otherwise, the system should limit the speed of data writing to maintain stability.
 *
 * 2. memtableSize. This parameter determines the threshold value for the MemTable in memory to be
 * flushed into disk. When the system load increases, the parameter should be set smaller so that
 * the data in memory can be flushed into disk as soon as possible.
 *
 * 3. tsFileSizeThreshold. This parameter determines the speed of the tsfile seal, and then
 * determines the maximum size of metadata information maintained in memory. When the system load
 * increases, the parameter should be smaller to seal the file as soon as possible, release the
 * memory occupied by the corresponding metadata information as soon as possible.
 *
 * The following equation is used to adjust the dynamic parameters of the data:
 *
 * Abbreviation of parameters:
 * 1 memtableSize: m
 * 2 maxMemTableNum: Nm
 * 3 maxSeriesNumberAmongStorageGroup: Ns
 * 4 tsFileSizeThreshold: Sf
 * 5 CompressionRatio: c
 * 6 chunk metadata size: a
 * 7 static memory: b
 * 8 allocate memory for write: S
 *
 * The equation: m * Nm + Nm * Ns * Sf / (m * a * c) + b = S
 * Namely: MemTable data memory size + chunk metadata memory size + static memory size = memory size for write
 *
 */
public class IoTDBConfigDynamicAdapter implements IDynamicAdapter {

  private static final Logger LOGGER = LoggerFactory.getLogger(IoTDBConfigDynamicAdapter.class);

  private static final IoTDBConfig CONFIG = IoTDBDescriptor.getInstance().getConfig();

  // static parameter section

  /**
   * When the size of the adjusted MemTable decreases more than this parameter, trigger the global
   * flush operation and flush all MemTable that meets the flush condition to disk.
   */
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
   * Static memory, includes all timeseries metadata, which equals to
   * TIMESERIES_METADATA_SIZE_IN_BYTE * totalTimeseriesNum, the unit is byte.
   *
   * Currently， we think that static memory only consists of time series metadata information.
   * We ignore the memory occupied by the tsfile information maintained in memory,
   * because we think that this part occupies very little memory.
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
    boolean canAdjust = true;
    int memtableSizeInByte = calcMemTableSize();
    int memTableSizeFloorThreshold = getMemTableSizeFloorThreshold();
    boolean shouldClose = false;
    long tsFileSize = CONFIG.getTsFileSizeThreshold();
    if (memtableSizeInByte < memTableSizeFloorThreshold) {
      LOGGER.debug("memtableSizeInByte {} is smaller than memTableSizeFloorThreshold {}",
          memtableSizeInByte, memTableSizeFloorThreshold);
      shouldClose = true;
      tsFileSize = calcTsFileSize(memTableSizeFloorThreshold);
      memtableSizeInByte = (int) tsFileSize;
      if (tsFileSize < memTableSizeFloorThreshold) {
        canAdjust = false;
      }
    }

    if (canAdjust) {
      CONFIG.setMaxMemtableNumber(maxMemTableNum);
      CONFIG.setTsFileSizeThreshold(tsFileSize);
      CONFIG.setMemtableSizeThreshold(memtableSizeInByte);
      LOGGER.debug(
          "After adjusting, max memTable num is {}, tsFile threshold is {}, memtableSize is {}, memTableSizeFloorThreshold is {}",
          maxMemTableNum, tsFileSize, memtableSizeInByte, memTableSizeFloorThreshold);
      if (initialized) {
        if (shouldClose) {
          StorageEngine.getInstance().asyncTryToCloseAllProcessor();
        } else if (memtableSizeInByte < currentMemTableSize
            && currentMemTableSize - memtableSizeInByte > currentMemTableSize * FLUSH_THRESHOLD) {
          StorageEngine.getInstance().asyncTryToFlushAllProcessor();
        }
      }
      currentMemTableSize = memtableSizeInByte;
    }
    if (!initialized) {
      CONFIG.setMaxMemtableNumber(maxMemTableNum);
      return true;
    }
    return canAdjust;
  }

  /**
   * Calculate appropriate MemTable size.
   * Computing method refers to class annotations.
   *
   * @return MemTable byte size. If the value is -1, there is no valid solution.
   */
  private int calcMemTableSize() {
    double ratio = CompressionRatio.getInstance().getRatio();
    // when unit is byte, it's likely to cause Long type overflow.
    // so when b is larger than Integer.MAC_VALUE use the unit KB.
    double a = (long) (ratio * maxMemTableNum);
    double b = (long) ((ALLOCATE_MEMORY_FOR_WRITE - staticMemory) * ratio);
    int magnification = b > Integer.MAX_VALUE ? 1024 : 1;
    b /= magnification;
    double c = (double) CONFIG.getTsFileSizeThreshold() * maxMemTableNum * CHUNK_METADATA_SIZE_IN_BYTE
        * MManager
        .getInstance().getMaximalSeriesNumberAmongStorageGroups() / magnification / magnification;
    double tempValue = b * b - 4 * a * c;
    double memTableSize = ((b + Math.sqrt(tempValue)) / (2 * a));
    return tempValue < 0 ? -1 : (int) (memTableSize * magnification);
  }

  /**
   * Calculate appropriate Tsfile size based on MemTable size.
   * Computing method refers to class annotations.
   *
   * @param memTableSize MemTable size
   * @return Tsfile byte threshold
   */
  private int calcTsFileSize(int memTableSize) {
    return (int) ((ALLOCATE_MEMORY_FOR_WRITE - maxMemTableNum * memTableSize - staticMemory) * CompressionRatio
        .getInstance().getRatio()
        * memTableSize / (maxMemTableNum * CHUNK_METADATA_SIZE_IN_BYTE * MManager.getInstance()
        .getMaximalSeriesNumberAmongStorageGroups()));
  }

  /**
   * Get the floor threshold MemTable size. For Primitive Array, we think that the maximum memory
   * occupied by each value is 8 bytes. The reason for multiplying 2 is that the timestamp also
   * takes 8 bytes.
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
    maxMemTableNum += 4 * diff;
    if(!CONFIG.isEnableParameterAdapter()){
      CONFIG.setMaxMemtableNumber(maxMemTableNum);
      return;
    }
    if (!tryToAdaptParameters()) {
      maxMemTableNum -= 4 * diff;
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
