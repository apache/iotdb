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
package org.apache.iotdb.db.conf.adapter;

import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.ConfigAdjusterException;
import org.apache.iotdb.db.rescon.PrimitiveArrayPool;
import org.apache.iotdb.db.service.IoTDB;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class is to dynamically adjust some important parameters of the system, determine the speed
 * of MenTable brushing disk, the speed of file sealing and so on, with the continuous change of
 * load in the process of system operation.
 * <p>
 * There are three dynamically adjustable parameters: maxMemTableNum, memtableSize and
 * tsFileSizeThreshold.
 * <p>
 * 1. maxMemTableNum. This parameter represents the size of the MemTable available in the MemTable
 * pool, which is closely related to the number of storage groups. When adding or deleting a storage
 * group, the parameter also adds or deletes four MemTables. The reason why adding or deleting four
 * MemTables is that when the system is running stably, the speed of the flush operation is faster
 * than that of data writing, so one is used for the Flush process and the other is used for data
 * writing. Otherwise, the system should limit the speed of data writing to maintain stability. And
 * two for sequence data, two for unsequence data.
 * <p>
 * 2. memtableSize. This parameter determines the threshold value for the MemTable in memory to be
 * flushed into disk. When the system load increases, the parameter should be set smaller so that
 * the data in memory can be flushed into disk as soon as possible.
 * <p>
 * 3. tsFileSizeThreshold. This parameter determines the speed of the tsfile seal, and then
 * determines the maximum size of metadata information maintained in memory. When the system load
 * increases, the parameter should be smaller to seal the file as soon as possible, release the
 * memory occupied by the corresponding metadata information as soon as possible.
 * <p>
 * The following equation is used to adjust the dynamic parameters of the data:
 * <p>
 * *
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
 * The equation: m * Nm + Nm * Ns * Sf * a * c / m + b = S
 * Namely: MemTable data memory size + chunk metadata memory size + static memory size = memory size for write
 *
 */
public class IoTDBConfigDynamicAdapter implements IDynamicAdapter {

  public static final String CREATE_STORAGE_GROUP = "create storage group";
  public static final String ADD_TIMESERIES = "add timeseries";
  /**
   * Average queue length in memtable pool
   */
  static final int MEM_TABLE_AVERAGE_QUEUE_LEN = 5;
  private static final Logger LOGGER = LoggerFactory.getLogger(IoTDBConfigDynamicAdapter.class);

  // static parameter section
  private static final IoTDBConfig CONFIG = IoTDBDescriptor.getInstance().getConfig();
  /**
   * Metadata size of per timeseries, the default value is 2KB.
   */
  private static final long TIMESERIES_METADATA_SIZE_IN_BYTE = 2L * 1024;
  private static final double WAL_MEMORY_RATIO = 0.1;
  public static final int MEMTABLE_NUM_FOR_EACH_PARTITION = 4;
  /**
   * Maximum amount of memory allocated for write process.
   */
  private static long allocateMemoryForWrite = CONFIG.getAllocateMemoryForWrite();
  /**
   * Metadata size of per chunk, the default value is 1.5 KB.
   */
  private static long CHUNK_METADATA_SIZE_IN_BYTE = 1536L;

  // static memory section
  /**
   * Static memory, includes all timeseries metadata, which equals to
   * TIMESERIES_METADATA_SIZE_IN_BYTE * totalTimeseriesNum, the unit is byte.
   * <p>
   * Currently, we think that static memory only consists of time series metadata information. We
   * ignore the memory occupied by the tsfile information maintained in memory, because we think
   * that this part occupies very little memory.
   */
  private long staticMemory;

  private int totalStorageGroup;

  private int totalTimeseries;

  // MemTable section

  private int maxMemTableNum = MEM_TABLE_AVERAGE_QUEUE_LEN;

  private long currentMemTableSize;

  // Adapter section

  private boolean initialized = false;

  private IoTDBConfigDynamicAdapter() {
  }

  public static void setChunkMetadataSizeInByte(long chunkMetadataSizeInByte) {
    CHUNK_METADATA_SIZE_IN_BYTE = chunkMetadataSizeInByte;
  }

  public static IoTDBConfigDynamicAdapter getInstance() {
    return IoTDBConfigAdapterHolder.INSTANCE;
  }

  @Override
  public synchronized boolean tryToAdaptParameters() {
    if (!CONFIG.isEnableParameterAdapter()) {
      return true;
    }
    boolean canAdjust = true;
    double ratio = CompressionRatio.getInstance().getRatio();
    long memtableSizeInByte = calcMemTableSize(ratio);
    long memTableSizeFloorThreshold = getMemTableSizeFloorThreshold();
    long tsFileSizeThreshold = CONFIG.getTsFileSizeThreshold();
    if (memtableSizeInByte < memTableSizeFloorThreshold) {
      if (LOGGER.isDebugEnabled() && initialized) {
        LOGGER.debug("memtableSizeInByte {} is smaller than memTableSizeFloorThreshold {}",
            memtableSizeInByte, memTableSizeFloorThreshold);
      }
      tsFileSizeThreshold = calcTsFileSizeThreshold(memTableSizeFloorThreshold, ratio);
      if ((long) (tsFileSizeThreshold * ratio) < memTableSizeFloorThreshold) {
        canAdjust = false;
      } else {
        // memtableSizeInByte need to be larger than memTableSizeFloorThreshold
        memtableSizeInByte = Math.max(memTableSizeFloorThreshold,
            memTableSizeFloorThreshold + (
                ((long) (tsFileSizeThreshold * ratio) - memTableSizeFloorThreshold)
                    >> 1));
      }
    }

    if (canAdjust) {
      CONFIG.setMaxMemtableNumber(maxMemTableNum);
      CONFIG.setWalBufferSize(
          (int) Math
              .min(Integer.MAX_VALUE, allocateMemoryForWrite * WAL_MEMORY_RATIO / maxMemTableNum));
      CONFIG.setTsFileSizeThreshold(tsFileSizeThreshold);
      CONFIG.setMemtableSizeThreshold(memtableSizeInByte);
      if (LOGGER.isDebugEnabled() && initialized) {
        LOGGER.debug(
            "After adjusting, max memTable num is {}, tsFile threshold is {}, memtableSize is {}, memTableSizeFloorThreshold is {}, storage group = {}, total timeseries = {}, the max number of timeseries among storage groups = {}",
            maxMemTableNum, tsFileSizeThreshold, memtableSizeInByte, memTableSizeFloorThreshold,
            totalStorageGroup, totalTimeseries,
            IoTDB.metaManager.getMaximalSeriesNumberAmongStorageGroups());
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
   * Calculate appropriate MemTable size. Computing method refers to class annotations.
   *
   * @return MemTable byte size. If the value is -1, there is no valid solution.
   */
  private long calcMemTableSize(double ratio) {
    // when unit is byte, it's likely to cause Long type overflow.
    // so when b is larger than Integer.MAC_VALUE use the unit KB.
    double a = maxMemTableNum;
    double b = allocateMemoryForWrite * (1 - WAL_MEMORY_RATIO) - staticMemory;
    int magnification = b > Integer.MAX_VALUE ? 1024 : 1;
    b /= magnification;
    double c =
        (double) CONFIG.getTsFileSizeThreshold() * maxMemTableNum * CHUNK_METADATA_SIZE_IN_BYTE
            * IoTDB.metaManager.getMaximalSeriesNumberAmongStorageGroups() * ratio
            / magnification / magnification;
    double tempValue = b * b - 4 * a * c;
    double memTableSize = ((b + Math.sqrt(tempValue)) / (2 * a));
    return tempValue < 0 ? -1 : (long) (memTableSize * magnification);
  }

  /**
   * Calculate appropriate Tsfile size based on MemTable size. Computing method refers to class
   * annotations.
   *
   * @param memTableSize MemTable size
   * @return Tsfile byte threshold
   */
  private long calcTsFileSizeThreshold(long memTableSize, double ratio) {
    return (long) ((allocateMemoryForWrite * (1 - WAL_MEMORY_RATIO) - maxMemTableNum * memTableSize
        - staticMemory) * memTableSize / (ratio * maxMemTableNum * CHUNK_METADATA_SIZE_IN_BYTE
        * IoTDB.metaManager.getMaximalSeriesNumberAmongStorageGroups()));
  }

  /**
   * Get the floor threshold MemTable size. For Primitive Array, we think that the maximum memory
   * occupied by each value is 8 bytes. The reason for multiplying 2 is that the timestamp also
   * takes 8 bytes.
   */
  private long getMemTableSizeFloorThreshold() {
    return IoTDB.metaManager.getMaximalSeriesNumberAmongStorageGroups()
        * PrimitiveArrayPool.ARRAY_SIZE * Long.BYTES * 2;
  }

  @Override
  public void addOrDeleteStorageGroup(int diff) throws ConfigAdjusterException {
    totalStorageGroup += diff;
    maxMemTableNum +=
        MEMTABLE_NUM_FOR_EACH_PARTITION * IoTDBDescriptor.getInstance().getConfig().getConcurrentWritingTimePartition() * diff
            + diff;
    if (!CONFIG.isEnableParameterAdapter()) {
      CONFIG.setMaxMemtableNumber(maxMemTableNum);
      return;
    }

    if (!tryToAdaptParameters()) {
      totalStorageGroup -= diff;
      maxMemTableNum -=
          MEMTABLE_NUM_FOR_EACH_PARTITION * IoTDBDescriptor.getInstance().getConfig().getConcurrentWritingTimePartition() * diff
              + diff;
      throw new ConfigAdjusterException(CREATE_STORAGE_GROUP);
    }
  }

  @Override
  public void addOrDeleteTimeSeries(int diff) throws ConfigAdjusterException {
    if (!CONFIG.isEnableParameterAdapter()) {
      return;
    }
    totalTimeseries += diff;
    staticMemory += diff * TIMESERIES_METADATA_SIZE_IN_BYTE;
    if (!tryToAdaptParameters()) {
      totalTimeseries -= diff;
      staticMemory -= diff * TIMESERIES_METADATA_SIZE_IN_BYTE;
      throw new ConfigAdjusterException(ADD_TIMESERIES);
    }
  }

  public void setInitialized(boolean initialized) {
    this.initialized = initialized;
  }

  long getCurrentMemTableSize() {
    return currentMemTableSize;
  }

  public int getTotalTimeseries() {
    return totalTimeseries;
  }

  public int getTotalStorageGroup() {
    return totalStorageGroup;
  }

  /**
   * Only for test
   */
  public void reset() {
    totalTimeseries = 0;
    staticMemory = 0;
    maxMemTableNum = MEM_TABLE_AVERAGE_QUEUE_LEN;
    allocateMemoryForWrite = CONFIG.getAllocateMemoryForWrite();
    initialized = false;
  }

  private static class IoTDBConfigAdapterHolder {

    private static final IoTDBConfigDynamicAdapter INSTANCE = new IoTDBConfigDynamicAdapter();

    private IoTDBConfigAdapterHolder() {

    }

  }

}
