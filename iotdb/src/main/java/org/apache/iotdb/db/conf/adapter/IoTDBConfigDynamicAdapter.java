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
package org.apache.iotdb.db.conf.adjuster;

import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBConstant;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.StorageEngine;
import org.apache.iotdb.db.exception.ConfigAdjusterException;
import org.apache.iotdb.db.exception.MetadataErrorException;
import org.apache.iotdb.db.exception.PathErrorException;
import org.apache.iotdb.db.metadata.MManager;
import org.apache.iotdb.db.rescon.PrimitiveArrayPool;
import org.apache.iotdb.tsfile.common.conf.TSFileConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IoTDBConfigDynamicAdjuster implements IDynamicAdjuster {

  private static final Logger LOGGER = LoggerFactory.getLogger(IoTDBConfigDynamicAdjuster.class);

  private static final IoTDBConfig CONFIG = IoTDBDescriptor.getInstance().getConfig();

  // static parameter section

  private static final float MAX_MEMORY_RATIO = 0.8f;

  private static final float COMPRESSION_RATIO = 0.8f;

  /**
   * Maximum amount of memory that the Java virtual machine will attempt to use
   */
  private static final long MAX_MEMORY_B = (long) (Runtime.getRuntime().maxMemory() * MAX_MEMORY_RATIO);

  /**
   * Metadata size of per timeseries, the default value is 2KB.
   */
  private static final long TIMESERIES_METADATA_SIZE_B = 2L * 1024;

  /**
   * Metadata size of per chunk, the default value is 1.5 KB.
   */
  private static final long CHUNK_METADATA_SIZE_B = 1536L;

  /**
   * Average queue length in memtable pool
   */
  private static final int MEM_TABLE_AVERAGE_QUEUE_LEN = 5;

  // static memory section

  private int totalTimeseriesNum;

  /**
   * Static memory, includes all timeseries metadata, which equals to TIMESERIES_METADATA_SIZE_B *
   * totalTimeseriesNum, the unit is byte
   */
  private long staticMemory;

  // MemTable section

  private int totalStorageGroupNum;

  private int maxMemTableNum;

  private int currentMemTableSize;

  @Override
  public void init() {
    try {
      totalStorageGroupNum = MManager.getInstance().getAllStorageGroup().size();
      totalTimeseriesNum = MManager.getInstance().getPaths(IoTDBConstant.PATH_ROOT).size();
    } catch (PathErrorException e) {
      LOGGER.error("Getting total storage group num meets error, use default value 0.", e);
    } catch (MetadataErrorException e) {
      LOGGER.error("Getting total timeseries num meets error, use default value 0.", e);
    }
    maxMemTableNum = (totalStorageGroupNum << 1) + MEM_TABLE_AVERAGE_QUEUE_LEN;
    staticMemory = totalTimeseriesNum * TIMESERIES_METADATA_SIZE_B;
    tryToAdjustParameters();
  }

  @Override
  public boolean tryToAdjustParameters() {
    boolean shouldAdjust = true;
    int memtableSizeInByte = calcuMemTableSize();
    int memTableSizeFloorThreshold = getMemTableSizeFloorThreshold();
    boolean shouldClose = false;
    long tsFileSize = CONFIG.getTsFileSizeThreshold();
    if (memtableSizeInByte < memTableSizeFloorThreshold) {
      memtableSizeInByte = memTableSizeFloorThreshold;
      shouldClose = true;
      tsFileSize = calcuTsFileSize(memTableSizeFloorThreshold);
      if (tsFileSize < memTableSizeFloorThreshold) {
        shouldAdjust = false;
      }
    } else if(memtableSizeInByte > tsFileSize){
      memtableSizeInByte = (int) tsFileSize;
    }

    if (shouldAdjust) {
      CONFIG.setMaxMemtableNumber(maxMemTableNum);
      CONFIG.setTsFileSizeThreshold(tsFileSize);
      TSFileConfig.groupSizeInByte = memtableSizeInByte;
      if (shouldClose) {
        StorageEngine.getInstance().asyncFlushAndSealAllFiles();
      } else if (memtableSizeInByte < currentMemTableSize) {
        StorageEngine.getInstance().asyncFlushAllProcessor();
      }
      currentMemTableSize = memtableSizeInByte;
    }
    return shouldAdjust;
  }

  private int calcuMemTableSize() {
    long a = (long) (COMPRESSION_RATIO * maxMemTableNum);
    long b = (long) ((staticMemory - MAX_MEMORY_B) * COMPRESSION_RATIO);
    long c = CONFIG.getTsFileSizeThreshold() * maxMemTableNum * CHUNK_METADATA_SIZE_B * MManager
        .getInstance().getMaximalSeriesNumberAmongStorageGroups();
    double memTableSize1 = ((-b + Math.sqrt(b * b - 4 * a * c)) / (2 * a));
    System.out.println(memTableSize1/IoTDBConstant.MB);
    System.out.println(((-b - Math.sqrt(b * b - 4 * a * c)) / (2 * a))/ IoTDBConstant.MB);
    return (int) memTableSize1;
  }

  private int calcuTsFileSize(int memTableSize) {
    return (int) ((MAX_MEMORY_B - maxMemTableNum * memTableSize - staticMemory) * COMPRESSION_RATIO
        * memTableSize / (maxMemTableNum * CHUNK_METADATA_SIZE_B * MManager.getInstance()
        .getMaximalSeriesNumberAmongStorageGroups()));
  }

  /**
   * Get the floor threshold memtable size
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
    totalStorageGroupNum += diff;
    maxMemTableNum += 2 * diff;
    if (!tryToAdjustParameters()) {
      totalStorageGroupNum -= diff;
      maxMemTableNum -= 2;
      throw new ConfigAdjusterException(
          "The IoTDB system load is too large to create storage group.");
    }
  }

  @Override
  public void addOrDeleteTimeSeries(int diff) throws ConfigAdjusterException {
    totalTimeseriesNum += diff;
    staticMemory += diff * TIMESERIES_METADATA_SIZE_B;
    if (!tryToAdjustParameters()) {
      totalTimeseriesNum -= diff;
      staticMemory -= diff * TIMESERIES_METADATA_SIZE_B;
      throw new ConfigAdjusterException("The IoTDB system load is too large to add timeseries.");
    }
  }

  public int getCurrentMemTableSize() {
    return currentMemTableSize;
  }

  private IoTDBConfigDynamicAdjuster() {
    init();
  }

  public static IoTDBConfigDynamicAdjuster getInstance() {
    return IoTDBConfigAdjusterHolder.INSTANCE;
  }

  private static class IoTDBConfigAdjusterHolder {

    private static final IoTDBConfigDynamicAdjuster INSTANCE = new IoTDBConfigDynamicAdjuster();

    private IoTDBConfigAdjusterHolder() {

    }

  }

}
