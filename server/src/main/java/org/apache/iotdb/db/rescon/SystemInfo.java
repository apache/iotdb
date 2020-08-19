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

import java.util.TreeMap;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.flush.FlushManager;
import org.apache.iotdb.db.engine.storagegroup.StorageGroupInfo;
import org.apache.iotdb.db.engine.storagegroup.TsFileProcessor;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SystemInfo {

  private static final IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();
  private static final Logger logger = LoggerFactory.getLogger(SystemInfo.class);

  private long totalSgInfoMemCost;
  private long arrayPoolMemCost;
  private boolean rejected = false;

  private TreeMap<StorageGroupInfo, Long> reportedSgMemCostMap = new TreeMap<>(
      (o1, o2) -> Long.compare(o2.getSgMemCost(), o1.getSgMemCost()));

  private static final double FLUSH_PROPORTION = config.getFlushProportion();
  private static final double REJECT_PROPORTION = config.getRejectProportion();

  /**
   * Report applying a new out of buffered array to system. Attention: It should be invoked before
   * applying new OOB array actually.
   *
   * @param dataType data type of array
   * @param size     size of array
   * @return Return true if it's agreed when memory is enough.
   */
  public synchronized boolean applyNewOOBArray(TSDataType dataType, int size) {
    // if current memory is enough
    if (arrayPoolMemCost + totalSgInfoMemCost + dataType.getDataTypeSize() * size
        < config.getAllocateMemoryForWrite() * FLUSH_PROPORTION) {
      arrayPoolMemCost += dataType.getDataTypeSize() * size;
      logger.debug("Current total mem cost is {}", (arrayPoolMemCost + totalSgInfoMemCost));
      rejected = false;
      return true;
    } else if (arrayPoolMemCost + totalSgInfoMemCost + dataType.getDataTypeSize() * size
        < config.getAllocateMemoryForWrite() * REJECT_PROPORTION) {
      arrayPoolMemCost += dataType.getDataTypeSize() * size;
      // invoke flush()
      logger.debug("Out of buffer arraies are too large, call for flushing. "
          + "Current total mem cost is {}", (arrayPoolMemCost + totalSgInfoMemCost));
      rejected = false;
      flush();
      return true;
    } else {
      logger.debug("Out of buffer arraies are too large, call for flushing "
          + "and change system to rejected status...Current total mem cost is {}", 
          (arrayPoolMemCost + totalSgInfoMemCost));
      rejected = true;
      flush();
      return false;
    }
  }

  /**
   * Report current mem cost of storage group to system.
   *
   * @param storageGroupInfo storage group
   * @param delta mem cost
   */
  public synchronized void reportStorageGroupStatus(StorageGroupInfo storageGroupInfo) {
    long delta = storageGroupInfo.getSgMemCost() - 
        reportedSgMemCostMap.getOrDefault(storageGroupInfo, 0L);
    this.totalSgInfoMemCost += delta;
    logger.debug("Report Storage Group Status to system. "
        + "Current array pool mem cost is {}, sg mem cost is {}.", arrayPoolMemCost,
        totalSgInfoMemCost);
    reportedSgMemCostMap.put(storageGroupInfo, storageGroupInfo.getSgMemCost());
    long newSgReportThreshold = (storageGroupInfo.getSgMemCost()
        / config.getStorageGroupMemBlockSize() + 1)
        * config.getStorageGroupMemBlockSize();
    storageGroupInfo.setStorageGroupReportThreshold(newSgReportThreshold);
    if (this.arrayPoolMemCost + this.totalSgInfoMemCost
        >= config.getAllocateMemoryForWrite() * FLUSH_PROPORTION) {
      logger.info("The total storage group mem costs are too large, call for flushing.");
      flush();
    } 
    if (this.arrayPoolMemCost + this.totalSgInfoMemCost
        >= config.getAllocateMemoryForWrite() * REJECT_PROPORTION) {
      logger.info("Change system to reject status...");
      rejected = true;
    }
  }

  /**
   * Update the current mem cost of buffered array pool.
   *
   * @param increasingArraySize increasing size of buffered array
   */
  public synchronized void reportIncreasingArraySize(int increasingArraySize) {
    this.arrayPoolMemCost += increasingArraySize;
    logger.debug("Report Array Pool size to system. "
        + "Current total array pool mem cost is {}, sg mem cost is {}.",
        arrayPoolMemCost, totalSgInfoMemCost);
  }

  /**
   * Report releasing an out of buffered array to system. Attention: It should be invoked after
   * releasing.
   *
   * @param dataType data type of array
   * @param size     size of array
   */
  public synchronized void reportReleaseOOBArray(TSDataType dataType, int size) {
    this.arrayPoolMemCost -= dataType.getDataTypeSize() * size;
    if (this.arrayPoolMemCost + this.totalSgInfoMemCost 
        < config.getAllocateMemoryForWrite() * REJECT_PROPORTION) {
      logger.debug("OOB array released, change system to normal status. "
          + "Current total array cost {}.", arrayPoolMemCost);
      this.rejected = false;
    }
    else {
      logger.debug("OOB array released, but system is still in reject status. "
          + "Current array cost {}.", arrayPoolMemCost);
    }
  }

  /**
   * Report resetting the mem cost of sg to system. It will be invoked after closing file.
   *
   * @param storageGroupInfo storage group
   */
  public synchronized void resetStorageGroupInfoStatus(StorageGroupInfo storageGroupInfo) {
    if (reportedSgMemCostMap.containsKey(storageGroupInfo)) {
      this.totalSgInfoMemCost -= reportedSgMemCostMap.get(storageGroupInfo)
          - storageGroupInfo.getSgMemCost();
      if (this.arrayPoolMemCost + this.totalSgInfoMemCost 
          < config.getAllocateMemoryForWrite() * REJECT_PROPORTION) {
        logger.info("Some sg memery released, set system to normal status. "
            + "Current array cost is {}, Sg cost is {}",
            arrayPoolMemCost, totalSgInfoMemCost);
        rejected = false;
      }
      else {
        logger.warn("Some sg memery released, but system is still in reject status. "
            + "Current array cost is {}, Sg cost is {}", arrayPoolMemCost, totalSgInfoMemCost);
      }
      reportedSgMemCostMap.put(storageGroupInfo, storageGroupInfo.getSgMemCost());
    }
  }

  /**
   * Flush the tsfileProcessor in SG with the max mem cost. If the queue size of flushing > threshold,
   * it's identified as flushing is in progress.
   */
  public void flush() {
    if (FlushManager.getInstance().getNumberOfWorkingTasks() > 0) {
      return;
    }

    // get the first processor which has the max mem cost
    StorageGroupInfo storageGroupInfo = reportedSgMemCostMap.firstKey();
    TsFileProcessor flushedProcessor = storageGroupInfo.getLargestTsFileProcessor();

    if (flushedProcessor != null) {
      flushedProcessor.asyncFlush();
    }
  }

  public synchronized boolean isRejected() {
    return rejected;
  }

  public long getTotalSgMemCost() {
    return totalSgInfoMemCost;
  }

  public long getArrayPoolMemCost() {
    return arrayPoolMemCost;
  }

  public synchronized long getTotalMemCost() {
    return totalSgInfoMemCost + arrayPoolMemCost;
  }

  public void close() {
    reportedSgMemCostMap.clear();
    totalSgInfoMemCost = 0;
    arrayPoolMemCost = 0;
    rejected = false;
  }

  public static SystemInfo getInstance() {
    return InstanceHolder.instance;
  }

  private static class InstanceHolder {

    private InstanceHolder() {
    }

    private static SystemInfo instance = new SystemInfo();
  }
}
