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

import com.clearspring.analytics.stream.cardinality.HyperLogLog;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.apache.iotdb.db.conf.IoTDBConstant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ActiveTimeSeriesCounter implements IActiveTimeSeriesCounter {

  private static final Logger LOGGER = LoggerFactory.getLogger(ActiveTimeSeriesCounter.class);
  /**
   * Map[StorageGroup, HyperLogLogCounter]
   */
  private static Map<String, HyperLogLog> storageGroupHllMap = new ConcurrentHashMap<>();

  /**
   * Map[StorageGroup, ActiveTimeSeriesRatio]
   */
  private static Map<String, Double> activeRatioMap = new ConcurrentHashMap<>();

  /**
   * Map[StorageGroup, ActiveTimeSeriesNumber]
   */
  private static Map<String, Long> activeTimeSeriesNumMap = new ConcurrentHashMap<>();

  /**
   * LOG2M decide the precision of the HyperLogLog algorithm
   */
  static final int LOG2M = 13;

  private ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

  @Override
  public void init(String storageGroup) {
    storageGroupHllMap.put(storageGroup, new HyperLogLog(LOG2M));
    activeRatioMap.put(storageGroup, 0D);
    activeTimeSeriesNumMap.put(storageGroup, 0L);
  }

  @Override
  public void offer(String storageGroup, String device, String measurement) {
    String path = device + IoTDBConstant.PATH_SEPARATOR + measurement;
    try {
      HyperLogLog log = storageGroupHllMap.get(storageGroup);
      if (log != null) {
        log.offer(path);
      } else {
        LOGGER.warn(
            "Unknown SG {} is calling HyperLogLog.offer(). It can be ignored the sg is deleted recently",
            storageGroup);
      }
    } catch (Exception e) {
      //we do not register it if there is no the storage group.
      LOGGER.error("Storage group {} registers active time series {} failed", storageGroup, path,
          e);
    }
  }

  @Override
  public void updateActiveRatio(String storageGroup) {
    lock.writeLock().lock();
    try {
      HyperLogLog log = storageGroupHllMap.get(storageGroup);
      if (log == null) {
        LOGGER.warn(
            "Unknown SG {} is calling updateActiveRatio. It can be ignored the sg is deleted recently",
            storageGroup);
        return;
      }
      long activeTimeSeriesNum = storageGroupHllMap.get(storageGroup).cardinality();
      if (activeTimeSeriesNum != activeTimeSeriesNumMap.get(storageGroup)) {
        // update the active time series number in the newest memtable to be flushed
        activeTimeSeriesNumMap.put(storageGroup, activeTimeSeriesNum);

        double totalActiveTsNum = 0;
        LOGGER.debug("{}: updating active ratio", Thread.currentThread().getName());
        for (double number : activeTimeSeriesNumMap.values()) {
          totalActiveTsNum += number;
        }
        for (Map.Entry<String, Long> entry : activeTimeSeriesNumMap.entrySet()) {
          double activeRatio = 0;
          if (totalActiveTsNum > 0) {
            activeRatio = entry.getValue() / totalActiveTsNum;
          }
          activeRatioMap.put(entry.getKey(), activeRatio);
          LOGGER.debug("{}: storage group {} has an active ratio: {}",
              Thread.currentThread().getName(),
              entry.getKey(), activeRatio);
        }
      }
      // initialize the HLL counter
      storageGroupHllMap.put(storageGroup, new HyperLogLog(LOG2M));
    } catch (Exception e) {
      LOGGER.error("Update {} active ratio failed", storageGroup, e);
    } finally {
      lock.writeLock().unlock();
    }
  }

  @Override
  public double getActiveRatio(String storageGroup) {
    lock.readLock().lock();
    double ratio;
    try {
      ratio = activeRatioMap.get(storageGroup);
    } catch (Exception e) {
      ratio = 0;
      LOGGER.error("Get active ratio failed", e);
    } finally {
      lock.readLock().unlock();
    }
    return ratio;
  }

  @Override
  public void delete(String storageGroup) {
    storageGroupHllMap.remove(storageGroup);
    activeRatioMap.remove(storageGroup);
    activeTimeSeriesNumMap.remove(storageGroup);
  }

  private static class ActiveTimeSeriesCounterHolder {
    private static final ActiveTimeSeriesCounter INSTANCE = new ActiveTimeSeriesCounter();
  }

  public static ActiveTimeSeriesCounter getInstance() {
    return ActiveTimeSeriesCounterHolder.INSTANCE;
  }

  /**
   * this method is for test
   */
  public static void clear() {
    storageGroupHllMap = new ConcurrentHashMap<>();
    activeRatioMap = new ConcurrentHashMap<>();
    activeTimeSeriesNumMap = new ConcurrentHashMap<>();
  }
}
