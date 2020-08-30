/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.db.monitor;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.iotdb.db.concurrent.WrappedRunnable;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.StorageEngine;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.exception.metadata.IllegalPathException;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.metadata.MManager;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.monitor.MonitorConstants.StatConstants;
import org.apache.iotdb.db.monitor.collector.FileSize;
import org.apache.iotdb.db.qp.physical.crud.InsertRowPlan;
import org.apache.iotdb.db.service.IoTDB;
import org.apache.iotdb.tsfile.common.conf.TSFileDescriptor;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.write.record.TSRecord;
import org.apache.iotdb.tsfile.write.record.datapoint.LongDataPoint;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StatMonitor {

  private static final Logger logger = LoggerFactory.getLogger(StatMonitor.class);
  private static IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();
  private static MManager mManager = IoTDB.metaManager;

  public static AtomicLong globalPointsNum = new AtomicLong();
  public static AtomicLong globalReqSuccessNum = new AtomicLong();
  public static AtomicLong globalReqFailNum = new AtomicLong();

  /**
   * key: is the statistics store seriesPath Value: is an interface that implements statistics
   * function.
   */
  private final HashMap<String, IStatistic> statisticMap;

  private StatMonitor() {
    if (config.isEnableStatMonitor()) {
      registerStatStorageGroup();
      registerStatTimeSeries();
    }
  }

  public static StatMonitor getInstance() {
    return StatMonitorHolder.INSTANCE;
  }

  /**
   * generate TSRecord.
   *
   * @param hashMap            key is statParams name, values is AtomicLong type
   * @param statGroupDeltaName is the deviceId seriesPath of this module
   * @param curTime            current time stamp
   * @return TSRecord contains the DataPoints of a statGroupDeltaName
   */
  public static TSRecord convertToTSRecord(Map<String, AtomicLong> hashMap,
      String statGroupDeltaName, long curTime) {
    TSRecord tsRecord = new TSRecord(curTime, statGroupDeltaName);
    tsRecord.dataPointList = new ArrayList<>();
    for (Map.Entry<String, AtomicLong> entry : hashMap.entrySet()) {
      AtomicLong value = entry.getValue();
      tsRecord.dataPointList.add(new LongDataPoint(entry.getKey(), value.get()));
    }
    return tsRecord;
  }

  public void registerStatStorageGroup() {
    PartialPath storageGroupPrefix = new PartialPath(MonitorConstants.STAT_STORAGE_GROUP_ARRAY);
    try {
      if (!mManager.isPathExist(storageGroupPrefix)) {
        mManager.setStorageGroup(storageGroupPrefix);
      }
    } catch (Exception e) {
      logger.error("MManager cannot set storage group to MTree.", e);
    }
  }

  /**
   * register monitor statistics time series metadata into MManager.
   */
  public synchronized void registerStatTimeSeries() {
    try {
      for (StatConstants statConstant : StatConstants.values()) {
        PartialPath fullPath = new PartialPath(MonitorConstants.STAT_GLOBAL_ARRAY)
            .concatNode(statConstant.getMeasurement());

        if (!mManager.isPathExist(fullPath)) {
          mManager.createTimeseries(fullPath,
              TSDataType.valueOf(MonitorConstants.INT64),
              TSEncoding.valueOf("RLE"),
              TSFileDescriptor.getInstance().getConfig().getCompressor(),
              Collections.emptyMap());
        }
      }
    } catch (MetadataException e) {
      logger.error("Initialize the metadata error.", e);
    }
  }

  /**
   * register class which implemented IStatistic interface into statisticMap
   *
   * @param path       the stat series prefix path, like root.stat.file.size
   * @param iStatistic instance of class which implemented IStatistic interface
   */
  public void registerStatistics(String path, IStatistic iStatistic) {
    synchronized (statisticMap) {
      logger.debug("Register {} to StatMonitor for statistics service", path);
      this.statisticMap.put(path, iStatistic);
    }
  }

  /**
   * deregister statistics.
   */
  public void deregisterStatistics(String path) {
    logger.debug("Deregister {} in StatMonitor for stopping statistics service", path);
    synchronized (statisticMap) {
      if (statisticMap.containsKey(path)) {
        statisticMap.put(path, null);
      }
    }
  }

  /**
   * get all statistics.
   */
  public Map<String, TSRecord> gatherStatistics() {
    synchronized (statisticMap) {
      long currentTimeMillis = System.currentTimeMillis();
      HashMap<String, TSRecord> tsRecordHashMap = new HashMap<>();
      for (Map.Entry<String, IStatistic> entry : statisticMap.entrySet()) {
        if (entry.getValue() == null) {
          switch (entry.getKey()) {
            case MonitorConstants.STAT_STORAGE_DELTA_NAME:
              tsRecordHashMap.put(entry.getKey(),
                  convertToTSRecord(
                      MonitorConstants.initValues(MonitorConstants.FILENODE_PROCESSOR_CONST),
                      entry.getKey(), currentTimeMillis));
              break;
            case MonitorConstants.FILE_SIZE_STORAGE_GROUP_NAME:
              tsRecordHashMap.put(entry.getKey(),
                  convertToTSRecord(
                      MonitorConstants.initValues(MonitorConstants.FILE_SIZE_CONST),
                      entry.getKey(), currentTimeMillis));
              break;
            default:
          }
        } else {
          tsRecordHashMap.putAll(entry.getValue().getAllStatisticsValue());
        }
      }
      for (TSRecord value : tsRecordHashMap.values()) {
        value.time = currentTimeMillis;
      }
      return tsRecordHashMap;
    }
  }

  private static class StatMonitorHolder {

    private StatMonitorHolder() {
      //allowed do nothing
    }

    private static final StatMonitor INSTANCE = new StatMonitor();
  }

  class StatBackLoop extends WrappedRunnable {

    FileSize fileSize = FileSize.getInstance();

    @Override
    public void runMayThrow() {
      try {
        long currentTimeMillis = System.currentTimeMillis();
        long seconds = (currentTimeMillis - runningTimeMillis) / 1000;
        if (seconds >= statMonitorDetectFreqSec) {
          runningTimeMillis = currentTimeMillis;
          // delete time-series data
          cleanOutDated();
        }
        Map<String, TSRecord> tsRecordHashMap = gatherStatistics();
        insert(tsRecordHashMap);
      } catch (Exception e) {
        logger.error("Error occurred in Stat Monitor thread", e);
      }
    }

    public void insert(Map<String, TSRecord> tsRecordHashMap) {
      StorageEngine fManager = StorageEngine.getInstance();
      int pointNum;
      for (Map.Entry<String, TSRecord> entry : tsRecordHashMap.entrySet()) {
        try {
          fManager.insert(new InsertRowPlan(entry.getValue()));
          numInsert.incrementAndGet();
          pointNum = entry.getValue().dataPointList.size();
          numPointsInsert.addAndGet(pointNum);
        } catch (StorageEngineException | IllegalPathException e) {
          numInsertError.incrementAndGet();
          logger.error("Inserting stat points error.", e);
        }
      }
    }
  }
}
