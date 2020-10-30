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
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.iotdb.db.concurrent.IoTDBThreadPoolFactory;
import org.apache.iotdb.db.concurrent.ThreadName;
import org.apache.iotdb.db.concurrent.WrappedRunnable;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.StorageEngine;
import org.apache.iotdb.db.exception.StartupException;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.exception.metadata.IllegalPathException;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.metadata.MManager;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.monitor.MonitorConstants.FileNodeManagerStatConstants;
import org.apache.iotdb.db.monitor.MonitorConstants.FileNodeProcessorStatConstants;
import org.apache.iotdb.db.monitor.collector.FileSize;
import org.apache.iotdb.db.qp.physical.crud.InsertRowPlan;
import org.apache.iotdb.db.service.IService;
import org.apache.iotdb.db.service.IoTDB;
import org.apache.iotdb.db.service.ServiceType;
import org.apache.iotdb.tsfile.common.conf.TSFileDescriptor;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.write.record.TSRecord;
import org.apache.iotdb.tsfile.write.record.datapoint.LongDataPoint;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StatMonitor implements IService {

  private static final Logger logger = LoggerFactory.getLogger(StatMonitor.class);
  private final int backLoopPeriod;
  private final int statMonitorDetectFreqSec;
  private final int statMonitorRetainIntervalSec;
  private long runningTimeMillis = System.currentTimeMillis();
  private static final ArrayList<String> temporaryStatList = new ArrayList<>();
  /**
   * key: is the statistics store seriesPath Value: is an interface that implements statistics
   * function.
   */
  private final HashMap<String, IStatistic> statisticMap;
  private ScheduledExecutorService service;

  /**
   * stats params.
   */
  private AtomicLong numBackLoop = new AtomicLong(0);
  private AtomicLong numInsert = new AtomicLong(0);
  private AtomicLong numPointsInsert = new AtomicLong(0);
  private AtomicLong numInsertError = new AtomicLong(0);

  private StatMonitor() {
    initTemporaryStatList();
    MManager mmanager = IoTDB.metaManager;
    statisticMap = new HashMap<>();
    IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();
    statMonitorDetectFreqSec = config.getStatMonitorDetectFreqSec();
    statMonitorRetainIntervalSec = config.getStatMonitorRetainIntervalSec();
    backLoopPeriod = config.getBackLoopPeriodSec();
    if (config.isEnableStatMonitor()) {
      try {
        PartialPath prefix = new PartialPath(MonitorConstants.getStatStorageGroupPrefixArray());
        if (!mmanager.isPathExist(prefix)) {
          mmanager.setStorageGroup(prefix);
        }
      } catch (MetadataException e) {
        logger.error("MManager cannot set storage group to MTree.", e);
      }
    }
  }

  private void initTemporaryStatList() {
    for (FileNodeManagerStatConstants constants : FileNodeManagerStatConstants.values()) {
      temporaryStatList.add(constants.name());
    }
    for (FileNodeProcessorStatConstants constants : FileNodeProcessorStatConstants.values()) {
      temporaryStatList.add(constants.name());
    }
  }

  public static StatMonitor getInstance() {
    return StatMonitorHolder.INSTANCE;
  }

  /**
   * generate TSRecord.
   *
   * @param hashMap key is statParams name, values is AtomicLong type
   * @param statGroupDeltaName is the deviceId seriesPath of this module
   * @param curTime current time stamp
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

  public long getNumPointsInsert() {
    return numPointsInsert.get();
  }

  public long getNumInsert() {
    return numInsert.get();
  }

  public long getNumInsertError() {
    return numInsertError.get();
  }

  void registerStatStorageGroup() {
    MManager mManager = IoTDB.metaManager;
    PartialPath prefix = new PartialPath(MonitorConstants.getStatStorageGroupPrefixArray());
    try {
      if (!mManager.isPathExist(prefix)) {
        mManager.setStorageGroup(prefix);
      }
    } catch (Exception e) {
      logger.error("MManager cannot set storage group to MTree.", e);
    }
  }

  /**
   * register monitor statistics time series metadata into MManager.
   *
   * @param hashMap series path and data type pair, for example: [root.stat.file.size.DATA, INT64]
   */
  public synchronized void registerStatStorageGroup(Map<String, String> hashMap) {
    MManager mManager = IoTDB.metaManager;
    try {
      for (Map.Entry<String, String> entry : hashMap.entrySet()) {
        if (entry.getValue() == null) {
          logger.error("Registering metadata but data type of {} is null", entry.getKey());
        }

        if (!mManager.isPathExist(new PartialPath(entry.getKey()))) {
          mManager.createTimeseries(new PartialPath(entry.getKey()), TSDataType.valueOf(entry.getValue()),
              TSEncoding.valueOf("RLE"),
              TSFileDescriptor.getInstance().getConfig().getCompressor(),
              Collections.emptyMap());
        }
      }
    } catch (MetadataException e) {
      logger.error("Initialize the metadata error.", e);
    }
  }

  public void recovery() {
    // // restore the FildeNode Manager TOTAL_POINTS statistics info
  }

  void activate() {
    service = IoTDBThreadPoolFactory.newScheduledThreadPool(1,
        ThreadName.STAT_MONITOR.getName());
    service.scheduleAtFixedRate(
        new StatBackLoop(), 1, backLoopPeriod, TimeUnit.SECONDS);
  }

  void clearIStatisticMap() {
    statisticMap.clear();
  }

  public long getNumBackLoop() {
    return numBackLoop.get();
  }

  /**
   * register class which implemented IStatistic interface into statisticMap
   *
   * @param path the stat series prefix path, like root.stat.file.size
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
   * This function is not used and need to complete the query key concept.
   *
   * @return TSRecord, query statistics params
   */
  public Map<String, TSRecord> getOneStatisticsValue(String key) {
    // queryPath like fileNode seriesPath: root.stats.car1,
    // or StorageEngine seriesPath:StorageEngine
    String queryPath;
    if (key.contains("\\.")) {
      queryPath =
          MonitorConstants.STAT_STORAGE_GROUP_PREFIX + MonitorConstants.MONITOR_PATH_SEPARATOR
              + key.replaceAll("\\.", "_");
    } else {
      queryPath = key;
    }
    if (statisticMap.containsKey(queryPath)) {
      return statisticMap.get(queryPath).getAllStatisticsValue();
    } else {
      long currentTimeMillis = System.currentTimeMillis();
      HashMap<String, TSRecord> hashMap = new HashMap<>();
      TSRecord tsRecord = convertToTSRecord(
          MonitorConstants.initValues(MonitorConstants.FILENODE_PROCESSOR_CONST), queryPath,
          currentTimeMillis);
      hashMap.put(queryPath, tsRecord);
      return hashMap;
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


  /**
   * close statistic service.
   */
  public void close() {

    if (service == null || service.isShutdown()) {
      return;
    }
    statisticMap.clear();
    service.shutdown();
    try {
      service.awaitTermination(10, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      logger.error("StatMonitor timing service could not be shutdown.", e);
      // Restore interrupted state...
      Thread.currentThread().interrupt();
    }
  }

  @Override
  public void start() throws StartupException {
    try {
      if (IoTDBDescriptor.getInstance().getConfig().isEnableStatMonitor()) {
        activate();
      }
    } catch (Exception e) {
      throw new StartupException(this.getID().getName(), e.getMessage());
    }
  }

  @Override
  public void stop() {
    if (IoTDBDescriptor.getInstance().getConfig().isEnableStatMonitor()) {
      close();
    }
  }

  @Override
  public ServiceType getID() {
    return ServiceType.STAT_MONITOR_SERVICE;
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
        numBackLoop.incrementAndGet();
      } catch (Exception e) {
        logger.error("Error occurred in Stat Monitor thread", e);
      }
    }

    public void cleanOutDated() {
      long currentTimeMillis = System.currentTimeMillis();
      try {
        StorageEngine fManager = StorageEngine.getInstance();
        for (Map.Entry<String, IStatistic> entry : statisticMap.entrySet()) {
          for (String statParamName : entry.getValue().getStatParamsHashMap().keySet()) {
            if (temporaryStatList.contains(statParamName)) {
              fManager.delete(new PartialPath(entry.getKey(), statParamName), Long.MIN_VALUE,
                  currentTimeMillis - statMonitorRetainIntervalSec * 1000, -1);
            }
          }
        }
      } catch (StorageEngineException | IllegalPathException e) {
        logger
            .error("Error occurred when deleting statistics information periodically, because",
                e);
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
