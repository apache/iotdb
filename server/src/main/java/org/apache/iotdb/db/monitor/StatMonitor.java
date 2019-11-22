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
import org.apache.iotdb.db.concurrent.IoTDBThreadPoolFactory;
import org.apache.iotdb.db.concurrent.ThreadName;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.StorageEngine;
import org.apache.iotdb.db.exception.StartupException;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.exception.path.PathException;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.metadata.MManager;
import org.apache.iotdb.db.monitor.MonitorConstants.FileSizeMetrics;
import org.apache.iotdb.db.monitor.MonitorConstants.StorageEngineMetrics;
import org.apache.iotdb.db.monitor.MonitorConstants.TSServiceImplMetrics;
import org.apache.iotdb.db.monitor.collector.FileSize;
import org.apache.iotdb.db.qp.physical.crud.InsertPlan;
import org.apache.iotdb.db.service.IService;
import org.apache.iotdb.db.service.ServiceType;
import org.apache.iotdb.tsfile.common.conf.TSFileDescriptor;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.read.common.Path;
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

  private StatMonitor() {
    initTemporaryStatList();
    MManager mmanager = MManager.getInstance();
    statisticMap = new HashMap<>();
    IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();
    statMonitorDetectFreqSec = config.getStatMonitorDetectFreqSec();
    statMonitorRetainIntervalSec = config.getStatMonitorRetainIntervalSec();
    backLoopPeriod = config.getBackLoopPeriodSec();
    if (config.isEnableStatMonitor()) {
      try {
        String prefix = MonitorConstants.MONITOR_STORAGE_GROUP;
        if (!mmanager.pathExist(prefix)) {
          mmanager.setStorageGroupToMTree(prefix);
        }
      } catch (MetadataException e) {
        logger.error("MManager cannot set storage group to MTree.", e);
      }
    }
  }

  private void initTemporaryStatList() {
    for (StorageEngineMetrics metrics : StorageEngineMetrics.values()) {
      temporaryStatList.add(metrics.name());
    }
    for (TSServiceImplMetrics metrics : TSServiceImplMetrics.values()) {
      temporaryStatList.add(metrics.name());
    }
    for (FileSizeMetrics metrics : FileSizeMetrics.values()) {
      temporaryStatList.add(metrics.name());
    }
  }

  public static StatMonitor getInstance() {
    return StatMonitorHolder.INSTANCE;
  }

  /**
   * generate TSRecord.
   *
   * @param hashMap key is the stat name, values is the stat value object
   * @param statGroupDeltaName is the deviceId seriesPath of this module
   * @return TSRecord contains the DataPoints of a statGroupDeltaName
   */
  public static TSRecord convertToTSRecord(Map<String, Object> hashMap,
      String statGroupDeltaName) {
    TSRecord tsRecord = new TSRecord(0, statGroupDeltaName);
    tsRecord.dataPointList = new ArrayList<>();
    for (Map.Entry<String, Object> entry : hashMap.entrySet()) {
      if(entry.getValue() instanceof Long){
        long value = (long) entry.getValue();
        tsRecord.dataPointList.add(new LongDataPoint(entry.getKey(), value));
      }
      // add other type data point if needed
    }
    return tsRecord;
  }

  /**
   * register monitor statistics time series metadata into MManager.
   *
   * @param hashMap series path and data type pair, for example: [root.stat.file.size.DATA, INT64]
   */
  public synchronized void registerMonitorTimeSeries(Map<String, String> hashMap) {
    MManager mManager = MManager.getInstance();
    try {
      for (Map.Entry<String, String> entry : hashMap.entrySet()) {
        if (entry.getValue() == null) {
          logger.error("Registering metadata but data type of {} is null", entry.getKey());
        }

        if (!mManager.pathExist(entry.getKey())) {
          mManager.addPathToMTree(new Path(entry.getKey()), TSDataType.valueOf(entry.getValue()),
              TSEncoding.valueOf("RLE"),
              CompressionType.valueOf(TSFileDescriptor.getInstance().getConfig().getCompressor()),
              Collections.emptyMap());
        }
      }
    } catch (MetadataException | PathException e) {
      logger.error("Initialize the metadata error.", e);
    }
  }

  public void recovery() {
    // currently we do not persist monitor metrics data
  }

  private void activate() {
    FileSize.getInstance().registerStatMetadata();
    service = IoTDBThreadPoolFactory.newScheduledThreadPool(1,
        ThreadName.STAT_MONITOR.getName());
    service.scheduleAtFixedRate(
        new StatBackLoop(), 1, backLoopPeriod, TimeUnit.SECONDS);
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

  class StatBackLoop implements Runnable {

    @Override
    public void run() {
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

    private void cleanOutDated() {
      long currentTimeMillis = System.currentTimeMillis();
      try {
        StorageEngine fManager = StorageEngine.getInstance();
        for (Map.Entry<String, IStatistic> entry : statisticMap.entrySet()) {
          for (String statParamName : entry.getValue().getStatParamsHashMap().keySet()) {
            if (temporaryStatList.contains(statParamName)) {
              fManager.delete(entry.getKey(), statParamName,
                  currentTimeMillis - statMonitorRetainIntervalSec * 1000);
            }
          }
        }
      } catch (StorageEngineException e) {
        logger
            .error("Error occurred when deleting statistics information periodically, because",
                e);
      }
    }

    public void insert(Map<String, TSRecord> tsRecordHashMap) {
      StorageEngine storageEngine = StorageEngine.getInstance();
      for (Map.Entry<String, TSRecord> entry : tsRecordHashMap.entrySet()) {
        try {
          storageEngine.insert(new InsertPlan(entry.getValue()));
        } catch (StorageEngineException | QueryProcessException e) {
          logger.error("Inserting stat points error.", e);
        }
      }
    }

    /**
     * get all statistics.
     */
    private Map<String, TSRecord> gatherStatistics() {
      synchronized (statisticMap) {
        long currentTimeMillis = System.currentTimeMillis();
        HashMap<String, TSRecord> tsRecordHashMap = new HashMap<>();
        for (Map.Entry<String, IStatistic> entry : statisticMap.entrySet()) {
          if (entry.getValue() != null) {
            tsRecordHashMap.putAll(entry.getValue().getAllStatisticsValue());
          }
        }
        for (TSRecord value : tsRecordHashMap.values()) {
          value.time = currentTimeMillis;
        }
        return tsRecordHashMap;
      }
    }
  }
}
