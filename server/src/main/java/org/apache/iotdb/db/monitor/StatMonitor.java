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

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBConstant;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.StorageEngine;
import org.apache.iotdb.db.engine.fileSystem.SystemFileFactory;
import org.apache.iotdb.db.exception.StartupException;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.exception.metadata.IllegalPathException;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.metadata.MManager;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.monitor.MonitorConstants.StatMeasurementConstants;
import org.apache.iotdb.db.qp.physical.crud.InsertRowPlan;
import org.apache.iotdb.db.service.IService;
import org.apache.iotdb.db.service.IoTDB;
import org.apache.iotdb.db.service.JMXService;
import org.apache.iotdb.db.service.ServiceType;
import org.apache.iotdb.tsfile.common.conf.TSFileDescriptor;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.write.record.TSRecord;
import org.apache.iotdb.tsfile.write.record.datapoint.LongDataPoint;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StatMonitor implements StatMonitorMBean, IService {

  private static final Logger logger = LoggerFactory.getLogger(StatMonitor.class);
  private static IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();
  private static MManager mManager = IoTDB.metaManager;
  private final String mbeanName = String
      .format("%s:%s=%s", IoTDBConstant.IOTDB_PACKAGE, IoTDBConstant.JMX_TYPE,
          getID().getJmxName());

  // storage group name -> monitor series of it.
  private Map<String, List<PartialPath>> monitorSeriesMap = new ConcurrentHashMap<>();
  // monitor series -> current value of it.   e.g. root.stats.global.TOTAL_POINTS -> value
  private Map<PartialPath, Long> cachedValueMap = new ConcurrentHashMap<>();

  public StatMonitor() {
    if (config.isEnableStatMonitor()) {
      registerStatGlobalInfo();
      List<PartialPath> storageGroupNames = mManager.getAllStorageGroupPaths();
      registerStatStorageGroupInfo(storageGroupNames);
    }
  }

  public static StatMonitor getInstance() {
    return StatMonitorHolder.INSTANCE;
  }

  public Map<String, List<PartialPath>> getMonitorSeriesMap() {
    return monitorSeriesMap;
  }

  /**
   * Register monitor storage group into system.
   */
  public void registerStatGlobalInfo() {
    PartialPath storageGroupPrefix = new PartialPath(MonitorConstants.STAT_STORAGE_GROUP_ARRAY);
    try {
      if (!mManager.isPathExist(storageGroupPrefix)) {
        mManager.setStorageGroup(storageGroupPrefix);
      }

      for (StatMeasurementConstants statConstant : StatMeasurementConstants.values()) {
        PartialPath fullPath = new PartialPath(MonitorConstants.STAT_GLOBAL_ARRAY)
            .concatNode(statConstant.getMeasurement());
        registSeriesToMManager(fullPath);

        List<PartialPath> seriesList = monitorSeriesMap
            .computeIfAbsent(MonitorConstants.STAT_STORAGE_GROUP_NAME, k -> new ArrayList<>());
        seriesList.add(fullPath);
        cachedValueMap.putIfAbsent(fullPath, (long) 0);
      }
    } catch (MetadataException e) {
      logger.error("Initialize the metadata error.", e);
    }
  }

  /**
   * Register monitor time series metadata of each storageGroup into MManager.
   */
  public void registerStatStorageGroupInfo(List<PartialPath> storageGroupNames) {
    try {
      for (StatMeasurementConstants statConstant : StatMeasurementConstants.values()) {
        for (PartialPath storageGroupName : storageGroupNames) {

          if (!storageGroupName.equals(MonitorConstants.STAT_STORAGE_GROUP_NAME)) {
            PartialPath fullPath = new PartialPath(MonitorConstants.STAT_STORAGE_GROUP_ARRAY)
                .concatNode("\"" + storageGroupName + "\"")
                .concatNode(statConstant.getMeasurement());
            registSeriesToMManager(fullPath);

            List<PartialPath> seriesList = monitorSeriesMap
                .computeIfAbsent(MonitorConstants.STAT_STORAGE_GROUP_NAME, k -> new ArrayList<>());
            seriesList.add(fullPath);
            cachedValueMap.putIfAbsent(fullPath, (long) 0);
          }
        }
      }
    } catch (MetadataException e) {
      logger.error("Initialize the metadata error.", e);
    }
  }

  private void registSeriesToMManager(PartialPath fullPath) throws MetadataException {
    if (!mManager.isPathExist(fullPath)) {
      mManager.createTimeseries(fullPath,
          TSDataType.valueOf(MonitorConstants.INT64),
          TSEncoding.valueOf("RLE"),
          TSFileDescriptor.getInstance().getConfig().getCompressor(),
          Collections.emptyMap());
    }
  }

  public void updateStatValue(String storageGroupName, int successPointsNum) {
    List<PartialPath> monitorSeries = monitorSeriesMap.get(storageGroupName);
    for (int i = 0; i < monitorSeries.size() - 1; i++) {
      // 0 -> TOTAL_POINTS, 1 -> REQ_SUCCESS, 2 -> REQ_FAIL
      switch (i) {
        case 0:
          cachedValueMap.computeIfPresent(monitorSeries.get(i),
              (key, oldValue) -> oldValue + successPointsNum);
          break;
        case 1:
          cachedValueMap.computeIfPresent(monitorSeries.get(i),
              (key, oldValue) -> oldValue + 1);
          break;
      }
    }
  }

  /**
   * Generate tsRecords for stat parameters and insert them into StorageEngine.
   */
  public void cacheStatValue() {
    StorageEngine storageEngine = StorageEngine.getInstance();
    for (Entry<PartialPath, Long> cachedValue : cachedValueMap.entrySet()) {
      TSRecord tsRecord = new TSRecord(System.currentTimeMillis(),
          cachedValue.getKey().getDevice());
      tsRecord.addTuple(
          new LongDataPoint(cachedValue.getKey().getMeasurement(), cachedValue.getValue()));
      try {
        storageEngine.insert(new InsertRowPlan(tsRecord));
      } catch (StorageEngineException | IllegalPathException e) {
        logger.error("Inserting stat points error.", e);
      }
    }
  }

  public void recovery() {
  }

  @Override
  public long getGlobalTotalPointsNum() {
    List<PartialPath> monitorSeries = monitorSeriesMap
        .get(MonitorConstants.STAT_STORAGE_GROUP_NAME);
    return cachedValueMap.get(monitorSeries.indexOf(0));
  }

  @Override
  public String getSystemDirectory() {
    try {
      File file = SystemFileFactory.INSTANCE.getFile(config.getSystemDir());
      return file.getAbsolutePath();
    } catch (Exception e) {
      logger.error("meet error while trying to get base dir.", e);
      return "Unavailable";
    }
  }

  @Override
  public void start() throws StartupException {
    try {
      JMXService.registerMBean(getInstance(), mbeanName);
    } catch (Exception e) {
      throw new StartupException(this.getID().getName(), e.getMessage());
    }
  }

  @Override
  public void stop() {
    JMXService.deregisterMBean(mbeanName);
  }

  @Override
  public ServiceType getID() {
    return ServiceType.MONITOR_SERVICE;
  }

  private static class StatMonitorHolder {

    private StatMonitorHolder() {
      //allowed do nothing
    }

    private static final StatMonitor INSTANCE = new StatMonitor();
  }
}
