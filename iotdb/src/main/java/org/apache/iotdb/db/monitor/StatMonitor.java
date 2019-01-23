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
package org.apache.iotdb.db.monitor;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.iotdb.db.concurrent.IoTDBThreadPoolFactory;
import org.apache.iotdb.db.concurrent.ThreadName;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.filenode.FileNodeManager;
import org.apache.iotdb.db.exception.FileNodeManagerException;
import org.apache.iotdb.db.exception.MetadataArgsErrorException;
import org.apache.iotdb.db.exception.PathErrorException;
import org.apache.iotdb.db.exception.StartupException;
import org.apache.iotdb.db.metadata.MManager;
import org.apache.iotdb.db.service.IService;
import org.apache.iotdb.db.service.ServiceType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.write.record.TSRecord;
import org.apache.iotdb.tsfile.write.record.datapoint.DataPoint;
import org.apache.iotdb.tsfile.write.record.datapoint.LongDataPoint;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StatMonitor implements IService {

  private static final Logger LOGGER = LoggerFactory.getLogger(StatMonitor.class);
  private final int backLoopPeriod;
  private final int statMonitorDetectFreqSec;
  private final int statMonitorRetainIntervalSec;
  private long runningTimeMillis = System.currentTimeMillis();
  /**
   * key: is the statistics store seriesPath Value: is an interface that implements statistics
   * function.
   */
  private HashMap<String, IStatistic> statisticMap;
  private ScheduledExecutorService service;

  /**
   * stats params.
   */
  private AtomicLong numBackLoop = new AtomicLong(0);
  private AtomicLong numInsert = new AtomicLong(0);
  private AtomicLong numPointsInsert = new AtomicLong(0);
  private AtomicLong numInsertError = new AtomicLong(0);

  private StatMonitor() {
    MManager mmanager = MManager.getInstance();
    statisticMap = new HashMap<>();
    IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();
    statMonitorDetectFreqSec = config.statMonitorDetectFreqSec;
    statMonitorRetainIntervalSec = config.statMonitorRetainIntervalSec;
    backLoopPeriod = config.backLoopPeriodSec;
    if (config.enableStatMonitor) {
      try {
        String prefix = MonitorConstants.statStorageGroupPrefix;
        if (!mmanager.pathExist(prefix)) {
          mmanager.setStorageLevelToMTree(prefix);
        }
      } catch (PathErrorException | IOException e) {
        LOGGER.error("MManager cannot set storage level to MTree.", e);
      }
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
   * @param curTime TODO need to be fixed because it may contain overflow
   * @return TSRecord contains the DataPoints of a statGroupDeltaName
   */
  public static TSRecord convertToTSRecord(HashMap<String, AtomicLong> hashMap,
      String statGroupDeltaName, long curTime) {
    TSRecord tsRecord = new TSRecord(curTime, statGroupDeltaName);
    tsRecord.dataPointList = new ArrayList<DataPoint>() {
      {
        for (Map.Entry<String, AtomicLong> entry : hashMap.entrySet()) {
          AtomicLong value = (AtomicLong) entry.getValue();
          add(new LongDataPoint(entry.getKey(), value.get()));
        }
      }
    };
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

  public void registStatStorageGroup() {
    MManager mManager = MManager.getInstance();
    String prefix = MonitorConstants.statStorageGroupPrefix;
    try {
      if (!mManager.pathExist(prefix)) {
        mManager.setStorageLevelToMTree(prefix);
      }
    } catch (Exception e) {
      LOGGER.error("MManager cannot set storage level to MTree, because {}", e.getMessage());
    }
  }

  public synchronized void registStatStorageGroup(HashMap<String, String> hashMap) {
    MManager mManager = MManager.getInstance();
    try {
      for (Map.Entry<String, String> entry : hashMap.entrySet()) {
        if (entry.getKey() == null) {
          LOGGER.error("Registering metadata but {} is null", entry.getKey());
        }

        if (!mManager.pathExist(entry.getKey())) {
          mManager.addPathToMTree(entry.getKey(), entry.getValue(), "RLE", new String[0]);
        }
      }
    } catch (MetadataArgsErrorException | IOException | PathErrorException e) {
      LOGGER.error("Initialize the metadata error.", e);
    }
  }

  public void recovery() {
    // // restore the FildeNode Manager TOTAL_POINTS statistics info
    // OverflowQueryEngine overflowQueryEngine = new OverflowQueryEngine();
    // List<Pair<Path, String>> pairList = new ArrayList<>();
    // List<String> stringList = FileNodeManager.getInstance().getAllPathForStatistic();
    // for (String string : stringList) {
    // Path path = new Path(string);
    // pairList.add(new Pair<>(path, StatisticConstant.LAST));
    // }
    // try {
    // QueryDataSet queryDataSet;
    // queryDataSet = overflowQueryEngine.aggregate(pairList, null);
    // ReadCacheManager.getInstance().unlockForOneRequest();
    // OldRowRecord rowRecord = queryDataSet.getNextRecord();
    // if (rowRecord!=null) {
    // FileNodeManager fManager = FileNodeManager.getInstance();
    // HashMap<String, AtomicLong> statParamsHashMap = fManager.getStatParamsHashMap();
    // List<Field> list = rowRecord.fields;
    // for (Field field: list) {
    // String statMeasurement = field.measurementId.substring(0,field.measurementId.length() - 1);
    // if (statParamsHashMap.containsKey(statMeasurement)) {
    // if (field.isNull()) {
    // continue;
    // }
    // long lastValue = field.getLongV();
    // statParamsHashMap.put(statMeasurement, new AtomicLong(lastValue));
    // }
    // }
    // }
    // } catch (ProcessorException e) {
    // LOGGER.error("Can't get the processor when recovering statistics of FileNodeManager,", e);
    // } catch (PathErrorException e) {
    // LOGGER.error("When recovering statistics of FileNodeManager, timeseries seriesPath does
    // not exist,", e);
    // } catch (IOException e) {
    // LOGGER.error("IO Error occurs when recovering statistics of FileNodeManager,", e);
    // }
  }

  public void activate() {

    service = IoTDBThreadPoolFactory.newScheduledThreadPool(1,
        ThreadName.STAT_MONITOR.getName());
    service.scheduleAtFixedRate(
        new StatMonitor.statBackLoop(), 1, backLoopPeriod, TimeUnit.SECONDS);
  }

  public void clearIStatisticMap() {
    statisticMap.clear();
  }

  public long getNumBackLoop() {
    return numBackLoop.get();
  }

  public void registStatistics(String path, IStatistic iStatistic) {
    synchronized (statisticMap) {
      LOGGER.debug("Register {} to StatMonitor for statistics service", path);
      this.statisticMap.put(path, iStatistic);
    }
  }

  /**
   * deregist statistics.
   */
  public void deregistStatistics(String path) {
    LOGGER.debug("Deregister {} in StatMonitor for stopping statistics service", path);
    synchronized (statisticMap) {
      if (statisticMap.containsKey(path)) {
        statisticMap.put(path, null);
      }
    }
  }

  /**
   * TODO: need to complete the query key concept.
   *
   * @return TSRecord, query statistics params
   */
  public HashMap<String, TSRecord> getOneStatisticsValue(String key) {
    // queryPath like fileNode seriesPath: root.stats.car1,
    // or FileNodeManager seriesPath:FileNodeManager
    String queryPath;
    if (key.contains("\\.")) {
      queryPath = MonitorConstants.statStorageGroupPrefix + MonitorConstants.MONITOR_PATH_SEPERATOR
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
  public HashMap<String, TSRecord> gatherStatistics() {
    synchronized (statisticMap) {
      long currentTimeMillis = System.currentTimeMillis();
      HashMap<String, TSRecord> tsRecordHashMap = new HashMap<>();
      for (Map.Entry<String, IStatistic> entry : statisticMap.entrySet()) {
        if (entry.getValue() == null) {
          tsRecordHashMap.put(entry.getKey(),
              convertToTSRecord(
                  MonitorConstants.initValues(MonitorConstants.FILENODE_PROCESSOR_CONST),
                  entry.getKey(), currentTimeMillis));
        } else {
          tsRecordHashMap.putAll(entry.getValue().getAllStatisticsValue());
        }
      }
      LOGGER.debug("Values of tsRecordHashMap is : {}", tsRecordHashMap.toString());
      for (TSRecord value : tsRecordHashMap.values()) {
        value.time = currentTimeMillis;
      }
      return tsRecordHashMap;
    }
  }

  private void insert(HashMap<String, TSRecord> tsRecordHashMap) {
    FileNodeManager fManager = FileNodeManager.getInstance();
    int count = 0;
    int pointNum;
    for (Map.Entry<String, TSRecord> entry : tsRecordHashMap.entrySet()) {
      try {
        fManager.insert(entry.getValue(), true);
        numInsert.incrementAndGet();
        pointNum = entry.getValue().dataPointList.size();
        numPointsInsert.addAndGet(pointNum);
        count += pointNum;
      } catch (FileNodeManagerException e) {
        numInsertError.incrementAndGet();
        LOGGER.error("Inserting stat points error.", e);
      }
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
      LOGGER.error("StatMonitor timing service could not be shutdown.", e);
    }
  }

  @Override
  public void start() throws StartupException {
    try {
      if (IoTDBDescriptor.getInstance().getConfig().enableStatMonitor) {
        activate();
      }
    } catch (Exception e) {
      String errorMessage = String
          .format("Failed to start %s because of %s", this.getID().getName(),
              e.getMessage());
      throw new StartupException(errorMessage);
    }
  }

  @Override
  public void stop() {
    if (IoTDBDescriptor.getInstance().getConfig().enableStatMonitor) {
      close();
    }
  }

  @Override
  public ServiceType getID() {
    return ServiceType.STAT_MONITOR_SERVICE;
  }

  private static class StatMonitorHolder {

    private static final StatMonitor INSTANCE = new StatMonitor();
  }

  class statBackLoop implements Runnable {

    public void run() {
      try {
        long currentTimeMillis = System.currentTimeMillis();
        long seconds = (currentTimeMillis - runningTimeMillis) / 1000;
        if (seconds - statMonitorDetectFreqSec >= 0) {
          runningTimeMillis = currentTimeMillis;
          // delete time-series data
          FileNodeManager fManager = FileNodeManager.getInstance();
          try {
            for (Map.Entry<String, IStatistic> entry : statisticMap.entrySet()) {
              for (String statParamName : entry.getValue().getStatParamsHashMap().keySet()) {
                fManager.delete(entry.getKey(), statParamName,
                    currentTimeMillis - statMonitorRetainIntervalSec * 1000, TSDataType.INT64);
              }
            }
          } catch (FileNodeManagerException e) {
            LOGGER
                .error("Error occurred when deleting statistics information periodically, because",
                    e);
            e.printStackTrace();
          }
        }
        HashMap<String, TSRecord> tsRecordHashMap = gatherStatistics();
        insert(tsRecordHashMap);
        numBackLoop.incrementAndGet();
      } catch (Exception e) {
        LOGGER.error("Error occurred in Stat Monitor thread", e);
      }
    }
  }
}