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

import java.util.Collections;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.StorageEngine;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.exception.metadata.IllegalPathException;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.metadata.MManager;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.monitor.MonitorConstants.StatConstants;
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
   * Register monitor storage group into system.
   */
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
   * Register monitor statistics time series metadata into MManager.
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
   * Generate tsRecords for stat parameters and insert them into StorageEngine.
   */
  public void updateStatValue() {
    StorageEngine storageEngine = StorageEngine.getInstance();
    for (StatConstants statConstant : StatConstants.values()) {
      TSRecord tsRecord = new TSRecord(System.currentTimeMillis(),
          MonitorConstants.STAT_STORAGE_DELTA_NAME);
      switch (statConstant) {
        case TOTAL_POINTS:
          tsRecord
              .addTuple(new LongDataPoint(statConstant.getMeasurement(), globalPointsNum.get()));
          break;
        case TOTAL_REQ_SUCCESS:
          tsRecord.addTuple(
              new LongDataPoint(statConstant.getMeasurement(), globalReqSuccessNum.get()));
          break;
        case TOTAL_REQ_FAIL:
          tsRecord
              .addTuple(new LongDataPoint(statConstant.getMeasurement(), globalReqFailNum.get()));
          break;
      }
      try {
        storageEngine.insert(new InsertRowPlan(tsRecord));
      } catch (StorageEngineException | IllegalPathException e) {
        logger.error("Inserting stat points error.", e);
      }
    }
  }

  public void recovery() {

  }

  private static class StatMonitorHolder {

    private StatMonitorHolder() {
      //allowed do nothing
    }

    private static final StatMonitor INSTANCE = new StatMonitor();
  }

}
