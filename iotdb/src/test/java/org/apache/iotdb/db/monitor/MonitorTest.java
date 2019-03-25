/**
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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.filenode.FileNodeManager;
import org.apache.iotdb.db.exception.FileNodeManagerException;
import org.apache.iotdb.db.metadata.MManager;
import org.apache.iotdb.db.monitor.MonitorConstants.FileSizeConstants;
import org.apache.iotdb.db.monitor.collector.FileSize;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.tsfile.write.record.TSRecord;
import org.apache.iotdb.tsfile.write.record.datapoint.DataPoint;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class MonitorTest {

  private IoTDBConfig ioTDBConfig = IoTDBDescriptor.getInstance().getConfig();
  private StatMonitor statMonitor;

  @Before
  public void setUp() throws Exception {
    // origin value
    // modify stat parameter
    EnvironmentUtils.closeMemControl();
    EnvironmentUtils.envSetUp();
    ioTDBConfig.setEnableStatMonitor(true);
    ioTDBConfig.setBackLoopPeriodSec(1);
  }

  @After
  public void tearDown() throws Exception {
    ioTDBConfig.setEnableStatMonitor(false);
    statMonitor.close();
    EnvironmentUtils.cleanEnv();
  }

  @Test
  public void testFileNodeManagerMonitorAndAddMetadata() {
    FileNodeManager fManager = FileNodeManager.getInstance();
    FileSize fileSize = FileSize.getInstance();
    statMonitor = StatMonitor.getInstance();
    statMonitor.registerStatStorageGroup();
    fManager.getStatParamsHashMap().forEach((key, value) -> value.set(0));
    fileSize.getStatParamsHashMap().forEach((key, value) -> value.set(0));
    statMonitor.clearIStatisticMap();
    statMonitor.registerStatistics(fManager.getClass().getSimpleName(), fManager);
    statMonitor
        .registerStatistics(MonitorConstants.FILE_SIZE_STORAGE_GROUP_NAME, FileSize.getInstance());
    // add metadata
    MManager mManager = MManager.getInstance();
    fManager.registerStatMetadata();
    fileSize.registerStatMetadata();
    Map<String, AtomicLong> statParamsHashMap = fManager.getStatParamsHashMap();
    Map<String, AtomicLong> fileSizeStatsHashMap = fileSize.getStatParamsHashMap();
    for (String statParam : statParamsHashMap.keySet()) {
      assertTrue(mManager.pathExist(
          MonitorConstants.STAT_STORAGE_GROUP_PREFIX + MonitorConstants.MONITOR_PATH_SEPARATOR
              + MonitorConstants.FILE_NODE_MANAGER_PATH + MonitorConstants.MONITOR_PATH_SEPARATOR
              + statParam));
    }
    for (String statParam : fileSizeStatsHashMap.keySet()) {
      assertTrue(mManager.pathExist(
          MonitorConstants.FILE_SIZE_STORAGE_GROUP_NAME + MonitorConstants.MONITOR_PATH_SEPARATOR
              + statParam));
    }
    statMonitor.activate();
    // wait for time second
    try {
      Thread.sleep(5000);
      statMonitor.close();
      Thread.sleep(1000);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }

    // Get stat data and test right

    Map<String, TSRecord> statHashMap = fManager.getAllStatisticsValue();
    Map<String, TSRecord> fileSizeStatMap = fileSize.getAllStatisticsValue();

    String path = fManager.getAllPathForStatistic().get(0);
    String fileSizeStatPath = fileSize.getAllPathForStatistic().get(0);
    int pos = path.lastIndexOf('.');
    int fileSizeStatPos = fileSizeStatPath.lastIndexOf('.');
    TSRecord fTSRecord = statHashMap.get(path.substring(0, pos));
    TSRecord fileSizeRecord = fileSizeStatMap.get(fileSizeStatPath.substring(0, fileSizeStatPos));

    assertNotEquals(null, fTSRecord);
    assertNotEquals(null, fileSizeRecord);
    for (DataPoint dataPoint : fTSRecord.dataPointList) {
      String m = dataPoint.getMeasurementId();
      Long v = (Long) dataPoint.getValue();
      if (m.equals("TOTAL_REQ_SUCCESS")) {
        assertEquals(v, new Long(0));
      }
      if (m.contains("FAIL")) {
        assertEquals(v, new Long(0));
      } else if (m.contains("POINTS")) {
        assertEquals(v, new Long(0));
      } else {
        assertEquals(v, new Long(0));
      }
    }
    for (DataPoint dataPoint : fileSizeRecord.dataPointList) {
      String m = dataPoint.getMeasurementId();
      Long v = (Long) dataPoint.getValue();
      if (m.equals(FileSizeConstants.OVERFLOW.name())) {
        assertEquals(v, new Long(0));
      }
    }

    try {
      fManager.deleteAll();
    } catch (FileNodeManagerException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }
}
