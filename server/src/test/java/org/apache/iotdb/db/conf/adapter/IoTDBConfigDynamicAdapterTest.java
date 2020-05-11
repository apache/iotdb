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

import static org.junit.Assert.assertEquals;

import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.ConfigAdjusterException;
import org.apache.iotdb.db.metadata.MManager;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class IoTDBConfigDynamicAdapterTest {

  private static final IoTDBConfig CONFIG = IoTDBDescriptor.getInstance().getConfig();

  private long oldTsFileThreshold = CONFIG.getTsFileSizeThreshold();

  private int oldMaxMemTableNumber = CONFIG.getMaxMemtableNumber();

  private long oldGroupSizeInByte = CONFIG.getMemtableSizeThreshold();

  @Before
  public void setUp() throws Exception {
    EnvironmentUtils.closeStatMonitor();
    EnvironmentUtils.envSetUp();
    IoTDBDescriptor.getInstance().getConfig().setEnableParameterAdapter(true);
  }

  @After
  public void tearDown() throws Exception {
    EnvironmentUtils.cleanEnv();
    CONFIG.setMaxMemtableNumber(oldMaxMemTableNumber);
    CONFIG.setTsFileSizeThreshold(oldTsFileThreshold);
    CONFIG.setMemtableSizeThreshold(oldGroupSizeInByte);
    MManager.getInstance().setMaxSeriesNumberAmongStorageGroup(0);
    IoTDBConfigDynamicAdapter.getInstance().reset();
  }

  @Test
  public void addOrDeleteStorageGroup() throws ConfigAdjusterException {
    int memTableNum = IoTDBConfigDynamicAdapter.MEM_TABLE_AVERAGE_QUEUE_LEN;
    for (int i = 1; i < 100; i++) {
      IoTDBConfigDynamicAdapter.getInstance().addOrDeleteTimeSeries(1);
    }
    MManager.getInstance().setMaxSeriesNumberAmongStorageGroup(100);
    for (int i = 1; i < 1000000; i++) {
      try {
        IoTDBConfigDynamicAdapter.getInstance().addOrDeleteStorageGroup(1);
        memTableNum += IoTDBDescriptor.getInstance().getConfig().getConcurrentWritingTimePartition() * 4 + 1;
        assertEquals(IoTDBConfigDynamicAdapter.getInstance().getCurrentMemTableSize(),
            CONFIG.getMemtableSizeThreshold());
        assertEquals(CONFIG.getMaxMemtableNumber(), memTableNum);
      } catch (ConfigAdjusterException e) {
        assertEquals(String.format(ConfigAdjusterException.ERROR_MSG_FORMAT,
            IoTDBConfigDynamicAdapter.CREATE_STORAGE_GROUP), e.getMessage());
        assertEquals(CONFIG.getMaxMemtableNumber(), memTableNum);
        break;
      }
    }
  }

  @Test
  public void addOrDeleteTimeSeries() throws ConfigAdjusterException {
    int totalTimeseries = 0;
    for (int i = 1; i < 100; i++) {
      IoTDBConfigDynamicAdapter.getInstance().addOrDeleteStorageGroup(1);
    }
    MManager.getInstance().setMaxSeriesNumberAmongStorageGroup(100);
    for (int i = 1; i < 1000000; i++) {
      try {
        IoTDBConfigDynamicAdapter.getInstance().addOrDeleteTimeSeries(1);

        if (i % 10 == 0) {
          MManager.getInstance().setMaxSeriesNumberAmongStorageGroup(i);
        }
        totalTimeseries += 1;
        assertEquals(IoTDBConfigDynamicAdapter.getInstance().getCurrentMemTableSize(),
            CONFIG.getMemtableSizeThreshold());
        assertEquals(IoTDBConfigDynamicAdapter.getInstance().getTotalTimeseries(),
            totalTimeseries);
      } catch (ConfigAdjusterException e) {
        assertEquals(String.format(ConfigAdjusterException.ERROR_MSG_FORMAT,
            IoTDBConfigDynamicAdapter.ADD_TIMESERIES), e.getMessage());
        assertEquals(IoTDBConfigDynamicAdapter.getInstance().getTotalTimeseries(),
            totalTimeseries);
        break;
      }
    }
  }

  @Test
  public void addOrDeleteTimeSeriesSyso() throws ConfigAdjusterException {
    int sgNum = 1;
    for (int i = 0; i < 30; i++) {
      IoTDBConfigDynamicAdapter.getInstance().addOrDeleteStorageGroup(sgNum);
    }
    int i = 1;
    try {
      for (; i <= 280 * 3200; i++) {
        IoTDBConfigDynamicAdapter.getInstance().addOrDeleteTimeSeries(1);
        MManager.getInstance().setMaxSeriesNumberAmongStorageGroup(i / 30 + 1);
      }
    } catch (ConfigAdjusterException e) {
      assertEquals(String.format(ConfigAdjusterException.ERROR_MSG_FORMAT,
          IoTDBConfigDynamicAdapter.ADD_TIMESERIES), e.getMessage());
    }
    try {
      while (true) {
        IoTDBConfigDynamicAdapter.getInstance().addOrDeleteTimeSeries(1);
        MManager.getInstance().setMaxSeriesNumberAmongStorageGroup(MManager.getInstance().getMaximalSeriesNumberAmongStorageGroups() + 1);
      }
    } catch (ConfigAdjusterException e ) {
      assertEquals(String.format(ConfigAdjusterException.ERROR_MSG_FORMAT,
          IoTDBConfigDynamicAdapter.ADD_TIMESERIES), e.getMessage());
    }
  }
}