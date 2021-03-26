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
package org.apache.iotdb.db.monitor;

import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.StorageEngine;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.jdbc.Config;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

/** This is a integration test for StatMonitor. */
public class IoTDBStatMonitorTest {
  private static final Logger logger = LoggerFactory.getLogger(IoTDBStatMonitorTest.class);

  private StatMonitor statMonitor;
  private final IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();
  private static final String STORAGE_GROUP_NAME = "root.sg";
  private static String[] dataset =
      new String[] {
        "SET STORAGE GROUP TO root.sg",
        "CREATE TIMESERIES root.sg.d1.s1 WITH DATATYPE=INT32, ENCODING=RLE",
        "insert into root.sg.d1(timestamp,s1) values(5,5)",
        "insert into root.sg.d1(timestamp,s1) values(12,12)",
        "insert into root.sg.d1(timestamp,s1) values(15,15)",
        "insert into root.sg.d1(timestamp,s1) values(25,25)",
        "insert into root.sg.d1(timestamp,s1) values(100,100)",
      };

  @Before
  public void setUp() throws Exception {
    config.setEnableStatMonitor(true);
    config.setEnableMonitorSeriesWrite(true);
    EnvironmentUtils.envSetUp();
    statMonitor = StatMonitor.getInstance();
    if (statMonitor.globalSeries.isEmpty()) {
      statMonitor.initMonitorSeriesInfo();
    }
    insertSomeData();
  }

  @After
  public void tearDown() throws IOException, StorageEngineException {
    // reset setEnableStatMonitor to false
    config.setEnableStatMonitor(false);
    config.setEnableMonitorSeriesWrite(false);

    statMonitor.close();
    EnvironmentUtils.cleanEnv();
  }

  @Test
  public void completeTest() throws Exception {
    getValueInMemoryTest();
    statMonitor.saveStatValue("root.sg");
    saveStatValueTest();

    // restart server
    EnvironmentUtils.restartDaemon();
    long time = 0;
    while (!StorageEngine.getInstance().isAllSgReady()) {
      Thread.sleep(500);
      time += 500;

      if (time > 10000) {
        logger.warn("wait for sg ready for : " + (time / 1000) + " s");
      }

      if (time > 30000) {
        throw new IllegalStateException("wait too long in IoTDBStatMonitorTest");
      }
    }
    recoveryTest();
  }

  private void getValueInMemoryTest() {
    Assert.assertEquals(true, statMonitor.getEnableStatMonitor());
    Assert.assertEquals(5, statMonitor.getGlobalTotalPointsNum());
    Assert.assertEquals(0, statMonitor.getGlobalReqFailNum());
    Assert.assertEquals(5, statMonitor.getGlobalReqSuccessNum());
    Assert.assertEquals(5, statMonitor.getStorageGroupTotalPointsNum(STORAGE_GROUP_NAME));
    Assert.assertEquals(0, statMonitor.getDataSizeInByte());
    Assert.assertEquals(
        new File(config.getSystemDir()).getAbsolutePath(), statMonitor.getSystemDirectory());
    Assert.assertEquals(config.isEnableWal(), statMonitor.getWriteAheadLogStatus());
  }

  private void saveStatValueTest() throws MetadataException, StorageEngineException {
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      boolean hasResult = statement.execute("select TOTAL_POINTS from root.stats.\"root.sg\"");
      Assert.assertTrue(hasResult);

      int cnt = 0;
      try (ResultSet resultSet = statement.getResultSet()) {
        while (resultSet.next()) {
          long ans = resultSet.getLong("root.stats.\"root.sg\".TOTAL_POINTS");
          Assert.assertEquals(5, ans);
          cnt++;
        }
        Assert.assertEquals(1, cnt);
      }

      hasResult = statement.execute("select TOTAL_POINTS from root.stats.\"global\"");
      Assert.assertTrue(hasResult);

      cnt = 0;
      try (ResultSet resultSet = statement.getResultSet()) {
        while (resultSet.next()) {
          long ans = resultSet.getLong("root.stats.\"global\".TOTAL_POINTS");
          Assert.assertEquals(6, ans);
          cnt++;
        }
        Assert.assertEquals(1, cnt);
      }

    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  private void recoveryTest() {
    statMonitor.recovery();
    Assert.assertEquals(5, statMonitor.getStorageGroupTotalPointsNum(STORAGE_GROUP_NAME));
    Assert.assertEquals(6, statMonitor.getGlobalTotalPointsNum());
    Assert.assertEquals(7, statMonitor.getGlobalReqSuccessNum());
    Assert.assertEquals(0, statMonitor.getGlobalReqFailNum());
  }

  private void insertSomeData() throws ClassNotFoundException {
    Class.forName(Config.JDBC_DRIVER_NAME);
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {

      for (String sql : dataset) {
        statement.execute(sql);
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}
