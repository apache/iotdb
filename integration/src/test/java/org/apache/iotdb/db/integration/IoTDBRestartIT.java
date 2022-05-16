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
package org.apache.iotdb.db.integration;

import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.StorageEngine;
import org.apache.iotdb.db.engine.memtable.IMemTable;
import org.apache.iotdb.db.engine.storagegroup.TsFileProcessor;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.itbase.category.LocalStandaloneTest;
import org.apache.iotdb.jdbc.Config;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import static org.apache.iotdb.db.constant.TestConstant.TIMESTAMP_STR;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@Category({LocalStandaloneTest.class})
public class IoTDBRestartIT {

  private final Logger logger = LoggerFactory.getLogger(IoTDBRestartIT.class);

  @Before
  public void setUp() throws Exception {
    EnvironmentUtils.envSetUp();
    Class.forName(Config.JDBC_DRIVER_NAME);
  }

  @After
  public void tearDown() throws Exception {
    EnvironmentUtils.cleanEnv();
  }

  @Test
  public void testRestart() throws SQLException, IOException, StorageEngineException {
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      statement.execute("insert into root.turbine.d1(timestamp,s1) values(1,1.0)");
      statement.execute("flush");
    }

    try {
      EnvironmentUtils.restartDaemon();
    } catch (Exception e) {
      Assert.fail();
    }

    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      statement.execute("insert into root.turbine.d1(timestamp,s1) values(2,1.0)");
    }

    try {
      EnvironmentUtils.restartDaemon();
    } catch (Exception e) {
      Assert.fail();
    }

    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      statement.execute("insert into root.turbine.d1(timestamp,s1) values(3,1.0)");

      boolean hasResultSet = statement.execute("SELECT s1 FROM root.turbine.d1");
      assertTrue(hasResultSet);
      String[] exp = new String[] {"1,1.0", "2,1.0", "3,1.0"};
      int cnt = 0;
      try (ResultSet resultSet = statement.getResultSet()) {
        while (resultSet.next()) {
          String result = resultSet.getString(TIMESTAMP_STR) + "," + resultSet.getString(2);
          assertEquals(exp[cnt], result);
          cnt++;
        }
      }
    }
  }

  @Test
  public void testRestartDelete() throws SQLException, IOException, StorageEngineException {
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      statement.execute("insert into root.turbine.d1(timestamp,s1) values(1,1)");
      statement.execute("insert into root.turbine.d1(timestamp,s1) values(2,2)");
      statement.execute("insert into root.turbine.d1(timestamp,s1) values(3,3)");
    }

    long time = 0;
    try {
      EnvironmentUtils.restartDaemon();
      StorageEngine.getInstance().recover();
      // wait for recover
      while (!StorageEngine.getInstance().isAllSgReady()) {
        Thread.sleep(500);
        time += 500;
        if (time > 10000) {
          logger.warn("wait too long in restart, wait for: " + time / 1000 + "s");
        }
      }
    } catch (Exception e) {
      Assert.fail();
    }

    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      statement.execute("delete from root.turbine.d1.s1 where time<=1");

      boolean hasResultSet = statement.execute("SELECT s1 FROM root.turbine.d1");
      assertTrue(hasResultSet);
      String[] exp = new String[] {"2,2.0", "3,3.0"};
      ResultSet resultSet = statement.getResultSet();
      int cnt = 0;
      try {
        while (resultSet.next()) {
          String result = resultSet.getString(TIMESTAMP_STR) + "," + resultSet.getString(2);
          assertEquals(exp[cnt], result);
          cnt++;
        }

        statement.execute("flush");
        statement.execute("delete from root.turbine.d1.s1 where time<=2");

        hasResultSet = statement.execute("SELECT s1 FROM root.turbine.d1");
        assertTrue(hasResultSet);
        exp = new String[] {"3,3.0"};
        resultSet = statement.getResultSet();
        cnt = 0;
        while (resultSet.next()) {
          String result = resultSet.getString(TIMESTAMP_STR) + "," + resultSet.getString(2);
          assertEquals(exp[cnt], result);
          cnt++;
        }
      } finally {
        resultSet.close();
      }
    }
  }

  @Test
  public void testRestartQueryLargerThanEndTime()
      throws SQLException, ClassNotFoundException, IOException, StorageEngineException {
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      statement.execute("insert into root.turbine.d1(timestamp,s1) values(1,1)");
      statement.execute("insert into root.turbine.d1(timestamp,s1) values(2,2)");
    }

    try {
      EnvironmentUtils.restartDaemon();
    } catch (Exception e) {
      Assert.fail();
    }

    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      statement.execute("insert into root.turbine.d1(timestamp,s1) values(3,1)");
      statement.execute("insert into root.turbine.d1(timestamp,s1) values(4,2)");
    }

    try {
      EnvironmentUtils.restartDaemon();
    } catch (Exception e) {
      Assert.fail();
    }

    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {

      boolean hasResultSet = statement.execute("SELECT s1 FROM root.turbine.d1 where time > 3");
      assertTrue(hasResultSet);
      String[] exp =
          new String[] {
            "4,2.0",
          };
      int cnt = 0;
      try (ResultSet resultSet = statement.getResultSet()) {
        while (resultSet.next()) {
          String result = resultSet.getString(TIMESTAMP_STR) + "," + resultSet.getString(2);
          assertEquals(exp[cnt], result);
          cnt++;
        }
      }
      assertEquals(1, cnt);
    }
  }

  @Test
  public void testRestartEndTime()
      throws SQLException, ClassNotFoundException, IOException, StorageEngineException {
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      statement.execute("insert into root.turbine.d1(timestamp,s1) values(1,1)");
      statement.execute("insert into root.turbine.d1(timestamp,s1) values(2,2)");
    }

    try {
      EnvironmentUtils.restartDaemon();
    } catch (Exception e) {
      Assert.fail();
    }

    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      statement.execute("insert into root.turbine.d1(timestamp,s2) values(1,1)");
      statement.execute("insert into root.turbine.d1(timestamp,s2) values(2,2)");
    }

    try {
      EnvironmentUtils.restartDaemon();
    } catch (Exception e) {
      Assert.fail();
    }

    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {

      boolean hasResultSet = statement.execute("SELECT s2 FROM root.turbine.d1");
      assertTrue(hasResultSet);
      String[] exp = new String[] {"1,1.0", "2,2.0"};
      int cnt = 0;
      try (ResultSet resultSet = statement.getResultSet()) {
        while (resultSet.next()) {
          String result = resultSet.getString(TIMESTAMP_STR) + "," + resultSet.getString(2);
          assertEquals(exp[cnt], result);
          cnt++;
        }
      }
      assertEquals(2, cnt);
    }
  }

  @Test
  public void testRecoverWALMismatchDataType() throws Exception {
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      statement.execute("insert into root.turbine1.d1(timestamp,s1,s2) values(1,1.1,2.2)");
      statement.execute("delete timeseries root.turbine1.d1.s1");
      statement.execute(
          "create timeseries root.turbine1.d1.s1 with datatype=INT32, encoding=RLE, compression=SNAPPY");
    }

    EnvironmentUtils.restartDaemon();

    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {

      boolean hasResultSet = statement.execute("select * from root.**");
      assertTrue(hasResultSet);
      try (ResultSet resultSet = statement.getResultSet()) {
        int cnt = 0;
        Assert.assertEquals(3, resultSet.getMetaData().getColumnCount());
        while (resultSet.next()) {
          Assert.assertEquals("1", resultSet.getString(1));
          Assert.assertEquals(null, resultSet.getString(2));
          Assert.assertEquals("2.2", resultSet.getString(3));
          cnt++;
        }
        assertEquals(1, cnt);
      }
    }
  }

  @Test
  public void testRecoverWALDeleteSchema() throws Exception {
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      statement.execute("insert into root.turbine1.d1(timestamp,s1,s2) values(1,1.1,2.2)");
      statement.execute("delete timeseries root.turbine1.d1.s1");
    }

    EnvironmentUtils.restartDaemon();

    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {

      boolean hasResultSet = statement.execute("select * from root.**");
      assertTrue(hasResultSet);
      try (ResultSet resultSet = statement.getResultSet()) {
        int cnt = 0;
        Assert.assertEquals(2, resultSet.getMetaData().getColumnCount());
        while (resultSet.next()) {
          Assert.assertEquals("1", resultSet.getString(1));
          Assert.assertEquals("2.2", resultSet.getString(2));
          cnt++;
        }
        assertEquals(1, cnt);
      }
    }
  }

  @Test
  public void testRecoverWALDeleteSchemaCheckResourceTime() throws Exception {
    IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();
    int avgSeriesPointNumberThreshold = config.getAvgSeriesPointNumberThreshold();
    config.setAvgSeriesPointNumberThreshold(2);
    long tsFileSize = config.getSeqTsFileSize();
    long unFsFileSize = config.getSeqTsFileSize();
    config.setSeqTsFileSize(10000000);
    config.setUnSeqTsFileSize(10000000);

    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      statement.execute("create timeseries root.turbine1.d1.s1 with datatype=INT64");
      statement.execute("insert into root.turbine1.d1(timestamp,s1) values(1,1)");
      statement.execute("insert into root.turbine1.d1(timestamp,s1) values(2,1)");
      statement.execute("create timeseries root.turbine1.d1.s2 with datatype=BOOLEAN");
      statement.execute("insert into root.turbine1.d1(timestamp,s2) values(3,true)");
      statement.execute("insert into root.turbine1.d1(timestamp,s2) values(4,true)");
    }

    Thread.sleep(1000);
    EnvironmentUtils.restartDaemon();

    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {

      long[] result = new long[] {1L, 2L};
      statement.execute("select s1 from root.turbine1.d1 where time < 3");
      ResultSet resultSet = statement.getResultSet();
      int cnt = 0;
      while (resultSet.next()) {
        assertEquals(resultSet.getLong(1), result[cnt]);
        cnt++;
      }
      assertEquals(2, cnt);
    }

    config.setAvgSeriesPointNumberThreshold(avgSeriesPointNumberThreshold);
    config.setSeqTsFileSize(tsFileSize);
    config.setUnSeqTsFileSize(unFsFileSize);
  }

  @Test
  public void testRecoverFromFlushMemTableError() throws Exception {
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      statement.execute("insert into root.turbine1.d1(timestamp,s1,s2) values(1,1.1,2.2)");
    }

    // mock exception
    TsFileProcessor[] tsFileProcessors =
        StorageEngine.getInstance()
            .getProcessorByDataRegionId(new PartialPath("root.turbine1"), 0)
            .getWorkSequenceTsFileProcessors()
            .toArray(new TsFileProcessor[0]);
    Assert.assertEquals(1, tsFileProcessors.length);
    IMemTable memTable = tsFileProcessors[0].getWorkMemTable();
    memTable.clear();
    memTable.addTextDataSize(1);
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      statement.execute("flush");
    }

    IoTDBDescriptor.getInstance().getConfig().setReadOnly(false);
    EnvironmentUtils.restartDaemon();

    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {

      boolean hasResultSet = statement.execute("select * from root.**");
      assertTrue(hasResultSet);

      try (ResultSet resultSet = statement.getResultSet()) {
        int cnt = 0;
        while (resultSet.next()) {
          Assert.assertEquals("1", resultSet.getString(1));
          Assert.assertEquals("1.1", resultSet.getString(2));
          Assert.assertEquals("2.2", resultSet.getString(3));
          cnt++;
        }
        Assert.assertEquals(1, cnt);
      }
    }
  }
}
