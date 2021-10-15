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

import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.StorageEngine;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.engine.storagegroup.timeindex.TimeIndexLevel;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.exception.metadata.IllegalPathException;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.rescon.TsFileResourceManager;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.jdbc.Config;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import static org.apache.iotdb.db.constant.TestConstant.TIMESTAMP_STR;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class IoTDBManageTsFileResourceIT {
  private static final IoTDBConfig CONFIG = IoTDBDescriptor.getInstance().getConfig();
  private TsFileResourceManager tsFileResourceManager = TsFileResourceManager.getInstance();
  private double prevTimeIndexMemoryProportion;
  private double prevTimeIndexMemoryThreshold;
  private int prevCompactionThreadNum;

  private static String[] unSeqSQLs =
      new String[] {
        "insert into root.sg1.d1(time,s1) values(1, 1)",
        "insert into root.sg1.d1(time,s2) values(2, 2)",
        "flush",
        "insert into root.sg1.d1(time,s1) values(9, 9)",
        "insert into root.sg1.d1(time,s2) values(10, 10)",
        "flush",
        "insert into root.sg1.d1(time,s1) values(5, 5)",
        "insert into root.sg1.d1(time,s2) values(6, 6)",
        "flush",
        "insert into root.sg1.d2(time,s1) values(11, 11)",
        "insert into root.sg1.d2(time,s2) values(12, 12)",
        "flush",
        "insert into root.sg1.d1(time,s1) values(13, 13)",
        "insert into root.sg1.d1(time,s2) values(14, 14)",
        "flush",
        "insert into root.sg1.d2(time,s1) values(7, 7)",
        "insert into root.sg1.d2(time,s2) values(8, 8)",
        "flush",
        "insert into root.sg1.d2(time,s1) values(3, 3)",
        "insert into root.sg1.d2(time,s2) values(4, 4)",
        "flush",
        "insert into root.sg1.d2(time,s1) values(15, 15)",
        "insert into root.sg1.d2(time,s2) values(16, 16)",
        "flush"
      };

  @Before
  public void setUp() throws ClassNotFoundException {
    EnvironmentUtils.closeStatMonitor();
    EnvironmentUtils.envSetUp();
    prevTimeIndexMemoryProportion = CONFIG.getTimeIndexMemoryProportion();
    prevCompactionThreadNum = CONFIG.getConcurrentCompactionThread();
    Class.forName(Config.JDBC_DRIVER_NAME);
  }

  @After
  public void tearDown() throws Exception {
    EnvironmentUtils.cleanEnv();
    prevTimeIndexMemoryThreshold =
        prevTimeIndexMemoryProportion * CONFIG.getAllocateMemoryForRead();
    tsFileResourceManager.setTimeIndexMemoryThreshold(prevTimeIndexMemoryThreshold);
    CONFIG.setConcurrentCompactionThread(prevCompactionThreadNum);
  }

  @Test
  public void multiResourceTest() throws SQLException {
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      CONFIG.setConcurrentCompactionThread(0);
      double curTimeIndexMemoryThreshold = 1288.5;
      tsFileResourceManager.setTimeIndexMemoryThreshold(curTimeIndexMemoryThreshold);
      for (String sql : unSeqSQLs) {
        statement.execute(sql);
      }
      statement.close();
      List<TsFileResource> seqResources =
          new ArrayList<>(
              StorageEngine.getInstance()
                  .getProcessor(new PartialPath("root.sg1"))
                  .getSequenceFileTreeSet());
      assertEquals(5, seqResources.size());
      // five tsFileResource are degraded in total, 2 are in seqResources and 3 are in
      // unSeqResources
      for (int i = 0; i < seqResources.size(); i++) {
        if (i < 2) {
          assertEquals(
              TimeIndexLevel.FILE_TIME_INDEX,
              TimeIndexLevel.valueOf(seqResources.get(i).getTimeIndexType()));
        } else {
          assertEquals(
              TimeIndexLevel.DEVICE_TIME_INDEX,
              TimeIndexLevel.valueOf(seqResources.get(i).getTimeIndexType()));
        }
      }
      List<TsFileResource> unSeqResources =
          new ArrayList<>(
              StorageEngine.getInstance()
                  .getProcessor(new PartialPath("root.sg1"))
                  .getUnSequenceFileList());
      assertEquals(3, unSeqResources.size());
      for (TsFileResource resource : unSeqResources) {
        assertEquals(
            TimeIndexLevel.FILE_TIME_INDEX, TimeIndexLevel.valueOf(resource.getTimeIndexType()));
      }
    } catch (StorageEngineException | IllegalPathException e) {
      Assert.fail();
    }

    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      boolean hasResultSet = statement.execute("SELECT s1 FROM root.sg1.d1");
      assertTrue(hasResultSet);
      String[] exp = new String[] {"1,1.0", "5,5.0", "9,9.0", "13,13.0"};
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
  public void oneResourceTest() throws SQLException {
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      double curTimeIndexMemoryThreshold = 290;
      tsFileResourceManager.setTimeIndexMemoryThreshold(curTimeIndexMemoryThreshold);
      statement.execute("insert into root.sg1.wf01.wt01(timestamp, status) values (1000, true)");
      statement.execute("insert into root.sg1.wf01.wt01(timestamp, status) values (2000, true)");
      statement.execute("insert into root.sg1.wf01.wt01(timestamp, status) values (3000, true)");
      statement.execute("flush");
      statement.close();
      List<TsFileResource> resources =
          new ArrayList<>(
              StorageEngine.getInstance()
                  .getProcessor(new PartialPath("root.sg1"))
                  .getSequenceFileTreeSet());
      assertEquals(1, resources.size());
      for (TsFileResource resource : resources) {
        assertEquals(
            TimeIndexLevel.FILE_TIME_INDEX, TimeIndexLevel.valueOf(resource.getTimeIndexType()));
      }
    } catch (StorageEngineException | IllegalPathException e) {
      Assert.fail();
    }
  }

  @Test
  public void restartResourceTest()
      throws SQLException, IllegalPathException, StorageEngineException {
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      CONFIG.setConcurrentCompactionThread(0);
      double curTimeIndexMemoryThreshold = 1288.5;
      tsFileResourceManager.setTimeIndexMemoryThreshold(curTimeIndexMemoryThreshold);
      for (int i = 0; i < unSeqSQLs.length - 1; i++) {
        statement.execute(unSeqSQLs[i]);
      }
      statement.close();
      List<TsFileResource> seqResources =
          new ArrayList<>(
              StorageEngine.getInstance()
                  .getProcessor(new PartialPath("root.sg1"))
                  .getSequenceFileTreeSet());
      assertEquals(5, seqResources.size());

      // Four tsFileResource are degraded in total, 1 are in seqResources and 3 are in
      // unSeqResources. The difference with the multiResourceTest is that last tsFileResource is
      // not close, so degrade method can't be called.
      for (int i = 0; i < seqResources.size(); i++) {
        if (i < 4) {
          assertTrue(seqResources.get(i).isClosed());
        } else {
          assertFalse(seqResources.get(i).isClosed());
        }
        if (i < 1) {
          assertEquals(
              TimeIndexLevel.FILE_TIME_INDEX,
              TimeIndexLevel.valueOf(seqResources.get(i).getTimeIndexType()));
        } else {
          assertEquals(
              TimeIndexLevel.DEVICE_TIME_INDEX,
              TimeIndexLevel.valueOf(seqResources.get(i).getTimeIndexType()));
        }
      }
      List<TsFileResource> unSeqResources =
          new ArrayList<>(
              StorageEngine.getInstance()
                  .getProcessor(new PartialPath("root.sg1"))
                  .getUnSequenceFileList());
      assertEquals(3, unSeqResources.size());
      for (TsFileResource resource : unSeqResources) {
        assertTrue(resource.isClosed());
        assertEquals(
            TimeIndexLevel.FILE_TIME_INDEX, TimeIndexLevel.valueOf(resource.getTimeIndexType()));
      }
    }

    try {
      EnvironmentUtils.restartDaemon();
    } catch (Exception e) {
      Assert.fail();
    }
    List<TsFileResource> seqResources =
        new ArrayList<>(
            StorageEngine.getInstance()
                .getProcessor(new PartialPath("root.sg1"))
                .getSequenceFileTreeSet());
    assertEquals(5, seqResources.size());
    for (int i = 0; i < seqResources.size(); i++) {
      assertTrue(seqResources.get(i).isClosed());
    }
    List<TsFileResource> unSeqResources =
        new ArrayList<>(
            StorageEngine.getInstance()
                .getProcessor(new PartialPath("root.sg1"))
                .getUnSequenceFileList());
    assertEquals(3, unSeqResources.size());
    for (TsFileResource resource : unSeqResources) {
      assertEquals(
          TimeIndexLevel.FILE_TIME_INDEX, TimeIndexLevel.valueOf(resource.getTimeIndexType()));
      assertTrue(resource.isClosed());
    }

    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      boolean hasResultSet = statement.execute("SELECT s1 FROM root.sg1.d1");
      assertTrue(hasResultSet);
      String[] exp = new String[] {"1,1.0", "5,5.0", "9,9.0", "13,13.0"};
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
}
