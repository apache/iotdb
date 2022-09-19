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
 *
 */
package org.apache.iotdb.db.integration;

import org.apache.iotdb.db.engine.StorageEngine;
import org.apache.iotdb.db.qp.utils.DatetimeUtils;
import org.apache.iotdb.db.utils.FileUtils;
import org.apache.iotdb.integration.env.EnvFactory;
import org.apache.iotdb.itbase.category.ClusterTest;
import org.apache.iotdb.itbase.category.LocalStandaloneTest;
import org.apache.iotdb.rpc.TSStatusCode;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.File;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.ZonedDateTime;
import java.util.HashSet;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@Category({LocalStandaloneTest.class})
public class IoTDBMigrationIT {
  File testTargetDir;
  final long MIGRATION_CHECK_TIME = 60L;

  @Before
  public void setUp() throws Exception {
    EnvFactory.getEnv().initBeforeTest();

    testTargetDir = new File("testTargetDir");
    testTargetDir.mkdirs();
  }

  @After
  public void tearDown() throws Exception {
    EnvFactory.getEnv().cleanAfterTest();

    FileUtils.deleteDirectory(testTargetDir);
  }

  @Test
  @Category({ClusterTest.class})
  public void testMigration() throws SQLException, InterruptedException {
    StorageEngine.getInstance().getMigrationManager().setCheckThreadTime(MIGRATION_CHECK_TIME);

    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      try {
        statement.execute(
            "SET MIGRATION TO root.MIGRATION_SG1 1999-01-01 0 '" + testTargetDir.getPath() + "'");
      } catch (SQLException e) {
        assertEquals(TSStatusCode.TIMESERIES_NOT_EXIST.getStatusCode(), e.getErrorCode());
      }
      try {
        statement.execute("CANCEL MIGRATION ON root.MIGRATION_SG1");
      } catch (SQLException e) {
        assertEquals(TSStatusCode.TIMESERIES_NOT_EXIST.getStatusCode(), e.getErrorCode());
      }
      try {
        statement.execute("PAUSE MIGRATION ON root.MIGRATION_SG1");
      } catch (SQLException e) {
        assertEquals(TSStatusCode.TIMESERIES_NOT_EXIST.getStatusCode(), e.getErrorCode());
      }
      try {
        statement.execute("RESUME MIGRATION ON root.MIGRATION_SG1");
      } catch (SQLException e) {
        assertEquals(TSStatusCode.TIMESERIES_NOT_EXIST.getStatusCode(), e.getErrorCode());
      }

      statement.execute("SET STORAGE GROUP TO root.MIGRATION_SG1");
      statement.execute(
          "CREATE TIMESERIES root.MIGRATION_SG1.s1 WITH DATATYPE=INT32, ENCODING=PLAIN");

      try {
        statement.execute("SET MIGRATION TO storage_group=root.MIGRATION_SG1");
      } catch (SQLException e) {
        assertEquals(TSStatusCode.METADATA_ERROR.getStatusCode(), e.getErrorCode());
      }

      // test set when ttl is in range

      long now = System.currentTimeMillis();
      for (int i = 0; i < 100; i++) {
        statement.execute(
            String.format(
                "INSERT INTO root.MIGRATION_SG1(timestamp, s1) VALUES (%d, %d)",
                now - 100000 + i, i));
      }

      try (ResultSet resultSet = statement.executeQuery("SELECT s1 FROM root.MIGRATION_SG1")) {
        int cnt = 0;
        while (resultSet.next()) {
          cnt++;
        }
        assertEquals(100, cnt);
      }

      // test set when ttl isn't in range
      StorageEngine.getInstance().syncCloseAllProcessor();

      statement.execute(
          "SET MIGRATION TO root.MIGRATION_SG1 1999-01-01 1000 '" + testTargetDir.getPath() + "'");

      Thread.sleep(MIGRATION_CHECK_TIME * 2);

      try (ResultSet resultSet = statement.executeQuery("SELECT s1 FROM root.MIGRATION_SG1")) {
        int cnt = 0;
        while (resultSet.next()) {
          cnt++;
        }
        assertEquals(0, cnt);
      }

      // test pause migration

      for (int i = 0; i < 100; i++) {
        statement.execute(
            String.format(
                "INSERT INTO root.MIGRATION_SG1(timestamp, s1) VALUES (%d, %d)",
                now - 5000 + i, i));
      }

      try (ResultSet resultSet = statement.executeQuery("SELECT s1 FROM root.MIGRATION_SG1")) {
        int cnt = 0;
        while (resultSet.next()) {
          cnt++;
        }
        assertEquals(100, cnt);
      }

      StorageEngine.getInstance().syncCloseAllProcessor();

      statement.execute(
          "SET MIGRATION TO root.MIGRATION_SG1 1999-01-01 100000 '"
              + testTargetDir.getPath()
              + "'");

      Thread.sleep(MIGRATION_CHECK_TIME * 2);

      try (ResultSet resultSet = statement.executeQuery("SELECT s1 FROM root.MIGRATION_SG1")) {
        int cnt = 0;
        while (resultSet.next()) {
          cnt++;
        }
        assertEquals(100, cnt);
      }

      StorageEngine.getInstance().syncCloseAllProcessor();

      StorageEngine.getInstance().getMigrationManager().setCheckThreadTime(Long.MAX_VALUE);

      statement.execute(
          "SET MIGRATION TO root.MIGRATION_SG1 1999-01-01 0 '" + testTargetDir.getPath() + "'");
      statement.execute("PAUSE MIGRATION ON root.MIGRATION_SG1");

      StorageEngine.getInstance().getMigrationManager().setCheckThreadTime(MIGRATION_CHECK_TIME);

      Thread.sleep(MIGRATION_CHECK_TIME * 2);

      try (ResultSet resultSet = statement.executeQuery("SELECT s1 FROM root.MIGRATION_SG1")) {
        int cnt = 0;
        while (resultSet.next()) {
          cnt++;
        }
        assertEquals(100, cnt);
      }

      StorageEngine.getInstance().syncCloseAllProcessor();

      // test resume migration
      statement.execute("RESUME MIGRATION ON root.MIGRATION_SG1");

      Thread.sleep(MIGRATION_CHECK_TIME * 2);

      try (ResultSet resultSet = statement.executeQuery("SELECT s1 FROM root.MIGRATION_SG1")) {
        int cnt = 0;
        while (resultSet.next()) {
          cnt++;
        }
        assertEquals(0, cnt);
      }

      // test cancel migration

      for (int i = 0; i < 100; i++) {
        statement.execute(
            String.format(
                "INSERT INTO root.MIGRATION_SG1(timestamp, s1) VALUES (%d, %d)",
                now - 5000 + i, i));
      }

      try (ResultSet resultSet = statement.executeQuery("SELECT s1 FROM root.MIGRATION_SG1")) {
        int cnt = 0;
        while (resultSet.next()) {
          cnt++;
        }
        assertEquals(100, cnt);
      }

      StorageEngine.getInstance().syncCloseAllProcessor();

      StorageEngine.getInstance().getMigrationManager().setCheckThreadTime(Long.MAX_VALUE);

      statement.execute(
          "SET MIGRATION TO root.MIGRATION_SG1 1999-01-01 0 '" + testTargetDir.getPath() + "'");
      statement.execute("CANCEL MIGRATION ON root.MIGRATION_SG1");

      StorageEngine.getInstance().getMigrationManager().setCheckThreadTime(MIGRATION_CHECK_TIME);

      Thread.sleep(MIGRATION_CHECK_TIME * 2);

      try (ResultSet resultSet = statement.executeQuery("SELECT s1 FROM root.MIGRATION_SG1")) {
        int cnt = 0;
        while (resultSet.next()) {
          cnt++;
        }
        assertEquals(100, cnt);
      }
    }
  }

  @Test
  @Category({ClusterTest.class})
  public void testShowMigration() throws SQLException {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("SET STORAGE GROUP TO root.MIGRATION_SG2");
      statement.execute(
          "CREATE TIMESERIES root.MIGRATION_SG2.s2 WITH DATATYPE=INT32, ENCODING=PLAIN");

      ZonedDateTime startDate =
          DatetimeUtils.convertMillsecondToZonedDateTime(DatetimeUtils.currentTime() + 10000);
      String startTimeStr = DatetimeUtils.ISO_OFFSET_DATE_TIME_WITH_MS.format(startDate);
      statement.execute(
          String.format(
              "SET MIGRATION TO root.MIGRATION_SG2 %s 100 '%s'",
              startTimeStr, testTargetDir.getPath()));

      ResultSet resultSet = statement.executeQuery("SHOW ALL MIGRATION");

      boolean flag = false;

      while (resultSet.next()) {
        if (resultSet.getString(3).equals("root.MIGRATION_SG2")) {
          flag = true;
          assertEquals("READY", resultSet.getString(4));
          assertEquals(startTimeStr, resultSet.getString(5));
          assertEquals(100, resultSet.getLong(6));
          assertEquals(testTargetDir.getPath(), resultSet.getString(7));
        }
      }

      assertTrue(flag);
    }
  }

  @Test
  @Category({ClusterTest.class})
  public void testSetMigration() throws SQLException {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("SET STORAGE GROUP TO root.MIGRATION_SG3");
      statement.execute(
          "CREATE TIMESERIES root.MIGRATION_SG3.s3 WITH DATATYPE=INT32, ENCODING=PLAIN");

      ZonedDateTime startDate =
          DatetimeUtils.convertMillsecondToZonedDateTime(DatetimeUtils.currentTime() + 10000);
      String startTimeStr = DatetimeUtils.ISO_OFFSET_DATE_TIME_WITH_MS.format(startDate);

      for (int i = 0; i < 1000; i++) {
        // test concurrent set
        statement.execute(
            String.format(
                "SET MIGRATION TO root.MIGRATION_SG3 %s %d '%s'",
                startTimeStr, i, testTargetDir.getPath()));
      }

      ResultSet resultSet = statement.executeQuery("SHOW ALL MIGRATION");
      Set<Integer> checkTaskId = new HashSet<>();
      Set<Integer> checkTTL = new HashSet<>();

      while (resultSet.next()) {
        if (resultSet.getString(3).equals("root.MIGRATION_SG3")) {
          int taskId = Integer.parseInt(resultSet.getString(1));
          int ttl = Integer.parseInt(resultSet.getString(6));
          assertFalse(checkTaskId.contains(taskId));
          checkTaskId.add(taskId);
          assertFalse(checkTTL.contains(ttl));
          checkTTL.add(ttl);
        }
      }

      assertEquals(1000, checkTaskId.size());
      assertEquals(1000, checkTTL.size());

      for (int i : checkTaskId) {
        // test concurrent cancel
        statement.execute(String.format("CANCEL MIGRATION %d", i));
      }

      resultSet = statement.executeQuery("SHOW ALL MIGRATION");

      while (resultSet.next()) {
        if (resultSet.getString(3).equals("root.MIGRATION_SG3")) {
          assertEquals("CANCELED", resultSet.getString(4));
        }
      }
    }
  }
}
