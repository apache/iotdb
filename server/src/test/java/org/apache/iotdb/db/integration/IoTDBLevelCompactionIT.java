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

import static org.junit.Assert.assertEquals;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.compaction.CompactionStrategy;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.jdbc.Config;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class IoTDBLevelCompactionIT {

  CompactionStrategy prevCompactionStrategy;

  @Before
  public void setUp() throws Exception {
    EnvironmentUtils.closeStatMonitor();
    prevCompactionStrategy = IoTDBDescriptor.getInstance().getConfig()
        .getCompactionStrategy();
    IoTDBDescriptor.getInstance().getConfig()
        .setCompactionStrategy(CompactionStrategy.LEVEL_COMPACTION);
    EnvironmentUtils.envSetUp();
    Class.forName(Config.JDBC_DRIVER_NAME);
  }

  @After
  public void tearDown() throws Exception {
    EnvironmentUtils.cleanEnv();
    IoTDBDescriptor.getInstance().getConfig().setCompactionStrategy(prevCompactionStrategy);
  }

  /**
   * test compaction files num > MAX_FILE_NUM_IN_LEVEL * MAX_LEVEL_NUM
   */
  @Test
  public void test() throws SQLException {
    int prevSeqLevelFileNum = IoTDBDescriptor.getInstance().getConfig().getSeqFileNumInEachLevel();
    int prevSeqLevelNum = IoTDBDescriptor.getInstance().getConfig().getSeqLevelNum();
    IoTDBDescriptor.getInstance().getConfig().setSeqFileNumInEachLevel(2);
    IoTDBDescriptor.getInstance().getConfig().setSeqLevelNum(3);
    try (Connection connection = DriverManager
        .getConnection(Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      statement.execute("SET STORAGE GROUP TO root.compactionTest");
      for (int i = 1; i <= 3; i++) {
        try {
          statement.execute("CREATE TIMESERIES root.compactionTest.s" + i + " WITH DATATYPE=INT64,"
              + "ENCODING=PLAIN");
        } catch (SQLException e) {
          // ignore
        }
      }

      int flushCount = 32;
      for (int i = 0; i < flushCount; i++) {
        statement
            .execute(
                String.format("INSERT INTO root.compactionTest(timestamp,s1,s2,s3) VALUES (%d,%d,"
                    + "%d,%d)", i, i + 1, i + 2, i + 3));
        statement.execute("FLUSH");
      }

      int cnt;
      try (ResultSet resultSet = statement.executeQuery("SELECT * FROM root.compactionTest")) {
        cnt = 0;
        while (resultSet.next()) {
          long time = resultSet.getLong("Time");
          long s1 = resultSet.getLong("root.compactionTest.s1");
          long s2 = resultSet.getLong("root.compactionTest.s2");
          long s3 = resultSet.getLong("root.compactionTest.s3");
          assertEquals(time + 1, s1);
          assertEquals(time + 2, s2);
          assertEquals(time + 3, s3);
          cnt++;
        }
      }
      assertEquals(flushCount, cnt);
    }
    IoTDBDescriptor.getInstance().getConfig().setSeqFileNumInEachLevel(prevSeqLevelFileNum);
    IoTDBDescriptor.getInstance().getConfig().setSeqLevelNum(prevSeqLevelNum);
  }

  /**
   * test compaction first use deserialize merge and then use append merge
   */
  @Test
  public void testAppendMergeAfterDeserializeMerge() throws SQLException {
    boolean prevEnableUnseqCompaction = IoTDBDescriptor.getInstance().getConfig()
        .isEnableUnseqCompaction();
    int prevSeqLevelFileNum = IoTDBDescriptor.getInstance().getConfig().getSeqFileNumInEachLevel();
    int prevSeqLevelNum = IoTDBDescriptor.getInstance().getConfig().getSeqLevelNum();
    IoTDBDescriptor.getInstance().getConfig().setSeqFileNumInEachLevel(2);
    IoTDBDescriptor.getInstance().getConfig().setSeqLevelNum(3);
    IoTDBDescriptor.getInstance().getConfig().setEnableUnseqCompaction(false);
    try (Connection connection = DriverManager
        .getConnection(Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      statement.execute("SET STORAGE GROUP TO root.compactionTest");
      try {
        statement.execute("CREATE TIMESERIES root.compactionTest.s1 WITH DATATYPE=INT64");
      } catch (SQLException e) {
        // ignore
      }

      long pageSize = 100;
      long timestamp = 1;

      for (long row = 0; row < 10000; row++) {
        statement
            .execute(
                String.format("INSERT INTO root.compactionTest(timestamp,s1) VALUES (%d,%d)",
                    timestamp, 1));
        if (row % pageSize == 0) {
          statement.execute("FLUSH");
        }
        timestamp++;
      }

      timestamp = 8322;

      for (long row = 0; row < 2400; row++) {
        statement
            .execute(
                String.format("INSERT INTO root.compactionTest(timestamp,s1) VALUES (%d,%d)",
                    timestamp, 1));
        if (row % pageSize == 0) {
          statement.execute("FLUSH");
        }
        timestamp++;
      }

      int cnt;
      try (ResultSet resultSet = statement
          .executeQuery("SELECT COUNT(s1) FROM root.compactionTest")) {
        cnt = 0;
        while (resultSet.next()) {
          System.out.println(resultSet.getLong(1));
          assertEquals(10721, resultSet.getLong(1));
          cnt++;
        }
      }
      assertEquals(1, cnt);
    }
    IoTDBDescriptor.getInstance().getConfig().setSeqFileNumInEachLevel(prevSeqLevelFileNum);
    IoTDBDescriptor.getInstance().getConfig().setSeqLevelNum(prevSeqLevelNum);
    IoTDBDescriptor.getInstance().getConfig().setEnableUnseqCompaction(prevEnableUnseqCompaction);
  }

  /**
   * test compaction first use append merge and then use deserialize merge
   */
  @Test
  public void testDeserializeMergeAfterAppendMerge() throws SQLException {
    boolean prevEnableUnseqCompaction = IoTDBDescriptor.getInstance().getConfig()
        .isEnableUnseqCompaction();
    int prevSeqLevelFileNum = IoTDBDescriptor.getInstance().getConfig().getSeqFileNumInEachLevel();
    int prevSeqLevelNum = IoTDBDescriptor.getInstance().getConfig().getSeqLevelNum();
    IoTDBDescriptor.getInstance().getConfig().setSeqFileNumInEachLevel(2);
    IoTDBDescriptor.getInstance().getConfig().setSeqLevelNum(3);
    IoTDBDescriptor.getInstance().getConfig().setEnableUnseqCompaction(false);
    try (Connection connection = DriverManager
        .getConnection(Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      statement.execute("SET STORAGE GROUP TO root.compactionTest");
      try {
        statement.execute("CREATE TIMESERIES root.compactionTest.s1 WITH DATATYPE=INT64");
      } catch (SQLException e) {
        // ignore
      }

      long pageSize = 100;
      long timestamp = 1;

      int prevMergePagePointNumberThreshold = IoTDBDescriptor.getInstance().getConfig()
          .getMergePagePointNumberThreshold();
      IoTDBDescriptor.getInstance().getConfig().setMergePagePointNumberThreshold(1);

      for (long row = 0; row < 10000; row++) {
        statement
            .execute(
                String.format("INSERT INTO root.compactionTest(timestamp,s1) VALUES (%d,%d)",
                    timestamp, 1));
        if (row % pageSize == 0) {
          statement.execute("FLUSH");
        }
        timestamp++;
      }

      timestamp = 8322;

      IoTDBDescriptor.getInstance().getConfig().setMergePagePointNumberThreshold(10000000);

      for (long row = 0; row < 2400; row++) {
        statement
            .execute(
                String.format("INSERT INTO root.compactionTest(timestamp,s1) VALUES (%d,%d)",
                    timestamp, 1));
        if (row % pageSize == 0) {
          statement.execute("FLUSH");
        }
        timestamp++;
      }

      int cnt;
      try (ResultSet resultSet = statement
          .executeQuery("SELECT COUNT(s1) FROM root.compactionTest")) {
        cnt = 0;
        while (resultSet.next()) {
          System.out.println(resultSet.getLong(1));
          assertEquals(10721, resultSet.getLong(1));
          cnt++;
        }
      }
      assertEquals(1, cnt);
      IoTDBDescriptor.getInstance().getConfig()
          .setMergePagePointNumberThreshold(prevMergePagePointNumberThreshold);
    }
    IoTDBDescriptor.getInstance().getConfig().setSeqFileNumInEachLevel(prevSeqLevelFileNum);
    IoTDBDescriptor.getInstance().getConfig().setSeqLevelNum(prevSeqLevelNum);
    IoTDBDescriptor.getInstance().getConfig().setEnableUnseqCompaction(prevEnableUnseqCompaction);
  }

  private void testCompactionNoUnseq(int mergeCount) throws SQLException {
    int prevSeqLevelFileNum = IoTDBDescriptor.getInstance().getConfig().getSeqFileNumInEachLevel();
    int prevSeqLevelNum = IoTDBDescriptor.getInstance().getConfig().getSeqLevelNum();
    IoTDBDescriptor.getInstance().getConfig().setSeqFileNumInEachLevel(2);
    IoTDBDescriptor.getInstance().getConfig().setSeqLevelNum(3);
    try (Connection connection = DriverManager
        .getConnection(Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      statement.execute("SET STORAGE GROUP TO root.compactionTest");
      for (int i = 1; i <= 3; i++) {
        try {
          statement.execute("CREATE TIMESERIES root.compactionTest.s" + i + " WITH DATATYPE=INT64,"
              + "ENCODING=PLAIN");
        } catch (SQLException e) {
          // ignore
        }
      }

      for (int i = 0; i < mergeCount; i++) {
        statement
            .execute(
                String.format("INSERT INTO root.compactionTest(timestamp,s1,s2,s3) VALUES (%d,%d,"
                    + "%d,%d)", i, i + 1, i + 2, i + 3));
        statement.execute("FLUSH");
      }

      int cnt;
      try (ResultSet resultSet = statement.executeQuery("SELECT * FROM root.compactionTest")) {
        cnt = 0;
        while (resultSet.next()) {
          long time = resultSet.getLong("Time");
          long s1 = resultSet.getLong("root.compactionTest.s1");
          long s2 = resultSet.getLong("root.compactionTest.s2");
          long s3 = resultSet.getLong("root.compactionTest.s3");
          assertEquals(time + 1, s1);
          assertEquals(time + 2, s2);
          assertEquals(time + 3, s3);
          cnt++;
        }
      }
      assertEquals(mergeCount, cnt);
    }
    IoTDBDescriptor.getInstance().getConfig().setSeqFileNumInEachLevel(prevSeqLevelFileNum);
    IoTDBDescriptor.getInstance().getConfig().setSeqLevelNum(prevSeqLevelNum);
  }

  /**
   * test compaction just once, no unseq
   */
  @Test
  public void testCompactionOnceNoUnseq() throws SQLException {
    this.testCompactionNoUnseq(2);
  }

  /**
   * test compaction to just once, with unseq
   */
  @Test
  public void testCompactionOnceWithUnseq() throws SQLException {
    int prevSeqLevelFileNum = IoTDBDescriptor.getInstance().getConfig().getSeqFileNumInEachLevel();
    int prevSeqLevelNum = IoTDBDescriptor.getInstance().getConfig().getSeqLevelNum();
    IoTDBDescriptor.getInstance().getConfig().setSeqFileNumInEachLevel(2);
    IoTDBDescriptor.getInstance().getConfig().setSeqLevelNum(3);
    try (Connection connection = DriverManager
        .getConnection(Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      statement.execute("SET STORAGE GROUP TO root.compactionTest");
      for (int i = 1; i <= 3; i++) {
        try {
          statement.execute("CREATE TIMESERIES root.compactionTest.s" + i + " WITH DATATYPE=INT64,"
              + "ENCODING=PLAIN");
        } catch (SQLException e) {
          // ignore
        }
      }

      statement
          .execute(
              String.format("INSERT INTO root.compactionTest(timestamp,s1,s2,s3) VALUES (%d,%d,"
                  + "%d,%d)", 1, 2, 3, 4));
      statement.execute("FLUSH");
      statement
          .execute(
              String.format("INSERT INTO root.compactionTest(timestamp,s1,s2,s3) VALUES (%d,%d,"
                  + "%d,%d)", 0, 1, 2, 3));
      statement.execute("FLUSH");

      int cnt;
      try (ResultSet resultSet = statement.executeQuery("SELECT * FROM root.compactionTest")) {
        cnt = 0;
        while (resultSet.next()) {
          long time = resultSet.getLong("Time");
          long s1 = resultSet.getLong("root.compactionTest.s1");
          long s2 = resultSet.getLong("root.compactionTest.s2");
          long s3 = resultSet.getLong("root.compactionTest.s3");
          assertEquals(time + 1, s1);
          assertEquals(time + 2, s2);
          assertEquals(time + 3, s3);
          cnt++;
        }
      }
      assertEquals(2, cnt);
    }
    IoTDBDescriptor.getInstance().getConfig().setSeqFileNumInEachLevel(prevSeqLevelFileNum);
    IoTDBDescriptor.getInstance().getConfig().setSeqLevelNum(prevSeqLevelNum);
  }

  /**
   * test compaction to second level once, no unseq
   */
  @Test
  public void testCompactionToSecondLevelNoUnseq() throws SQLException {
    this.testCompactionNoUnseq(4);
  }

  /**
   * test compaction to second level once, with unseq
   */
  @Test
  public void testCompactionToSecondLevelWithUnseq() throws SQLException {
    int prevSeqLevelFileNum = IoTDBDescriptor.getInstance().getConfig().getSeqFileNumInEachLevel();
    int prevSeqLevelNum = IoTDBDescriptor.getInstance().getConfig().getSeqLevelNum();
    IoTDBDescriptor.getInstance().getConfig().setSeqFileNumInEachLevel(2);
    IoTDBDescriptor.getInstance().getConfig().setSeqLevelNum(3);
    try (Connection connection = DriverManager
        .getConnection(Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      statement.execute("SET STORAGE GROUP TO root.compactionTest");
      for (int i = 1; i <= 3; i++) {
        try {
          statement.execute("CREATE TIMESERIES root.compactionTest.s" + i + " WITH DATATYPE=INT64,"
              + "ENCODING=PLAIN");
        } catch (SQLException e) {
          // ignore
        }
      }

      for (int i = 1; i < 3; i++) {
        statement
            .execute(
                String.format("INSERT INTO root.compactionTest(timestamp,s1,s2,s3) VALUES (%d,%d,"
                    + "%d,%d)", i, i + 1, i + 2, i + 3));
        statement.execute("FLUSH");
      }

      for (int i = 0; i < 1; i++) {
        statement
            .execute(
                String.format("INSERT INTO root.compactionTest(timestamp,s1,s2,s3) VALUES (%d,%d,"
                    + "%d,%d)", i, i + 1, i + 2, i + 3));
        statement.execute("FLUSH");
      }

      for (int i = 3; i < 5; i++) {
        statement
            .execute(
                String.format("INSERT INTO root.compactionTest(timestamp,s1,s2,s3) VALUES (%d,%d,"
                    + "%d,%d)", i, i + 1, i + 2, i + 3));
        statement.execute("FLUSH");
      }

      int cnt;
      try (ResultSet resultSet = statement.executeQuery("SELECT * FROM root.compactionTest")) {
        cnt = 0;
        while (resultSet.next()) {
          long time = resultSet.getLong("Time");
          long s1 = resultSet.getLong("root.compactionTest.s1");
          long s2 = resultSet.getLong("root.compactionTest.s2");
          long s3 = resultSet.getLong("root.compactionTest.s3");
          assertEquals(time + 1, s1);
          assertEquals(time + 2, s2);
          assertEquals(time + 3, s3);
          cnt++;
        }
      }
      assertEquals(5, cnt);
    }
    IoTDBDescriptor.getInstance().getConfig().setSeqFileNumInEachLevel(prevSeqLevelFileNum);
    IoTDBDescriptor.getInstance().getConfig().setSeqLevelNum(prevSeqLevelNum);
  }

  /**
   * test compaction to second level once, with unseq, disable unseqCompaction
   */
  @Test
  public void testCompactionToSecondLevelWithUnseqDisableUnseqCompaction() throws SQLException {
    int prevSeqLevelFileNum = IoTDBDescriptor.getInstance().getConfig().getSeqFileNumInEachLevel();
    int prevSeqLevelNum = IoTDBDescriptor.getInstance().getConfig().getSeqLevelNum();
    boolean prevEnableUnseqCompaction = IoTDBDescriptor.getInstance().getConfig()
        .isEnableUnseqCompaction();
    IoTDBDescriptor.getInstance().getConfig().setSeqFileNumInEachLevel(2);
    IoTDBDescriptor.getInstance().getConfig().setSeqLevelNum(3);
    try (Connection connection = DriverManager
        .getConnection(Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      statement.execute("SET STORAGE GROUP TO root.compactionTest");
      for (int i = 1; i <= 3; i++) {
        try {
          statement.execute("CREATE TIMESERIES root.compactionTest.s" + i + " WITH DATATYPE=INT64,"
              + "ENCODING=PLAIN");
        } catch (SQLException e) {
          // ignore
        }
      }

      for (int i = 1; i < 3; i++) {
        statement
            .execute(
                String.format("INSERT INTO root.compactionTest(timestamp,s1,s2,s3) VALUES (%d,%d,"
                    + "%d,%d)", i, i + 1, i + 2, i + 3));
        statement.execute("FLUSH");
      }

      for (int i = 0; i < 1; i++) {
        statement
            .execute(
                String.format("INSERT INTO root.compactionTest(timestamp,s1,s2,s3) VALUES (%d,%d,"
                    + "%d,%d)", i, i + 1, i + 2, i + 3));
        statement.execute("FLUSH");
      }

      for (int i = 3; i < 5; i++) {
        statement
            .execute(
                String.format("INSERT INTO root.compactionTest(timestamp,s1,s2,s3) VALUES (%d,%d,"
                    + "%d,%d)", i, i + 1, i + 2, i + 3));
        statement.execute("FLUSH");
      }

      int cnt;
      try (ResultSet resultSet = statement.executeQuery("SELECT * FROM root.compactionTest")) {
        cnt = 0;
        while (resultSet.next()) {
          long time = resultSet.getLong("Time");
          long s1 = resultSet.getLong("root.compactionTest.s1");
          long s2 = resultSet.getLong("root.compactionTest.s2");
          long s3 = resultSet.getLong("root.compactionTest.s3");
          assertEquals(time + 1, s1);
          assertEquals(time + 2, s2);
          assertEquals(time + 3, s3);
          cnt++;
        }
      }
      assertEquals(5, cnt);
    }
    IoTDBDescriptor.getInstance().getConfig().setSeqFileNumInEachLevel(prevSeqLevelFileNum);
    IoTDBDescriptor.getInstance().getConfig().setSeqLevelNum(prevSeqLevelNum);
    IoTDBDescriptor.getInstance().getConfig().setEnableUnseqCompaction(prevEnableUnseqCompaction);
  }

  /**
   * test compaction to stable level once, No unseq
   */
  @Test
  public void testCompactionToStableLevelNoUnseq() throws SQLException {
    this.testCompactionNoUnseq(8);
  }

  /**
   * test compaction to stable level once, with unseq
   */
  @Test
  public void testCompactionToStableLevelWithUnseq() throws SQLException {
    int prevSeqLevelFileNum = IoTDBDescriptor.getInstance().getConfig().getSeqFileNumInEachLevel();
    int prevSeqLevelNum = IoTDBDescriptor.getInstance().getConfig().getSeqLevelNum();
    IoTDBDescriptor.getInstance().getConfig().setSeqFileNumInEachLevel(2);
    IoTDBDescriptor.getInstance().getConfig().setSeqLevelNum(3);
    try (Connection connection = DriverManager
        .getConnection(Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      statement.execute("SET STORAGE GROUP TO root.compactionTest");
      for (int i = 1; i <= 3; i++) {
        try {
          statement.execute("CREATE TIMESERIES root.compactionTest.s" + i + " WITH DATATYPE=INT64,"
              + "ENCODING=PLAIN");
        } catch (SQLException e) {
          // ignore
        }
      }

      for (int i = 4; i < 8; i++) {
        statement
            .execute(
                String.format("INSERT INTO root.compactionTest(timestamp,s1,s2,s3) VALUES (%d,%d,"
                    + "%d,%d)", i, i + 1, i + 2, i + 3));
        statement.execute("FLUSH");
      }

      for (int i = 0; i < 4; i++) {
        statement
            .execute(
                String.format("INSERT INTO root.compactionTest(timestamp,s1,s2,s3) VALUES (%d,%d,"
                    + "%d,%d)", i, i + 1, i + 2, i + 3));
        statement.execute("FLUSH");
      }

      for (int i = 8; i < 12; i++) {
        statement
            .execute(
                String.format("INSERT INTO root.compactionTest(timestamp,s1,s2,s3) VALUES (%d,%d,"
                    + "%d,%d)", i, i + 1, i + 2, i + 3));
        statement.execute("FLUSH");
      }

      int cnt;
      try (ResultSet resultSet = statement.executeQuery("SELECT * FROM root.compactionTest")) {
        cnt = 0;
        while (resultSet.next()) {
          long time = resultSet.getLong("Time");
          long s1 = resultSet.getLong("root.compactionTest.s1");
          long s2 = resultSet.getLong("root.compactionTest.s2");
          long s3 = resultSet.getLong("root.compactionTest.s3");
          assertEquals(time + 1, s1);
          assertEquals(time + 2, s2);
          assertEquals(time + 3, s3);
          cnt++;
        }
      }
      assertEquals(12, cnt);
    }
    IoTDBDescriptor.getInstance().getConfig().setSeqFileNumInEachLevel(prevSeqLevelFileNum);
    IoTDBDescriptor.getInstance().getConfig().setSeqLevelNum(prevSeqLevelNum);
  }

  /**
   * test compaction to stable level once, with unseq, disable unseqCompaction
   */
  @Test
  public void testCompactionToStableLevelWithUnseqDisableUnseqCompaction() throws SQLException {
    int prevSeqLevelFileNum = IoTDBDescriptor.getInstance().getConfig().getSeqFileNumInEachLevel();
    int prevSeqLevelNum = IoTDBDescriptor.getInstance().getConfig().getSeqLevelNum();
    boolean prevEnableUnseqCompaction = IoTDBDescriptor.getInstance().getConfig()
        .isEnableUnseqCompaction();
    IoTDBDescriptor.getInstance().getConfig().setSeqFileNumInEachLevel(2);
    IoTDBDescriptor.getInstance().getConfig().setSeqLevelNum(3);
    try (Connection connection = DriverManager
        .getConnection(Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      statement.execute("SET STORAGE GROUP TO root.compactionTest");
      for (int i = 1; i <= 3; i++) {
        try {
          statement.execute("CREATE TIMESERIES root.compactionTest.s" + i + " WITH DATATYPE=INT64,"
              + "ENCODING=PLAIN");
        } catch (SQLException e) {
          // ignore
        }
      }

      for (int i = 4; i < 8; i++) {
        statement
            .execute(
                String.format("INSERT INTO root.compactionTest(timestamp,s1,s2,s3) VALUES (%d,%d,"
                    + "%d,%d)", i, i + 1, i + 2, i + 3));
        statement.execute("FLUSH");
      }

      for (int i = 0; i < 4; i++) {
        statement
            .execute(
                String.format("INSERT INTO root.compactionTest(timestamp,s1,s2,s3) VALUES (%d,%d,"
                    + "%d,%d)", i, i + 1, i + 2, i + 3));
        statement.execute("FLUSH");
      }

      for (int i = 8; i < 12; i++) {
        statement
            .execute(
                String.format("INSERT INTO root.compactionTest(timestamp,s1,s2,s3) VALUES (%d,%d,"
                    + "%d,%d)", i, i + 1, i + 2, i + 3));
        statement.execute("FLUSH");
      }

      int cnt;
      try (ResultSet resultSet = statement.executeQuery("SELECT * FROM root.compactionTest")) {
        cnt = 0;
        while (resultSet.next()) {
          long time = resultSet.getLong("Time");
          long s1 = resultSet.getLong("root.compactionTest.s1");
          long s2 = resultSet.getLong("root.compactionTest.s2");
          long s3 = resultSet.getLong("root.compactionTest.s3");
          assertEquals(time + 1, s1);
          assertEquals(time + 2, s2);
          assertEquals(time + 3, s3);
          cnt++;
        }
      }
      assertEquals(12, cnt);
    }
    IoTDBDescriptor.getInstance().getConfig().setSeqFileNumInEachLevel(prevSeqLevelFileNum);
    IoTDBDescriptor.getInstance().getConfig().setSeqLevelNum(prevSeqLevelNum);
    IoTDBDescriptor.getInstance().getConfig().setEnableUnseqCompaction(prevEnableUnseqCompaction);
  }

  /**
   * test seq max level num = 0
   */
  @Test
  public void testCompactionSeqMaxLevelNumError0() throws SQLException {
    int prevSeqLevelFileNum = IoTDBDescriptor.getInstance().getConfig().getSeqFileNumInEachLevel();
    int prevSeqLevelNum = IoTDBDescriptor.getInstance().getConfig().getSeqLevelNum();
    IoTDBDescriptor.getInstance().getConfig().setSeqFileNumInEachLevel(2);
    IoTDBDescriptor.getInstance().getConfig().setSeqLevelNum(0);
    try (Connection connection = DriverManager
        .getConnection(Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      statement.execute("SET STORAGE GROUP TO root.compactionTest");
      for (int i = 1; i <= 3; i++) {
        try {
          statement.execute("CREATE TIMESERIES root.compactionTest.s" + i + " WITH DATATYPE=INT64,"
              + "ENCODING=PLAIN");
        } catch (SQLException e) {
          // ignore
        }
      }

      for (int i = 4; i < 8; i++) {
        statement
            .execute(
                String.format("INSERT INTO root.compactionTest(timestamp,s1,s2,s3) VALUES (%d,%d,"
                    + "%d,%d)", i, i + 1, i + 2, i + 3));
        statement.execute("FLUSH");
      }

      for (int i = 0; i < 4; i++) {
        statement
            .execute(
                String.format("INSERT INTO root.compactionTest(timestamp,s1,s2,s3) VALUES (%d,%d,"
                    + "%d,%d)", i, i + 1, i + 2, i + 3));
        statement.execute("FLUSH");
      }

      for (int i = 8; i < 12; i++) {
        statement
            .execute(
                String.format("INSERT INTO root.compactionTest(timestamp,s1,s2,s3) VALUES (%d,%d,"
                    + "%d,%d)", i, i + 1, i + 2, i + 3));
        statement.execute("FLUSH");
      }

      int cnt;
      try (ResultSet resultSet = statement.executeQuery("SELECT * FROM root.compactionTest")) {
        cnt = 0;
        while (resultSet.next()) {
          long time = resultSet.getLong("Time");
          long s1 = resultSet.getLong("root.compactionTest.s1");
          long s2 = resultSet.getLong("root.compactionTest.s2");
          long s3 = resultSet.getLong("root.compactionTest.s3");
          assertEquals(time + 1, s1);
          assertEquals(time + 2, s2);
          assertEquals(time + 3, s3);
          cnt++;
        }
      }
      assertEquals(12, cnt);
    }
    IoTDBDescriptor.getInstance().getConfig().setSeqFileNumInEachLevel(prevSeqLevelFileNum);
    IoTDBDescriptor.getInstance().getConfig().setSeqLevelNum(prevSeqLevelNum);
  }

  /**
   * test seq max level num = -1
   */
  @Test
  public void testCompactionSeqMaxLevelNumError1() throws SQLException {
    int prevSeqLevelFileNum = IoTDBDescriptor.getInstance().getConfig().getSeqFileNumInEachLevel();
    int prevSeqLevelNum = IoTDBDescriptor.getInstance().getConfig().getSeqLevelNum();
    IoTDBDescriptor.getInstance().getConfig().setSeqFileNumInEachLevel(2);
    IoTDBDescriptor.getInstance().getConfig().setSeqLevelNum(-1);
    try (Connection connection = DriverManager
        .getConnection(Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      statement.execute("SET STORAGE GROUP TO root.compactionTest");
      for (int i = 1; i <= 3; i++) {
        try {
          statement.execute("CREATE TIMESERIES root.compactionTest.s" + i + " WITH DATATYPE=INT64,"
              + "ENCODING=PLAIN");
        } catch (SQLException e) {
          // ignore
        }
      }

      for (int i = 4; i < 8; i++) {
        statement
            .execute(
                String.format("INSERT INTO root.compactionTest(timestamp,s1,s2,s3) VALUES (%d,%d,"
                    + "%d,%d)", i, i + 1, i + 2, i + 3));
        statement.execute("FLUSH");
      }

      for (int i = 0; i < 4; i++) {
        statement
            .execute(
                String.format("INSERT INTO root.compactionTest(timestamp,s1,s2,s3) VALUES (%d,%d,"
                    + "%d,%d)", i, i + 1, i + 2, i + 3));
        statement.execute("FLUSH");
      }

      for (int i = 8; i < 12; i++) {
        statement
            .execute(
                String.format("INSERT INTO root.compactionTest(timestamp,s1,s2,s3) VALUES (%d,%d,"
                    + "%d,%d)", i, i + 1, i + 2, i + 3));
        statement.execute("FLUSH");
      }

      int cnt;
      try (ResultSet resultSet = statement.executeQuery("SELECT * FROM root.compactionTest")) {
        cnt = 0;
        while (resultSet.next()) {
          long time = resultSet.getLong("Time");
          long s1 = resultSet.getLong("root.compactionTest.s1");
          long s2 = resultSet.getLong("root.compactionTest.s2");
          long s3 = resultSet.getLong("root.compactionTest.s3");
          assertEquals(time + 1, s1);
          assertEquals(time + 2, s2);
          assertEquals(time + 3, s3);
          cnt++;
        }
      }
      assertEquals(12, cnt);
    }
    IoTDBDescriptor.getInstance().getConfig().setSeqFileNumInEachLevel(prevSeqLevelFileNum);
    IoTDBDescriptor.getInstance().getConfig().setSeqLevelNum(prevSeqLevelNum);
  }

  /**
   * test seq file num in each level = 0
   */
  @Test
  public void testCompactionSeqFileNumInEachLevelError0() throws SQLException {
    int prevSeqLevelFileNum = IoTDBDescriptor.getInstance().getConfig().getSeqFileNumInEachLevel();
    int prevSeqLevelNum = IoTDBDescriptor.getInstance().getConfig().getSeqLevelNum();
    IoTDBDescriptor.getInstance().getConfig().setSeqFileNumInEachLevel(0);
    IoTDBDescriptor.getInstance().getConfig().setSeqLevelNum(3);
    try (Connection connection = DriverManager
        .getConnection(Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      statement.execute("SET STORAGE GROUP TO root.compactionTest");
      for (int i = 1; i <= 3; i++) {
        try {
          statement.execute("CREATE TIMESERIES root.compactionTest.s" + i + " WITH DATATYPE=INT64,"
              + "ENCODING=PLAIN");
        } catch (SQLException e) {
          // ignore
        }
      }

      for (int i = 4; i < 8; i++) {
        statement
            .execute(
                String.format("INSERT INTO root.compactionTest(timestamp,s1,s2,s3) VALUES (%d,%d,"
                    + "%d,%d)", i, i + 1, i + 2, i + 3));
        statement.execute("FLUSH");
      }

      for (int i = 0; i < 4; i++) {
        statement
            .execute(
                String.format("INSERT INTO root.compactionTest(timestamp,s1,s2,s3) VALUES (%d,%d,"
                    + "%d,%d)", i, i + 1, i + 2, i + 3));
        statement.execute("FLUSH");
      }

      for (int i = 8; i < 12; i++) {
        statement
            .execute(
                String.format("INSERT INTO root.compactionTest(timestamp,s1,s2,s3) VALUES (%d,%d,"
                    + "%d,%d)", i, i + 1, i + 2, i + 3));
        statement.execute("FLUSH");
      }

      int cnt;
      try (ResultSet resultSet = statement.executeQuery("SELECT * FROM root.compactionTest")) {
        cnt = 0;
        while (resultSet.next()) {
          long time = resultSet.getLong("Time");
          long s1 = resultSet.getLong("root.compactionTest.s1");
          long s2 = resultSet.getLong("root.compactionTest.s2");
          long s3 = resultSet.getLong("root.compactionTest.s3");
          assertEquals(time + 1, s1);
          assertEquals(time + 2, s2);
          assertEquals(time + 3, s3);
          cnt++;
        }
      }
      assertEquals(12, cnt);
    }
    IoTDBDescriptor.getInstance().getConfig().setSeqFileNumInEachLevel(prevSeqLevelFileNum);
    IoTDBDescriptor.getInstance().getConfig().setSeqLevelNum(prevSeqLevelNum);
  }

  /**
   * test seq max level num = -1
   */
  @Test
  public void testCompactionSeqFileNumInEachLevelError1() throws SQLException {
    int prevSeqLevelFileNum = IoTDBDescriptor.getInstance().getConfig().getSeqFileNumInEachLevel();
    int prevSeqLevelNum = IoTDBDescriptor.getInstance().getConfig().getSeqLevelNum();
    IoTDBDescriptor.getInstance().getConfig().setSeqFileNumInEachLevel(-1);
    IoTDBDescriptor.getInstance().getConfig().setSeqLevelNum(3);
    try (Connection connection = DriverManager
        .getConnection(Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      statement.execute("SET STORAGE GROUP TO root.compactionTest");
      for (int i = 1; i <= 3; i++) {
        try {
          statement.execute("CREATE TIMESERIES root.compactionTest.s" + i + " WITH DATATYPE=INT64,"
              + "ENCODING=PLAIN");
        } catch (SQLException e) {
          // ignore
        }
      }

      for (int i = 4; i < 8; i++) {
        statement
            .execute(
                String.format("INSERT INTO root.compactionTest(timestamp,s1,s2,s3) VALUES (%d,%d,"
                    + "%d,%d)", i, i + 1, i + 2, i + 3));
        statement.execute("FLUSH");
      }

      for (int i = 0; i < 4; i++) {
        statement
            .execute(
                String.format("INSERT INTO root.compactionTest(timestamp,s1,s2,s3) VALUES (%d,%d,"
                    + "%d,%d)", i, i + 1, i + 2, i + 3));
        statement.execute("FLUSH");
      }

      for (int i = 8; i < 12; i++) {
        statement
            .execute(
                String.format("INSERT INTO root.compactionTest(timestamp,s1,s2,s3) VALUES (%d,%d,"
                    + "%d,%d)", i, i + 1, i + 2, i + 3));
        statement.execute("FLUSH");
      }

      int cnt;
      try (ResultSet resultSet = statement.executeQuery("SELECT * FROM root.compactionTest")) {
        cnt = 0;
        while (resultSet.next()) {
          long time = resultSet.getLong("Time");
          long s1 = resultSet.getLong("root.compactionTest.s1");
          long s2 = resultSet.getLong("root.compactionTest.s2");
          long s3 = resultSet.getLong("root.compactionTest.s3");
          assertEquals(time + 1, s1);
          assertEquals(time + 2, s2);
          assertEquals(time + 3, s3);
          cnt++;
        }
      }
      assertEquals(12, cnt);
    }
    IoTDBDescriptor.getInstance().getConfig().setSeqFileNumInEachLevel(prevSeqLevelFileNum);
    IoTDBDescriptor.getInstance().getConfig().setSeqLevelNum(prevSeqLevelNum);
  }

  /**
   * test compaction with unseq compaction
   */
  @Test
  public void testCompactionWithUnseqCompaction() throws SQLException {
    int prevSeqLevelFileNum = IoTDBDescriptor.getInstance().getConfig().getSeqFileNumInEachLevel();
    int prevSeqLevelNum = IoTDBDescriptor.getInstance().getConfig().getSeqLevelNum();
    int prevUnSeqLevelFileNum = IoTDBDescriptor.getInstance().getConfig()
        .getUnseqFileNumInEachLevel();
    int prevUnSeqLevelNum = IoTDBDescriptor.getInstance().getConfig().getUnseqLevelNum();
    boolean prevEnableUnseqCompaction = IoTDBDescriptor.getInstance().getConfig()
        .isEnableUnseqCompaction();
    IoTDBDescriptor.getInstance().getConfig().setEnableUnseqCompaction(false);
    IoTDBDescriptor.getInstance().getConfig().setSeqFileNumInEachLevel(2);
    IoTDBDescriptor.getInstance().getConfig().setSeqLevelNum(3);
    IoTDBDescriptor.getInstance().getConfig().setUnseqFileNumInEachLevel(2);
    IoTDBDescriptor.getInstance().getConfig().setUnseqLevelNum(3);
    try (Connection connection = DriverManager
        .getConnection(Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      statement.execute("SET STORAGE GROUP TO root.compactionTest");
      for (int i = 1; i <= 3; i++) {
        try {
          statement.execute("CREATE TIMESERIES root.compactionTest.s" + i + " WITH DATATYPE=INT64,"
              + "ENCODING=PLAIN");
        } catch (SQLException e) {
          // ignore
        }
      }

      for (int i = 10000; i < 10001; i++) {
        statement
            .execute(
                String.format("INSERT INTO root.compactionTest(timestamp,s1,s2,s3) VALUES (%d,%d,"
                    + "%d,%d)", i, i + 1, i + 2, i + 3));
        statement.execute("FLUSH");
      }

      for (int i = 0; i < 50; i++) {
        statement
            .execute(
                String.format("INSERT INTO root.compactionTest(timestamp,s1,s2,s3) VALUES (%d,%d,"
                    + "%d,%d)", i, i + 1, i + 2, i + 3));
        statement.execute("FLUSH");
      }

      for (int i = 10001; i < 10005; i++) {
        statement
            .execute(
                String.format("INSERT INTO root.compactionTest(timestamp,s1,s2,s3) VALUES (%d,%d,"
                    + "%d,%d)", i, i + 1, i + 2, i + 3));
        statement.execute("FLUSH");
      }

      int cnt;
      try (ResultSet resultSet = statement.executeQuery("SELECT * FROM root.compactionTest")) {
        cnt = 0;
        while (resultSet.next()) {
          long time = resultSet.getLong("Time");
          long s1 = resultSet.getLong("root.compactionTest.s1");
          long s2 = resultSet.getLong("root.compactionTest.s2");
          long s3 = resultSet.getLong("root.compactionTest.s3");
          assertEquals(time + 1, s1);
          assertEquals(time + 2, s2);
          assertEquals(time + 3, s3);
          cnt++;
        }
      }
      assertEquals(55, cnt);

      IoTDBDescriptor.getInstance().getConfig().setEnableUnseqCompaction(true);
      for (int i = 10010; i < 10055; i++) {
        statement
            .execute(
                String.format("INSERT INTO root.compactionTest(timestamp,s1,s2,s3) VALUES (%d,%d,"
                    + "%d,%d)", i, i + 1, i + 2, i + 3));
        statement.execute("FLUSH");
      }

      try (ResultSet resultSet = statement.executeQuery("SELECT * FROM root.compactionTest")) {
        cnt = 0;
        while (resultSet.next()) {
          long time = resultSet.getLong("Time");
          long s1 = resultSet.getLong("root.compactionTest.s1");
          long s2 = resultSet.getLong("root.compactionTest.s2");
          long s3 = resultSet.getLong("root.compactionTest.s3");
          assertEquals(time + 1, s1);
          assertEquals(time + 2, s2);
          assertEquals(time + 3, s3);
          cnt++;
        }
      }
      assertEquals(100, cnt);
    }

    IoTDBDescriptor.getInstance().getConfig().setEnableUnseqCompaction(prevEnableUnseqCompaction);
    IoTDBDescriptor.getInstance().getConfig().setSeqFileNumInEachLevel(prevSeqLevelFileNum);
    IoTDBDescriptor.getInstance().getConfig().setSeqLevelNum(prevSeqLevelNum);
    IoTDBDescriptor.getInstance().getConfig().setUnseqFileNumInEachLevel(prevUnSeqLevelFileNum);
    IoTDBDescriptor.getInstance().getConfig().setUnseqLevelNum(prevUnSeqLevelNum);
  }

  /**
   * test compaction with deletion timeseries
   */
  @Test
  public void testCompactionWithDeletionTimeseries() throws SQLException {
    int prevSeqLevelFileNum = IoTDBDescriptor.getInstance().getConfig().getSeqFileNumInEachLevel();
    int prevSeqLevelNum = IoTDBDescriptor.getInstance().getConfig().getSeqLevelNum();
    int prevUnSeqLevelFileNum = IoTDBDescriptor.getInstance().getConfig()
        .getUnseqFileNumInEachLevel();
    int prevUnSeqLevelNum = IoTDBDescriptor.getInstance().getConfig().getUnseqLevelNum();
    boolean prevEnableUnseqCompaction = IoTDBDescriptor.getInstance().getConfig()
        .isEnableUnseqCompaction();
    IoTDBDescriptor.getInstance().getConfig().setEnableUnseqCompaction(false);
    IoTDBDescriptor.getInstance().getConfig().setSeqFileNumInEachLevel(2);
    IoTDBDescriptor.getInstance().getConfig().setSeqLevelNum(3);
    IoTDBDescriptor.getInstance().getConfig().setUnseqFileNumInEachLevel(2);
    IoTDBDescriptor.getInstance().getConfig().setUnseqLevelNum(3);
    try (Connection connection = DriverManager
        .getConnection(Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      statement.execute("SET STORAGE GROUP TO root.compactionTest");
      for (int i = 1; i <= 3; i++) {
        try {
          statement.execute("CREATE TIMESERIES root.compactionTest.s" + i + " WITH DATATYPE=INT64,"
              + "ENCODING=PLAIN");
        } catch (SQLException e) {
          // ignore
        }
      }

      for (int i = 0; i < 1; i++) {
        statement
            .execute(
                String.format("INSERT INTO root.compactionTest(timestamp,s1,s2,s3) VALUES (%d,%d,"
                    + "%d,%d)", i, i + 1, i + 2, i + 3));
        statement.execute("FLUSH");
      }
      statement.execute("DELETE timeseries root.compactionTest.s1");

      for (int i = 1; i < 2; i++) {
        statement
            .execute(
                String.format("INSERT INTO root.compactionTest(timestamp,s2,s3) VALUES (%d,"
                    + "%d,%d)", i, i + 2, i + 3));
        statement.execute("FLUSH");
      }

      int cnt;
      try (ResultSet resultSet = statement.executeQuery("SELECT * FROM root.compactionTest")) {
        cnt = 0;
        while (resultSet.next()) {
          long time = resultSet.getLong("Time");
          long s2 = resultSet.getLong("root.compactionTest.s2");
          long s3 = resultSet.getLong("root.compactionTest.s3");
          assertEquals(time + 2, s2);
          assertEquals(time + 3, s3);
          cnt++;
        }
      }
      assertEquals(2, cnt);
    }

    IoTDBDescriptor.getInstance().getConfig().setEnableUnseqCompaction(prevEnableUnseqCompaction);
    IoTDBDescriptor.getInstance().getConfig().setSeqFileNumInEachLevel(prevSeqLevelFileNum);
    IoTDBDescriptor.getInstance().getConfig().setSeqLevelNum(prevSeqLevelNum);
    IoTDBDescriptor.getInstance().getConfig().setUnseqFileNumInEachLevel(prevUnSeqLevelFileNum);
    IoTDBDescriptor.getInstance().getConfig().setUnseqLevelNum(prevUnSeqLevelNum);
  }

  /**
   * test compaction with deletion timeseries and create different type
   */
  @Test
  public void testCompactionWithDeletionTimeseriesAndCreateDifferentTypeTest() throws SQLException {
    int prevSeqLevelFileNum = IoTDBDescriptor.getInstance().getConfig().getSeqFileNumInEachLevel();
    int prevSeqLevelNum = IoTDBDescriptor.getInstance().getConfig().getSeqLevelNum();
    int prevUnSeqLevelFileNum = IoTDBDescriptor.getInstance().getConfig()
        .getUnseqFileNumInEachLevel();
    int prevUnSeqLevelNum = IoTDBDescriptor.getInstance().getConfig().getUnseqLevelNum();
    boolean prevEnableUnseqCompaction = IoTDBDescriptor.getInstance().getConfig()
        .isEnableUnseqCompaction();
    IoTDBDescriptor.getInstance().getConfig().setEnableUnseqCompaction(false);
    IoTDBDescriptor.getInstance().getConfig().setSeqFileNumInEachLevel(2);
    IoTDBDescriptor.getInstance().getConfig().setSeqLevelNum(3);
    IoTDBDescriptor.getInstance().getConfig().setUnseqFileNumInEachLevel(2);
    IoTDBDescriptor.getInstance().getConfig().setUnseqLevelNum(3);
    try (Connection connection = DriverManager
        .getConnection(Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      statement.execute("SET STORAGE GROUP TO root.compactionTest");
      for (int i = 1; i <= 3; i++) {
        try {
          statement.execute("CREATE TIMESERIES root.compactionTest.s" + i + " WITH DATATYPE=INT64,"
              + "ENCODING=PLAIN");
        } catch (SQLException e) {
          // ignore
        }
      }

      for (int i = 10000; i < 10001; i++) {
        statement
            .execute(
                String.format("INSERT INTO root.compactionTest(timestamp,s1,s2,s3) VALUES (%d,%d,"
                    + "%d,%d)", i, i + 1, i + 2, i + 3));
        statement.execute("FLUSH");
      }

      for (int i = 0; i < 50; i++) {
        statement
            .execute(
                String.format("INSERT INTO root.compactionTest(timestamp,s1,s2,s3) VALUES (%d,%d,"
                    + "%d,%d)", i, i + 1, i + 2, i + 3));
        if (i % 2 == 1) {
          statement.execute("FLUSH");
        }
      }
      statement.execute("FLUSH");
      statement.execute("DELETE timeseries root.compactionTest.s1");
      statement.execute(
          "create timeseries root.compactionTest.s1 with datatype=FLOAT, encoding=PLAIN");

      for (int i = 10001; i < 10005; i++) {
        statement
            .execute(
                String.format("INSERT INTO root.compactionTest(timestamp,s1,s2,s3) VALUES (%d,%d,"
                    + "%d,%d)", i, i + 1, i + 2, i + 3));
        statement.execute("FLUSH");
      }

      int cnt;
      try (ResultSet resultSet = statement.executeQuery("SELECT * FROM root.compactionTest")) {
        cnt = 0;
        while (resultSet.next()) {
          long time = resultSet.getLong("Time");
          long s2 = resultSet.getLong("root.compactionTest.s2");
          long s3 = resultSet.getLong("root.compactionTest.s3");
          assertEquals(time + 2, s2);
          assertEquals(time + 3, s3);
          cnt++;
        }
      }
      assertEquals(55, cnt);

      IoTDBDescriptor.getInstance().getConfig().setEnableUnseqCompaction(true);
      for (int i = 10010; i < 10055; i++) {
        statement
            .execute(
                String.format("INSERT INTO root.compactionTest(timestamp,s1,s2,s3) VALUES (%d,%d,"
                    + "%d,%d)", i, i + 1, i + 2, i + 3));
        statement.execute("FLUSH");
      }

      try (ResultSet resultSet = statement.executeQuery("SELECT * FROM root.compactionTest")) {
        cnt = 0;
        while (resultSet.next()) {
          long time = resultSet.getLong("Time");
          long s2 = resultSet.getLong("root.compactionTest.s2");
          long s3 = resultSet.getLong("root.compactionTest.s3");
          assertEquals(time + 2, s2);
          assertEquals(time + 3, s3);
          cnt++;
        }
      }
      assertEquals(100, cnt);
    }

    IoTDBDescriptor.getInstance().getConfig().setEnableUnseqCompaction(prevEnableUnseqCompaction);
    IoTDBDescriptor.getInstance().getConfig().setSeqFileNumInEachLevel(prevSeqLevelFileNum);
    IoTDBDescriptor.getInstance().getConfig().setSeqLevelNum(prevSeqLevelNum);
    IoTDBDescriptor.getInstance().getConfig().setUnseqFileNumInEachLevel(prevUnSeqLevelFileNum);
    IoTDBDescriptor.getInstance().getConfig().setUnseqLevelNum(prevUnSeqLevelNum);
  }
}
