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

package org.apache.iotdb.db.integration;

// import org.apache.iotdb.commons.exception.IllegalPathException;
// import org.apache.iotdb.commons.path.PartialPath;
// import org.apache.iotdb.db.engine.StorageEngine;
// import org.apache.iotdb.db.utils.EnvironmentUtils;
// import org.apache.iotdb.itbase.category.LocalStandaloneTest;
// import org.apache.iotdb.jdbc.Config;
//
// import org.junit.After;
// import org.junit.Before;
// import org.junit.Test;
// import org.junit.experimental.categories.Category;
// import org.slf4j.Logger;
// import org.slf4j.LoggerFactory;
//
// import java.sql.Connection;
// import java.sql.DriverManager;
// import java.sql.ResultSet;
// import java.sql.Statement;
// import java.util.ArrayList;
// import java.util.Arrays;
// import java.util.List;
//
// import static org.junit.Assert.assertEquals;
// import static org.junit.Assert.assertFalse;

// @Category({LocalStandaloneTest.class})
// public class IoTDBRemovePartitionIT {
//
//  private static final Logger logger = LoggerFactory.getLogger(IoTDBRemovePartitionIT.class);
//
//  private static int partitionInterval = 100;
//
//  @Before
//  public void setUp() throws Exception {
//    EnvironmentUtils.envSetUp();
////    StorageEngine.setTimePartitionInterval(partitionInterval);
//    insertData();
//  }
//
//  @After
//  public void tearDown() throws Exception {
////    StorageEngine.setTimePartitionInterval(-1);
//    EnvironmentUtils.cleanEnv();
//  }
//
//  @Test
//  public void testRemoveNoPartition() throws IllegalPathException {
//    logger.warn("running testRemoveNoPartition");
//    StorageEngine.getInstance()
//        .removePartitions(
//            new PartialPath("root.test1"), (storageGroupName, timePartitionId) -> false);
//
//    try (Connection connection =
//            DriverManager.getConnection(
//                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
//        Statement statement = connection.createStatement()) {
//      try (ResultSet resultSet = statement.executeQuery("SELECT * FROM root.test1")) {
//        int count = 0;
//        while (resultSet.next()) {
//          assertEquals(count / 2 * 100L + count % 2 * 50, resultSet.getLong(1));
//          assertEquals(count / 2 * 100L + count % 2 * 50, resultSet.getLong(2));
//          count++;
//        }
//        assertEquals(20, count);
//      }
//    } catch (Exception e) {
//      e.printStackTrace();
//    }
//  }
//
//  @Test
//  public void testRemovePartialPartition() throws IllegalPathException {
//    logger.warn("running testRemovePartialPartition");
//    StorageEngine.getInstance()
//        .removePartitions(
//            new PartialPath("root.test1"),
//            (storageGroupName, timePartitionId) -> timePartitionId >= 5);
//    StorageEngine.getInstance()
//        .removePartitions(
//            new PartialPath("root.test2"),
//            (storageGroupName, timePartitionId) -> timePartitionId < 5);
//
//    try (Connection connection =
//            DriverManager.getConnection(
//                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
//        Statement statement = connection.createStatement()) {
//      try (ResultSet resultSet = statement.executeQuery("SELECT * FROM root.test1")) {
//        int count = 0;
//        while (resultSet.next()) {
//          assertEquals(count / 2 * 100 + count % 2 * 50, resultSet.getLong(1));
//          assertEquals(count / 2 * 100 + count % 2 * 50, resultSet.getLong(2));
//          count++;
//        }
//        assertEquals(10, count);
//      }
//
//      try (ResultSet resultSet = statement.executeQuery("SELECT * FROM root.test2")) {
//        int count = 0;
//        while (resultSet.next()) {
//          assertEquals(count / 2 * 100 + count % 2 * 50 + 500, resultSet.getLong(1));
//          assertEquals(count / 2 * 100 + count % 2 * 50 + 500, resultSet.getLong(2));
//          count++;
//        }
//        assertEquals(10, count);
//      }
//    } catch (Exception e) {
//      e.printStackTrace();
//    }
//  }
//
//  @Test
//  public void testRemoveAllPartition() throws IllegalPathException {
//    logger.warn("running testRemoveAllPartition");
//    StorageEngine.getInstance()
//        .removePartitions(
//            new PartialPath("root.test1"), (storageGroupName, timePartitionId) -> true);
//
//    try (Connection connection =
//            DriverManager.getConnection(
//                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
//        Statement statement = connection.createStatement()) {
//      try (ResultSet resultSet = statement.executeQuery("SELECT * FROM root.test1")) {
//        assertFalse(resultSet.next());
//      }
//    } catch (Exception e) {
//      e.printStackTrace();
//    }
//  }
//
//  @Test
//  public void testSQLRemovePartition() {
//    logger.warn("running testSQLRemovePartition");
//    try (Connection connection =
//            DriverManager.getConnection(
//                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
//        Statement statement = connection.createStatement()) {
//      statement.execute("DELETE PARTITION root.test2 0,1,2,3,4");
//      try (ResultSet resultSet = statement.executeQuery("SELECT * FROM root.test2")) {
//        int count = 0;
//        while (resultSet.next()) {
//          assertEquals(count / 2 * 100 + count % 2 * 50 + 500, resultSet.getLong(1));
//          assertEquals(count / 2 * 100 + count % 2 * 50 + 500, resultSet.getLong(2));
//          count++;
//        }
//        assertEquals(10, count);
//      }
//    } catch (Exception e) {
//      e.printStackTrace();
//    }
//  }
//
//  @Test
//  public void testRemoveOnePartitionAndInsertData() {
//    logger.warn("running testRemoveOnePartitionAndInsertData");
//    try (Connection connection =
//            DriverManager.getConnection(
//                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
//        Statement statement = connection.createStatement()) {
//      statement.execute("create database root.test");
//      statement.execute("insert into root.test.wf02.wt02(timestamp,status) values(1,true)");
//      statement.execute("select * from root.test.wf02.wt02");
//      statement.execute("DELETE PARTITION root.test 0");
//      statement.execute("select * from root.test.wf02.wt02");
//      statement.execute("insert into root.test.wf02.wt02(timestamp,status) values(1,true)");
//      try (ResultSet resultSet = statement.executeQuery("select * from root.test.wf02.wt02")) {
//        assertEquals(true, resultSet.next());
//      }
//      statement.execute("flush");
//      try (ResultSet resultSet = statement.executeQuery("select * from root.test.wf02.wt02")) {
//        assertEquals(true, resultSet.next());
//      }
//    } catch (Exception e) {
//      e.printStackTrace();
//    }
//  }
//
//  @Test
//  public void testRemovePartitionAndInsertUnSeqDataAndMerge() {
//    logger.warn("running testRemovePartitionAndInsertUnSeqDataAndMerge");
//    try (Connection connection =
//            DriverManager.getConnection(
//                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
//        Statement statement = connection.createStatement()) {
//      statement.execute("create database root.test");
//      statement.execute("insert into root.test.wf02.wt02(timestamp,status) values(2,true)");
//      statement.execute("select * from root.test.wf02.wt02");
//      statement.execute("DELETE PARTITION root.test 0");
//      statement.execute("select * from root.test.wf02.wt02");
//      statement.execute("insert into root.test.wf02.wt02(timestamp,status) values(1,true)");
//      try (ResultSet resultSet = statement.executeQuery("select * from root.test.wf02.wt02")) {
//        assertEquals(true, resultSet.next());
//      }
//      statement.execute("insert into root.test.wf02.wt02(timestamp,status) values(3,true)");
//      statement.execute("merge");
//      int count = 0;
//      try (ResultSet resultSet = statement.executeQuery("select * from root.test.wf02.wt02")) {
//        while (resultSet.next()) {
//          count++;
//        }
//        assertEquals(2, count);
//      }
//    } catch (Exception e) {
//      e.printStackTrace();
//    }
//  }
//
//  @Test
//  public void testRemovePartitionAndInsertUnSeqDataAndUnSeqDataMerge() {
//    logger.warn("running testRemovePartitionAndInsertUnSeqDataAndUnSeqDataMerge");
//    try (Connection connection =
//            DriverManager.getConnection(
//                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
//        Statement statement = connection.createStatement()) {
//      statement.execute("create database root.test");
//      statement.execute("insert into root.test.wf02.wt02(timestamp,status) values(2,true)");
//      statement.execute("select * from root.test.wf02.wt02");
//      statement.execute("DELETE PARTITION root.test 0");
//      statement.execute("select * from root.test.wf02.wt02");
//      statement.execute("insert into root.test.wf02.wt02(timestamp,status) values(1,true)");
//      try (ResultSet resultSet = statement.executeQuery("select * from root.test.wf02.wt02")) {
//        assertEquals(true, resultSet.next());
//      }
//      statement.execute("insert into root.test.wf02.wt02(timestamp,status) values(2,true)");
//      statement.execute("merge");
//      int count = 0;
//      try (ResultSet resultSet = statement.executeQuery("select * from root.test.wf02.wt02")) {
//        while (resultSet.next()) {
//          count++;
//        }
//        assertEquals(2, count);
//      }
//    } catch (Exception e) {
//      e.printStackTrace();
//    }
//  }
//
//  @Test
//  public void testFlushAndRemoveOnePartitionAndInsertData() {
//    logger.warn("running testFlushAndRemoveOnePartitionAndInsertData");
//    try (Connection connection =
//            DriverManager.getConnection(
//                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
//        Statement statement = connection.createStatement()) {
//      statement.execute("create database root.test");
//      statement.execute("insert into root.test.wf02.wt02(timestamp,status) values(1,true)");
//      statement.execute("flush");
//      statement.execute("DELETE PARTITION root.test 0");
//      statement.execute("select * from root.test.wf02.wt02");
//      statement.execute("insert into root.test.wf02.wt02(timestamp,status) values(1,true)");
//      try (ResultSet resultSet = statement.executeQuery("select * from root.test.wf02.wt02")) {
//        assertEquals(true, resultSet.next());
//      }
//      statement.execute("flush");
//      int count = 0;
//      try (ResultSet resultSet = statement.executeQuery("select * from root.test.wf02.wt02")) {
//        assertEquals(true, resultSet.next());
//      }
//    } catch (Exception e) {
//      e.printStackTrace();
//    }
//  }
//
//  private static void insertData() throws ClassNotFoundException {
//    List<String> sqls =
//        new ArrayList<>(
//            Arrays.asList(
//                "CREATE DATABASE root.test1",
//                "CREATE DATABASE root.test2",
//                "CREATE TIMESERIES root.test1.s0 WITH DATATYPE=INT64,ENCODING=PLAIN",
//                "CREATE TIMESERIES root.test2.s0 WITH DATATYPE=INT64,ENCODING=PLAIN"));
//    // 10 partitions, each one with one seq file and one unseq file
//    for (int i = 0; i < 10; i++) {
//      // seq files
//      for (int j = 1; j <= 2; j++) {
//        sqls.add(
//            String.format(
//                "INSERT INTO root.test%d(timestamp, s0) VALUES (%d, %d)",
//                j, i * partitionInterval + 50, i * partitionInterval + 50));
//      }
//      // last file is unclosed
//      if (i < 9) {
//        sqls.add("FLUSH");
//      }
//      // unseq files
//      for (int j = 1; j <= 2; j++) {
//        sqls.add(
//            String.format(
//                "INSERT INTO root.test%d(timestamp, s0) VALUES (%d, %d)",
//                j, i * partitionInterval, i * partitionInterval));
//      }
//      sqls.add("MERGE");
//      // last file is unclosed
//      if (i < 9) {
//        sqls.add("FLUSH");
//      }
//    }
//    Class.forName(Config.JDBC_DRIVER_NAME);
//    try (Connection connection =
//            DriverManager.getConnection(
//                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
//        Statement statement = connection.createStatement()) {
//
//      for (String sql : sqls) {
//        statement.execute(sql);
//      }
//    } catch (Exception e) {
//      e.printStackTrace();
//    }
//  }
// }
