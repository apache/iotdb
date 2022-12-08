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
package org.apache.iotdb.db.integration.aligned;
//
// import org.apache.iotdb.commons.exception.IllegalPathException;
// import org.apache.iotdb.commons.path.PartialPath;
// import org.apache.iotdb.db.conf.IoTDBConfig;
// import org.apache.iotdb.db.conf.IoTDBDescriptor;
// import org.apache.iotdb.db.engine.StorageEngine;
// import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
// import org.apache.iotdb.db.engine.storagegroup.timeindex.TimeIndexLevel;
// import org.apache.iotdb.db.exception.StorageEngineException;
// import org.apache.iotdb.db.utils.EnvironmentUtils;
// import org.apache.iotdb.itbase.category.LocalStandaloneTest;
// import org.apache.iotdb.jdbc.Config;
//
// import org.junit.After;
// import org.junit.Assert;
// import org.junit.Before;
// import org.junit.Test;
// import org.junit.experimental.categories.Category;
// import org.slf4j.Logger;
// import org.slf4j.LoggerFactory;
//
// import java.io.File;
// import java.sql.Connection;
// import java.sql.DriverManager;
// import java.sql.ResultSet;
// import java.sql.SQLException;
// import java.sql.Statement;
// import java.util.ArrayList;
// import java.util.Arrays;
// import java.util.HashSet;
// import java.util.List;
// import java.util.Set;
//
// import static org.junit.Assert.assertEquals;
// import static org.junit.Assert.assertNotNull;
// import static org.junit.Assert.assertTrue;
// import static org.junit.Assert.fail;
//
// @Category({LocalStandaloneTest.class})
// public class IoTDBLoadExternalAlignedTsFileIT {
//
//  private static final Logger LOGGER =
//      LoggerFactory.getLogger(IoTDBLoadExternalAlignedTsFileIT.class);
//
//  private static final IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();
//
//  private static String[] insertSequenceSqls =
//      new String[] {
//        "CREATE DATABASE root.vehicle",
//        "CREATE DATABASE root.test",
//        "CREATE ALIGNED TIMESERIES root.vehicle.d0(s0 INT32 encoding=RLE, s1 TEXT
// encoding=PLAIN)",
//        "CREATE ALIGNED TIMESERIES root.vehicle.d1(s2 FLOAT encoding=RLE, s3 BOOLEAN
// encoding=PLAIN)",
//        "CREATE ALIGNED TIMESERIES root.test.d0(s0 INT32 encoding=RLE, s1 TEXT encoding=PLAIN)",
//        "CREATE ALIGNED TIMESERIES root.test.d1.g0(s0 INT32 encoding=RLE)",
//        "insert into root.vehicle.d0(timestamp,s0) aligned values(10,100)",
//        "insert into root.vehicle.d0(timestamp,s0,s1) aligned values(12,101,'102')",
//        "insert into root.vehicle.d0(timestamp,s1) aligned values(19,'103')",
//        "insert into root.vehicle.d1(timestamp,s2) aligned values(11,104.0)",
//        "insert into root.vehicle.d1(timestamp,s2,s3) aligned values(15,105.0,true)",
//        "insert into root.vehicle.d1(timestamp,s3) aligned values(17,false)",
//        "insert into root.vehicle.d0(timestamp,s0) aligned values(20,1000)",
//        "insert into root.test.d0(timestamp,s0) aligned values(10,106)",
//        "insert into root.test.d0(timestamp,s0,s1) aligned values(14,107,'108')",
//        "insert into root.test.d0(timestamp,s1) aligned values(16,'109')",
//        "insert into root.test.d1.g0(timestamp,s0) aligned values(1,110)",
//        "insert into root.test.d0(timestamp,s0) aligned values(30,1006)",
//        "insert into root.test.d0(timestamp,s0,s1) aligned values(34,1007,'1008')",
//        "insert into root.test.d0(timestamp,s1) aligned values(36,'1090')",
//        "insert into root.test.d1.g0(timestamp,s0) aligned values(10,1100)",
//        "flush",
//        "insert into root.test.d0(timestamp,s0) aligned values(150,126)",
//        "insert into root.test.d0(timestamp,s0,s1) aligned values(80,127,'128')",
//        "insert into root.test.d0(timestamp,s1) aligned values(200,'129')",
//        "insert into root.test.d1.g0(timestamp,s0) aligned values(140,430)",
//        "insert into root.test.d0(timestamp,s0) aligned values(150,426)",
//        "flush"
//      };
//
//  private static String[] insertUnsequenceSqls =
//      new String[] {
//        "insert into root.vehicle.d0(timestamp,s0) aligned values(6,120)",
//        "insert into root.vehicle.d0(timestamp,s0,s1) aligned values(38,121,'122')",
//        "insert into root.vehicle.d0(timestamp,s1) aligned values(9,'123')",
//        "insert into root.vehicle.d0(timestamp,s0) aligned values(16,128)",
//        "insert into root.vehicle.d0(timestamp,s0,s1) aligned values(18,189,'198')",
//        "insert into root.vehicle.d0(timestamp,s1) aligned values(99,'1234')",
//        "insert into root.vehicle.d1(timestamp,s2) aligned values(14,1024.0)",
//        "insert into root.vehicle.d1(timestamp,s2,s3) aligned values(29,1205.0,true)",
//        "insert into root.vehicle.d1(timestamp,s3) aligned values(33,true)",
//        "insert into root.test.d0(timestamp,s0) aligned values(45,126)",
//        "insert into root.test.d0(timestamp,s0,s1) aligned values(68,127,'128')",
//        "insert into root.test.d0(timestamp,s1) aligned values(78,'129')",
//        "insert into root.test.d1.g0(timestamp,s0) aligned values(14,430)",
//        "flush",
//        "insert into root.test.d0(timestamp,s0) aligned values(20,426)",
//        "insert into root.test.d0(timestamp,s0,s1) aligned values(13,427,'528')",
//        "insert into root.test.d0(timestamp,s1) aligned values(2,'1209')",
//        "insert into root.test.d1.g0(timestamp,s0) aligned values(4,330)",
//        "flush",
//      };
//
//  private static String[] deleteTimeseiresSqls =
//      new String[] {"delete from root.vehicle.** where time >= 10 and time<=20", "flush"};
//
//  private static final String TIMESTAMP_STR = "Time";
//  private static final String VEHICLE_D0_S0_STR = "root.vehicle.d0.s0";
//  private static final String VEHICLE_D0_S1_STR = "root.vehicle.d0.s1";
//  private static final String VEHICLE_D0_S2_STR = "root.vehicle.d1.s2";
//  private static final String VEHICLE_D0_S3_STR = "root.vehicle.d1.s3";
//  private static final String TEST_D0_S0_STR = "root.test.d0.s0";
//  private static final String TEST_D0_S1_STR = "root.test.d0.s1";
//  private static final String TEST_D1_STR = "root.test.d1.g0.s0";
//
//  private int prevVirtualPartitionNum;
//  private int prevCompactionThread;
//
//  private static String[] deleteSqls =
//      new String[] {"DELETE DATABASE root.vehicle", "DELETE DATABASE root.test"};
//
//  @Before
//  public void setUp() throws Exception {
//    prevVirtualPartitionNum = IoTDBDescriptor.getInstance().getConfig().getDataRegionNum();
//    IoTDBDescriptor.getInstance().getConfig().setDataRegionNum(1);
//    prevCompactionThread = IoTDBDescriptor.getInstance().getConfig().getCompactionThreadCount();
//    EnvironmentUtils.envSetUp();
//    Class.forName(Config.JDBC_DRIVER_NAME);
//    prepareData(insertSequenceSqls);
//  }
//
//  @After
//  public void tearDown() throws Exception {
//    EnvironmentUtils.cleanEnv();
//    IoTDBDescriptor.getInstance().getConfig().setCompactionThreadCount(prevCompactionThread);
//    IoTDBDescriptor.getInstance().getConfig().setDataRegionNum(prevVirtualPartitionNum);
//  }
//
//  @Test
//  public void unloadTsfileTest() throws SQLException {
//    try (Connection connection =
//            DriverManager.getConnection("jdbc:iotdb://127.0.0.1:6667/", "root", "root");
//        Statement statement = connection.createStatement()) {
//
//      // move root.vehicle
//      List<TsFileResource> resources =
//          new ArrayList<>(
//              StorageEngine.getInstance()
//                  .getProcessor(new PartialPath("root.vehicle"))
//                  .getSequenceFileList());
//      assertEquals(1, resources.size());
//      File tmpDir =
//          new File(
//              resources.get(0).getTsFile().getParentFile().getParentFile(),
//              "tmp" + File.separator + new PartialPath("root.vehicle"));
//      if (!tmpDir.exists()) {
//        tmpDir.mkdirs();
//      }
//      for (TsFileResource resource : resources) {
//        statement.execute(String.format("unload '%s' '%s'", resource.getTsFilePath(), tmpDir));
//      }
//      assertEquals(
//          0,
//          StorageEngine.getInstance()
//              .getProcessor(new PartialPath("root.vehicle"))
//              .getSequenceFileList()
//              .size());
//      assertNotNull(tmpDir.listFiles());
//      assertEquals(1, tmpDir.listFiles().length >> 1);
//
//      // move root.test
//      resources =
//          new ArrayList<>(
//              StorageEngine.getInstance()
//                  .getProcessor(new PartialPath("root.test"))
//                  .getSequenceFileList());
//      assertEquals(2, resources.size());
//      tmpDir =
//          new File(
//              resources.get(0).getTsFile().getParentFile().getParentFile(),
//              "tmp" + File.separator + new PartialPath("root.test"));
//      if (!tmpDir.exists()) {
//        tmpDir.mkdirs();
//      }
//      for (TsFileResource resource : resources) {
//        // test unload using relative path
//        statement.execute(
//            String.format("unload '%s' '%s'", "./" + resource.getTsFilePath(), tmpDir));
//      }
//      assertEquals(
//          0,
//          StorageEngine.getInstance()
//              .getProcessor(new PartialPath("root.test"))
//              .getSequenceFileList()
//              .size());
//      assertNotNull(tmpDir.listFiles());
//      assertEquals(2, tmpDir.listFiles().length >> 1);
//    } catch (StorageEngineException | IllegalPathException e) {
//      Assert.fail();
//    }
//  }
//
//  @Test
//  public void loadSequenceTsfileTest() throws SQLException {
//    try (Connection connection =
//            DriverManager.getConnection("jdbc:iotdb://127.0.0.1:6667/", "root", "root");
//        Statement statement = connection.createStatement()) {
//
//      // move root.vehicle
//      List<TsFileResource> resources =
//          new ArrayList<>(
//              StorageEngine.getInstance()
//                  .getProcessor(new PartialPath("root.vehicle"))
//                  .getSequenceFileList());
//      File tmpDir =
//          new File(
//              resources
//                  .get(0)
//                  .getTsFile()
//                  .getParentFile()
//                  .getParentFile()
//                  .getParentFile()
//                  .getParentFile(),
//              "tmp"
//                  + File.separator
//                  + new PartialPath("root.vehicle")
//                  + File.separator
//                  + "0"
//                  + File.separator
//                  + "0");
//      if (!tmpDir.exists()) {
//        tmpDir.mkdirs();
//      }
//      for (TsFileResource resource : resources) {
//        statement.execute(String.format("unload '%s' '%s'", resource.getTsFilePath(), tmpDir));
//      }
//
//      // move root.test
//      resources =
//          new ArrayList<>(
//              StorageEngine.getInstance()
//                  .getProcessor(new PartialPath("root.test"))
//                  .getSequenceFileList());
//      tmpDir =
//          new File(
//              resources
//                  .get(0)
//                  .getTsFile()
//                  .getParentFile()
//                  .getParentFile()
//                  .getParentFile()
//                  .getParentFile(),
//              "tmp"
//                  + File.separator
//                  + new PartialPath("root.test")
//                  + File.separator
//                  + "0"
//                  + File.separator
//                  + "0");
//      if (!tmpDir.exists()) {
//        tmpDir.mkdirs();
//      }
//      for (TsFileResource resource : resources) {
//        statement.execute(String.format("unload '%s' '%s'", resource.getTsFilePath(), tmpDir));
//      }
//
//      // load all tsfile in tmp dir
//      tmpDir =
//          new File(
//              resources
//                  .get(0)
//                  .getTsFile()
//                  .getParentFile()
//                  .getParentFile()
//                  .getParentFile()
//                  .getParentFile(),
//              "tmp");
//      statement.execute(String.format("load '%s'", tmpDir.getAbsolutePath()));
//      resources =
//          new ArrayList<>(
//              StorageEngine.getInstance()
//                  .getProcessor(new PartialPath("root.vehicle"))
//                  .getSequenceFileList());
//      assertEquals(1, resources.size());
//      resources =
//          new ArrayList<>(
//              StorageEngine.getInstance()
//                  .getProcessor(new PartialPath("root.test"))
//                  .getSequenceFileList());
//      assertEquals(2, resources.size());
//      assertNotNull(tmpDir.listFiles());
//      assertEquals(
//          0,
//          new File(
//                  tmpDir,
//                  new PartialPath("root.vehicle") + File.separator + "0" + File.separator + "0")
//              .listFiles()
//              .length);
//      assertEquals(
//          0,
//          new File(
//                  tmpDir,
//                  new PartialPath("root.test") + File.separator + "0" + File.separator + "0")
//              .listFiles()
//              .length);
//    } catch (StorageEngineException | IllegalPathException e) {
//      Assert.fail();
//    }
//  }
//
//  @Test
//  public void loadUnsequenceTsfileTest() throws SQLException {
//    prepareData(insertUnsequenceSqls);
//    String[] queryRes =
//        new String[] {
//          "1,null,null,null,null,null,null,110",
//          "2,null,null,null,null,null,1209,null",
//          "4,null,null,null,null,null,null,330",
//          "6,120,null,null,null,null,null,null",
//          "9,null,123,null,null,null,null,null",
//          "10,100,null,null,null,106,null,1100",
//          "11,null,null,104.0,null,null,null,null",
//          "12,101,102,null,null,null,null,null",
//          "13,null,null,null,null,427,528,null",
//          "14,null,null,1024.0,null,107,108,430",
//          "15,null,null,105.0,true,null,null,null",
//          "16,128,null,null,null,null,109,null",
//          "17,null,null,null,false,null,null,null",
//          "18,189,198,null,null,null,null,null",
//          "19,null,103,null,null,null,null,null",
//          "20,1000,null,null,null,426,null,null",
//          "29,null,null,1205.0,true,null,null,null",
//          "30,null,null,null,null,1006,null,null",
//          "33,null,null,null,true,null,null,null",
//          "34,null,null,null,null,1007,1008,null",
//          "36,null,null,null,null,null,1090,null",
//          "38,121,122,null,null,null,null,null",
//          "45,null,null,null,null,126,null,null",
//          "68,null,null,null,null,127,128,null",
//          "78,null,null,null,null,null,129,null",
//          "80,null,null,null,null,127,128,null",
//          "99,null,1234,null,null,null,null,null",
//          "140,null,null,null,null,null,null,430",
//          "150,null,null,null,null,426,null,null",
//          "200,null,null,null,null,null,129,null"
//        };
//    try (Connection connection =
//            DriverManager.getConnection("jdbc:iotdb://127.0.0.1:6667/", "root", "root");
//        Statement statement = connection.createStatement()) {
//
//      // check query result
//      boolean hasResultSet = statement.execute("SELECT * FROM root.**");
//      Assert.assertTrue(hasResultSet);
//      try (ResultSet resultSet = statement.getResultSet()) {
//        int cnt = 0;
//        while (resultSet.next()) {
//          String queryString =
//              resultSet.getString(TIMESTAMP_STR)
//                  + ","
//                  + resultSet.getString(VEHICLE_D0_S0_STR)
//                  + ","
//                  + resultSet.getString(VEHICLE_D0_S1_STR)
//                  + ","
//                  + resultSet.getString(VEHICLE_D0_S2_STR)
//                  + ","
//                  + resultSet.getString(VEHICLE_D0_S3_STR)
//                  + ","
//                  + resultSet.getString(TEST_D0_S0_STR)
//                  + ","
//                  + resultSet.getString(TEST_D0_S1_STR)
//                  + ","
//                  + resultSet.getString(TEST_D1_STR);
//          Assert.assertEquals(queryRes[cnt++], queryString);
//        }
//      }
//
//      // move root.vehicle
//      List<TsFileResource> resources =
//          new ArrayList<>(
//              StorageEngine.getInstance()
//                  .getProcessor(new PartialPath("root.vehicle"))
//                  .getSequenceFileList());
//      assertEquals(2, resources.size());
//      File tmpDir =
//          new File(
//              resources.get(0).getTsFile().getParentFile().getParentFile().getParentFile(),
//              "tmp" + File.separator + new PartialPath("root.vehicle") + File.separator + "0");
//      if (!tmpDir.exists()) {
//        tmpDir.mkdirs();
//      }
//      for (TsFileResource resource : resources) {
//        statement.execute(String.format("unload '%s' '%s'", resource.getTsFilePath(), tmpDir));
//      }
//      resources =
//          new ArrayList<>(
//              StorageEngine.getInstance()
//                  .getProcessor(new PartialPath("root.vehicle"))
//                  .getUnSequenceFileList());
//      assertEquals(1, resources.size());
//      for (TsFileResource resource : resources) {
//        statement.execute(String.format("unload '%s' '%s'", resource.getTsFilePath(), tmpDir));
//      }
//
//      // move root.test
//      resources =
//          new ArrayList<>(
//              StorageEngine.getInstance()
//                  .getProcessor(new PartialPath("root.test"))
//                  .getSequenceFileList());
//      assertEquals(2, resources.size());
//      tmpDir = new File(tmpDir.getParentFile().getParentFile(), "root.test" + File.separator +
// "0");
//      if (!tmpDir.exists()) {
//        tmpDir.mkdirs();
//      }
//      for (TsFileResource resource : resources) {
//        statement.execute(String.format("unload '%s' '%s'", resource.getTsFilePath(), tmpDir));
//      }
//      resources =
//          new ArrayList<>(
//              StorageEngine.getInstance()
//                  .getProcessor(new PartialPath("root.test"))
//                  .getUnSequenceFileList());
//      assertEquals(2, resources.size());
//      for (TsFileResource resource : resources) {
//        statement.execute(String.format("unload '%s' '%s'", resource.getTsFilePath(), tmpDir));
//      }
//
//      // load all tsfile in tmp dir
//      tmpDir = tmpDir.getParentFile().getParentFile();
//      statement.execute(String.format("load '%s'", tmpDir.getAbsolutePath()));
//      assertEquals(
//          2,
//          StorageEngine.getInstance()
//              .getProcessor(new PartialPath("root.vehicle"))
//              .getSequenceFileList()
//              .size());
//      assertEquals(
//          1,
//          StorageEngine.getInstance()
//              .getProcessor(new PartialPath("root.vehicle"))
//              .getUnSequenceFileList()
//              .size());
//      if (config.getTimeIndexLevel().equals(TimeIndexLevel.DEVICE_TIME_INDEX)) {
//        if (StorageEngine.getInstance()
//                .getProcessor(new PartialPath("root.test"))
//                .getUnSequenceFileList()
//                .size()
//            == 1) {
//          assertEquals(
//              3,
//              StorageEngine.getInstance()
//                  .getProcessor(new PartialPath("root.test"))
//                  .getSequenceFileList()
//                  .size());
//        } else {
//          assertEquals(
//              2,
//              StorageEngine.getInstance()
//                  .getProcessor(new PartialPath("root.test"))
//                  .getSequenceFileList()
//                  .size());
//        }
//      } else if (config.getTimeIndexLevel().equals(TimeIndexLevel.FILE_TIME_INDEX)) {
//        assertEquals(
//            2,
//            StorageEngine.getInstance()
//                .getProcessor(new PartialPath("root.test"))
//                .getUnSequenceFileList()
//                .size());
//        assertEquals(
//            2,
//            StorageEngine.getInstance()
//                .getProcessor(new PartialPath("root.test"))
//                .getSequenceFileList()
//                .size());
//      }
//      assertNotNull(tmpDir.listFiles());
//      assertEquals(
//          0,
//          new File(tmpDir, new PartialPath("root.vehicle") + File.separator + "0")
//              .listFiles()
//              .length);
//      assertEquals(
//          0,
//          new File(tmpDir, new PartialPath("root.test") + File.separator +
// "0").listFiles().length);
//
//      // check query result
//      hasResultSet = statement.execute("SELECT * FROM root.**");
//      Assert.assertTrue(hasResultSet);
//      try (ResultSet resultSet = statement.getResultSet()) {
//        int cnt = 0;
//        while (resultSet.next()) {
//          String queryString =
//              resultSet.getString(TIMESTAMP_STR)
//                  + ","
//                  + resultSet.getString(VEHICLE_D0_S0_STR)
//                  + ","
//                  + resultSet.getString(VEHICLE_D0_S1_STR)
//                  + ","
//                  + resultSet.getString(VEHICLE_D0_S2_STR)
//                  + ","
//                  + resultSet.getString(VEHICLE_D0_S3_STR)
//                  + ","
//                  + resultSet.getString(TEST_D0_S0_STR)
//                  + ","
//                  + resultSet.getString(TEST_D0_S1_STR)
//                  + ","
//                  + resultSet.getString(TEST_D1_STR);
//          Assert.assertEquals(queryRes[cnt++], queryString);
//        }
//      }
//    } catch (StorageEngineException | IllegalPathException e) {
//      Assert.fail();
//    }
//  }
//
//  @Test
//  public void loadTsfileWithModsTest() throws SQLException {
//    prepareData(insertUnsequenceSqls);
//    prepareData(deleteTimeseiresSqls);
//    String[] queryRes =
//        new String[] {
//          "1,null,null,null,null,null,null,110",
//          "2,null,null,null,null,null,1209,null",
//          "4,null,null,null,null,null,null,330",
//          "6,120,null,null,null,null,null,null",
//          "9,null,123,null,null,null,null,null",
//          "10,null,null,null,null,106,null,1100",
//          "13,null,null,null,null,427,528,null",
//          "14,null,null,null,null,107,108,430",
//          "16,null,null,null,null,null,109,null",
//          "20,null,null,null,null,426,null,null",
//          "29,null,null,1205.0,true,null,null,null",
//          "30,null,null,null,null,1006,null,null",
//          "33,null,null,null,true,null,null,null",
//          "34,null,null,null,null,1007,1008,null",
//          "36,null,null,null,null,null,1090,null",
//          "38,121,122,null,null,null,null,null",
//          "45,null,null,null,null,126,null,null",
//          "68,null,null,null,null,127,128,null",
//          "78,null,null,null,null,null,129,null",
//          "80,null,null,null,null,127,128,null",
//          "99,null,1234,null,null,null,null,null",
//          "140,null,null,null,null,null,null,430",
//          "150,null,null,null,null,426,null,null",
//          "200,null,null,null,null,null,129,null"
//        };
//    try (Connection connection =
//            DriverManager.getConnection("jdbc:iotdb://127.0.0.1:6667/", "root", "root");
//        Statement statement = connection.createStatement()) {
//
//      // check query result
//      boolean hasResultSet = statement.execute("SELECT * FROM root.**");
//      Assert.assertTrue(hasResultSet);
//      try (ResultSet resultSet = statement.getResultSet()) {
//        int cnt = 0;
//        while (resultSet.next()) {
//          String queryString =
//              resultSet.getString(TIMESTAMP_STR)
//                  + ","
//                  + resultSet.getString(VEHICLE_D0_S0_STR)
//                  + ","
//                  + resultSet.getString(VEHICLE_D0_S1_STR)
//                  + ","
//                  + resultSet.getString(VEHICLE_D0_S2_STR)
//                  + ","
//                  + resultSet.getString(VEHICLE_D0_S3_STR)
//                  + ","
//                  + resultSet.getString(TEST_D0_S0_STR)
//                  + ","
//                  + resultSet.getString(TEST_D0_S1_STR)
//                  + ","
//                  + resultSet.getString(TEST_D1_STR);
//          Assert.assertEquals(queryRes[cnt++], queryString);
//        }
//      }
//
//      // move root.vehicle
//      List<TsFileResource> resources =
//          new ArrayList<>(
//              StorageEngine.getInstance()
//                  .getProcessor(new PartialPath("root.vehicle"))
//                  .getSequenceFileList());
//      assertEquals(2, resources.size());
//      File tmpDir =
//          new File(
//              resources.get(0).getTsFile().getParentFile().getParentFile().getParentFile(),
//              "tmp" + File.separator + new PartialPath("root.vehicle") + File.separator + "0");
//      if (!tmpDir.exists()) {
//        tmpDir.mkdirs();
//      }
//      for (TsFileResource resource : resources) {
//        statement.execute(String.format("unload '%s' '%s'", resource.getTsFilePath(), tmpDir));
//      }
//      resources =
//          new ArrayList<>(
//              StorageEngine.getInstance()
//                  .getProcessor(new PartialPath("root.vehicle"))
//                  .getUnSequenceFileList());
//      assertEquals(1, resources.size());
//      for (TsFileResource resource : resources) {
//        statement.execute(String.format("unload '%s' '%s'", resource.getTsFilePath(), tmpDir));
//      }
//
//      // move root.test
//      resources =
//          new ArrayList<>(
//              StorageEngine.getInstance()
//                  .getProcessor(new PartialPath("root.test"))
//                  .getSequenceFileList());
//      assertEquals(2, resources.size());
//      tmpDir = new File(tmpDir.getParentFile().getParentFile(), "root.test" + File.separator +
// "0");
//      if (!tmpDir.exists()) {
//        tmpDir.mkdirs();
//      }
//      for (TsFileResource resource : resources) {
//        statement.execute(String.format("unload '%s' '%s'", resource.getTsFilePath(), tmpDir));
//      }
//      resources =
//          new ArrayList<>(
//              StorageEngine.getInstance()
//                  .getProcessor(new PartialPath("root.test"))
//                  .getUnSequenceFileList());
//      assertEquals(2, resources.size());
//      for (TsFileResource resource : resources) {
//        statement.execute(String.format("unload '%s' '%s'", resource.getTsFilePath(), tmpDir));
//      }
//
//      // load all tsfile in tmp dir
//      tmpDir = tmpDir.getParentFile().getParentFile();
//      statement.execute(String.format("load '%s'", tmpDir.getAbsolutePath()));
//      assertEquals(
//          2,
//          StorageEngine.getInstance()
//              .getProcessor(new PartialPath("root.vehicle"))
//              .getSequenceFileList()
//              .size());
//      assertEquals(
//          1,
//          StorageEngine.getInstance()
//              .getProcessor(new PartialPath("root.vehicle"))
//              .getUnSequenceFileList()
//              .size());
//      if (config.getTimeIndexLevel().equals(TimeIndexLevel.DEVICE_TIME_INDEX)) {
//        if (StorageEngine.getInstance()
//                .getProcessor(new PartialPath("root.test"))
//                .getUnSequenceFileList()
//                .size()
//            == 1) {
//          assertEquals(
//              3,
//              StorageEngine.getInstance()
//                  .getProcessor(new PartialPath("root.test"))
//                  .getSequenceFileList()
//                  .size());
//        } else {
//          assertEquals(
//              2,
//              StorageEngine.getInstance()
//                  .getProcessor(new PartialPath("root.test"))
//                  .getSequenceFileList()
//                  .size());
//        }
//      } else if (config.getTimeIndexLevel().equals(TimeIndexLevel.FILE_TIME_INDEX)) {
//        assertEquals(
//            2,
//            StorageEngine.getInstance()
//                .getProcessor(new PartialPath("root.test"))
//                .getUnSequenceFileList()
//                .size());
//        assertEquals(
//            2,
//            StorageEngine.getInstance()
//                .getProcessor(new PartialPath("root.test"))
//                .getSequenceFileList()
//                .size());
//      }
//      assertNotNull(tmpDir.listFiles());
//      assertEquals(
//          0,
//          new File(tmpDir, new PartialPath("root.vehicle") + File.separator + "0")
//              .listFiles()
//              .length);
//      assertEquals(
//          0,
//          new File(tmpDir, new PartialPath("root.test") + File.separator +
// "0").listFiles().length);
//
//      // check query result
//      hasResultSet = statement.execute("SELECT * FROM root.**");
//      Assert.assertTrue(hasResultSet);
//      try (ResultSet resultSet = statement.getResultSet()) {
//        int cnt = 0;
//        while (resultSet.next()) {
//          String queryString =
//              resultSet.getString(TIMESTAMP_STR)
//                  + ","
//                  + resultSet.getString(VEHICLE_D0_S0_STR)
//                  + ","
//                  + resultSet.getString(VEHICLE_D0_S1_STR)
//                  + ","
//                  + resultSet.getString(VEHICLE_D0_S2_STR)
//                  + ","
//                  + resultSet.getString(VEHICLE_D0_S3_STR)
//                  + ","
//                  + resultSet.getString(TEST_D0_S0_STR)
//                  + ","
//                  + resultSet.getString(TEST_D0_S1_STR)
//                  + ","
//                  + resultSet.getString(TEST_D1_STR);
//          Assert.assertEquals(queryRes[cnt++], queryString);
//        }
//      }
//    } catch (StorageEngineException | IllegalPathException e) {
//      Assert.fail();
//    }
//  }
//
//  @Test
//  public void loadTsFileTestWithAutoCreateSchema() throws SQLException {
//    try (Connection connection =
//            DriverManager.getConnection("jdbc:iotdb://127.0.0.1:6667/", "root", "root");
//        Statement statement = connection.createStatement()) {
//
//      // move root.vehicle
//      List<TsFileResource> resources =
//          new ArrayList<>(
//              StorageEngine.getInstance()
//                  .getProcessor(new PartialPath("root.vehicle"))
//                  .getSequenceFileList());
//
//      File tmpDir =
//          new File(
//              resources
//                  .get(0)
//                  .getTsFile()
//                  .getParentFile()
//                  .getParentFile()
//                  .getParentFile()
//                  .getParentFile(),
//              "tmp"
//                  + File.separator
//                  + "root.vehicle"
//                  + File.separator
//                  + "0"
//                  + File.separator
//                  + "0");
//      if (!tmpDir.exists()) {
//        tmpDir.mkdirs();
//      }
//      for (TsFileResource resource : resources) {
//        statement.execute(String.format("unload '%s' '%s'", resource.getTsFilePath(), tmpDir));
//      }
//
//      // move root.test
//      resources =
//          new ArrayList<>(
//              StorageEngine.getInstance()
//                  .getProcessor(new PartialPath("root.test"))
//                  .getSequenceFileList());
//      tmpDir =
//          new File(
//              resources
//                  .get(0)
//                  .getTsFile()
//                  .getParentFile()
//                  .getParentFile()
//                  .getParentFile()
//                  .getParentFile(),
//              "tmp" + File.separator + "root.test" + File.separator + "0" + File.separator + "0");
//      if (!tmpDir.exists()) {
//        tmpDir.mkdirs();
//      }
//      for (TsFileResource resource : resources) {
//        statement.execute(String.format("unload '%s' '%s'", resource.getTsFilePath(), tmpDir));
//      }
//
//      Set<String> expectedSet =
//          new HashSet<>(
//              Arrays.asList(
//                  "root.vehicle.d0.s0,root.vehicle,INT32",
//                  "root.vehicle.d0.s1,root.vehicle,TEXT",
//                  "root.vehicle.d1.s2,root.vehicle,FLOAT",
//                  "root.vehicle.d1.s3,root.vehicle,BOOLEAN",
//                  "root.test.d0.s0,root.test,INT32",
//                  "root.test.d0.s1,root.test,TEXT",
//                  "root.test.d1.g0.s0,root.test,INT32"));
//
//      boolean hasResultSet = statement.execute("SHOW timeseries");
//      Assert.assertTrue(hasResultSet);
//      try (ResultSet resultSet = statement.getResultSet()) {
//        while (resultSet.next()) {
//          Assert.assertTrue(
//              expectedSet.contains(
//                  resultSet.getString(1)
//                      + ","
//                      + resultSet.getString(3)
//                      + ","
//                      + resultSet.getString(4)));
//        }
//      }
//
//      // remove metadata
//      for (String sql : deleteSqls) {
//        statement.execute(sql);
//      }
//
//      // test not load metadata automatically, it will occur errors.
//      // UPDATE: load grammar is updated in 0.14, change this into load metadata automatically
//      boolean hasError = false;
//      try {
//        statement.execute(String.format("load '%s' sglevel=1", tmpDir.getAbsolutePath()));
//      } catch (Exception e) {
//        hasError = true;
//      }
//      Assert.assertFalse(hasError);
//
//      // test load metadata automatically, it will succeed.
//      tmpDir = tmpDir.getParentFile().getParentFile().getParentFile();
//      statement.execute(String.format("load '%s' sglevel=1", tmpDir.getAbsolutePath()));
//      resources =
//          new ArrayList<>(
//              StorageEngine.getInstance()
//                  .getProcessor(new PartialPath("root.vehicle"))
//                  .getSequenceFileList());
//      assertEquals(1, resources.size());
//      resources =
//          new ArrayList<>(
//              StorageEngine.getInstance()
//                  .getProcessor(new PartialPath("root.test"))
//                  .getSequenceFileList());
//      assertEquals(2, resources.size());
//      assertEquals(2, tmpDir.listFiles().length);
//      for (File dir : tmpDir.listFiles()) {
//        assertEquals(0, dir.listFiles()[0].listFiles()[0].listFiles().length);
//      }
//    } catch (StorageEngineException | IllegalPathException e) {
//      e.printStackTrace();
//      Assert.fail();
//    }
//  }
//
//  @Test
//  public void loadTsFileTestWithVerifyMetadata() throws Exception {
//    try (Connection connection =
//            DriverManager.getConnection("jdbc:iotdb://127.0.0.1:6667/", "root", "root");
//        Statement statement = connection.createStatement()) {
//      List<TsFileResource> resources =
//          new ArrayList<>(
//              StorageEngine.getInstance()
//                  .getProcessor(new PartialPath("root.vehicle"))
//                  .getSequenceFileList());
//      assertEquals(1, resources.size());
//      File vehicleTmpDir =
//          new File(
//              resources
//                  .get(0)
//                  .getTsFile()
//                  .getParentFile()
//                  .getParentFile()
//                  .getParentFile()
//                  .getParentFile(),
//              "tmp" + File.separator + "root.vehicle");
//      if (!vehicleTmpDir.exists()) {
//        vehicleTmpDir.mkdirs();
//      }
//
//      for (TsFileResource resource : resources) {
//        statement.execute(
//            String.format("unload '%s' '%s'", resource.getTsFilePath(), vehicleTmpDir));
//      }
//
//      resources =
//          new ArrayList<>(
//              StorageEngine.getInstance()
//                  .getProcessor(new PartialPath("root.test"))
//                  .getSequenceFileList());
//      assertEquals(2, resources.size());
//
//      File testTmpDir = new File(vehicleTmpDir.getParentFile(), "root.test");
//      if (!testTmpDir.exists()) {
//        testTmpDir.mkdirs();
//      }
//
//      for (TsFileResource resource : resources) {
//        statement.execute(String.format("unload '%s' '%s'", resource.getTsFilePath(),
// testTmpDir));
//      }
//
//      for (String sql : deleteSqls) {
//        statement.execute(sql);
//      }
//
//      List<String> metaDataSqls =
//          new ArrayList<>(
//              Arrays.asList(
//                  "CREATE DATABASE root.vehicle",
//                  "CREATE DATABASE root.test",
//                  "CREATE TIMESERIES root.vehicle.d0.s0 WITH DATATYPE=INT64, ENCODING=RLE",
//                  "CREATE TIMESERIES root.vehicle.d0.s1 WITH DATATYPE=TEXT, ENCODING=PLAIN",
//                  "CREATE TIMESERIES root.vehicle.d1.s2 WITH DATATYPE=FLOAT, ENCODING=RLE",
//                  "CREATE TIMESERIES root.vehicle.d1.s3 WITH DATATYPE=BOOLEAN, ENCODING=PLAIN",
//                  "CREATE ALIGNED TIMESERIES root.test.d0(s0 FLOAT encoding=RLE, s1 TEXT
// encoding=PLAIN)",
//                  "CREATE ALIGNED TIMESERIES root.test.d1.g0(s0 INT32 encoding=RLE)"));
//
//      for (String sql : metaDataSqls) {
//        statement.execute(sql);
//      }
//
//      // load vehicle
//      boolean hasError = false;
//      try {
//        statement.execute(String.format("load '%s'", vehicleTmpDir));
//      } catch (Exception e) {
//        hasError = true;
//        assertTrue(
//            e.getMessage()
//                .contains("is aligned in the loading TsFile but is not aligned in IoTDB."));
//      }
//      assertTrue(hasError);
//
//      statement.execute(String.format("load '%s' verify=false", vehicleTmpDir));
//      assertEquals(
//          1,
//          StorageEngine.getInstance()
//              .getProcessor(new PartialPath("root.vehicle"))
//              .getSequenceFileList()
//              .size());
//
//      // load test
//      hasError = false;
//      try {
//        statement.execute(String.format("load '%s'", testTmpDir));
//      } catch (Exception e) {
//        hasError = true;
//        assertTrue(
//            e.getMessage()
//                .contains(
//                    "because root.test.d0.s0 is INT32 in the loading TsFile but is FLOAT in
// IoTDB."));
//      }
//      assertTrue(hasError);
//
//      statement.execute(String.format("load '%s' verify=false", testTmpDir));
//      assertEquals(
//          2,
//          StorageEngine.getInstance()
//              .getProcessor(new PartialPath("root.test"))
//              .getSequenceFileList()
//              .size());
//
//    } catch (Exception e) {
//      e.printStackTrace();
//      Assert.fail();
//    }
//  }
//
//  @Test
//  public void removeTsFileTest() throws SQLException {
//    try (Connection connection =
//            DriverManager.getConnection("jdbc:iotdb://127.0.0.1:6667/", "root", "root");
//        Statement statement = connection.createStatement()) {
//      List<TsFileResource> resources =
//          new ArrayList<>(
//              StorageEngine.getInstance()
//                  .getProcessor(new PartialPath("root.vehicle"))
//                  .getSequenceFileList());
//      assertEquals(1, resources.size());
//      for (TsFileResource resource : resources) {
//        statement.execute(String.format("remove '%s'", resource.getTsFilePath()));
//      }
//      assertEquals(
//          0,
//          StorageEngine.getInstance()
//              .getProcessor(new PartialPath("root.vehicle"))
//              .getSequenceFileList()
//              .size());
//
//      resources =
//          new ArrayList<>(
//              StorageEngine.getInstance()
//                  .getProcessor(new PartialPath("root.test"))
//                  .getSequenceFileList());
//      assertEquals(2, resources.size());
//      for (TsFileResource resource : resources) {
//        statement.execute(String.format("remove '%s'", resource.getTsFilePath()));
//      }
//      assertEquals(
//          0,
//          StorageEngine.getInstance()
//              .getProcessor(new PartialPath("root.test"))
//              .getSequenceFileList()
//              .size());
//    } catch (StorageEngineException | IllegalPathException e) {
//      Assert.fail();
//    }
//  }
//
//  private void prepareData(String[] sqls) {
//    try (Connection connection =
//            DriverManager.getConnection(
//                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
//        Statement statement = connection.createStatement()) {
//
//      for (String sql : sqls) {
//        statement.execute(sql);
//      }
//
//    } catch (Exception e) {
//      LOGGER.error("Can not execute sql.", e);
//      fail();
//    }
//  }
// }
