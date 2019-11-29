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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import org.apache.iotdb.db.engine.StorageEngine;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.service.IoTDB;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.jdbc.Config;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class IoTDBLoadExternalTsfileTest {

  private static IoTDB daemon;
  private static String[] sqls = new String[]{
      "SET STORAGE GROUP TO root.vehicle",
      "SET STORAGE GROUP TO root.test",
      "CREATE TIMESERIES root.vehicle.d0.s0 WITH DATATYPE=INT32, ENCODING=RLE",
      "CREATE TIMESERIES root.vehicle.d0.s1 WITH DATATYPE=TEXT, ENCODING=PLAIN",
      "CREATE TIMESERIES root.vehicle.d1.s2 WITH DATATYPE=FLOAT, ENCODING=RLE",
      "CREATE TIMESERIES root.vehicle.d1.s3 WITH DATATYPE=BOOLEAN, ENCODING=PLAIN",
      "CREATE TIMESERIES root.test.d0.s0 WITH DATATYPE=INT32, ENCODING=RLE",
      "CREATE TIMESERIES root.test.d0.s1 WITH DATATYPE=TEXT, ENCODING=PLAIN",
      "CREATE TIMESERIES root.test.d1.g0.s0 WITH DATATYPE=INT32, ENCODING=RLE",
      "insert into root.vehicle.d0(timestamp,s0) values(10,100)",
      "insert into root.vehicle.d0(timestamp,s0,s1) values(12,101,'102')",
      "insert into root.vehicle.d0(timestamp,s1) values(19,'103')",
      "insert into root.vehicle.d1(timestamp,s2) values(11,104.0)",
      "insert into root.vehicle.d1(timestamp,s2,s3) values(15,105.0,true)",
      "insert into root.vehicle.d1(timestamp,s3) values(17,false)",
      "insert into root.vehicle.d0(timestamp,s0) values(20,1000)",
      "insert into root.test.d0(timestamp,s0) values(10,106)",
      "insert into root.test.d0(timestamp,s0,s1) values(14,107,'108')",
      "insert into root.test.d0(timestamp,s1) values(16,'109')",
      "insert into root.test.d1.g0(timestamp,s0) values(1,110)",
      "insert into root.test.d0(timestamp,s0) values(30,1006)",
      "insert into root.test.d0(timestamp,s0,s1) values(34,1007,'1008')",
      "insert into root.test.d0(timestamp,s1) values(36,'1090')",
      "insert into root.test.d1.g0(timestamp,s0) values(10,1100)",
      "flush",
      "insert into root.test.d0(timestamp,s0) values(150,126)",
      "insert into root.test.d0(timestamp,s0,s1) values(80,127,'128')",
      "insert into root.test.d0(timestamp,s1) values(200,'129')",
      "insert into root.test.d1.g0(timestamp,s0) values(140,430)",
      "insert into root.test.d0(timestamp,s0) values(150,426)",
      "flush"
  };

  private static final String TIMESTAMP_STR = "Time";
  private static final String TEMPERATURE_STR = "root.ln.wf01.wt01.temperature";
  private static final String STATUS_STR = "root.ln.wf01.wt01.status";
  private static final String HARDWARE_STR = "root.ln.wf01.wt01.hardware";

  @Before
  public void setUp() throws Exception {
    EnvironmentUtils.closeStatMonitor();
    daemon = IoTDB.getInstance();
    daemon.active();
    EnvironmentUtils.envSetUp();
    Class.forName(Config.JDBC_DRIVER_NAME);
    prepareData();
  }

  @After
  public void tearDown() throws Exception {
    daemon.stop();
    EnvironmentUtils.cleanEnv();
  }

  @Test
  public void moveTsfileTest() throws SQLException {
    try (Connection connection = DriverManager.
        getConnection("jdbc:iotdb://127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {

      // move root.vehicle
      List<TsFileResource> resources = new ArrayList<>(
          StorageEngine.getInstance().getProcessor("root.vehicle")
              .getSequenceFileList());
      assertEquals(1, resources.size());
      File tmpDir = new File(resources.get(0).getFile().getParentFile().getParentFile(), "tmp");
      if (!tmpDir.exists()) {
        tmpDir.mkdirs();
      }
      for (TsFileResource resource : resources) {
        statement.execute(String.format("move %s %s", resource.getFile().getPath(), tmpDir));
      }
      assertEquals(0, StorageEngine.getInstance().getProcessor("root.vehicle")
          .getSequenceFileList().size());
      assertNotNull(tmpDir.listFiles());
      assertEquals(1, tmpDir.listFiles().length >> 1);

      // move root.test
      resources = new ArrayList<>(
          StorageEngine.getInstance().getProcessor("root.test")
              .getSequenceFileList());
      assertEquals(2, resources.size());
      for (TsFileResource resource : resources) {
        statement.execute(String.format("move %s %s", resource.getFile().getPath(), tmpDir));
      }
      assertEquals(0, StorageEngine.getInstance().getProcessor("root.test")
          .getSequenceFileList().size());
      assertNotNull(tmpDir.listFiles());
      assertEquals(3, tmpDir.listFiles().length >> 1);
    } catch (StorageEngineException e) {
      Assert.fail();
    }
  }

  @Test
  public void loadTsfileTest() throws SQLException {
    try (Connection connection = DriverManager.
        getConnection("jdbc:iotdb://127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {

      // move root.vehicle
      List<TsFileResource> resources = new ArrayList<>(
          StorageEngine.getInstance().getProcessor("root.vehicle")
              .getSequenceFileList());
      File tmpDir = new File(resources.get(0).getFile().getParentFile().getParentFile(), "tmp");
      if (!tmpDir.exists()) {
        tmpDir.mkdirs();
      }
      for (TsFileResource resource : resources) {
        statement.execute(String.format("move %s %s", resource.getFile().getPath(), tmpDir));
      }

      // move root.test
      resources = new ArrayList<>(
          StorageEngine.getInstance().getProcessor("root.test")
              .getSequenceFileList());
      for (TsFileResource resource : resources) {
        statement.execute(String.format("move %s %s", resource.getFile().getPath(), tmpDir));
      }

      // load all tsfile in tmp dir
      statement.execute(String.format("load %s", tmpDir.getAbsolutePath()));
      resources = new ArrayList<>(
          StorageEngine.getInstance().getProcessor("root.vehicle")
              .getSequenceFileList());
      assertEquals(1, resources.size());
      resources = new ArrayList<>(
          StorageEngine.getInstance().getProcessor("root.test")
              .getSequenceFileList());
      assertEquals(2, resources.size());
      assertNotNull(tmpDir.listFiles());
      assertEquals(0, tmpDir.listFiles().length >> 1);
    } catch (StorageEngineException e) {
      Assert.fail();
    }
  }

  @Test
  public void removeTsfileTest() throws SQLException {
    try (Connection connection = DriverManager.
        getConnection("jdbc:iotdb://127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      List<TsFileResource> resources = new ArrayList<>(
          StorageEngine.getInstance().getProcessor("root.vehicle")
              .getSequenceFileList());
      assertEquals(1, resources.size());
      for (TsFileResource resource : resources) {
        statement.execute(String.format("remove %s", resource.getFile().getPath()));
      }
      assertEquals(0, StorageEngine.getInstance().getProcessor("root.vehicle")
          .getSequenceFileList().size());

      resources = new ArrayList<>(
          StorageEngine.getInstance().getProcessor("root.test")
              .getSequenceFileList());
      assertEquals(2, resources.size());
      for (TsFileResource resource : resources) {
        statement.execute(String.format("remove %s", resource.getFile().getPath()));
      }
      assertEquals(0, StorageEngine.getInstance().getProcessor("root.test")
          .getSequenceFileList().size());
    } catch (StorageEngineException e) {
      Assert.fail();
    }
  }

  private void prepareData() throws SQLException {
    try (Connection connection = DriverManager
        .getConnection(Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root",
            "root");
        Statement statement = connection.createStatement()) {

      for (String sql : sqls) {
        statement.execute(sql);
      }

    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}

