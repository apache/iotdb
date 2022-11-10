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

import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.StorageEngineV2;
import org.apache.iotdb.db.integration.sync.SyncTestUtil;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.itbase.category.LocalStandaloneTest;
import org.apache.iotdb.jdbc.Config;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

@Category({LocalStandaloneTest.class})
public class IoTDBLoadExternalTsfileWithVirtualSGIT extends IoTDBLoadExternalTsfileIT {
  @Before
  public void setUp() throws Exception {
    prevVirtualPartitionNum = IoTDBDescriptor.getInstance().getConfig().getDataRegionNum();
    IoTDBDescriptor.getInstance().getConfig().setDataRegionNum(2);
    prevCompactionThread = IoTDBDescriptor.getInstance().getConfig().getCompactionThreadCount();
    EnvironmentUtils.envSetUp();
    StorageEngineV2.getInstance().reset();
    Class.forName(Config.JDBC_DRIVER_NAME);
    prepareData(insertSequenceSqls);
  }

  @Test
  public void unloadTsfileWithVSGTest() throws SQLException {
    try (Connection connection =
            DriverManager.getConnection("jdbc:iotdb://127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {

      // move root.vehicle
      File vehicleDir =
          new File(
              IoTDBDescriptor.getInstance().getConfig().getDataDirs()[0],
              IoTDBConstant.SEQUENCE_FLODER_NAME + File.separator + "root.vehicle");
      List<File> vehicleFiles = SyncTestUtil.getTsFilePaths(vehicleDir);
      File tmpDir =
          new File(
              IoTDBDescriptor.getInstance().getConfig().getDataDirs()[0],
              "tmp" + File.separator + new PartialPath("root.vehicle"));
      if (!tmpDir.exists()) {
        tmpDir.mkdirs();
      }
      for (File tsFile : vehicleFiles) {
        statement.execute(String.format("unload \"%s\" \"%s\"", tsFile.getAbsolutePath(), tmpDir));
      }
      assertEquals(0, SyncTestUtil.getTsFilePaths(vehicleDir).size());
      assertNotNull(tmpDir.listFiles());
      assertEquals(2, tmpDir.listFiles().length >> 1);

      // move root.test
      File testDir =
          new File(
              IoTDBDescriptor.getInstance().getConfig().getDataDirs()[0],
              IoTDBConstant.SEQUENCE_FLODER_NAME + File.separator + "root.test");
      List<File> testFiles = SyncTestUtil.getTsFilePaths(testDir);
      tmpDir =
          new File(
              IoTDBDescriptor.getInstance().getConfig().getDataDirs()[0],
              "tmp" + File.separator + new PartialPath("root.test"));
      if (!tmpDir.exists()) {
        tmpDir.mkdirs();
      }
      for (File tsFile : testFiles) {
        statement.execute(String.format("unload \"%s\" \"%s\"", tsFile.getAbsolutePath(), tmpDir));
      }
      assertEquals(0, SyncTestUtil.getTsFilePaths(testDir).size());
      assertNotNull(tmpDir.listFiles());
      assertEquals(2, tmpDir.listFiles().length >> 1);
    } catch (IllegalPathException e) {
      Assert.fail();
    }
  }

  @Test
  public void removeTsfileWithVSGTest() throws SQLException {
    try (Connection connection =
            DriverManager.getConnection("jdbc:iotdb://127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {

      // remove root.vehicle
      File vehicleDir =
          new File(
              IoTDBDescriptor.getInstance().getConfig().getDataDirs()[0],
              IoTDBConstant.SEQUENCE_FLODER_NAME + File.separator + "root.vehicle");
      List<File> vehicleFiles = SyncTestUtil.getTsFilePaths(vehicleDir);
      for (File tsFile : vehicleFiles) {
        statement.execute(String.format("remove \"%s\"", tsFile.getAbsolutePath()));
      }
      assertEquals(0, SyncTestUtil.getTsFilePaths(vehicleDir).size());
      // remove root.test
      File testDir =
          new File(
              IoTDBDescriptor.getInstance().getConfig().getDataDirs()[0],
              IoTDBConstant.SEQUENCE_FLODER_NAME + File.separator + "root.test");
      List<File> testFiles = SyncTestUtil.getTsFilePaths(testDir);
      for (File tsFile : testFiles) {
        statement.execute(String.format("remove \"%s\"", tsFile.getAbsolutePath()));
      }
      assertEquals(0, SyncTestUtil.getTsFilePaths(testDir).size());
    }
  }
}
