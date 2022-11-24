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

import org.apache.iotdb.commons.consensus.DataRegionId;
import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.exception.MetadataException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.constant.TestConstant;
import org.apache.iotdb.db.engine.StorageEngine;
import org.apache.iotdb.db.engine.StorageEngineV2;
import org.apache.iotdb.db.engine.cache.ChunkCache;
import org.apache.iotdb.db.engine.cache.TimeSeriesMetadataCache;
import org.apache.iotdb.db.engine.snapshot.SnapshotLoader;
import org.apache.iotdb.db.engine.snapshot.SnapshotTaker;
import org.apache.iotdb.db.engine.snapshot.exception.DirectoryNotLegalException;
import org.apache.iotdb.db.engine.storagegroup.DataRegion;
import org.apache.iotdb.db.exception.DataRegionException;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.integration.env.EnvFactory;

import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;

public class IoTDBSnapshotIT {
  final String SG_NAME = "root.snapshotTest";

  @Before
  public void setUp() throws Exception {
    EnvFactory.getEnv().initBeforeTest();
    IoTDBDescriptor.getInstance().getConfig().setEnableCrossSpaceCompaction(false);
  }

  @After
  public void tearDown() throws Exception {
    EnvFactory.getEnv().cleanAfterTest();
    IoTDBDescriptor.getInstance().getConfig().setEnableCrossSpaceCompaction(true);
  }

  @Test
  public void testTakeSnapshot()
      throws SQLException, IllegalPathException, StorageEngineException, IOException,
          DirectoryNotLegalException, DataRegionException {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("CREATE DATABASE " + SG_NAME);
      for (int i = 0; i < 10; ++i) {
        for (int j = 0; j < 10; ++j) {
          statement.execute(
              String.format("insert into %s.d%d(time, s) values (%d, %d)", SG_NAME, i, j, j));
        }
        statement.execute("flush");
        for (int j = 0; j < 10; ++j) {
          statement.execute(
              String.format("insert into %s.d%d(time, s) values (%d, %d)", SG_NAME, i, j, j + 1));
        }
        statement.execute("flush");
      }

      DataRegion region =
          StorageEngine.getInstance().getProcessor(new PartialPath(SG_NAME + ".d0.s"));
      File snapshotDir = new File(TestConstant.OUTPUT_DATA_DIR, "snapshot");
      if (snapshotDir.exists()) {
        FileUtils.forceDelete(snapshotDir);
      }

      new SnapshotTaker(region).takeFullSnapshot(snapshotDir.getAbsolutePath(), true);

      Assert.assertTrue(snapshotDir.exists());
      Assert.assertTrue(snapshotDir.isDirectory());
      File[] seqTsfiles =
          new File(
                  snapshotDir.getAbsolutePath()
                      + File.separator
                      + "snapshot"
                      + File.separator
                      + "unsequence"
                      + File.separator
                      + "root.snapshotTest"
                      + File.separator
                      + "0"
                      + File.separator
                      + "0")
              .listFiles();
      File[] unseqTsfiles =
          new File(
                  snapshotDir.getAbsolutePath()
                      + File.separator
                      + "snapshot"
                      + File.separator
                      + "sequence"
                      + File.separator
                      + "root.snapshotTest"
                      + File.separator
                      + "0"
                      + File.separator
                      + "0")
              .listFiles();
      Assert.assertNotNull(seqTsfiles);
      Assert.assertNotNull(unseqTsfiles);
      Assert.assertEquals(20, seqTsfiles.length);
      Assert.assertEquals(20, unseqTsfiles.length);
    }
  }

  @Test(expected = DirectoryNotLegalException.class)
  public void testTakeSnapshotInNotEmptyDir()
      throws SQLException, IOException, IllegalPathException, StorageEngineException,
          DirectoryNotLegalException, DataRegionException {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("CREATE DATABASE " + SG_NAME);
      for (int i = 0; i < 10; ++i) {
        for (int j = 0; j < 10; ++j) {
          statement.execute(
              String.format("insert into %s.d%d(time, s) values (%d, %d)", SG_NAME, i, j, j));
        }
        statement.execute("flush");
        for (int j = 0; j < 10; ++j) {
          statement.execute(
              String.format("insert into %s.d%d(time, s) values (%d, %d)", SG_NAME, i, j, j + 1));
        }
        statement.execute("flush");
      }

      DataRegion region =
          StorageEngine.getInstance().getProcessor(new PartialPath(SG_NAME + ".d0.s"));
      File snapshotDir = new File(TestConstant.OUTPUT_DATA_DIR, "snapshot");
      if (!snapshotDir.exists()) {
        snapshotDir.mkdirs();
      }

      File tmpFile = new File(snapshotDir, "test");
      tmpFile.createNewFile();

      new SnapshotTaker(region).takeFullSnapshot(snapshotDir.getAbsolutePath(), true);
    }
  }

  @Test
  public void testLoadSnapshot()
      throws SQLException, MetadataException, StorageEngineException, DirectoryNotLegalException,
          IOException, DataRegionException {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      Map<String, Integer> resultMap = new HashMap<>();
      statement.execute("CREATE DATABASE " + SG_NAME);
      for (int i = 0; i < 10; ++i) {
        for (int j = 0; j < 10; ++j) {
          statement.execute(
              String.format("insert into %s.d%d(time, s) values (%d, %d)", SG_NAME, i, j, j));
        }
        statement.execute("flush");
        for (int j = 0; j < 10; ++j) {
          statement.execute(
              String.format("insert into %s.d%d(time, s) values (%d, %d)", SG_NAME, i, j, j + 1));
        }
        statement.execute("flush");
      }
      ResultSet resultSet = statement.executeQuery("select ** from root");
      while (resultSet.next()) {
        long time = resultSet.getLong("Time");
        for (int i = 0; i < 10; ++i) {
          String measurment = SG_NAME + ".d" + i + ".s";
          int res = resultSet.getInt(SG_NAME + ".d" + i + ".s");
          resultMap.put(time + measurment, res);
        }
      }

      DataRegion region =
          StorageEngine.getInstance().getProcessor(new PartialPath(SG_NAME + ".d0.s"));
      File snapshotDir = new File(TestConstant.OUTPUT_DATA_DIR, "snapshot");
      if (!snapshotDir.exists()) {
        snapshotDir.mkdirs();
      }
      new SnapshotTaker(region).takeFullSnapshot(snapshotDir.getAbsolutePath(), true);
      StorageEngineV2.getInstance()
          .setDataRegion(
              new DataRegionId(0),
              new SnapshotLoader(snapshotDir.getAbsolutePath(), SG_NAME, "0")
                  .loadSnapshotForStateMachine());

      ChunkCache.getInstance().clear();
      TimeSeriesMetadataCache.getInstance().clear();
      resultSet = statement.executeQuery("select ** from root");
      while (resultSet.next()) {
        long time = resultSet.getLong("Time");
        for (int i = 0; i < 10; ++i) {
          String measurment = SG_NAME + ".d" + i + ".s";
          int res = resultSet.getInt(SG_NAME + ".d" + i + ".s");
          Assert.assertEquals(resultMap.get(time + measurment).intValue(), res);
        }
      }
    }
  }

  @Test
  public void testTakeAndLoadSnapshotWhenCompaction()
      throws SQLException, MetadataException, StorageEngineException, InterruptedException,
          DirectoryNotLegalException, IOException, DataRegionException {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      Map<String, Integer> resultMap = new HashMap<>();
      statement.execute("CREATE DATABASE " + SG_NAME);
      for (int i = 0; i < 10; ++i) {
        for (int j = 0; j < 10; ++j) {
          statement.execute(
              String.format("insert into %s.d%d(time, s) values (%d, %d)", SG_NAME, i, j, j));
        }
        statement.execute("flush");
        for (int j = 0; j < 10; ++j) {
          statement.execute(
              String.format("insert into %s.d%d(time, s) values (%d, %d)", SG_NAME, i, j, j + 1));
        }
        statement.execute("flush");
      }

      ResultSet resultSet = statement.executeQuery("select ** from root");
      while (resultSet.next()) {
        long time = resultSet.getLong("Time");
        for (int i = 0; i < 10; ++i) {
          String measurment = SG_NAME + ".d" + i + ".s";
          int res = resultSet.getInt(SG_NAME + ".d" + i + ".s");
          resultMap.put(time + measurment, res);
        }
      }

      File snapshotDir = new File(TestConstant.OUTPUT_DATA_DIR, "snapshot");
      if (!snapshotDir.exists()) {
        snapshotDir.mkdirs();
      }
      IoTDBDescriptor.getInstance().getConfig().setEnableCrossSpaceCompaction(true);
      statement.execute("compact");
      DataRegion region =
          StorageEngine.getInstance().getProcessor(new PartialPath(SG_NAME + ".d0.s"));
      new SnapshotTaker(region).takeFullSnapshot(snapshotDir.getAbsolutePath(), true);
      region.abortCompaction();
      StorageEngineV2.getInstance()
          .setDataRegion(
              new DataRegionId(0),
              new SnapshotLoader(snapshotDir.getAbsolutePath(), SG_NAME, "0")
                  .loadSnapshotForStateMachine());
      ChunkCache.getInstance().clear();
      TimeSeriesMetadataCache.getInstance().clear();
      resultSet = statement.executeQuery("select ** from root");
      while (resultSet.next()) {
        long time = resultSet.getLong("Time");
        for (int i = 0; i < 10; ++i) {
          String measurment = SG_NAME + ".d" + i + ".s";
          int res = resultSet.getInt(SG_NAME + ".d" + i + ".s");
          Assert.assertEquals(resultMap.get(time + measurment).intValue(), res);
        }
      }
    }
  }
}
