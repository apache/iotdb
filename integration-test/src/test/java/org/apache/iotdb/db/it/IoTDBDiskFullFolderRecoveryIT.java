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

package org.apache.iotdb.db.it;

import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.LocalStandaloneIT;

import org.awaitility.Awaitility;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import static org.apache.tsfile.common.constant.TsFileConstant.TSFILE_SUFFIX;

@RunWith(IoTDBTestRunner.class)
@Category({LocalStandaloneIT.class})
public class IoTDBDiskFullFolderRecoveryIT {
  private static final String DATABASE = "root.disk_full_folder_recovery";
  private static final String FULL_THRESHOLD = "0.9999999999999999";
  private static final String NORMAL_THRESHOLD = "0.05";

  @BeforeClass
  public static void setUp() throws Exception {
    EnvFactory.getEnv().initClusterEnvironment();
  }

  @AfterClass
  public static void tearDown() {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  @Test
  public void testNewTsFileAfterFullDirectoryReloadAndRecovery() throws Exception {
    int dataNodeId = EnvFactory.getEnv().getConfigNodeWrapperList().size();
    File dataDirectory = new File(EnvFactory.getEnv().getDataNodeWrapper(0).getDataPath());
    Assert.assertTrue(dataDirectory.mkdirs() || dataDirectory.isDirectory());
    Assert.assertTrue(dataDirectory.getTotalSpace() > 0);
    Assert.assertTrue(
        (double) dataDirectory.getFreeSpace() / dataDirectory.getTotalSpace()
            < Double.parseDouble(FULL_THRESHOLD));

    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("CREATE DATABASE " + DATABASE);
      statement.execute("CREATE TIMESERIES " + DATABASE + ".d1.s1 WITH DATATYPE=INT64");
      try {
        // The first reload installs the threshold; the second rebuilds FolderManagers under it.
        setDataNodeConfiguration(
            statement, dataNodeId, "disk_space_warning_threshold", FULL_THRESHOLD);
        setDataNodeConfiguration(statement, dataNodeId, "enable_partial_insert", "true");
        awaitDataNodeStatus(statement, dataNodeId, "ReadOnly", "DiskFull");

        // A successful reload of the same configuration must not clear the full-disk status.
        setDataNodeConfiguration(statement, dataNodeId, "enable_partial_insert", "true");
        Assert.assertEquals("ReadOnly", getDataNodeStatus(statement, dataNodeId));
      } finally {
        setDataNodeConfiguration(
            statement, dataNodeId, "disk_space_warning_threshold", NORMAL_THRESHOLD);
      }

      awaitDataNodeStatus(statement, dataNodeId, "Running", null);
      statement.execute("INSERT INTO " + DATABASE + ".d1(time,s1) VALUES(1,1)");
      statement.execute("FLUSH");

      try (ResultSet resultSet = statement.executeQuery("SELECT s1 FROM " + DATABASE + ".d1")) {
        Assert.assertTrue(resultSet.next());
        Assert.assertEquals(1, resultSet.getLong("Time"));
        Assert.assertEquals(1, resultSet.getLong(DATABASE + ".d1.s1"));
        Assert.assertFalse(resultSet.next());
      }
      Assert.assertTrue(hasSequenceTsFile());
    }
  }

  private static void setDataNodeConfiguration(
      Statement statement, int nodeId, String key, String value) throws Exception {
    statement.execute("SET CONFIGURATION \"" + key + "\"=\"" + value + "\" ON " + nodeId);
  }

  private static void awaitDataNodeStatus(
      Statement statement, int nodeId, String expectedStatus, String expectedReason) {
    Awaitility.await()
        .atMost(30, TimeUnit.SECONDS)
        .until(
            () -> {
              try (ResultSet resultSet = statement.executeQuery("SHOW DATANODES")) {
                while (resultSet.next()) {
                  if (resultSet.getInt("NodeID") == nodeId) {
                    if (!expectedStatus.equals(resultSet.getString("Status"))) {
                      return false;
                    }
                    return expectedReason == null
                        || expectedReason.equals(resultSet.getString("StatusReason"));
                  }
                }
              }
              return false;
            });
  }

  private static String getDataNodeStatus(Statement statement, int nodeId) throws Exception {
    try (ResultSet resultSet = statement.executeQuery("SHOW DATANODES")) {
      while (resultSet.next()) {
        if (resultSet.getInt("NodeID") == nodeId) {
          return resultSet.getString("Status");
        }
      }
    }
    Assert.fail("DataNode not found: " + nodeId);
    return null;
  }

  private static boolean hasSequenceTsFile() throws Exception {
    Path sequenceDirectory =
        Paths.get(
            EnvFactory.getEnv().getDataNodeWrapper(0).getDataPath(),
            IoTDBConstant.SEQUENCE_FOLDER_NAME,
            DATABASE);
    if (!Files.exists(sequenceDirectory)) {
      return false;
    }
    try (Stream<Path> paths = Files.walk(sequenceDirectory)) {
      return paths.anyMatch(path -> path.toString().endsWith(TSFILE_SUFFIX));
    }
  }
}
