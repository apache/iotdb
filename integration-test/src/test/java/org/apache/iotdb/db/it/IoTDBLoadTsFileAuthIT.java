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

import org.apache.iotdb.commons.auth.entity.PrivilegeType;
import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.env.cluster.node.DataNodeWrapper;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.it.utils.TsFileGenerator;
import org.apache.iotdb.itbase.category.ClusterIT;
import org.apache.iotdb.itbase.category.LocalStandaloneIT;
import org.apache.iotdb.jdbc.IoTDBSQLException;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.external.commons.io.FileUtils;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.write.schema.IMeasurementSchema;
import org.apache.tsfile.write.schema.MeasurementSchema;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.File;
import java.nio.file.Files;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Collections;
import java.util.concurrent.TimeUnit;

import static org.apache.iotdb.db.it.utils.TestUtils.assertNonQueryTestFail;
import static org.apache.iotdb.db.it.utils.TestUtils.createUser;
import static org.apache.iotdb.db.it.utils.TestUtils.executeNonQuery;
import static org.apache.iotdb.db.it.utils.TestUtils.grantUserSeriesPrivilege;

@RunWith(IoTDBTestRunner.class)
@Category({LocalStandaloneIT.class, ClusterIT.class})
public class IoTDBLoadTsFileAuthIT {
  private static final long PARTITION_INTERVAL = 10 * 1000L;
  private static final String DATABASE = "root.load_auth";
  private static final String DEVICE = DATABASE + ".d1";
  private static final IMeasurementSchema MEASUREMENT =
      new MeasurementSchema("s1", TSDataType.INT32, TSEncoding.RLE);
  private static final String NO_WRITE_USER = "load_no_write_user";
  private static final String WRITE_USER = "load_write_user";
  private static final String OTHER_PATH_WRITE_USER = "load_other_path_write_user";
  private static final String ASYNC_NO_WRITE_USER = "async_load_no_write_user";
  private static final String ASYNC_WRITE_USER = "async_load_write_user";
  private static final String PASSWORD = "test123123456";
  private static final long UNALLOCATABLE_TABLET_CONVERSION_BATCH_MEMORY_SIZE_IN_BYTES =
      Long.MAX_VALUE / 4;

  private static File tmpDir;

  @BeforeClass
  public static void setUp() throws Exception {
    tmpDir = new File(Files.createTempDirectory("load-auth").toUri());
    EnvFactory.getEnv().getConfig().getCommonConfig().setTimePartitionInterval(PARTITION_INTERVAL);
    EnvFactory.getEnv().getConfig().getCommonConfig().setEnforceStrongPassword(false);
    EnvFactory.getEnv().getConfig().getCommonConfig().setAutoCreateSchemaEnabled(false);
    EnvFactory.getEnv()
        .getConfig()
        .getDataNodeConfig()
        .setMaxAllocateMemoryRatioForLoad(1.0)
        .setLoadTsFileAnalyzeSchemaMemorySizeInBytes(10 * 1024L)
        .setLoadTsFileTabletConversionBatchMemorySizeInBytes(
            UNALLOCATABLE_TABLET_CONVERSION_BATCH_MEMORY_SIZE_IN_BYTES)
        .setLoadActiveListeningCheckIntervalSeconds(1);

    EnvFactory.getEnv().initClusterEnvironment();
  }

  @AfterClass
  public static void tearDown() throws Exception {
    deleteDatabase();
    EnvFactory.getEnv().cleanClusterEnvironment();
    FileUtils.deleteDirectory(tmpDir);
  }

  @After
  public void cleanData() throws Exception {
    deleteDatabase();
  }

  @Test
  public void testLoadWithoutSchemaCheckStillChecksWriteDataPermission() throws Exception {
    final File tsFile = new File(tmpDir, "1-0-0-0.tsfile");
    prepareSchemaAndTsFile(tsFile);
    createUser(NO_WRITE_USER, PASSWORD);

    assertNonQueryTestFail(
        String.format("load \"%s\" with ('database-level'='2', 'verify'='false')", tsFile),
        "No permissions for this operation, please add privilege WRITE_DATA",
        NO_WRITE_USER,
        PASSWORD);
  }

  @Test
  public void testLoadWithoutSchemaCheckAllowsUserWithWriteDataPermission() throws Exception {
    final File tsFile = new File(tmpDir, "2-0-0-0.tsfile");
    prepareSchemaAndTsFile(tsFile);
    createUser(WRITE_USER, PASSWORD);
    grantUserSeriesPrivilege(WRITE_USER, PrivilegeType.WRITE_DATA, DATABASE + ".**");

    executeNonQuery(
        String.format("load \"%s\" with ('database-level'='2', 'verify'='false')", tsFile),
        WRITE_USER,
        PASSWORD);

    try (final Connection connection = EnvFactory.getEnv().getConnection();
        final Statement statement = connection.createStatement();
        final ResultSet resultSet = statement.executeQuery("select count(s1) from " + DEVICE)) {
      Assert.assertTrue(resultSet.next());
      Assert.assertEquals(10, resultSet.getLong(1));
    }
  }

  @Test
  public void testLoadWithoutSchemaCheckRejectsUserWithOtherPathWriteDataPermission()
      throws Exception {
    final File tsFile = new File(tmpDir, "3-0-0-0.tsfile");
    prepareSchemaAndTsFile(tsFile);
    createUser(OTHER_PATH_WRITE_USER, PASSWORD);
    grantUserSeriesPrivilege(OTHER_PATH_WRITE_USER, PrivilegeType.WRITE_DATA, "root.other.**");

    assertNonQueryTestFail(
        String.format("load \"%s\" with ('database-level'='2', 'verify'='false')", tsFile),
        "No permissions for this operation, please add privilege WRITE_DATA",
        OTHER_PATH_WRITE_USER,
        PASSWORD);
  }

  @Test
  public void testAsyncLoadShouldCheckWriteDataPermissionWithStoredUser() throws Exception {
    final File noWriteTsFile = new File(tmpDir, "4-0-0-0.tsfile");
    final File writeTsFile = new File(tmpDir, "5-0-0-0.tsfile");
    prepareSchemaAndTsFile(noWriteTsFile);
    generateTsFile(writeTsFile);
    createUser(ASYNC_NO_WRITE_USER, PASSWORD);
    createUser(ASYNC_WRITE_USER, PASSWORD);
    grantUserSeriesPrivilege(ASYNC_WRITE_USER, PrivilegeType.WRITE_DATA, DATABASE + ".**");

    executeNonQuery(
        String.format(
            "load \"%s\" with ('database-level'='2', 'async'='true', 'on-success'='none', "
                + "'verify'='false')",
            noWriteTsFile.getAbsolutePath()),
        ASYNC_NO_WRITE_USER,
        PASSWORD);
    executeNonQuery(
        String.format(
            "load \"%s\" with ('database-level'='2', 'async'='true', 'on-success'='none', "
                + "'verify'='false')",
            writeTsFile.getAbsolutePath()),
        ASYNC_WRITE_USER,
        PASSWORD);

    waitUntilAllActiveLoadPendingDirsAreEmpty(TimeUnit.SECONDS.toMillis(60));
    assertCountEventually(10, TimeUnit.SECONDS.toMillis(60));
  }

  private static void prepareSchemaAndTsFile(final File tsFile) throws Exception {
    prepareSchema(MEASUREMENT.getType());
    generateTsFile(tsFile);
  }

  private static void prepareSchema(final TSDataType dataType) throws Exception {
    try (final Connection connection = EnvFactory.getEnv().getConnection();
        final Statement statement = connection.createStatement()) {
      statement.execute("create database " + DATABASE);
      statement.execute(
          String.format(
              "create timeseries %s.%s %s", DEVICE, MEASUREMENT.getMeasurementName(), dataType));
    }
  }

  private static void generateTsFile(final File tsFile) throws Exception {
    try (final TsFileGenerator generator = new TsFileGenerator(tsFile)) {
      generator.registerTimeseries(DEVICE, Collections.singletonList(MEASUREMENT));
      generator.generateData(DEVICE, 10, PARTITION_INTERVAL / 10, false);
    }
  }

  private static void deleteDatabase() throws Exception {
    try (final Connection connection = EnvFactory.getEnv().getConnection();
        final Statement statement = connection.createStatement()) {
      statement.execute("delete database " + DATABASE);
    } catch (final IoTDBSQLException ignored) {
    }
  }

  private File getActiveLoadPendingDir(final DataNodeWrapper dataNodeWrapper) {
    return new File(
        dataNodeWrapper.getNodePath()
            + File.separator
            + "ext"
            + File.separator
            + "load"
            + File.separator
            + "pending");
  }

  private void waitUntilAllActiveLoadPendingDirsAreEmpty(final long timeoutMs)
      throws InterruptedException {
    final long deadline = System.currentTimeMillis() + timeoutMs;
    while (System.currentTimeMillis() < deadline) {
      boolean hasTsFile = false;
      for (final DataNodeWrapper dataNodeWrapper : EnvFactory.getEnv().getDataNodeWrapperList()) {
        if (containsTsFile(getActiveLoadPendingDir(dataNodeWrapper))) {
          hasTsFile = true;
          break;
        }
      }
      if (!hasTsFile) {
        return;
      }
      Thread.sleep(500L);
    }
    Assert.fail("Timed out waiting for active load pending dirs to become empty");
  }

  private void assertCountEventually(final long expected, final long timeoutMs) throws Exception {
    final long deadline = System.currentTimeMillis() + timeoutMs;
    AssertionError lastError = null;
    while (System.currentTimeMillis() < deadline) {
      try (final Connection connection = EnvFactory.getEnv().getConnection();
          final Statement statement = connection.createStatement();
          final ResultSet resultSet =
              statement.executeQuery(
                  "select count(" + MEASUREMENT.getMeasurementName() + ") from " + DEVICE)) {
        Assert.assertTrue(resultSet.next());
        Assert.assertEquals(expected, resultSet.getLong(1));
        return;
      } catch (final AssertionError e) {
        lastError = e;
      }
      Thread.sleep(500L);
    }
    if (lastError != null) {
      throw lastError;
    }
    Assert.fail("Timed out waiting for count " + expected);
  }

  private boolean containsTsFile(final File root) {
    if (root == null || !root.exists()) {
      return false;
    }
    if (root.isFile()) {
      return root.getName().endsWith(".tsfile");
    }

    final File[] children = root.listFiles();
    if (children == null) {
      return false;
    }
    for (final File child : children) {
      if (containsTsFile(child)) {
        return true;
      }
    }
    return false;
  }
}
