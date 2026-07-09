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

import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.env.cluster.node.DataNodeWrapper;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.it.utils.TsFileGenerator;
import org.apache.iotdb.itbase.category.ClusterIT;
import org.apache.iotdb.itbase.category.LocalStandaloneIT;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.write.schema.MeasurementSchema;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.File;
import java.nio.file.Files;
import java.sql.Connection;
import java.sql.Statement;
import java.util.Collections;
import java.util.concurrent.TimeUnit;

@RunWith(IoTDBTestRunner.class)
@Category({LocalStandaloneIT.class, ClusterIT.class})
public class IoTDBLoadTsFileActiveRetryIT {

  private static final String DATABASE = "root.sg.test_0";
  private static final String DEVICE = DATABASE + ".d_0";
  private static final String MEASUREMENT = "sensor_00";
  private static final long UNALLOCATABLE_TABLET_CONVERSION_BATCH_MEMORY_SIZE_IN_BYTES =
      Long.MAX_VALUE / 4;
  private static final MeasurementSchema TSFILE_SCHEMA =
      new MeasurementSchema(MEASUREMENT, TSDataType.INT32, TSEncoding.RLE);

  private File tmpDir;

  @Before
  public void setUp() throws Exception {
    tmpDir = new File(Files.createTempDirectory("load-active-retry").toUri());
    EnvFactory.getEnv().getConfig().getCommonConfig().setPipeMemoryManagementEnabled(false);
    EnvFactory.getEnv()
        .getConfig()
        .getDataNodeConfig()
        .setMaxAllocateMemoryRatioForLoad(1.0)
        .setLoadTsFileAnalyzeSchemaMemorySizeInBytes(1)
        .setLoadTsFileTabletConversionBatchMemorySizeInBytes(
            UNALLOCATABLE_TABLET_CONVERSION_BATCH_MEMORY_SIZE_IN_BYTES)
        .setLoadActiveListeningCheckIntervalSeconds(1);

    EnvFactory.getEnv().initClusterEnvironment();
  }

  @After
  public void tearDown() throws Exception {
    try (final Connection connection = EnvFactory.getEnv().getConnection();
        final Statement statement = connection.createStatement()) {
      statement.execute("delete database " + DATABASE);
    } catch (final Exception ignored) {
      // ignore cleanup failure
    } finally {
      EnvFactory.getEnv().cleanClusterEnvironment();
      deleteRecursively(tmpDir);
    }
  }

  @Test
  public void testActiveLoadTemporaryUnavailableShouldKeepFileForRetry() throws Exception {
    final DataNodeWrapper dataNodeWrapper = EnvFactory.getEnv().getDataNodeWrapper(0);
    final File retryTsFile = new File(tmpDir, "1-0-0-0.tsfile");
    final File permanentFailureTsFile = new File(tmpDir, "2-0-0-0.tsfile");
    generateTsFile(retryTsFile);
    generateTsFile(permanentFailureTsFile);

    try (final Connection connection =
            EnvFactory.getEnv().getConnectionWithSpecifiedDataNode(dataNodeWrapper);
        final Statement statement = connection.createStatement()) {
      statement.execute("create database " + DATABASE);
      statement.execute(
          String.format(
              "create timeseries %s.%s %s", DEVICE, MEASUREMENT, TSDataType.INT64.name()));

      statement.execute(
          String.format(
              "load \"%s\" with ('database-level'='3', 'async'='true', 'on-success'='none', "
                  + "'convert-on-type-mismatch'='true')",
              retryTsFile.getAbsolutePath()));
      statement.execute(
          String.format(
              "load \"%s\" with ('database-level'='3', 'async'='true', 'on-success'='none', "
                  + "'convert-on-type-mismatch'='false')",
              permanentFailureTsFile.getAbsolutePath()));

      final File activeDir = getActiveLoadDir(dataNodeWrapper);
      final File failDir = getActiveLoadFailDir(dataNodeWrapper);
      final File activeTsFile = waitForFile(activeDir, retryTsFile.getName(), 30_000L);

      Assert.assertNotNull(
          "Async load should copy tsfile into active load directory", activeTsFile);

      Assert.assertNotNull(
          "Permanent active load failure should be moved to fail dir",
          waitForFile(failDir, permanentFailureTsFile.getName(), TimeUnit.SECONDS.toMillis(60)));

      assertFileKeptForRetry(
          activeDir, failDir, retryTsFile.getName(), TimeUnit.SECONDS.toMillis(12));
    }
  }

  private void generateTsFile(final File tsFile) throws Exception {
    try (final TsFileGenerator generator = new TsFileGenerator(tsFile)) {
      generator.registerTimeseries(DEVICE, Collections.singletonList(TSFILE_SCHEMA));
      generator.generateData(DEVICE, 10, 1, false);
    }
  }

  private File getActiveLoadDir(final DataNodeWrapper dataNodeWrapper) {
    return new File(
        dataNodeWrapper.getNodePath()
            + File.separator
            + "ext"
            + File.separator
            + "load"
            + File.separator
            + "pending");
  }

  private File getActiveLoadFailDir(final DataNodeWrapper dataNodeWrapper) {
    return new File(
        dataNodeWrapper.getNodePath()
            + File.separator
            + "ext"
            + File.separator
            + "load"
            + File.separator
            + "failed");
  }

  private File waitForFile(final File root, final String fileName, final long timeoutMs)
      throws InterruptedException {
    final long deadline = System.currentTimeMillis() + timeoutMs;
    while (System.currentTimeMillis() < deadline) {
      final File file = findFile(root, fileName);
      if (file != null) {
        return file;
      }
      Thread.sleep(500L);
    }
    return null;
  }

  private boolean containsFile(final File root, final String fileName) {
    return findFile(root, fileName) != null;
  }

  private void assertFileKeptForRetry(
      final File activeDir, final File failDir, final String fileName, final long observationMs)
      throws InterruptedException {
    final long deadline = System.currentTimeMillis() + observationMs;
    while (System.currentTimeMillis() < deadline) {
      Assert.assertTrue(
          "Temporary unavailable active load should keep tsfile for retry",
          containsFile(activeDir, fileName));
      Assert.assertFalse(
          "Temporary unavailable active load must not move tsfile to fail dir",
          containsFile(failDir, fileName));
      Thread.sleep(500L);
    }
  }

  private File findFile(final File root, final String fileName) {
    if (root == null || !root.exists()) {
      return null;
    }
    if (root.isFile()) {
      return root.getName().equals(fileName) ? root : null;
    }

    final File[] children = root.listFiles();
    if (children == null) {
      return null;
    }
    for (final File child : children) {
      final File file = findFile(child, fileName);
      if (file != null) {
        return file;
      }
    }
    return null;
  }

  private void deleteRecursively(final File file) {
    if (file == null || !file.exists()) {
      return;
    }
    final File[] children = file.listFiles();
    if (children != null) {
      for (final File child : children) {
        deleteRecursively(child);
      }
    }
    Assert.assertTrue(file.delete());
  }
}
