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

import org.apache.iotdb.db.it.utils.TestUtils;
import org.apache.iotdb.db.storageengine.load.active.ActiveLoadPathHelper;
import org.apache.iotdb.db.storageengine.load.config.LoadTsFileConfigurator;
import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.env.cluster.config.MppCommonConfig;
import org.apache.iotdb.it.env.cluster.config.MppDataNodeConfig;
import org.apache.iotdb.it.env.cluster.node.DataNodeWrapper;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.TableClusterIT;
import org.apache.iotdb.itbase.category.TableLocalStandaloneIT;
import org.apache.iotdb.itbase.env.BaseEnv;

import org.apache.tsfile.enums.ColumnCategory;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.external.commons.io.FileUtils;
import org.apache.tsfile.file.metadata.TableSchema;
import org.apache.tsfile.write.TsFileWriter;
import org.apache.tsfile.write.record.Tablet;
import org.apache.tsfile.write.schema.IMeasurementSchema;
import org.apache.tsfile.write.schema.MeasurementSchema;
import org.awaitility.Awaitility;
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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.apache.iotdb.db.it.IoTDBLoadTsFileActiveRetryIT.activeDir;
import static org.apache.iotdb.db.it.IoTDBLoadTsFileActiveRetryIT.assertRetryQueueEmpty;
import static org.apache.iotdb.db.it.IoTDBLoadTsFileActiveRetryIT.assertSchemaMemoryRejectionWasLogged;
import static org.apache.iotdb.db.it.IoTDBLoadTsFileActiveRetryIT.awaitNode;
import static org.apache.iotdb.db.it.IoTDBLoadTsFileActiveRetryIT.configureRetryMetrics;
import static org.apache.iotdb.db.it.IoTDBLoadTsFileActiveRetryIT.failedDir;
import static org.apache.iotdb.db.it.IoTDBLoadTsFileActiveRetryIT.findTsFiles;

@RunWith(IoTDBTestRunner.class)
@Category({TableLocalStandaloneIT.class, TableClusterIT.class})
public class IoTDBLoadTsFileActiveRetryTableIT {
  private static final String TABLE = "retry_table";
  private static final String[] DATABASES = {"load_retry_a", "load_retry_b"};
  private static File tmpDir;

  @BeforeClass
  public static void setUpClass() throws Exception {
    tmpDir = Files.createTempDirectory("load-active-retry-table").toFile();
    configureRetryMetrics();
    // This fixture needs a different startup budget from the tree conversion-rejection cases.
    EnvFactory.getEnv().getConfig().getCommonConfig().setPipeMemoryManagementEnabled(false);
    EnvFactory.getEnv()
        .getConfig()
        .getDataNodeConfig()
        .setLoadTsFileAnalyzeSchemaMemorySizeInBytes(Long.MAX_VALUE / 4)
        .setLoadActiveListeningCheckIntervalSeconds(1);
    EnvFactory.getEnv().initClusterEnvironment();
  }

  @AfterClass
  public static void tearDownClass() throws Exception {
    try {
      // Stop every process before filesystem cleanup can fail on an earlier node's directory.
      EnvFactory.getEnv().shutdownForciblyAllDataNodes();
      EnvFactory.getEnv().shutdownForciblyAllConfigNodes();
      EnvFactory.getEnv().cleanClusterEnvironment();
    } finally {
      FileUtils.deleteDirectory(tmpDir);
    }
  }

  /**
   * LOAD-P0-06 Table difference: two accepted same-named files must keep their explicit target
   * databases and TAG/FIELD mapping, including null TAGs and all-null FIELD rows, across a process
   * crash. A logged memory rejection proves a background attempt, rather than just an idle queue.
   * Restoring the startup budget really removes the failure; no handoff is resubmitted or copied.
   */
  @Test
  public void testPendingTableLoadsRestoreTheirDatabasesAfterRestart() throws Exception {
    final DataNodeWrapper node = EnvFactory.getEnv().getDataNodeWrapper(0);
    final List<File> sources = new ArrayList<>();
    try (final Connection connection =
            EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        final Statement statement = connection.createStatement()) {
      for (int i = 0; i < DATABASES.length; i++) {
        statement.execute("create database " + DATABASES[i]);
        final File directory = new File(tmpDir, "source-" + i);
        Assert.assertTrue(directory.mkdirs());
        final File source = new File(directory, "1-0-0-0.tsfile");
        generateTableFile(source, i);
        sources.add(source);
      }
    }
    // A pinned connection makes the node that owns both handoffs unambiguous.
    try (final Connection connection =
            EnvFactory.getEnv().getConnectionWithSpecifiedDataNode(node);
        final Statement statement = connection.createStatement()) {
      statement.execute("set sql_dialect=table");
      for (int i = 0; i < sources.size(); i++) {
        statement.execute(
            String.format(
                "load '%s' with ('async'='true', 'database'='%s', 'on-success'='%s', 'verify'='true', 'tablet-conversion-threshold'='0')",
                sources.get(i).getAbsolutePath(), DATABASES[i], i == 0 ? "none" : "delete"));
      }
    }
    final List<File> pending = new ArrayList<>();
    Awaitility.await()
        .atMost(60, TimeUnit.SECONDS)
        .untilAsserted(
            () -> {
              pending.clear();
              pending.addAll(findTsFiles(activeDir(node)));
              Assert.assertEquals(2, pending.size());
              final Set<String> pendingDatabases = new HashSet<>();
              for (final File file : pending) {
                assertSchemaMemoryRejectionWasLogged(node, file);
                final Map<String, String> attributes =
                    ActiveLoadPathHelper.parseAttributes(file, activeDir(node));
                final String database = attributes.get(LoadTsFileConfigurator.DATABASE_NAME_KEY);
                Assert.assertTrue(Arrays.asList(DATABASES).contains(database));
                Assert.assertEquals("true", attributes.get("verify"));
                pendingDatabases.add(database);
              }
              Assert.assertEquals(2, pendingDatabases.size());
              Assert.assertTrue(findTsFiles(failedDir(node)).isEmpty());
            });
    Assert.assertTrue(sources.get(0).isFile());
    Assert.assertFalse(sources.get(1).exists());
    node.stopForcibly();
    Assert.assertFalse(node.isAlive());
    for (final File file : pending) {
      Assert.assertTrue(file.isFile());
    }
    final MppDataNodeConfig config =
        (MppDataNodeConfig) EnvFactory.getEnv().getConfig().getDataNodeConfig();
    config.setLoadTsFileAnalyzeSchemaMemorySizeInBytes(8 * 1024);
    node.changeConfig(
        config, (MppCommonConfig) EnvFactory.getEnv().getConfig().getDataNodeCommonConfig(), null);
    node.start();
    awaitNode(node);

    Awaitility.await()
        .atMost(90, TimeUnit.SECONDS)
        .untilAsserted(
            () -> {
              for (int i = 0; i < DATABASES.length; i++) {
                final List<String> expected = new ArrayList<>();
                for (int row = 0; row < 6; row++) {
                  expected.add(
                      (row + 1)
                          + ",site_"
                          + i
                          + ","
                          + (row % 2 == 0 ? "null" : "sensor_" + row)
                          + ","
                          + (row == 4 ? "null" : Integer.toString(1000 * i + row))
                          + ","
                          + (row % 2 == 0 ? "null" : "note_" + i + "_" + row)
                          + ",");
                }
                TestUtils.tableResultSetEqualTestUnordered(
                    "select cast(time as int64) as t,site,sensor,v,note from " + TABLE,
                    new String[] {"t", "site", "sensor", "v", "note"},
                    expected.toArray(new String[0]),
                    DATABASES[i]);
              }
              assertRetryQueueEmpty(node);
              Assert.assertTrue(findTsFiles(failedDir(node)).isEmpty());
            });
    for (final File file : pending) {
      Assert.assertFalse("the original handoff must be cleaned", file.exists());
    }
    for (final String database : DATABASES) {
      // Values and JDBC types cannot distinguish TAGs from FIELDs or swapped TAG ordinals.
      // DESCRIBE emits schema order, including the TAG order that defines device identity.
      TestUtils.tableResultSetEqualTest(
          "describe " + TABLE,
          new String[] {"ColumnName", "DataType", "Category"},
          new String[] {
            "time,TIMESTAMP,TIME,",
            "site,STRING,TAG,",
            "sensor,STRING,TAG,",
            "v,INT64,FIELD,",
            "note,STRING,FIELD,"
          },
          database);
    }
    try (final Connection connection = EnvFactory.getEnv().getTableConnection();
        final Statement statement = connection.createStatement()) {
      for (final String database : DATABASES) {
        statement.execute("use " + database);
        try (final ResultSet result =
            statement.executeQuery("select site,sensor,v,note from " + TABLE + " limit 1")) {
          Assert.assertEquals(TSDataType.STRING.name(), result.getMetaData().getColumnTypeName(1));
          Assert.assertEquals(TSDataType.STRING.name(), result.getMetaData().getColumnTypeName(2));
          Assert.assertEquals(TSDataType.INT64.name(), result.getMetaData().getColumnTypeName(3));
          Assert.assertEquals(TSDataType.STRING.name(), result.getMetaData().getColumnTypeName(4));
        }
      }
    }
    Assert.assertTrue(sources.get(0).isFile());
    Assert.assertFalse(sources.get(1).exists());
  }

  private static void generateTableFile(final File file, final int fileIndex) throws Exception {
    final List<String> names = Arrays.asList("site", "sensor", "v", "note");
    final List<TSDataType> types =
        Arrays.asList(TSDataType.STRING, TSDataType.STRING, TSDataType.INT64, TSDataType.STRING);
    final List<ColumnCategory> categories =
        Arrays.asList(
            ColumnCategory.TAG, ColumnCategory.TAG, ColumnCategory.FIELD, ColumnCategory.FIELD);
    final List<IMeasurementSchema> schemas = new ArrayList<>();
    for (int i = 0; i < names.size(); i++) {
      schemas.add(new MeasurementSchema(names.get(i), types.get(i)));
    }
    try (final TsFileWriter writer = new TsFileWriter(file)) {
      writer.registerTableSchema(new TableSchema(TABLE, schemas, categories));
      final Tablet tablet = new Tablet(TABLE, names, types, categories);
      for (int row = 0; row < 6; row++) {
        tablet.addTimestamp(row, row + 1);
        tablet.addValue("site", row, "site_" + fileIndex);
        tablet.addValue("sensor", row, row % 2 == 0 ? null : "sensor_" + row);
        tablet.addValue("v", row, row == 4 ? null : 1000L * fileIndex + row);
        tablet.addValue("note", row, row % 2 == 0 ? null : "note_" + fileIndex + "_" + row);
      }
      writer.writeTable(tablet);
    }
  }
}
