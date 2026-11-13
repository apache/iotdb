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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.iotdb.relational.it.db.it;

import org.apache.iotdb.db.it.utils.TestUtils;
import org.apache.iotdb.db.storageengine.dataregion.modification.DeletionPredicate;
import org.apache.iotdb.db.storageengine.dataregion.modification.ModificationFile;
import org.apache.iotdb.db.storageengine.dataregion.modification.TableDeletionEntry;
import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.TableClusterIT;
import org.apache.iotdb.itbase.category.TableLocalStandaloneIT;
import org.apache.iotdb.itbase.env.BaseEnv;

import org.apache.tsfile.enums.ColumnCategory;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.exception.write.WriteProcessException;
import org.apache.tsfile.file.metadata.TableSchema;
import org.apache.tsfile.read.TsFileSequenceReader;
import org.apache.tsfile.read.TsFileSequenceReaderTimeseriesMetadataIterator;
import org.apache.tsfile.read.common.Path;
import org.apache.tsfile.read.common.TimeRange;
import org.apache.tsfile.write.TsFileWriter;
import org.apache.tsfile.write.record.Tablet;
import org.apache.tsfile.write.schema.IMeasurementSchema;
import org.apache.tsfile.write.schema.MeasurementSchema;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

@RunWith(IoTDBTestRunner.class)
@Category({TableLocalStandaloneIT.class, TableClusterIT.class})
public class IoTDBLoadTsFileMixedModelIT {
  private static final int TABLE_DEVICE_BATCH_SIZE = 2;
  private static final String TABLE_DATABASE = "load_p116_target";
  private static final String SESSION_DATABASE = "load_p116_session";
  private static final String TREE_DATABASE = "root.load_p116_tree";
  private static final String TELEMETRY_TABLE = "telemetry";
  private static final String ALARM_TABLE = "alarm";

  private File tmpDir;

  @BeforeClass
  public static void setUpClass() throws Exception {
    EnvFactory.getEnv().getConfig().getCommonConfig().setTimePartitionInterval(10_000L);
    EnvFactory.getEnv().getConfig().getCommonConfig().setEnforceStrongPassword(false);
    EnvFactory.getEnv()
        .getConfig()
        .getCommonConfig()
        .setAutoCreateSchemaEnabled(true)
        .setDatanodeMemoryProportion("3:3:1:1:1:0");
    EnvFactory.getEnv()
        .getConfig()
        .getDataNodeConfig()
        .setLoadTsFileAnalyzeSchemaBatchFlushTableDeviceNumber(TABLE_DEVICE_BATCH_SIZE)
        .setMetricReporterType(Collections.singletonList("PROMETHEUS"));
    EnvFactory.getEnv()
        .getConfig()
        .getConfigNodeConfig()
        .setMetricReporterType(Collections.singletonList("PROMETHEUS"));
    EnvFactory.getEnv().initClusterEnvironment();
  }

  @Before
  public void setUp() throws Exception {
    tmpDir = Files.createTempDirectory("load-mixed-model").toFile();
  }

  @After
  public void tearDown() throws Exception {
    try (Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      statement.execute("drop database if exists " + TABLE_DATABASE);
      statement.execute("drop database if exists " + SESSION_DATABASE);
      try (Connection treeConnection = EnvFactory.getEnv().getConnection();
          Statement treeStatement = treeConnection.createStatement()) {
        try {
          treeStatement.execute("delete database " + TREE_DATABASE);
        } catch (SQLException ignored) {
          // A table-only case need not create the Tree database.
        }
      }
    } finally {
      deleteRecursively(tmpDir);
    }
  }

  @AfterClass
  public static void tearDownClass() {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  @Test
  public void testMixedTreeTableTreeLoadRestoresSessionAndTableFileState() throws Exception {
    final File loadDir = new File(tmpDir, "mixed");
    Assert.assertTrue(loadDir.mkdir());

    writeTreeFile(new File(loadDir, "01-tree-before.tsfile"), TREE_DATABASE + ".before", 1, 101);
    final File firstTableFile = new File(loadDir, "02-table-first.tsfile");
    writeFirstTableFile(firstTableFile);
    writeTreeFile(new File(loadDir, "03-tree-middle.tsfile"), TREE_DATABASE + ".middle", 2, 202);
    final File secondTableFile = new File(loadDir, "04-table-second.tsfile");
    writeSecondTableFile(secondTableFile);
    writeTreeFile(new File(loadDir, "05-tree-after.tsfile"), TREE_DATABASE + ".after", 3, 303);
    final File missingTagTableFile = new File(loadDir, "06-table-missing-tag.tsfile");
    writeMissingTagTableFile(missingTagTableFile);
    assertTsFileOrder(
        loadDir,
        "01-tree-before.tsfile",
        "02-table-first.tsfile",
        "03-tree-middle.tsfile",
        "04-table-second.tsfile",
        "05-tree-after.tsfile",
        "06-table-missing-tag.tsfile");

    createTreeSchema("before", "middle", "after");
    try (Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      statement.execute("create database " + TABLE_DATABASE);
      statement.execute("create database " + SESSION_DATABASE);
      statement.execute("use " + TABLE_DATABASE);
      statement.execute(
          "create table telemetry (site STRING TAG, rack STRING TAG, zone STRING TAG, "
              + "reading INT32 FIELD)");
      statement.execute("create table alarm (sensor STRING TAG, state INT32 FIELD)");
      statement.execute("use " + SESSION_DATABASE);
      statement.execute(
          String.format(
              "load '%s' with ('database'='%s','database-level'='2',"
                  + "'tablet-conversion-threshold'='-1','convert-on-type-mismatch'='false',"
                  + "'auto-create-schema'='false','on-success'='none')",
              loadDir.getAbsolutePath(), TABLE_DATABASE));

      statement.execute("create table session_probe (site STRING TAG, reading INT32 FIELD)");
      statement.execute(
          "insert into session_probe(time, site, reading) values (800, 'session', 8)");
      try (ResultSet resultSet = statement.executeQuery("show tables")) {
        Assert.assertTrue(resultSet.next());
        Assert.assertEquals("session_probe", resultSet.getString(1));
        Assert.assertFalse(resultSet.next());
      }
      try (ResultSet resultSet =
          statement.executeQuery("select time, site, reading from session_probe")) {
        Assert.assertTrue(resultSet.next());
        Assert.assertEquals(800, resultSet.getLong("time"));
        Assert.assertEquals("session", resultSet.getString("site"));
        Assert.assertEquals(8, resultSet.getInt("reading"));
        Assert.assertFalse(resultSet.next());
      }
    }

    try (Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      statement.execute("use " + TABLE_DATABASE);
      statement.execute(
          "insert into telemetry(time, site, rack, zone, reading) "
              + "values (800, 'north', 'r4', 'z4', 88)");
    }

    assertTreeValue(TREE_DATABASE + ".before", 1, 101);
    assertTreeValue(TREE_DATABASE + ".middle", 2, 202);
    assertTreeValue(TREE_DATABASE + ".after", 3, 303);
    assertTableContents();

    TestUtils.restartDataNodes();
    assertTreeValue(TREE_DATABASE + ".before", 1, 101);
    assertTreeValue(TREE_DATABASE + ".middle", 2, 202);
    assertTreeValue(TREE_DATABASE + ".after", 3, 303);
    assertTableContents();

    Assert.assertTrue(firstTableFile.exists());
    Assert.assertTrue(secondTableFile.exists());
    Assert.assertTrue(
        new File(firstTableFile.getAbsolutePath() + ModificationFile.FILE_SUFFIX).exists());
  }

  @Test
  public void testTableMiniConversionHonorsExplicitAndSessionDatabases() throws Exception {
    final File explicitFile = new File(tmpDir, "mini-explicit.tsfile");
    final File sessionFile = new File(tmpDir, "mini-session.tsfile");
    writeMiniTableFile(explicitFile, "mini_explicit", true);
    writeMiniTableFile(sessionFile, "mini_session", false);
    final long threshold = Math.max(explicitFile.length(), sessionFile.length());

    try (Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      statement.execute("create database " + TABLE_DATABASE);
      statement.execute("create database " + SESSION_DATABASE);
      statement.execute("use " + TABLE_DATABASE);
      statement.execute(
          "create table mini_explicit (site STRING TAG, zone STRING TAG, reading INT32 FIELD)");
      statement.execute("use " + SESSION_DATABASE);
      statement.execute(
          "create table mini_session (site STRING TAG, zone STRING TAG, reading INT32 FIELD)");
    }

    final long conversionsBefore = getMiniConversionCount();
    try (Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      statement.execute("use " + SESSION_DATABASE);
      statement.execute(
          String.format(
              "load '%s' with ('database'='%s','database-level'='2',"
                  + "'tablet-conversion-threshold'='%d','auto-create-schema'='false',"
                  + "'on-success'='none')",
              explicitFile.getAbsolutePath(), TABLE_DATABASE, threshold));
      try (ResultSet resultSet =
          statement.executeQuery("select time, site, zone, reading from mini_session")) {
        Assert.assertFalse("Explicit database LOAD changed the session database", resultSet.next());
      }

      statement.execute(
          String.format(
              "load '%s' with ('database-level'='2','tablet-conversion-threshold'='%d',"
                  + "'auto-create-schema'='false','on-success'='none')",
              sessionFile.getAbsolutePath(), threshold));
      try (ResultSet resultSet =
          statement.executeQuery("select time, site, zone, reading from mini_session")) {
        Assert.assertTrue(resultSet.next());
        Assert.assertEquals(20, resultSet.getLong("time"));
        Assert.assertEquals("east", resultSet.getString("site"));
        Assert.assertNull(resultSet.getString("zone"));
        Assert.assertEquals(9, resultSet.getInt("reading"));
        Assert.assertFalse(resultSet.next());
      }
      try (ResultSet resultSet = statement.executeQuery("show tables")) {
        Assert.assertTrue(resultSet.next());
        Assert.assertEquals("mini_session", resultSet.getString(1));
        Assert.assertFalse(resultSet.next());
      }
    }

    try (Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      statement.execute("use " + TABLE_DATABASE);
      try (ResultSet resultSet =
          statement.executeQuery("select time, site, zone, reading from mini_explicit")) {
        Assert.assertTrue(resultSet.next());
        Assert.assertEquals(11, resultSet.getLong("time"));
        Assert.assertEquals("south", resultSet.getString("site"));
        Assert.assertEquals("south-1", resultSet.getString("zone"));
        Assert.assertEquals(8, resultSet.getInt("reading"));
        Assert.assertFalse(resultSet.next());
      }
    }
    Assert.assertEquals(2, getMiniConversionCount() - conversionsBefore);
    Assert.assertTrue(explicitFile.exists());
    Assert.assertTrue(sessionFile.exists());
  }

  @Test
  public void testMixedMiniAndNormalFilesKeepModelState() throws Exception {
    final File loadDir = new File(tmpDir, "mixed-mini");
    Assert.assertTrue(loadDir.mkdir());
    final File firstMini = new File(loadDir, "01-tree-mini.tsfile");
    final File normalTable = new File(loadDir, "02-table-normal.tsfile");
    final File lastMini = new File(loadDir, "03-tree-mini.tsfile");
    writeTreeFile(firstMini, TREE_DATABASE + ".before", 1, 101);
    writeMiniTableFile(normalTable, "normal_table", true);
    writeTreeFile(lastMini, TREE_DATABASE + ".after", 3, 303);
    assertTsFileOrder(
        loadDir, "01-tree-mini.tsfile", "02-table-normal.tsfile", "03-tree-mini.tsfile");
    final long threshold = Math.max(firstMini.length(), lastMini.length());
    Assert.assertTrue(normalTable.length() > threshold);
    createTreeSchema("before", "after");
    final long conversionsBefore = getMiniConversionCount();
    try (Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      statement.execute("create database " + TABLE_DATABASE);
      statement.execute("create database " + SESSION_DATABASE);
      statement.execute("use " + TABLE_DATABASE);
      statement.execute(
          "create table normal_table (site STRING TAG, zone STRING TAG, reading INT32 FIELD)");
      statement.execute("use " + SESSION_DATABASE);
      statement.execute(
          String.format(
              "load '%s' with ('database'='%s','database-level'='2',"
                  + "'tablet-conversion-threshold'='%d','auto-create-schema'='false',"
                  + "'convert-on-type-mismatch'='false','on-success'='none')",
              loadDir.getAbsolutePath(), TABLE_DATABASE, threshold));
      statement.execute("create table session_probe (site STRING TAG, reading INT32 FIELD)");
      statement.execute("insert into session_probe(time, site, reading) values (20, 'session', 2)");
      try (ResultSet resultSet = statement.executeQuery("select reading from session_probe")) {
        Assert.assertTrue(resultSet.next());
        Assert.assertEquals(2, resultSet.getInt(1));
        Assert.assertFalse(resultSet.next());
      }
      statement.execute("use " + TABLE_DATABASE);
      try (ResultSet resultSet =
          statement.executeQuery("select time, site, zone, reading from normal_table")) {
        Assert.assertTrue(resultSet.next());
        Assert.assertEquals(11, resultSet.getLong("time"));
        Assert.assertEquals("south", resultSet.getString("site"));
        Assert.assertEquals("south-1", resultSet.getString("zone"));
        Assert.assertEquals(8, resultSet.getInt("reading"));
        Assert.assertFalse(resultSet.next());
      }
    }
    Assert.assertEquals(2, getMiniConversionCount() - conversionsBefore);
    assertTreeValue(TREE_DATABASE + ".before", 1, 101);
    assertTreeValue(TREE_DATABASE + ".after", 3, 303);
    Assert.assertTrue(firstMini.exists());
    Assert.assertTrue(normalTable.exists());
    Assert.assertTrue(lastMini.exists());
  }

  @Test
  public void testTableDeviceBatchIsRegisteredBeforeLaterFileFails() throws Exception {
    final File loadDir = new File(tmpDir, "table-batch-failure");
    Assert.assertTrue(loadDir.mkdir());
    final File batchAndFailureFile = new File(loadDir, "01-batch-before-schema-failure.tsfile");
    final Set<String> registeredDevices = new HashSet<>();
    try (TsFileWriter writer = new TsFileWriter(batchAndFailureFile)) {
      writeTable(
          writer,
          "batch_probe",
          Arrays.asList(
              new MeasurementSchema("site", TSDataType.STRING),
              new MeasurementSchema("reading", TSDataType.INT32)),
          Arrays.asList(ColumnCategory.TAG, ColumnCategory.FIELD),
          Arrays.asList(
              new Object[] {10L, "first", 10},
              new Object[] {20L, "second", 20},
              new Object[] {30L, "tail", 30}));
      writeTable(
          writer,
          "aaa_failure_probe",
          Arrays.asList(
              new MeasurementSchema("site", TSDataType.STRING),
              new MeasurementSchema("reading", TSDataType.DOUBLE)),
          Arrays.asList(ColumnCategory.TAG, ColumnCategory.FIELD),
          Collections.singletonList(new Object[] {40L, "unprocessed", 40.0D}));
    }

    final List<String> batchDeviceReadOrder = new ArrayList<>();
    try (TsFileSequenceReader reader =
        new TsFileSequenceReader(batchAndFailureFile.getAbsolutePath())) {
      final TsFileSequenceReaderTimeseriesMetadataIterator iterator =
          new TsFileSequenceReaderTimeseriesMetadataIterator(reader, false);
      final List<String> analysisOrder = new ArrayList<>();
      while (iterator.hasNext()) {
        iterator
            .next()
            .keySet()
            .forEach(
                device -> {
                  if (!analysisOrder.contains(device.getTableName())) {
                    analysisOrder.add(device.getTableName());
                  }
                  if ("batch_probe".equals(device.getTableName())) {
                    batchDeviceReadOrder.add(String.valueOf(device.getSegments()[1]));
                  }
                });
      }
      Assert.assertEquals(Arrays.asList("batch_probe", "aaa_failure_probe"), analysisOrder);
      Assert.assertEquals(3, batchDeviceReadOrder.size());
      Assert.assertEquals(
          new HashSet<>(Arrays.asList("first", "second", "tail")),
          new HashSet<>(batchDeviceReadOrder));
    }
    final Set<String> expectedRegisteredDevices =
        new HashSet<>(batchDeviceReadOrder.subList(0, TABLE_DEVICE_BATCH_SIZE));

    try (Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      statement.execute("create database " + TABLE_DATABASE);
      statement.execute("use " + TABLE_DATABASE);
      statement.execute("create table batch_probe (site STRING TAG, reading INT32 FIELD)");
      statement.execute("create table aaa_failure_probe (site STRING TAG, reading INT32 FIELD)");

      try {
        statement.execute(
            String.format(
                "load '%s' with ('database'='%s','database-level'='2',"
                    + "'tablet-conversion-threshold'='-1','convert-on-type-mismatch'='false',"
                    + "'auto-create-schema'='false','on-success'='none')",
                batchAndFailureFile.getAbsolutePath(), TABLE_DATABASE));
        Assert.fail("The later table's incompatible field type must fail LOAD");
      } catch (SQLException expected) {
        Assert.assertTrue(
            "The later table's schema error should be preserved",
            expected.getMessage().contains("aaa_failure_probe"));
      }

      try (ResultSet resultSet = statement.executeQuery("show devices from batch_probe")) {
        while (resultSet.next()) {
          registeredDevices.add(resultSet.getString("site"));
        }
      }
      Assert.assertEquals(
          "The full batch must contain the first devices returned by the TsFile reader",
          expectedRegisteredDevices,
          registeredDevices);
      Assert.assertEquals(
          "The device after the full batch must remain pending when the next table fails",
          1,
          batchDeviceReadOrder.size() - registeredDevices.size());
      try (ResultSet resultSet = statement.executeQuery("select count(reading) from batch_probe")) {
        Assert.assertTrue(resultSet.next());
        Assert.assertEquals(
            "Analysis failure must not execute the LOAD data phase", 0, resultSet.getLong(1));
      }
    }
    Assert.assertTrue(batchAndFailureFile.exists());

    TestUtils.restartDataNodes();
    try (Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      statement.execute("use " + TABLE_DATABASE);
      final Set<String> devicesAfterRestart = new HashSet<>();
      try (ResultSet resultSet = statement.executeQuery("show devices from batch_probe")) {
        while (resultSet.next()) {
          devicesAfterRestart.add(resultSet.getString("site"));
        }
      }
      Assert.assertEquals(registeredDevices, devicesAfterRestart);
      try (ResultSet resultSet = statement.executeQuery("select count(reading) from batch_probe")) {
        Assert.assertTrue(resultSet.next());
        Assert.assertEquals(0, resultSet.getLong(1));
      }
    }
  }

  @Test
  public void testFailureInMixedLoadPreservesErrorAndSession() throws Exception {
    final File loadDir = new File(tmpDir, "mixed-failure");
    Assert.assertTrue(loadDir.mkdir());
    writeTreeFile(new File(loadDir, "01-tree-before.tsfile"), TREE_DATABASE + ".before", 1, 101);
    final File failingTableFile = new File(loadDir, "02-table-fails.tsfile");
    try (TsFileWriter writer = new TsFileWriter(failingTableFile)) {
      writeTable(
          writer,
          "failure_probe",
          Arrays.asList(
              new MeasurementSchema("site", TSDataType.STRING),
              new MeasurementSchema("reading", TSDataType.INT32)),
          Arrays.asList(ColumnCategory.TAG, ColumnCategory.FIELD),
          Collections.singletonList(new Object[] {10L, "station", 10}));
    }
    writeTreeFile(new File(loadDir, "03-tree-after.tsfile"), TREE_DATABASE + ".after", 3, 303);
    assertTsFileOrder(
        loadDir, "01-tree-before.tsfile", "02-table-fails.tsfile", "03-tree-after.tsfile");

    createTreeSchema("before", "after");
    try (Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      statement.execute("create database " + TABLE_DATABASE);
      statement.execute("use " + TABLE_DATABASE);
      statement.execute("create table failure_probe (site STRING TAG, reading INT64 FIELD)");
      statement.execute("create database " + SESSION_DATABASE);
      statement.execute("use " + SESSION_DATABASE);

      try {
        statement.execute(
            String.format(
                "load '%s' with ('database'='%s','database-level'='2',"
                    + "'tablet-conversion-threshold'='-1','convert-on-type-mismatch'='false',"
                    + "'auto-create-schema'='false',"
                    + "'on-success'='none')",
                loadDir.getAbsolutePath(), TABLE_DATABASE));
        Assert.fail("An incompatible middle Table schema must fail LOAD");
      } catch (SQLException expected) {
        Assert.assertNotNull(expected.getMessage());
        Assert.assertTrue(
            "The middle-file error should be preserved",
            expected.getMessage().contains("failure_probe"));
      }

      statement.execute("create table after_failure (site STRING TAG, reading INT32 FIELD)");
      statement.execute("insert into after_failure(time, site, reading) values (20, 'session', 2)");
      try (ResultSet resultSet =
          statement.executeQuery("select time, site, reading from after_failure")) {
        Assert.assertTrue(resultSet.next());
        Assert.assertEquals(20, resultSet.getLong("time"));
        Assert.assertEquals("session", resultSet.getString("site"));
        Assert.assertEquals(2, resultSet.getInt("reading"));
        Assert.assertFalse(resultSet.next());
      }
    }
  }

  private void assertTableContents() throws SQLException {
    try (Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      statement.execute("use " + TABLE_DATABASE);

      final Set<String> expectedDevices =
          new HashSet<>(
              Arrays.asList(
                  "north|r1|z1",
                  "north|r2|<null>",
                  "south|r3|z3",
                  "north|r4|z4",
                  "south|r5|<null>",
                  "central|<null>|z6",
                  "west|r6|<null>",
                  "east|r7|<null>"));
      try (ResultSet resultSet = statement.executeQuery("show devices from " + TELEMETRY_TABLE)) {
        while (resultSet.next()) {
          final String zone = resultSet.getString("zone");
          final String rack = resultSet.getString("rack");
          final String device =
              resultSet.getString("site")
                  + "|"
                  + (rack == null ? "<null>" : rack)
                  + "|"
                  + (zone == null ? "<null>" : zone);
          Assert.assertTrue(
              "Unexpected or duplicate device mapping: " + device, expectedDevices.remove(device));
        }
      }
      Assert.assertTrue(
          "Some TAG devices were not registered: " + expectedDevices, expectedDevices.isEmpty());

      try (ResultSet resultSet =
          statement.executeQuery(
              "select time, site, rack, zone, reading from telemetry order by time, site, rack")) {
        Assert.assertTrue(resultSet.next());
        Assert.assertEquals(100, resultSet.getLong("time"));
        Assert.assertEquals("north", resultSet.getString("site"));
        Assert.assertEquals("r1", resultSet.getString("rack"));
        Assert.assertEquals("z1", resultSet.getString("zone"));
        Assert.assertEquals(20, resultSet.getInt("reading"));

        Assert.assertTrue(resultSet.next());
        Assert.assertEquals(200, resultSet.getLong("time"));
        Assert.assertEquals("north", resultSet.getString("site"));
        Assert.assertEquals("r2", resultSet.getString("rack"));
        Assert.assertNull(resultSet.getString("zone"));
        Assert.assertEquals(21, resultSet.getInt("reading"));

        Assert.assertTrue(resultSet.next());
        Assert.assertEquals(300, resultSet.getLong("time"));
        Assert.assertEquals("south", resultSet.getString("site"));
        Assert.assertEquals("r3", resultSet.getString("rack"));
        Assert.assertEquals("z3", resultSet.getString("zone"));
        resultSet.getInt("reading");
        Assert.assertTrue(resultSet.wasNull());

        Assert.assertTrue(resultSet.next());
        Assert.assertEquals(400, resultSet.getLong("time"));
        Assert.assertEquals("north", resultSet.getString("site"));
        Assert.assertEquals("r4", resultSet.getString("rack"));
        Assert.assertEquals("z4", resultSet.getString("zone"));
        Assert.assertEquals(44, resultSet.getInt("reading"));

        Assert.assertTrue(resultSet.next());
        Assert.assertEquals(500, resultSet.getLong("time"));
        Assert.assertEquals("south", resultSet.getString("site"));
        Assert.assertEquals("r5", resultSet.getString("rack"));
        Assert.assertNull(resultSet.getString("zone"));
        Assert.assertEquals(55, resultSet.getInt("reading"));

        Assert.assertTrue(resultSet.next());
        Assert.assertEquals(550, resultSet.getLong("time"));
        Assert.assertEquals("central", resultSet.getString("site"));
        Assert.assertNull(resultSet.getString("rack"));
        Assert.assertEquals("z6", resultSet.getString("zone"));
        Assert.assertEquals(56, resultSet.getInt("reading"));

        Assert.assertTrue(resultSet.next());
        Assert.assertEquals(700, resultSet.getLong("time"));
        Assert.assertEquals("west", resultSet.getString("site"));
        Assert.assertEquals("r6", resultSet.getString("rack"));
        Assert.assertNull(resultSet.getString("zone"));
        Assert.assertEquals(66, resultSet.getInt("reading"));

        Assert.assertTrue(resultSet.next());
        Assert.assertEquals(750, resultSet.getLong("time"));
        Assert.assertEquals("east", resultSet.getString("site"));
        Assert.assertEquals("r7", resultSet.getString("rack"));
        Assert.assertNull(resultSet.getString("zone"));
        Assert.assertEquals(77, resultSet.getInt("reading"));

        Assert.assertTrue(resultSet.next());
        Assert.assertEquals(800, resultSet.getLong("time"));
        Assert.assertEquals("north", resultSet.getString("site"));
        Assert.assertEquals("r4", resultSet.getString("rack"));
        Assert.assertEquals("z4", resultSet.getString("zone"));
        Assert.assertEquals(88, resultSet.getInt("reading"));
        Assert.assertFalse(resultSet.next());
      }

      try (ResultSet resultSet =
          statement.executeQuery(
              "select time, reading from telemetry where site = 'central' and rack is null and zone = 'z6'")) {
        Assert.assertTrue(resultSet.next());
        Assert.assertEquals(550, resultSet.getLong("time"));
        Assert.assertEquals(56, resultSet.getInt("reading"));
        Assert.assertFalse(resultSet.next());
      }

      try (ResultSet resultSet =
          statement.executeQuery("select time, sensor, state from alarm where sensor = 'pump-1'")) {
        Assert.assertTrue(resultSet.next());
        Assert.assertEquals(600, resultSet.getLong("time"));
        Assert.assertEquals("pump-1", resultSet.getString("sensor"));
        Assert.assertEquals(1, resultSet.getInt("state"));
        Assert.assertFalse(resultSet.next());
      }
    }
  }

  private void writeFirstTableFile(final File file) throws Exception {
    try (TsFileWriter writer = new TsFileWriter(file)) {
      writeTable(
          writer,
          TELEMETRY_TABLE,
          telemetrySchema(false, true),
          telemetryCategories(true),
          Arrays.asList(
              new Object[] {100L, "north", "r1", "z1", 10},
              new Object[] {200L, "north", "r2", null, 21},
              new Object[] {300L, "south", "r3", "z3", null}));
      writeTable(
          writer,
          ALARM_TABLE,
          List.of(
              new MeasurementSchema("sensor", TSDataType.STRING),
              new MeasurementSchema("state", TSDataType.INT32)),
          List.of(ColumnCategory.TAG, ColumnCategory.FIELD),
          Collections.singletonList(new Object[] {600L, "pump-1", 1}));
    }
    try (ModificationFile modifications =
        new ModificationFile(
            new File(file.getAbsolutePath() + ModificationFile.FILE_SUFFIX), false)) {
      modifications.write(
          new TableDeletionEntry(new DeletionPredicate(TELEMETRY_TABLE), new TimeRange(100, 100)));
    }
  }

  private void writeSecondTableFile(final File file) throws Exception {
    try (TsFileWriter writer = new TsFileWriter(file)) {
      writeTable(
          writer,
          TELEMETRY_TABLE,
          telemetrySchema(false, true),
          telemetryCategories(true),
          Arrays.asList(
              new Object[] {100L, "north", "r1", "z1", 20},
              new Object[] {400L, "north", "r4", "z4", 44},
              new Object[] {500L, "south", "r5", null, 55},
              new Object[] {550L, "central", null, "z6", 56}));
    }
  }

  private void writeMissingTagTableFile(final File file) throws Exception {
    try (TsFileWriter writer = new TsFileWriter(file)) {
      writeTable(
          writer,
          TELEMETRY_TABLE,
          telemetrySchema(false, false),
          telemetryCategories(false),
          Arrays.asList(
              new Object[] {700L, "west", "r6", 66}, new Object[] {750L, "east", "r7", 77}));
    }
  }

  private void writeMiniTableFile(
      final File file, final String tableName, final boolean withDeletion) throws Exception {
    try (TsFileWriter writer = new TsFileWriter(file)) {
      writeTable(
          writer,
          tableName,
          Arrays.asList(
              new MeasurementSchema("site", TSDataType.STRING),
              new MeasurementSchema("zone", TSDataType.STRING),
              new MeasurementSchema("reading", TSDataType.INT32)),
          Arrays.asList(ColumnCategory.TAG, ColumnCategory.TAG, ColumnCategory.FIELD),
          withDeletion
              ? Arrays.asList(
                  new Object[] {10L, "north", null, 7}, new Object[] {11L, "south", "south-1", 8})
              : Collections.singletonList(new Object[] {20L, "east", null, 9}));
    }
    if (withDeletion) {
      try (ModificationFile modifications =
          new ModificationFile(
              new File(file.getAbsolutePath() + ModificationFile.FILE_SUFFIX), false)) {
        modifications.write(
            new TableDeletionEntry(new DeletionPredicate(tableName), new TimeRange(10, 10)));
      }
    }
  }

  private long getMiniConversionCount() {
    long count = 0;
    for (final String content : EnvFactory.getEnv().getMetricPrometheusReporterContents(null)) {
      if (content == null) {
        continue;
      }
      for (final String line : content.split("\\n")) {
        final int labelStart = line.indexOf('{');
        if (labelStart < 0
            || !line.substring(0, labelStart).endsWith("_count")
            || !line.contains("load_time_cost")
            || !line.contains("name=\"analysis_cast_tablets\"")) {
          continue;
        }
        count += Math.round(Double.parseDouble(line.substring(line.lastIndexOf(' ') + 1).trim()));
      }
    }
    return count;
  }

  private void createTreeSchema(final String... devices) throws SQLException {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("create database " + TREE_DATABASE);
      for (final String device : devices) {
        statement.execute("create timeseries " + TREE_DATABASE + "." + device + ".value INT64");
      }
    }
  }

  private void writeTable(
      final TsFileWriter writer,
      final String tableName,
      final List<IMeasurementSchema> schemas,
      final List<ColumnCategory> categories,
      final List<Object[]> rows)
      throws IOException, WriteProcessException {
    writer.registerTableSchema(new TableSchema(tableName, schemas, categories));
    final List<String> names =
        schemas.stream().map(IMeasurementSchema::getMeasurementName).collect(Collectors.toList());
    final List<TSDataType> types =
        schemas.stream().map(IMeasurementSchema::getType).collect(Collectors.toList());
    final Tablet tablet = new Tablet(tableName, names, types, categories);
    for (final Object[] values : rows) {
      final int row = tablet.getRowSize();
      tablet.addTimestamp(row, (Long) values[0]);
      for (int column = 0; column < schemas.size(); column++) {
        tablet.addValue(names.get(column), row, values[column + 1]);
      }
    }
    writer.writeTable(tablet);
  }

  private void writeTableFile(
      final File file,
      final String tableName,
      final List<IMeasurementSchema> schemas,
      final List<ColumnCategory> categories,
      final List<Object[]> rows)
      throws IOException, WriteProcessException {
    try (TsFileWriter writer = new TsFileWriter(file)) {
      writeTable(writer, tableName, schemas, categories, rows);
    }
  }

  private List<IMeasurementSchema> telemetrySchema(
      final boolean rackFirst, final boolean includeZone) {
    if (!includeZone) {
      return rackFirst
          ? Arrays.asList(
              new MeasurementSchema("rack", TSDataType.STRING),
              new MeasurementSchema("site", TSDataType.STRING),
              new MeasurementSchema("reading", TSDataType.INT32))
          : Arrays.asList(
              new MeasurementSchema("site", TSDataType.STRING),
              new MeasurementSchema("rack", TSDataType.STRING),
              new MeasurementSchema("reading", TSDataType.INT32));
    }
    return rackFirst
        ? Arrays.asList(
            new MeasurementSchema("rack", TSDataType.STRING),
            new MeasurementSchema("site", TSDataType.STRING),
            new MeasurementSchema("zone", TSDataType.STRING),
            new MeasurementSchema("reading", TSDataType.INT32))
        : Arrays.asList(
            new MeasurementSchema("site", TSDataType.STRING),
            new MeasurementSchema("rack", TSDataType.STRING),
            new MeasurementSchema("zone", TSDataType.STRING),
            new MeasurementSchema("reading", TSDataType.INT32));
  }

  private List<ColumnCategory> telemetryCategories(final boolean includeZone) {
    if (!includeZone) {
      return Arrays.asList(ColumnCategory.TAG, ColumnCategory.TAG, ColumnCategory.FIELD);
    }
    return Arrays.asList(
        ColumnCategory.TAG, ColumnCategory.TAG, ColumnCategory.TAG, ColumnCategory.FIELD);
  }

  private void assertTsFileOrder(final File directory, final String... expectedNames) {
    final File[] files = directory.listFiles((dir, name) -> name.endsWith(".tsfile"));
    Assert.assertNotNull(files);
    Arrays.sort(files, Comparator.comparing(File::getName));
    Assert.assertEquals(
        Arrays.asList(expectedNames),
        Arrays.stream(files).map(File::getName).collect(Collectors.toList()));
  }

  private void writeTreeFile(
      final File file, final String device, final long timestamp, final long value)
      throws Exception {
    final MeasurementSchema schema = new MeasurementSchema("value", TSDataType.INT64);
    try (TsFileWriter writer = new TsFileWriter(file)) {
      writer.registerTimeseries(new Path(device), Collections.singletonList(schema));
      final Tablet tablet = new Tablet(device, Collections.singletonList(schema));
      tablet.addTimestamp(0, timestamp);
      tablet.addValue(0, 0, value);
      writer.writeTree(tablet);
    }
  }

  private void assertTreeValue(
      final String device, final long expectedTime, final long expectedValue) throws SQLException {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement();
        ResultSet resultSet =
            statement.executeQuery("select time, value from " + device + " order by time")) {
      Assert.assertTrue(resultSet.next());
      Assert.assertEquals(expectedTime, resultSet.getLong("time"));
      Assert.assertEquals(expectedValue, resultSet.getLong("value"));
      Assert.assertFalse(resultSet.next());
    }
  }

  private void deleteRecursively(final File file) throws IOException {
    if (file == null || !file.exists()) {
      return;
    }
    final File[] children = file.listFiles();
    if (children != null) {
      for (final File child : children) {
        deleteRecursively(child);
      }
    }
    Files.deleteIfExists(file.toPath());
  }
}
