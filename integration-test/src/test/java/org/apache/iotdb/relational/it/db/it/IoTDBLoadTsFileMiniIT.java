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
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

@RunWith(IoTDBTestRunner.class)
@Category({TableLocalStandaloneIT.class, TableClusterIT.class})
public class IoTDBLoadTsFileMiniIT {
  private static final String TABLE_DATABASE = "load_p116_target";
  private static final String SESSION_DATABASE = "load_p116_session";
  private static final String TREE_DATABASE = "root.load_p116_tree";

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
        .setMetricReporterType(Collections.singletonList("PROMETHEUS"))
        .setMetricPrometheusReporterAsyncUpdate(false);
    EnvFactory.getEnv()
        .getConfig()
        .getConfigNodeConfig()
        .setMetricReporterType(Collections.singletonList("PROMETHEUS"));
    EnvFactory.getEnv().initClusterEnvironment();
  }

  @Before
  public void setUp() throws Exception {
    tmpDir = Files.createTempDirectory("load-mini").toFile();
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
          // The table-only case does not create the Tree database.
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

  private void writeMiniTableFile(
      final File file, final String tableName, final boolean withDeletion) throws Exception {
    final List<IMeasurementSchema> schemas =
        Arrays.asList(
            new MeasurementSchema("site", TSDataType.STRING),
            new MeasurementSchema("zone", TSDataType.STRING),
            new MeasurementSchema("reading", TSDataType.INT32));
    final List<ColumnCategory> categories =
        Arrays.asList(ColumnCategory.TAG, ColumnCategory.TAG, ColumnCategory.FIELD);
    try (TsFileWriter writer = new TsFileWriter(file)) {
      writeTable(
          writer,
          tableName,
          schemas,
          categories,
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
            || !line.contains("analysis_cast_tablets")) {
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
            statement.executeQuery("select value from " + device + " order by time")) {
      Assert.assertTrue(resultSet.next());
      Assert.assertEquals(expectedTime, resultSet.getLong(1));
      Assert.assertEquals(expectedValue, resultSet.getLong(2));
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
