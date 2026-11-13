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

package org.apache.iotdb.relational.it.db.it;

import org.apache.iotdb.commons.i18n.CommonMessages;
import org.apache.iotdb.db.i18n.DataNodeQueryMessages;
import org.apache.iotdb.db.it.utils.TestUtils;
import org.apache.iotdb.db.storageengine.dataregion.modification.DeletionPredicate;
import org.apache.iotdb.db.storageengine.dataregion.modification.ModificationFile;
import org.apache.iotdb.db.storageengine.dataregion.modification.TableDeletionEntry;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;
import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.env.cluster.config.MppDataNodeConfig;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.TableClusterIT;
import org.apache.iotdb.itbase.category.TableLocalStandaloneIT;
import org.apache.iotdb.itbase.env.BaseEnv;

import org.apache.tsfile.enums.ColumnCategory;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.file.metadata.TableSchema;
import org.apache.tsfile.read.common.TimeRange;
import org.apache.tsfile.write.TsFileWriter;
import org.apache.tsfile.write.record.Tablet;
import org.apache.tsfile.write.schema.IMeasurementSchema;
import org.apache.tsfile.write.schema.MeasurementSchema;
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
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;

@RunWith(IoTDBTestRunner.class)
@Category({TableLocalStandaloneIT.class, TableClusterIT.class})
public class IoTDBLoadTsFileTableBoundaryIT {
  private static File tmpDir;

  @BeforeClass
  public static void setUpClass() throws Exception {
    tmpDir = Files.createTempDirectory("load-table-boundary-").toFile();
    EnvFactory.getEnv().getConfig().getCommonConfig().setTimePartitionInterval(10000);
    EnvFactory.getEnv().getConfig().getCommonConfig().setDatanodeMemoryProportion("3:3:1:1:1:0");
    EnvFactory.getEnv()
        .getConfig()
        .getDataNodeConfig()
        .setLoadTsFileAnalyzeSchemaMemorySizeInBytes(10 * 1024L);
    final Properties split = new Properties();
    split.setProperty("load_tsfile_statement_split_threshold", "2");
    split.setProperty("load_tsfile_sub_statement_batch_size", "2");
    ((MppDataNodeConfig) EnvFactory.getEnv().getConfig().getDataNodeConfig())
        .updateProperties(split);
    EnvFactory.getEnv().initClusterEnvironment();
  }

  @AfterClass
  public static void tearDownClass() throws Exception {
    try {
      EnvFactory.getEnv().cleanClusterEnvironment();
    } finally {
      if (tmpDir != null && tmpDir.exists()) {
        try (java.util.stream.Stream<java.nio.file.Path> paths = Files.walk(tmpDir.toPath())) {
          for (java.nio.file.Path file :
              paths.sorted(java.util.Comparator.reverseOrder()).collect(Collectors.toList())) {
            Files.delete(file);
          }
        }
      }
    }
  }

  /** P1-11: external resource metadata cannot hide TAG devices, tail partitions or table mods. */
  @Test
  public void testMissingAndStaleResourcePreservesTableDevicesAfterRestart() throws Exception {
    final long[] times = {-10001, -10000, -1, 0, 9999, 10000};
    final List<IMeasurementSchema> schemas =
        Arrays.asList(
            new MeasurementSchema("tag1", TSDataType.STRING),
            new MeasurementSchema("tag2", TSDataType.STRING),
            new MeasurementSchema("f", TSDataType.INT64),
            new MeasurementSchema("txt", TSDataType.TEXT));
    final List<ColumnCategory> categories =
        Arrays.asList(
            ColumnCategory.TAG, ColumnCategory.TAG, ColumnCategory.FIELD, ColumnCategory.FIELD);
    for (int mode = 0; mode < 3; mode++) {
      final String database = "resource_table_" + mode;
      final File file = new File(tmpDir, (800 + mode) + "-0-0-0.tsfile");
      try (TsFileWriter writer = new TsFileWriter(file)) {
        writer.registerTableSchema(new TableSchema("t", schemas, categories));
        for (int device = 0; device < 2; device++) {
          final Tablet tablet =
              new Tablet(
                  "t",
                  Arrays.asList("tag1", "tag2", "f", "txt"),
                  Arrays.asList(
                      TSDataType.STRING, TSDataType.STRING, TSDataType.INT64, TSDataType.TEXT),
                  categories);
          for (int row = 0; row < times.length; row++) {
            tablet.addTimestamp(row, times[row]);
            tablet.addValue(row, "tag1", "d" + device);
            tablet.addValue(row, "tag2", device == 0 ? null : "tail");
            tablet.addValue("f", row, row == 3 ? null : device * 100L + row);
            tablet.addValue(row, "txt", row == 0 || row == 3 ? null : "v" + device + "_" + row);
          }
          writer.writeTable(tablet);
        }
      }
      final TsFileResource resource = new TsFileResource(file);
      for (int device = 0; device < (mode == 2 ? 1 : 2); device++) {
        final IDeviceID id =
            IDeviceID.Factory.DEFAULT_FACTORY.create(
                new String[] {"t", "d" + device, device == 0 ? null : "tail"});
        resource.updateStartTime(id, times[0]);
        resource.updateEndTime(id, mode == 2 ? times[0] : times[times.length - 1]);
      }
      if (mode != 0) {
        resource.serialize();
      }
      try (ModificationFile mods = resource.getExclusiveModFile()) {
        mods.write(new TableDeletionEntry(new DeletionPredicate("t"), new TimeRange(-1, -1)));
      }
      try (Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
          Statement statement = connection.createStatement()) {
        statement.execute(
            "load '"
                + file.getAbsolutePath()
                + "' with ('database'='"
                + database
                + "','on-success'='none','tablet-conversion-threshold'='-1')");
      }
      assertResourceTableRows(database, times);
    }
    TestUtils.restartCluster(EnvFactory.getEnv());
    for (int mode = 0; mode < 3; mode++) {
      assertResourceTableRows("resource_table_" + mode, times);
    }
  }

  private static void assertResourceTableRows(String database, long[] times) throws Exception {
    assertResourceTableRows(database, "t", times);
  }

  private static void assertResourceTableRows(String database, String table, long[] times)
      throws Exception {
    assertResourceTableRows(database, table, times, false);
  }

  private static void assertResourceTableRows(
      String database, String table, long[] times, boolean doubleField) throws Exception {
    try (Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      statement.execute("use " + database);
      for (int device = 0; device < 2; device++) {
        final String filter =
            "tag1='d" + device + "' and tag2 " + (device == 0 ? "is null" : "='tail'");
        try (ResultSet rows =
            statement.executeQuery(
                "select time,tag1,tag2,f,txt from "
                    + table
                    + " where "
                    + filter
                    + " order by time")) {
          Assert.assertEquals(
              doubleField ? "DOUBLE" : "INT64", rows.getMetaData().getColumnTypeName(4));
          for (int row = 0; row < times.length; row++) {
            if (row == 2) {
              continue;
            }
            Assert.assertTrue(database + " device=" + device + " time=" + times[row], rows.next());
            Assert.assertEquals(times[row], rows.getLong(1));
            Assert.assertEquals("d" + device, rows.getString(2));
            Assert.assertEquals(device == 0 ? null : "tail", rows.getString(3));
            final Object expectedField;
            if (row == 3) {
              expectedField = null;
            } else if (doubleField) {
              expectedField = (double) (device * 100L + row);
            } else {
              expectedField = device * 100L + row;
            }
            Assert.assertEquals(expectedField, rows.getObject(4));
            Assert.assertEquals(
                row == 0 || row == 3 ? null : "v" + device + "_" + row, rows.getString(5));
          }
          Assert.assertFalse(rows.next());
        }
        for (int row : new int[] {0, 2, 5}) {
          try (ResultSet rows =
              statement.executeQuery(
                  "select time,f from " + table + " where " + filter + " and time=" + times[row])) {
            Assert.assertEquals(row != 2, rows.next());
            if (row != 2) {
              Assert.assertEquals(times[row], rows.getLong(1));
              if (doubleField) {
                Assert.assertEquals((double) (device * 100L + row), rows.getObject(2));
              } else {
                Assert.assertEquals(device * 100L + row, rows.getObject(2));
              }
              Assert.assertFalse(rows.next());
            }
          }
        }
      }
    }
  }

  /** P1-13: exercise the independent table RPC splitter, including a failing middle batch. */
  @Test
  public void testTableSubStatementsKeepExplicitDatabaseAndStopAfterFailure() throws Exception {
    final long[] times = {-10001, -10000, -1, 0, 9999, 10000};
    final String[] parameters = {
      "2-none",
      "3-delete",
      "5-none",
      "5-delete",
      "5-failure-none",
      "5-failure-delete",
      "5-missing-schema-none",
      "5-missing-schema-delete",
      "5-no-verify-none",
      "5-no-verify-delete"
    };
    final List<Throwable> parameterFailures = new ArrayList<>();
    for (String parameter : parameters) {
      try {
        final int count = Integer.parseInt(parameter.substring(0, 1));
        final boolean missingSchema = parameter.contains("missing-schema");
        final boolean failure = parameter.contains("failure") || missingSchema;
        final boolean delete = parameter.endsWith("delete");
        final boolean verify = !parameter.contains("no-verify");
        final String database = "split_table_" + parameter.replace('-', '_');
        final File batch = new File(tmpDir, parameter);
        Assert.assertTrue(batch.mkdir());
        try (Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
            Statement statement = connection.createStatement()) {
          statement.execute("create database " + database);
          statement.execute("create database if not exists split_wrong_default");
          statement.execute("use split_wrong_default");
          for (int file = 0; file < count; file++) {
            final String table = "t" + file;
            writeSplitTableSource(new File(batch, (900 + file) + "-0-0-0.tsfile"), table, times);
            // FIELD type checks depend on verify. TAG-name checks do not. Disable conversion below
            // so verify=true must fail rather than succeeding through tablet type conversion.
            if (missingSchema && file == 2) {
              continue;
            }
            final String fieldType = !verify || (failure && file == 2) ? "DOUBLE" : "INT64";
            statement.execute(
                "create table "
                    + database
                    + "."
                    + table
                    + "(tag1 STRING TAG,tag2 STRING TAG,f "
                    + fieldType
                    + " FIELD,txt TEXT FIELD)");
          }
          final String sql =
              "load '"
                  + batch.getAbsolutePath()
                  + "' with ('database'='"
                  + database
                  + "','auto-create-schema'='false','verify'='"
                  + verify
                  + "','convert-on-type-mismatch'='false','tablet-conversion-threshold'='-1',"
                  + "'on-success'='"
                  + (delete ? "delete" : "none")
                  + "')";
          final int oldBatches = tableLogCount("Start batch executing");
          final String executionRecord =
              failure
                  ? "Failed to execute sub-statement 2/3"
                  : "Successfully executed sub-statement "
                      + ((count + 1) / 2)
                      + "/"
                      + ((count + 1) / 2);
          final int oldRecords = tableLogCount(executionRecord);
          if (failure) {
            final java.sql.SQLException error =
                Assert.assertThrows(
                    parameter, java.sql.SQLException.class, () -> statement.execute(sql));
            Assert.assertEquals(
                org.apache.iotdb.rpc.TSStatusCode.LOAD_FILE_ERROR.getStatusCode(),
                error.getErrorCode());
            final String expectedCause =
                missingSchema
                    ? String.format(
                        CommonMessages.EXCEPTION_TABLE_ARG_ARG_DOES_NOT_EXIST_796E503B,
                        database,
                        "t2")
                    : String.format(
                        DataNodeQueryMessages
                            .MESSAGE_DATA_TYPE_MISMATCH_FOR_COLUMN_ARG_IN_TABLE_ARG_TYPE_IN_TSFILE_ARG_TYPE_IN_IOTDB_ARG_ECAD0C63,
                        "f",
                        "t2",
                        TSDataType.INT64,
                        TSDataType.DOUBLE);
            Assert.assertTrue(error.getMessage(), error.getMessage().contains(expectedCause));
          } else {
            statement.execute(sql);
          }
          Assert.assertEquals(
              count > 2 ? 1 : 0, tableLogCount("Start batch executing") - oldBatches);
          if (count > 2) {
            Assert.assertEquals(1, tableLogCount(executionRecord) - oldRecords);
          }
          for (int file = 0; file < count; file++) {
            final boolean completed = !failure || file < 2;
            final File source = new File(batch, (900 + file) + "-0-0-0.tsfile");
            for (String suffix :
                new String[] {"", TsFileResource.RESOURCE_SUFFIX, ModificationFile.FILE_SUFFIX}) {
              Assert.assertEquals(
                  source + suffix, !completed || !delete, new File(source + suffix).exists());
            }
            if (completed) {
              assertResourceTableRows(database, "t" + file, times, !verify);
            } else if (missingSchema && file == 2) {
              try (ResultSet tables = statement.executeQuery("show tables from " + database)) {
                while (tables.next()) {
                  Assert.assertNotEquals(
                      "auto-create-schema=false must survive each child statement",
                      "t2",
                      tables.getString(1));
                }
              }
            } else {
              try (ResultSet rows =
                  statement.executeQuery("select * from " + database + ".t" + file)) {
                Assert.assertFalse("Failed and later batches must not write rows", rows.next());
              }
            }
          }
          try (ResultSet tables = statement.executeQuery("show tables")) {
            Assert.assertFalse(
                "Explicit database must survive each child statement", tables.next());
          }
          if (failure) {
            if (!missingSchema) {
              statement.execute("drop table " + database + ".t2");
            }
            statement.execute(
                "create table "
                    + database
                    + ".t2(tag1 STRING TAG,tag2 STRING TAG,f INT64 FIELD,txt TEXT FIELD)");
            for (int file = 2; file < count; file++) {
              statement.execute(
                  sql.replace(
                      batch.getAbsolutePath(),
                      new File(batch, (900 + file) + "-0-0-0.tsfile").getAbsolutePath()));
            }
            for (int file = 0; file < count; file++) {
              assertResourceTableRows(database, "t" + file, times);
              final File source = new File(batch, (900 + file) + "-0-0-0.tsfile");
              for (String suffix :
                  new String[] {"", TsFileResource.RESOURCE_SUFFIX, ModificationFile.FILE_SUFFIX}) {
                Assert.assertEquals(source + suffix, !delete, new File(source + suffix).exists());
              }
            }
          }
        } finally {
          // Remove retained sources even when a parameter group fails.
          try (java.util.stream.Stream<java.nio.file.Path> paths = Files.walk(batch.toPath())) {
            for (java.nio.file.Path path :
                paths.sorted(java.util.Comparator.reverseOrder()).collect(Collectors.toList())) {
              Files.delete(path);
            }
          }
        }
      } catch (Exception | AssertionError error) {
        parameterFailures.add(new AssertionError("Directory LOAD parameter " + parameter, error));
      }
    }
    if (!parameterFailures.isEmpty()) {
      final AssertionError failure = new AssertionError("Directory LOAD parameter groups failed");
      parameterFailures.forEach(failure::addSuppressed);
      throw failure;
    }
  }

  private static int tableLogCount(String message) throws Exception {
    int count = 0;
    for (org.apache.iotdb.it.env.cluster.node.DataNodeWrapper node :
        EnvFactory.getEnv().getDataNodeWrapperList()) {
      try (java.util.stream.Stream<String> lines =
          Files.lines(new File(node.getLogDirPath(), node.getId() + ".log").toPath())) {
        count +=
            (int)
                lines
                    .filter(line -> line.contains(message) && line.contains("in table model"))
                    .count();
      }
    }
    return count;
  }

  private static void writeSplitTableSource(File file, String table, long[] times)
      throws Exception {
    final List<IMeasurementSchema> schemas =
        Arrays.asList(
            new MeasurementSchema("tag1", TSDataType.STRING),
            new MeasurementSchema("tag2", TSDataType.STRING),
            new MeasurementSchema("f", TSDataType.INT64),
            new MeasurementSchema("txt", TSDataType.TEXT));
    final List<ColumnCategory> categories =
        Arrays.asList(
            ColumnCategory.TAG, ColumnCategory.TAG, ColumnCategory.FIELD, ColumnCategory.FIELD);
    try (TsFileWriter writer = new TsFileWriter(file)) {
      writer.registerTableSchema(new TableSchema(table, schemas, categories));
      for (int device = 0; device < 2; device++) {
        final Tablet tablet =
            new Tablet(
                table,
                Arrays.asList("tag1", "tag2", "f", "txt"),
                Arrays.asList(
                    TSDataType.STRING, TSDataType.STRING, TSDataType.INT64, TSDataType.TEXT),
                categories);
        for (int row = 0; row < times.length; row++) {
          tablet.addTimestamp(row, times[row]);
          tablet.addValue(row, "tag1", "d" + device);
          tablet.addValue(row, "tag2", device == 0 ? null : "tail");
          tablet.addValue("f", row, row == 3 ? null : device * 100L + row);
          tablet.addValue(row, "txt", row == 0 || row == 3 ? null : "v" + device + "_" + row);
        }
        writer.writeTable(tablet);
      }
    }
    final TsFileResource resource = new TsFileResource(file);
    for (int device = 0; device < 2; device++) {
      final IDeviceID id =
          IDeviceID.Factory.DEFAULT_FACTORY.create(
              new String[] {table, "d" + device, device == 0 ? null : "tail"});
      resource.updateStartTime(id, times[0]);
      resource.updateEndTime(id, times[times.length - 1]);
    }
    resource.serialize();
    try (ModificationFile mods = resource.getExclusiveModFile()) {
      mods.write(new TableDeletionEntry(new DeletionPredicate(table), new TimeRange(-1, -1)));
    }
  }
}
