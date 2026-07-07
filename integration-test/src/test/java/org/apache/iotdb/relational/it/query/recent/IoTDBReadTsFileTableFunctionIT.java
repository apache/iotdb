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

package org.apache.iotdb.relational.it.query.recent;

import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.env.cluster.node.DataNodeWrapper;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.TableClusterIT;
import org.apache.iotdb.itbase.category.TableLocalStandaloneIT;
import org.apache.iotdb.itbase.env.BaseEnv;

import org.apache.tsfile.enums.ColumnCategory;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.exception.write.WriteProcessException;
import org.apache.tsfile.file.metadata.TableSchema;
import org.apache.tsfile.write.TsFileWriter;
import org.apache.tsfile.write.record.Tablet;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.apache.iotdb.db.it.utils.TestUtils.tableAssertTestFail;
import static org.apache.iotdb.db.it.utils.TestUtils.tableResultSetEqualTest;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(IoTDBTestRunner.class)
@Category({TableLocalStandaloneIT.class, TableClusterIT.class})
public class IoTDBReadTsFileTableFunctionIT {
  private static final String DATABASE_NAME = "test_read_tsfile";

  private static File tmpDir;

  @BeforeClass
  public static void setUp() throws Exception {
    EnvFactory.getEnv().initClusterEnvironment();
    try (Connection connection = EnvFactory.getEnv().getTableConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("CREATE DATABASE " + DATABASE_NAME);
    }
  }

  @Before
  public void setUpBeforeTest() throws IOException {
    tmpDir = new File(Files.createTempDirectory("read-tsfile").toUri());
  }

  @After
  public void tearDownAfterTest() {
    deleteTmpDir();
  }

  @AfterClass
  public static void tearDown() {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  @Test
  public void testReadSingleTsFile() throws Exception {
    File tsFile = new File(tmpDir, "single.tsfile");
    try (TsFileWriter writer = new TsFileWriter(tsFile)) {
      generateTable(
          writer, "table1", Arrays.asList("tag1", "tag2"), Arrays.asList("s1", "s2"), 1, 2);
      generateTable(writer, "table2", Arrays.asList("tag1"), Arrays.asList("s1"), 1, 2);
    }

    String[] expectedHeader = new String[] {"time", "tag1", "tag2", "s1", "s2"};
    String[] retArray =
        new String[] {
          "1970-01-01T00:00:00.001Z,tag1_1,tag2_1,1,1,",
          "1970-01-01T00:00:00.002Z,tag1_1,tag2_1,2,2,",
          "1970-01-01T00:00:00.001Z,tag1_2,tag2_2,1,1,",
          "1970-01-01T00:00:00.002Z,tag1_2,tag2_2,2,2,",
        };
    tableResultSetEqualTest(
        "SELECT time, tag1, tag2, s1, s2 FROM read_tsfile(PATHS => '"
            + toSqlPath(tsFile)
            + "', TABLE_NAME => 'table1') ORDER BY tag1, tag2, time",
        expectedHeader,
        retArray,
        DATABASE_NAME);
  }

  @Test
  public void testReadTsFileWithNoMatchedDevice() throws Exception {
    File tsFile = new File(tmpDir, "empty-device.tsfile");
    try (TsFileWriter writer = new TsFileWriter(tsFile)) {
      generateTable(
          writer, "table1", Arrays.asList("tag1", "tag2"), Arrays.asList("s1", "s2"), 1, 2);
    }

    tableResultSetEqualTest(
        "SELECT time, tag1, tag2, s1, s2 FROM read_tsfile(PATHS => '"
            + toSqlPath(tsFile)
            + "', TABLE_NAME => 'table1') WHERE tag1 = 'not_exists'",
        new String[] {"time", "tag1", "tag2", "s1", "s2"},
        new String[] {},
        DATABASE_NAME);
  }

  @Test
  public void testReadTsFileAggregationWithNoMatchedDevice() throws Exception {
    File tsFile = new File(tmpDir, "empty-device-aggregation.tsfile");
    try (TsFileWriter writer = new TsFileWriter(tsFile)) {
      generateTable(
          writer, "table1", Arrays.asList("tag1", "tag2"), Arrays.asList("s1", "s2"), 1, 2);
    }

    tableResultSetEqualTest(
        "SELECT count(*) AS count_star, count(s1) AS count_s1, sum(s1) AS sum_s1"
            + " FROM read_tsfile(PATHS => '"
            + toSqlPath(tsFile)
            + "', TABLE_NAME => 'table1') WHERE tag1 = 'not_exists'",
        new String[] {"count_star", "count_s1", "sum_s1"},
        new String[] {"0,0,null,"},
        DATABASE_NAME);
  }

  @Test
  public void testReadTsFileGroupedAggregationAcrossDevices() throws Exception {
    File tsFile1 = new File(tmpDir, "grouped-aggregation-1.tsfile");
    try (TsFileWriter writer = new TsFileWriter(tsFile1)) {
      generateTable(
          writer, "table1", Arrays.asList("tag1", "tag2"), Arrays.asList("s1", "s2"), 1, 2);
    }
    File tsFile2 = new File(tmpDir, "grouped-aggregation-2.tsfile");
    try (TsFileWriter writer = new TsFileWriter(tsFile2)) {
      generateTable(
          writer, "table1", Arrays.asList("tag1", "tag2"), Arrays.asList("s1", "s2"), 3, 4);
    }

    tableResultSetEqualTest(
        "SELECT tag1, count(s1) AS count_s1, sum(s1) AS sum_s1"
            + " FROM read_tsfile(PATHS => '"
            + toSqlPath(tsFile1)
            + ","
            + toSqlPath(tsFile2)
            + "', TABLE_NAME => 'table1') GROUP BY tag1 ORDER BY tag1",
        new String[] {"tag1", "count_s1", "sum_s1"},
        new String[] {"tag1_1,4,10.0,", "tag1_2,4,10.0,"},
        DATABASE_NAME);
  }

  @Test
  public void testReadMultipleTsFilesWithDeviceFilter() throws Exception {
    File tsFile1 = new File(tmpDir, "multi-1.tsfile");
    try (TsFileWriter writer = new TsFileWriter(tsFile1)) {
      generateTable(
          writer, "table1", Arrays.asList("tag1", "tag2"), Arrays.asList("s1", "s2"), 1, 2);
    }
    File tsFile2 = new File(tmpDir, "multi-2.tsfile");
    try (TsFileWriter writer = new TsFileWriter(tsFile2)) {
      generateTable(
          writer, "table1", Arrays.asList("tag1", "tag2"), Arrays.asList("s1", "s2"), 3, 4);
    }

    String[] expectedHeader = new String[] {"time", "tag1", "tag2", "s1", "s2"};
    String[] retArray =
        new String[] {
          "1970-01-01T00:00:00.001Z,tag1_1,tag2_1,1,1,",
          "1970-01-01T00:00:00.002Z,tag1_1,tag2_1,2,2,",
          "1970-01-01T00:00:00.003Z,tag1_1,tag2_1,3,3,",
          "1970-01-01T00:00:00.004Z,tag1_1,tag2_1,4,4,",
        };
    tableResultSetEqualTest(
        "SELECT time, tag1, tag2, s1, s2 FROM read_tsfile(PATHS => '"
            + toSqlPath(tsFile1)
            + ","
            + toSqlPath(tsFile2)
            + "', TABLE_NAME => 'table1') WHERE tag1 = 'tag1_1' ORDER BY time",
        expectedHeader,
        retArray,
        DATABASE_NAME);
  }

  @Test
  public void testReadMultipleTsFilesWithSchemaMerge() throws Exception {
    File tsFile1 = new File(tmpDir, "schema-merge-1.tsfile");
    try (TsFileWriter writer = new TsFileWriter(tsFile1)) {
      generateTable(writer, "table1", Arrays.asList("tag1"), Arrays.asList("s1"), 1, 2);
    }
    File tsFile2 = new File(tmpDir, "schema-merge-2.tsfile");
    try (TsFileWriter writer = new TsFileWriter(tsFile2)) {
      generateTable(writer, "table1", Arrays.asList("tag1"), Arrays.asList("s1", "s2"), 3, 4);
    }

    String[] expectedHeader = new String[] {"time", "tag1", "s1", "s2"};
    String[] retArray =
        new String[] {
          "1970-01-01T00:00:00.001Z,tag1_1,1,null,",
          "1970-01-01T00:00:00.002Z,tag1_1,2,null,",
          "1970-01-01T00:00:00.003Z,tag1_1,3,3,",
          "1970-01-01T00:00:00.004Z,tag1_1,4,4,",
          "1970-01-01T00:00:00.001Z,tag1_2,1,null,",
          "1970-01-01T00:00:00.002Z,tag1_2,2,null,",
          "1970-01-01T00:00:00.003Z,tag1_2,3,3,",
          "1970-01-01T00:00:00.004Z,tag1_2,4,4,",
        };
    tableResultSetEqualTest(
        "SELECT time, tag1, s1, s2 FROM read_tsfile(PATHS => '"
            + toSqlPath(tsFile1)
            + ","
            + toSqlPath(tsFile2)
            + "', TABLE_NAME => 'table1') ORDER BY tag1, time",
        expectedHeader,
        retArray,
        DATABASE_NAME);
  }

  @Test
  public void testTimeJoinReadTsFileWithSameTableNameFromDifferentTsFiles() throws Exception {
    File leftTsFile = new File(tmpDir, "time-join-left.tsfile");
    try (TsFileWriter writer = new TsFileWriter(leftTsFile)) {
      generateTable(writer, "table1", Arrays.asList("left_tag"), Arrays.asList("left_value"), 1, 3);
    }
    File rightTsFile = new File(tmpDir, "time-join-right.tsfile");
    try (TsFileWriter writer = new TsFileWriter(rightTsFile)) {
      generateTable(
          writer, "table1", Arrays.asList("right_tag"), Arrays.asList("right_value"), 2, 4);
    }

    String[] expectedHeader =
        new String[] {"time", "left_tag", "left_value", "right_tag", "right_value"};
    String[] retArray =
        new String[] {
          "1970-01-01T00:00:00.002Z,left_tag_1,2,right_tag_1,2,",
          "1970-01-01T00:00:00.002Z,left_tag_1,2,right_tag_2,2,",
          "1970-01-01T00:00:00.002Z,left_tag_2,2,right_tag_1,2,",
          "1970-01-01T00:00:00.002Z,left_tag_2,2,right_tag_2,2,",
          "1970-01-01T00:00:00.003Z,left_tag_1,3,right_tag_1,3,",
          "1970-01-01T00:00:00.003Z,left_tag_1,3,right_tag_2,3,",
          "1970-01-01T00:00:00.003Z,left_tag_2,3,right_tag_1,3,",
          "1970-01-01T00:00:00.003Z,left_tag_2,3,right_tag_2,3,",
        };
    tableResultSetEqualTest(
        "SELECT l.time, l.left_tag, l.left_value, r.right_tag, r.right_value"
            + " FROM read_tsfile(PATHS => '"
            + toSqlPath(leftTsFile)
            + "', TABLE_NAME => 'table1') l"
            + " JOIN read_tsfile(PATHS => '"
            + toSqlPath(rightTsFile)
            + "', TABLE_NAME => 'table1') r ON l.time = r.time"
            + " ORDER BY l.time, l.left_tag, r.right_tag",
        expectedHeader,
        retArray,
        DATABASE_NAME);
  }

  @Test
  public void testReadMultipleTsFilesWithTagSchemaMerge() throws Exception {
    File tsFile1 = new File(tmpDir, "tag-schema-merge-1.tsfile");
    try (TsFileWriter writer = new TsFileWriter(tsFile1)) {
      generateTable(writer, "table1", Arrays.asList("tag1"), Arrays.asList("s1"), 1, 2);
    }
    File tsFile2 = new File(tmpDir, "tag-schema-merge-2.tsfile");
    try (TsFileWriter writer = new TsFileWriter(tsFile2)) {
      generateTable(writer, "table1", Arrays.asList("tag1", "tag2"), Arrays.asList("s1"), 3, 4);
    }

    String[] expectedHeader = new String[] {"time", "tag1", "tag2", "s1"};
    String[] retArray =
        new String[] {
          "1970-01-01T00:00:00.001Z,tag1_1,null,1,",
          "1970-01-01T00:00:00.002Z,tag1_1,null,2,",
          "1970-01-01T00:00:00.003Z,tag1_1,tag2_1,3,",
          "1970-01-01T00:00:00.004Z,tag1_1,tag2_1,4,",
          "1970-01-01T00:00:00.001Z,tag1_2,null,1,",
          "1970-01-01T00:00:00.002Z,tag1_2,null,2,",
          "1970-01-01T00:00:00.003Z,tag1_2,tag2_2,3,",
          "1970-01-01T00:00:00.004Z,tag1_2,tag2_2,4,",
        };
    tableResultSetEqualTest(
        "SELECT time, tag1, tag2, s1 FROM read_tsfile(PATHS => '"
            + toSqlPath(tsFile1)
            + ","
            + toSqlPath(tsFile2)
            + "', TABLE_NAME => 'table1') ORDER BY tag1, time",
        expectedHeader,
        retArray,
        DATABASE_NAME);
  }

  @Test
  public void testReadMultipleTsFilesWithConflictingFieldType() throws Exception {
    File tsFile1 = new File(tmpDir, "conflict-1.tsfile");
    try (TsFileWriter writer = new TsFileWriter(tsFile1)) {
      generateTable(writer, "table1", Arrays.asList("tag1"), Arrays.asList("s1"), 1, 2);
    }
    File tsFile2 = new File(tmpDir, "conflict-2.tsfile");
    try (TsFileWriter writer = new TsFileWriter(tsFile2)) {
      generateTable(
          writer, "table1", Arrays.asList("tag1"), Arrays.asList("s1"), TSDataType.DOUBLE, 3, 4);
    }

    tableAssertTestFail(
        "SELECT * FROM read_tsfile(PATHS => '"
            + toSqlPath(tsFile1)
            + ","
            + toSqlPath(tsFile2)
            + "', TABLE_NAME => 'table1')",
        "has conflicting data types when merging table schema",
        DATABASE_NAME);
  }

  @Test
  public void testReadMultipleTsFilesWithConflictingTagColumns() throws Exception {
    File tsFile1 = new File(tmpDir, "tag-conflict-1.tsfile");
    try (TsFileWriter writer = new TsFileWriter(tsFile1)) {
      generateTable(writer, "table1", Arrays.asList("tag1"), Arrays.asList("s1"), 1, 2);
    }
    File tsFile2 = new File(tmpDir, "tag-conflict-2.tsfile");
    try (TsFileWriter writer = new TsFileWriter(tsFile2)) {
      generateTable(writer, "table1", Arrays.asList("tag2"), Arrays.asList("s1"), 3, 4);
    }

    tableAssertTestFail(
        "SELECT * FROM read_tsfile(PATHS => '"
            + toSqlPath(tsFile1)
            + ","
            + toSqlPath(tsFile2)
            + "', TABLE_NAME => 'table1')",
        "Tag columns conflict when merging table schema",
        DATABASE_NAME);
  }

  @Test
  public void testReadMultipleTsFilesWithConflictingTagAndFieldColumns() throws Exception {
    File tsFile1 = new File(tmpDir, "tag-field-conflict-1.tsfile");
    try (TsFileWriter writer = new TsFileWriter(tsFile1)) {
      registerTableSchema(writer, "table1", Arrays.asList("shared"), Arrays.asList("s1"));
    }
    File tsFile2 = new File(tmpDir, "tag-field-conflict-2.tsfile");
    try (TsFileWriter writer = new TsFileWriter(tsFile2)) {
      registerTableSchema(writer, "table1", new ArrayList<>(), Arrays.asList("shared"));
    }

    tableAssertTestFail(
        "SELECT * FROM read_tsfile(PATHS => '"
            + toSqlPath(tsFile1)
            + ","
            + toSqlPath(tsFile2)
            + "', TABLE_NAME => 'table1')",
        "conflicting categories when merging table schema",
        DATABASE_NAME);
  }

  @Test
  public void testReadMultipleTsFilesWithConflictingFieldAndTagColumns() throws Exception {
    File tsFile1 = new File(tmpDir, "field-tag-conflict-1.tsfile");
    try (TsFileWriter writer = new TsFileWriter(tsFile1)) {
      registerTableSchema(writer, "table1", new ArrayList<>(), Arrays.asList("shared"));
    }
    File tsFile2 = new File(tmpDir, "field-tag-conflict-2.tsfile");
    try (TsFileWriter writer = new TsFileWriter(tsFile2)) {
      registerTableSchema(writer, "table1", Arrays.asList("shared"), Arrays.asList("s1"));
    }

    tableAssertTestFail(
        "SELECT * FROM read_tsfile(PATHS => '"
            + toSqlPath(tsFile1)
            + ","
            + toSqlPath(tsFile2)
            + "', TABLE_NAME => 'table1')",
        "conflicting categories when merging table schema",
        DATABASE_NAME);
  }

  @Test
  public void testReadTsFileWithoutTableNameWhenMultipleTablesExist() throws Exception {
    File tsFile = new File(tmpDir, "multiple-tables.tsfile");
    try (TsFileWriter writer = new TsFileWriter(tsFile)) {
      generateTable(writer, "table1", Arrays.asList("tag1"), Arrays.asList("s1"), 1, 2);
      generateTable(writer, "table2", Arrays.asList("tag1"), Arrays.asList("s1"), 1, 2);
    }

    tableAssertTestFail(
        "SELECT * FROM read_tsfile(PATHS => '" + toSqlPath(tsFile) + "')",
        "Cannot infer table name from TsFile because multiple tables are found",
        DATABASE_NAME);
  }

  @Test
  public void testReadSpecifiedInvalidFileFails() throws IOException {
    File invalidFile = new File(tmpDir, "invalid-file.txt");
    Files.write(invalidFile.toPath(), new byte[] {1, 2, 3});

    tableAssertTestFail(
        "SELECT * FROM read_tsfile(PATHS => '" + toSqlPath(invalidFile) + "')",
        "not a valid TsFile",
        DATABASE_NAME);
  }

  @Test
  public void testReadDirectoryOnlyScansValidTsFileSuffixFiles() throws Exception {
    File scanDir = new File(tmpDir, "scan-dir");
    File nestedDir = new File(scanDir, "nested");
    Files.createDirectories(nestedDir.toPath());

    File validTsFile = new File(nestedDir, "valid.tsfile");
    try (TsFileWriter writer = new TsFileWriter(validTsFile)) {
      generateTable(writer, "table1", Arrays.asList("tag1"), Arrays.asList("s1"), 1, 2);
    }

    File invalidTsFile = new File(scanDir, "invalid.tsfile");
    Files.write(invalidTsFile.toPath(), new byte[] {1, 2, 3});

    File validFileWithoutTsFileSuffix = new File(scanDir, "valid.data");
    try (TsFileWriter writer = new TsFileWriter(validFileWithoutTsFileSuffix)) {
      generateTable(writer, "table1", Arrays.asList("tag1"), Arrays.asList("s1"), 3, 4);
    }

    String[] expectedHeader = new String[] {"time", "tag1", "s1"};
    String[] retArray =
        new String[] {
          "1970-01-01T00:00:00.001Z,tag1_1,1,",
          "1970-01-01T00:00:00.002Z,tag1_1,2,",
          "1970-01-01T00:00:00.001Z,tag1_2,1,",
          "1970-01-01T00:00:00.002Z,tag1_2,2,",
        };
    tableResultSetEqualTest(
        "SELECT time, tag1, s1 FROM read_tsfile(PATHS => '"
            + toSqlPath(scanDir)
            + "', TABLE_NAME => 'table1') ORDER BY tag1, time",
        expectedHeader,
        retArray,
        DATABASE_NAME);
  }

  @Test
  public void testReadTsFileWithInvalidPaths() throws IOException {
    File missingFile = new File(tmpDir, "missing.tsfile");
    tableAssertTestFail(
        "SELECT * FROM read_tsfile(PATHS => '" + toSqlPath(missingFile) + "')",
        "TsFile path does not exist",
        DATABASE_NAME);

    DataNodeWrapper dataNodeWrapper = EnvFactory.getEnv().getDataNodeWrapper(0);
    File dataDir =
        new File(dataNodeWrapper.getDataNodeDir() + File.separator + "data", "forbidden.tsfile");
    Files.createDirectories(dataDir.getParentFile().toPath());
    Files.write(dataDir.toPath(), new byte[0]);
    try (Connection connection =
            EnvFactory.getEnv()
                .getWriteOnlyConnectionWithSpecifiedDataNode(
                    dataNodeWrapper, BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      statement.execute("USE " + DATABASE_NAME);
      statement.execute("SELECT * FROM read_tsfile(PATHS => '" + toSqlPath(dataDir) + "')");
      fail("Expected read_tsfile to reject paths under the current DataNode data directory");
    } catch (SQLException e) {
      assertTrue(
          e.getMessage(),
          e.getMessage().contains("is not allowed because it may access IoTDB data directory"));
    }
  }

  private static void generateTable(
      TsFileWriter writer,
      String tableName,
      List<String> tagColumns,
      List<String> fieldColumns,
      long startTime,
      long endTime)
      throws IOException, WriteProcessException {
    generateTable(
        writer, tableName, tagColumns, fieldColumns, TSDataType.INT64, startTime, endTime);
  }

  private static void generateTable(
      TsFileWriter writer,
      String tableName,
      List<String> tagColumns,
      List<String> fieldColumns,
      TSDataType fieldType,
      long startTime,
      long endTime)
      throws IOException, WriteProcessException {
    List<String> columnNames = new ArrayList<>(tagColumns.size() + fieldColumns.size());
    List<TSDataType> columnTypes = new ArrayList<>(tagColumns.size() + fieldColumns.size());
    List<ColumnCategory> columnCategories =
        new ArrayList<>(tagColumns.size() + fieldColumns.size());
    for (String tagColumn : tagColumns) {
      columnNames.add(tagColumn);
      columnTypes.add(TSDataType.STRING);
      columnCategories.add(ColumnCategory.TAG);
    }
    for (String fieldColumn : fieldColumns) {
      columnNames.add(fieldColumn);
      columnTypes.add(fieldType);
      columnCategories.add(ColumnCategory.FIELD);
    }

    writer.registerTableSchema(
        new TableSchema(tableName, columnNames, columnTypes, columnCategories));
    Tablet tablet = new Tablet(tableName, columnNames, columnTypes, columnCategories);
    for (int deviceIndex = 1; deviceIndex <= 2; deviceIndex++) {
      for (long time = startTime; time <= endTime; time++) {
        int row = tablet.getRowSize();
        tablet.addTimestamp(row, time);
        for (int i = 0; i < tagColumns.size(); i++) {
          tablet.addValue(row, i, tagColumns.get(i) + "_" + deviceIndex);
        }
        for (int i = 0; i < fieldColumns.size(); i++) {
          addFieldValue(tablet, row, tagColumns.size() + i, fieldType, time);
        }
        if (tablet.getRowSize() == tablet.getMaxRowNumber()) {
          writer.writeTable(tablet);
          tablet.reset();
        }
      }
    }
    if (tablet.getRowSize() != 0) {
      writer.writeTable(tablet);
    }
  }

  private static void registerTableSchema(
      TsFileWriter writer, String tableName, List<String> tagColumns, List<String> fieldColumns)
      throws IOException {
    List<String> columnNames = new ArrayList<>(tagColumns.size() + fieldColumns.size());
    List<TSDataType> columnTypes = new ArrayList<>(tagColumns.size() + fieldColumns.size());
    List<ColumnCategory> columnCategories =
        new ArrayList<>(tagColumns.size() + fieldColumns.size());
    for (String tagColumn : tagColumns) {
      columnNames.add(tagColumn);
      columnTypes.add(TSDataType.STRING);
      columnCategories.add(ColumnCategory.TAG);
    }
    for (String fieldColumn : fieldColumns) {
      columnNames.add(fieldColumn);
      columnTypes.add(TSDataType.INT64);
      columnCategories.add(ColumnCategory.FIELD);
    }

    writer.registerTableSchema(
        new TableSchema(tableName, columnNames, columnTypes, columnCategories));
  }

  private static void addFieldValue(
      Tablet tablet, int row, int column, TSDataType fieldType, long time) {
    if (fieldType == TSDataType.DOUBLE) {
      tablet.addValue(row, column, (double) time);
      return;
    }
    tablet.addValue(row, column, time);
  }

  private static String toSqlPath(File file) {
    return file.getAbsolutePath().replace("\\", "\\\\").replace("'", "''");
  }

  private static void clearTmpDir() {
    if (tmpDir == null || !tmpDir.exists()) {
      return;
    }
    File[] files = tmpDir.listFiles();
    if (files != null) {
      for (File file : files) {
        deleteRecursively(file);
      }
    }
  }

  private static void deleteRecursively(File file) {
    if (file.isDirectory()) {
      File[] children = file.listFiles();
      if (children != null) {
        for (File child : children) {
          deleteRecursively(child);
        }
      }
    }
    try {
      Files.delete(file.toPath());
    } catch (IOException ignored) {
      // ignore
    }
  }

  private static void deleteTmpDir() {
    clearTmpDir();
    if (tmpDir == null || !tmpDir.exists()) {
      return;
    }
    try {
      Files.delete(tmpDir.toPath());
    } catch (IOException ignored) {
      // ignore
    }
  }
}
