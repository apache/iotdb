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
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.sql.Connection;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.apache.iotdb.db.it.utils.TestUtils.prepareTableData;
import static org.apache.iotdb.db.it.utils.TestUtils.tableResultSetEqualTest;

@RunWith(IoTDBTestRunner.class)
@Category({TableLocalStandaloneIT.class, TableClusterIT.class})
public class IoTDBTopKRuntimeFilterScanPathIT {

  private static final String TREE_VIEW_DATABASE = "db_topk_rf_view";
  private static final String TABLE_DATABASE = "db_topk_rf_table";
  private static final String READ_TSFILE_DATABASE = "db_topk_rf_read_tsfile";

  private static File tmpDir;

  @BeforeClass
  public static void setUp() throws Exception {
    EnvFactory.getEnv().initClusterEnvironment();
    tmpDir = new File(Files.createTempDirectory("topk-rf-scan-path").toUri());
    insertTreeModelData();
    createTreeView();
    createTableModelDatabase();
    createReadTsFileDatabase();
  }

  @AfterClass
  public static void tearDown() throws Exception {
    if (tmpDir != null) {
      deleteDirectory(tmpDir);
    }
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  @Test
  public void orderByTimeLimitOnTreeView() {
    String[] expectedHeader = new String[] {"time", "device_id", "s1"};
    String[] retArray =
        new String[] {
          "1970-01-01T00:00:00.032Z,d3,333,", "1970-01-01T00:00:00.031Z,d2,222,",
        };
    tableResultSetEqualTest(
        "SELECT * FROM table1 ORDER BY time DESC LIMIT 2",
        expectedHeader,
        retArray,
        TREE_VIEW_DATABASE);
  }

  @Test
  public void orderByTimeAscLimitOnTreeView() {
    String[] expectedHeader = new String[] {"time", "device_id", "s1"};
    String[] retArray =
        new String[] {
          "1970-01-01T00:00:00.010Z,d1,1,", "1970-01-01T00:00:00.011Z,d2,2,",
        };
    tableResultSetEqualTest(
        "SELECT * FROM table1 ORDER BY time ASC LIMIT 2",
        expectedHeader,
        retArray,
        TREE_VIEW_DATABASE);
  }

  @Test
  public void orderByTimeDescLimitOnTable() {
    String[] expectedHeader = new String[] {"time", "device_id", "s1"};
    String[] retArray =
        new String[] {
          "1970-01-01T00:00:00.032Z,d3,333,", "1970-01-01T00:00:00.031Z,d2,222,",
        };
    tableResultSetEqualTest(
        "SELECT * FROM table1 ORDER BY time DESC LIMIT 2",
        expectedHeader,
        retArray,
        TABLE_DATABASE);
  }

  @Test
  public void orderByTimeAscLimitOnTable() {
    String[] expectedHeader = new String[] {"time", "device_id", "s1"};
    String[] retArray =
        new String[] {
          "1970-01-01T00:00:00.010Z,d1,1,", "1970-01-01T00:00:00.011Z,d2,2,",
        };
    tableResultSetEqualTest(
        "SELECT * FROM table1 ORDER BY time ASC LIMIT 2", expectedHeader, retArray, TABLE_DATABASE);
  }

  @Test
  public void unionAllSiblingTopKOrderByTimeLimit() {
    String[] expectedHeader = new String[] {"time", "device_id", "s1"};
    String[] retArray =
        new String[] {
          "1970-01-01T00:00:00.030Z,d1,111,", "1970-01-01T00:00:00.032Z,d3,333,",
        };
    tableResultSetEqualTest(
        "(SELECT time, device_id, s1 FROM table1 WHERE device_id = 'd1' ORDER BY time DESC LIMIT 1)"
            + " UNION ALL"
            + " (SELECT time, device_id, s1 FROM table1 WHERE device_id = 'd3' ORDER BY time DESC LIMIT 1)",
        expectedHeader,
        retArray,
        TABLE_DATABASE);
  }

  @Test
  public void orderByTimeAscLimitOnReadTsFile() throws Exception {
    File tsFile = new File(tmpDir, "topk-rf-asc.tsfile");
    try (TsFileWriter writer = new TsFileWriter(tsFile)) {
      generateTableWithDistinctDeviceTimes(
          writer, "table1", Arrays.asList("tag1", "tag2"), Arrays.asList("s1", "s2"), 1, 3);
    }

    String[] expectedHeader = new String[] {"time", "tag1", "tag2", "s1", "s2"};
    String[] retArray =
        new String[] {
          "1970-01-01T00:00:00.001Z,tag1_1,tag2_1,1,1,",
          "1970-01-01T00:00:00.002Z,tag1_2,tag2_2,2,2,",
        };
    tableResultSetEqualTest(
        "SELECT time, tag1, tag2, s1, s2 FROM read_tsfile(PATHS => '"
            + toSqlPath(tsFile)
            + "', TABLE_NAME => 'table1') ORDER BY time ASC LIMIT 2",
        expectedHeader,
        retArray,
        READ_TSFILE_DATABASE);
  }

  @Test
  public void orderByTimeLimitOnReadTsFile() throws Exception {
    File tsFile = new File(tmpDir, "topk-rf.tsfile");
    try (TsFileWriter writer = new TsFileWriter(tsFile)) {
      generateTableWithDistinctDeviceTimes(
          writer, "table1", Arrays.asList("tag1", "tag2"), Arrays.asList("s1", "s2"), 1, 3);
    }

    String[] expectedHeader = new String[] {"time", "tag1", "tag2", "s1", "s2"};
    String[] retArray =
        new String[] {
          "1970-01-01T00:00:00.003Z,tag1_3,tag2_3,3,3,",
          "1970-01-01T00:00:00.002Z,tag1_2,tag2_2,2,2,",
        };
    tableResultSetEqualTest(
        "SELECT time, tag1, tag2, s1, s2 FROM read_tsfile(PATHS => '"
            + toSqlPath(tsFile)
            + "', TABLE_NAME => 'table1') ORDER BY time DESC LIMIT 2",
        expectedHeader,
        retArray,
        READ_TSFILE_DATABASE);
  }

  private static void insertTreeModelData() throws Exception {
    try (Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TREE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      statement.execute("CREATE DATABASE root.db_topk_rf");
      statement.execute(
          "CREATE TIMESERIES root.db_topk_rf.d1.s1 WITH DATATYPE=INT32, ENCODING=RLE");
      statement.execute(
          "CREATE TIMESERIES root.db_topk_rf.d2.s1 WITH DATATYPE=INT32, ENCODING=RLE");
      statement.execute(
          "CREATE TIMESERIES root.db_topk_rf.d3.s1 WITH DATATYPE=INT32, ENCODING=RLE");
      statement.execute(
          "INSERT INTO root.db_topk_rf.d1(timestamp,s1) VALUES(10, 1), (20, 11), (30, 111)");
      statement.execute(
          "INSERT INTO root.db_topk_rf.d2(timestamp,s1) VALUES(11, 2), (21, 22), (31, 222)");
      statement.execute(
          "INSERT INTO root.db_topk_rf.d3(timestamp,s1) VALUES(12, 3), (22, 33), (32, 333)");
    }
  }

  private static void createTreeView() throws Exception {
    try (Connection connection = EnvFactory.getEnv().getTableConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("CREATE DATABASE " + TREE_VIEW_DATABASE);
      statement.execute("USE " + TREE_VIEW_DATABASE);
      statement.execute(
          "CREATE VIEW table1(device_id STRING TAG, s1 INT32 FIELD) AS root.db_topk_rf.**");
    }
  }

  private static void createTableModelDatabase() throws Exception {
    prepareTableData(
        new String[] {
          "CREATE DATABASE " + TABLE_DATABASE,
          "USE " + TABLE_DATABASE,
          "CREATE TABLE table1(device_id STRING TAG, s1 INT32 FIELD)",
          "INSERT INTO table1(time, device_id, s1) VALUES (10, 'd1', 1), (20, 'd1', 11), (30, 'd1', 111)",
          "INSERT INTO table1(time, device_id, s1) VALUES (11, 'd2', 2), (21, 'd2', 22), (31, 'd2', 222)",
          "INSERT INTO table1(time, device_id, s1) VALUES (12, 'd3', 3), (22, 'd3', 33), (32, 'd3', 333)",
        });
  }

  private static void createReadTsFileDatabase() throws Exception {
    try (Connection connection = EnvFactory.getEnv().getTableConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("CREATE DATABASE " + READ_TSFILE_DATABASE);
    }
  }

  private static void generateTableWithDistinctDeviceTimes(
      TsFileWriter writer,
      String tableName,
      List<String> tagColumns,
      List<String> fieldColumns,
      int deviceStart,
      int deviceEnd)
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
      columnTypes.add(TSDataType.INT32);
      columnCategories.add(ColumnCategory.FIELD);
    }

    writer.registerTableSchema(
        new TableSchema(tableName, columnNames, columnTypes, columnCategories));
    Tablet tablet = new Tablet(tableName, columnNames, columnTypes, columnCategories);
    for (int deviceIndex = deviceStart; deviceIndex <= deviceEnd; deviceIndex++) {
      int time = deviceIndex;
      int row = tablet.getRowSize();
      tablet.addTimestamp(row, time);
      for (int i = 0; i < tagColumns.size(); i++) {
        tablet.addValue(row, i, tagColumns.get(i) + "_" + deviceIndex);
      }
      for (int i = 0; i < fieldColumns.size(); i++) {
        tablet.addValue(row, tagColumns.size() + i, time);
      }
      if (tablet.getRowSize() == tablet.getMaxRowNumber()) {
        writer.writeTable(tablet);
        tablet.reset();
      }
    }
    if (tablet.getRowSize() != 0) {
      writer.writeTable(tablet);
    }
  }

  private static void generateTable(
      TsFileWriter writer,
      String tableName,
      List<String> tagColumns,
      List<String> fieldColumns,
      int deviceStart,
      int deviceEnd)
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
      columnTypes.add(TSDataType.INT32);
      columnCategories.add(ColumnCategory.FIELD);
    }

    writer.registerTableSchema(
        new TableSchema(tableName, columnNames, columnTypes, columnCategories));
    Tablet tablet = new Tablet(tableName, columnNames, columnTypes, columnCategories);
    for (int deviceIndex = deviceStart; deviceIndex <= deviceEnd; deviceIndex++) {
      for (int time = 1; time <= 3; time++) {
        int row = tablet.getRowSize();
        tablet.addTimestamp(row, time);
        for (int i = 0; i < tagColumns.size(); i++) {
          tablet.addValue(row, i, tagColumns.get(i) + "_" + deviceIndex);
        }
        for (int i = 0; i < fieldColumns.size(); i++) {
          tablet.addValue(row, tagColumns.size() + i, time);
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

  private static String toSqlPath(File file) {
    return file.getAbsolutePath().replace("\\", "\\\\").replace("'", "''");
  }

  private static void deleteDirectory(File directory) {
    File[] files = directory.listFiles();
    if (files != null) {
      for (File file : files) {
        if (file.isDirectory()) {
          deleteDirectory(file);
        } else {
          file.delete();
        }
      }
    }
    directory.delete();
  }
}
