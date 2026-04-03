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

package org.apache.iotdb.relational.it.query.recent.copyto;

import org.apache.iotdb.isession.ITableSession;
import org.apache.iotdb.isession.SessionDataSet;
import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.TableClusterIT;
import org.apache.iotdb.itbase.category.TableLocalStandaloneIT;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;

import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.file.metadata.StringArrayDeviceID;
import org.apache.tsfile.file.metadata.TimeseriesMetadata;
import org.apache.tsfile.read.TsFileSequenceReader;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.List;
import java.util.Map;

import static org.apache.iotdb.db.it.utils.TestUtils.prepareTableData;

@RunWith(IoTDBTestRunner.class)
@Category({TableLocalStandaloneIT.class, TableClusterIT.class})
public class IoTDBCopyToTsFileIT {

  private static final Logger LOGGER = LoggerFactory.getLogger(IoTDBCopyToTsFileIT.class);
  private static final String DATABASE_NAME = "test_db";

  protected static final String[] createSqls =
      new String[] {
        "CREATE DATABASE " + DATABASE_NAME,
        "USE " + DATABASE_NAME,
        "create table table1(tag1 string tag, tag2 string tag, s1 int32 field, s2 int32 field)",
        "insert into table1(time, tag1, tag2, s1, s2) values (1, 't1_1', 't2', 1, 1)",
        "insert into table1(time, tag1, tag2, s1, s2) values (2, 't1_1', 't2', 2, 2)",
        "insert into table1(time, tag1, tag2, s1, s2) values (3, 't1_1', 't2', 3, 3)",
        "insert into table1(time, tag1, tag2, s1, s2) values (1, 't1_2', 't2', 1, 1)",
        "insert into table1(time, tag1, tag2, s1, s2) values (2, 't1_2', 't2', 2, 2)",
        "insert into table1(time, tag1, tag2, s1, s2) values (3, 't1_2', 't2', 3, 3)",
        "create table table2(tag1 string tag, tag2 string tag, s1 int32 field, s2 int32 field)",
        "insert into table2(time, tag1, tag2, s1, s2) values (1, 't1_1', 't2', 1, 1)",
      };

  private String targetFilePath = null;

  @BeforeClass
  public static void setUp() throws Exception {
    EnvFactory.getEnv().initClusterEnvironment();
    prepareTableData(createSqls);
  }

  @After
  public void tearDownAfterTest() {
    if (targetFilePath != null) {
      try {
        Files.deleteIfExists(new File(targetFilePath).toPath());
      } catch (Exception e) {
        LOGGER.error("Failed to delete {}", targetFilePath, e);
      }
      targetFilePath = null;
    }
  }

  @AfterClass
  public static void tearDown() throws Exception {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  @Test
  public void testCopyTable()
      throws IoTDBConnectionException, StatementExecutionException, IOException {
    try (ITableSession session =
        EnvFactory.getEnv().getTableSessionConnectionWithDB(DATABASE_NAME)) {
      SessionDataSet sessionDataSet =
          session.executeQueryStatement("copy table1 to '1.tsfile' (memory_threshold 1000000)");
      SessionDataSet.DataIterator iterator = sessionDataSet.iterator();
      while (iterator.next()) {
        targetFilePath = iterator.getString(1);
        long rowCount = iterator.getLong(2);
        long deviceCount = iterator.getLong(3);
        long sizeInBytes = iterator.getLong(4);
        String tableName = iterator.getString(5);
        String timeColumn = iterator.getString(6);
        String tagColumns = iterator.getString(7);

        Assert.assertTrue(new File(targetFilePath).exists());
        Assert.assertEquals(6, rowCount);
        Assert.assertEquals(2, deviceCount);
        Assert.assertTrue(sizeInBytes > 0);
        Assert.assertEquals("table1", tableName);
        Assert.assertEquals("time", timeColumn);
        Assert.assertEquals("[tag1, tag2]", tagColumns);

        try (TsFileSequenceReader reader = new TsFileSequenceReader(targetFilePath)) {
          Map<IDeviceID, List<TimeseriesMetadata>> allTimeseriesMetadata =
              reader.getAllTimeseriesMetadata(false);
          Assert.assertEquals(2, allTimeseriesMetadata.size());
          List<TimeseriesMetadata> timeseriesMetadataList =
              allTimeseriesMetadata.get(new StringArrayDeviceID("table1", "t1_1", "t2"));
          Assert.assertEquals(3, timeseriesMetadataList.size());
          Assert.assertEquals(1, timeseriesMetadataList.get(0).getStatistics().getStartTime());
          Assert.assertEquals(3, timeseriesMetadataList.get(0).getStatistics().getEndTime());
          timeseriesMetadataList =
              allTimeseriesMetadata.get(new StringArrayDeviceID("table1", "t1_2", "t2"));
          Assert.assertEquals(3, timeseriesMetadataList.size());
          Assert.assertEquals(1, timeseriesMetadataList.get(0).getStatistics().getStartTime());
          Assert.assertEquals(3, timeseriesMetadataList.get(0).getStatistics().getEndTime());
        }
      }
    }
  }

  @Test
  public void testCopySelectAllColumns()
      throws IoTDBConnectionException, StatementExecutionException, IOException {
    try (ITableSession session =
        EnvFactory.getEnv().getTableSessionConnectionWithDB(DATABASE_NAME)) {
      SessionDataSet sessionDataSet =
          session.executeQueryStatement(
              "copy (select * from table1) to '2.tsfile' (memory_threshold 1000000)");
      SessionDataSet.DataIterator iterator = sessionDataSet.iterator();
      while (iterator.next()) {
        targetFilePath = iterator.getString(1);
        long rowCount = iterator.getLong(2);
        long deviceCount = iterator.getLong(3);
        long sizeInBytes = iterator.getLong(4);
        String tableName = iterator.getString(5);
        String timeColumn = iterator.getString(6);
        String tagColumns = iterator.getString(7);

        Assert.assertTrue(new File(targetFilePath).exists());
        Assert.assertEquals(6, rowCount);
        Assert.assertEquals(2, deviceCount);
        Assert.assertTrue(sizeInBytes > 0);
        Assert.assertEquals("table1", tableName);
        Assert.assertEquals("time", timeColumn);
        Assert.assertEquals("[tag1, tag2]", tagColumns);

        try (TsFileSequenceReader reader = new TsFileSequenceReader(targetFilePath)) {
          Map<IDeviceID, List<TimeseriesMetadata>> allTimeseriesMetadata =
              reader.getAllTimeseriesMetadata(false);
          Assert.assertEquals(2, allTimeseriesMetadata.size());
          List<TimeseriesMetadata> timeseriesMetadataList =
              allTimeseriesMetadata.get(new StringArrayDeviceID("table1", "t1_1", "t2"));
          Assert.assertEquals(3, timeseriesMetadataList.size());
          Assert.assertEquals(1, timeseriesMetadataList.get(0).getStatistics().getStartTime());
          Assert.assertEquals(3, timeseriesMetadataList.get(0).getStatistics().getEndTime());
          timeseriesMetadataList =
              allTimeseriesMetadata.get(new StringArrayDeviceID("table1", "t1_2", "t2"));
          Assert.assertEquals(3, timeseriesMetadataList.size());
          Assert.assertEquals(1, timeseriesMetadataList.get(0).getStatistics().getStartTime());
          Assert.assertEquals(3, timeseriesMetadataList.get(0).getStatistics().getEndTime());
        }
      }
    }
  }

  @Test
  public void testCopySelectSpecifiedColumns()
      throws IoTDBConnectionException, StatementExecutionException, IOException {
    try (ITableSession session =
        EnvFactory.getEnv().getTableSessionConnectionWithDB(DATABASE_NAME)) {
      SessionDataSet sessionDataSet =
          session.executeQueryStatement(
              "copy table1(time,tag2,tag1,s1) to '3.tsfile' (memory_threshold 1000000)");
      SessionDataSet.DataIterator iterator = sessionDataSet.iterator();
      while (iterator.next()) {
        targetFilePath = iterator.getString(1);
        long rowCount = iterator.getLong(2);
        long deviceCount = iterator.getLong(3);
        long sizeInBytes = iterator.getLong(4);
        String tableName = iterator.getString(5);
        String timeColumn = iterator.getString(6);
        String tagColumns = iterator.getString(7);

        Assert.assertTrue(new File(targetFilePath).exists());
        Assert.assertEquals(6, rowCount);
        Assert.assertEquals(2, deviceCount);
        Assert.assertTrue(sizeInBytes > 0);
        Assert.assertEquals("table1", tableName);
        Assert.assertEquals("time", timeColumn);
        Assert.assertEquals("[tag1, tag2]", tagColumns);

        try (TsFileSequenceReader reader = new TsFileSequenceReader(targetFilePath)) {
          Map<IDeviceID, List<TimeseriesMetadata>> allTimeseriesMetadata =
              reader.getAllTimeseriesMetadata(false);
          Assert.assertEquals(2, allTimeseriesMetadata.size());
          List<TimeseriesMetadata> timeseriesMetadataList =
              allTimeseriesMetadata.get(new StringArrayDeviceID("table1", "t1_1", "t2"));
          Assert.assertEquals(2, timeseriesMetadataList.size());
          Assert.assertEquals(1, timeseriesMetadataList.get(0).getStatistics().getStartTime());
          Assert.assertEquals(3, timeseriesMetadataList.get(0).getStatistics().getEndTime());
          timeseriesMetadataList =
              allTimeseriesMetadata.get(new StringArrayDeviceID("table1", "t1_2", "t2"));
          Assert.assertEquals(2, timeseriesMetadataList.size());
          Assert.assertEquals(1, timeseriesMetadataList.get(0).getStatistics().getStartTime());
          Assert.assertEquals(3, timeseriesMetadataList.get(0).getStatistics().getEndTime());
        }
      }
    }
  }

  @Test
  public void testCopySelectSpecifiedColumns2()
      throws IoTDBConnectionException, StatementExecutionException, IOException {
    try (ITableSession session =
        EnvFactory.getEnv().getTableSessionConnectionWithDB(DATABASE_NAME)) {
      SessionDataSet sessionDataSet =
          session.executeQueryStatement(
              "copy (select time, tag2, s1, tag1, s2 from table1) to '4.tsfile' (memory_threshold 1000000)");
      SessionDataSet.DataIterator iterator = sessionDataSet.iterator();
      while (iterator.next()) {
        targetFilePath = iterator.getString(1);
        long rowCount = iterator.getLong(2);
        long deviceCount = iterator.getLong(3);
        long sizeInBytes = iterator.getLong(4);
        String tableName = iterator.getString(5);
        String timeColumn = iterator.getString(6);
        String tagColumns = iterator.getString(7);

        Assert.assertTrue(new File(targetFilePath).exists());
        Assert.assertEquals(6, rowCount);
        Assert.assertEquals(2, deviceCount);
        Assert.assertTrue(sizeInBytes > 0);
        Assert.assertEquals("table1", tableName);
        Assert.assertEquals("time", timeColumn);
        Assert.assertEquals("[tag1, tag2]", tagColumns);

        try (TsFileSequenceReader reader = new TsFileSequenceReader(targetFilePath)) {
          Map<IDeviceID, List<TimeseriesMetadata>> allTimeseriesMetadata =
              reader.getAllTimeseriesMetadata(false);
          Assert.assertEquals(2, allTimeseriesMetadata.size());
          List<TimeseriesMetadata> timeseriesMetadataList =
              allTimeseriesMetadata.get(new StringArrayDeviceID("table1", "t1_1", "t2"));
          Assert.assertEquals(3, timeseriesMetadataList.size());
          Assert.assertEquals(1, timeseriesMetadataList.get(0).getStatistics().getStartTime());
          Assert.assertEquals(3, timeseriesMetadataList.get(0).getStatistics().getEndTime());
          timeseriesMetadataList =
              allTimeseriesMetadata.get(new StringArrayDeviceID("table1", "t1_2", "t2"));
          Assert.assertEquals(3, timeseriesMetadataList.size());
          Assert.assertEquals(1, timeseriesMetadataList.get(0).getStatistics().getStartTime());
          Assert.assertEquals(3, timeseriesMetadataList.get(0).getStatistics().getEndTime());
        }
      }
    }
  }

  @Test
  public void testCopyWithSpecifiedTag()
      throws IoTDBConnectionException, StatementExecutionException, IOException {
    try (ITableSession session =
        EnvFactory.getEnv().getTableSessionConnectionWithDB(DATABASE_NAME)) {
      SessionDataSet sessionDataSet =
          session.executeQueryStatement(
              "copy table1(time,tag1,tag2,s1) to '5.tsfile' with (tags(tag2,tag1), memory_threshold 1000000)");
      SessionDataSet.DataIterator iterator = sessionDataSet.iterator();
      while (iterator.next()) {
        targetFilePath = iterator.getString(1);
        long rowCount = iterator.getLong(2);
        long deviceCount = iterator.getLong(3);
        long sizeInBytes = iterator.getLong(4);
        String tableName = iterator.getString(5);
        String timeColumn = iterator.getString(6);
        String tagColumns = iterator.getString(7);

        Assert.assertTrue(new File(targetFilePath).exists());
        Assert.assertEquals(6, rowCount);
        Assert.assertEquals(2, deviceCount);
        Assert.assertTrue(sizeInBytes > 0);
        Assert.assertEquals("table1", tableName);
        Assert.assertEquals("time", timeColumn);
        Assert.assertEquals("[tag2, tag1]", tagColumns);

        try (TsFileSequenceReader reader = new TsFileSequenceReader(targetFilePath)) {
          Map<IDeviceID, List<TimeseriesMetadata>> allTimeseriesMetadata =
              reader.getAllTimeseriesMetadata(false);
          Assert.assertEquals(2, allTimeseriesMetadata.size());
          List<TimeseriesMetadata> timeseriesMetadataList =
              allTimeseriesMetadata.get(new StringArrayDeviceID("table1", "t2", "t1_1"));
          Assert.assertEquals(2, timeseriesMetadataList.size());
          Assert.assertEquals(1, timeseriesMetadataList.get(0).getStatistics().getStartTime());
          Assert.assertEquals(3, timeseriesMetadataList.get(0).getStatistics().getEndTime());
          timeseriesMetadataList =
              allTimeseriesMetadata.get(new StringArrayDeviceID("table1", "t2", "t1_2"));
          Assert.assertEquals(2, timeseriesMetadataList.size());
          Assert.assertEquals(1, timeseriesMetadataList.get(0).getStatistics().getStartTime());
          Assert.assertEquals(3, timeseriesMetadataList.get(0).getStatistics().getEndTime());
        }
      }
    }
  }

  @Test
  public void testCopyWithSpecifiedTagAndTime()
      throws IoTDBConnectionException, StatementExecutionException, IOException {
    try (ITableSession session =
        EnvFactory.getEnv().getTableSessionConnectionWithDB(DATABASE_NAME)) {
      SessionDataSet sessionDataSet =
          session.executeQueryStatement(
              "copy (select time as t, s1, tag2 as renamed_tag2, tag1 as renamed_tag1 from table1) to '6.tsfile' with (TIME t, tags(renamed_tag1,renamed_tag2),  memory_threshold 1000000)");
      SessionDataSet.DataIterator iterator = sessionDataSet.iterator();
      while (iterator.next()) {
        targetFilePath = iterator.getString(1);
        long rowCount = iterator.getLong(2);
        long deviceCount = iterator.getLong(3);
        long sizeInBytes = iterator.getLong(4);
        String tableName = iterator.getString(5);
        String timeColumn = iterator.getString(6);
        String tagColumns = iterator.getString(7);

        Assert.assertTrue(new File(targetFilePath).exists());
        Assert.assertEquals(6, rowCount);
        Assert.assertEquals(2, deviceCount);
        Assert.assertTrue(sizeInBytes > 0);
        Assert.assertEquals("table1", tableName);
        Assert.assertEquals("t", timeColumn);
        Assert.assertEquals("[renamed_tag1, renamed_tag2]", tagColumns);

        try (TsFileSequenceReader reader = new TsFileSequenceReader(targetFilePath)) {
          Map<IDeviceID, List<TimeseriesMetadata>> allTimeseriesMetadata =
              reader.getAllTimeseriesMetadata(false);
          Assert.assertEquals(2, allTimeseriesMetadata.size());
          List<TimeseriesMetadata> timeseriesMetadataList =
              allTimeseriesMetadata.get(new StringArrayDeviceID("table1", "t1_1", "t2"));
          Assert.assertEquals(2, timeseriesMetadataList.size());
          Assert.assertEquals(1, timeseriesMetadataList.get(0).getStatistics().getStartTime());
          Assert.assertEquals(3, timeseriesMetadataList.get(0).getStatistics().getEndTime());
          timeseriesMetadataList =
              allTimeseriesMetadata.get(new StringArrayDeviceID("table1", "t1_2", "t2"));
          Assert.assertEquals(2, timeseriesMetadataList.size());
          Assert.assertEquals(1, timeseriesMetadataList.get(0).getStatistics().getStartTime());
          Assert.assertEquals(3, timeseriesMetadataList.get(0).getStatistics().getEndTime());
        }
      }
    }
  }

  @Test
  public void testAutoGenerateTimeColumn()
      throws IoTDBConnectionException, StatementExecutionException, IOException {
    try (ITableSession session =
        EnvFactory.getEnv().getTableSessionConnectionWithDB(DATABASE_NAME)) {
      SessionDataSet sessionDataSet =
          session.executeQueryStatement(
              "copy (select time as t, tag1, tag2, s1, s2 from table1) to '7.tsfile' (memory_threshold 1000000)");
      SessionDataSet.DataIterator iterator = sessionDataSet.iterator();
      while (iterator.next()) {
        targetFilePath = iterator.getString(1);
        long rowCount = iterator.getLong(2);
        long deviceCount = iterator.getLong(3);
        long sizeInBytes = iterator.getLong(4);
        String tableName = iterator.getString(5);
        String timeColumn = iterator.getString(6);
        String tagColumns = iterator.getString(7);

        Assert.assertTrue(new File(targetFilePath).exists());
        Assert.assertEquals(6, rowCount);
        Assert.assertEquals(2, deviceCount);
        Assert.assertTrue(sizeInBytes > 0);
        Assert.assertEquals("table1", tableName);
        Assert.assertEquals("time(auto_gen)", timeColumn);
        Assert.assertEquals("[tag1, tag2]", tagColumns);

        try (TsFileSequenceReader reader = new TsFileSequenceReader(targetFilePath)) {
          Map<IDeviceID, List<TimeseriesMetadata>> allTimeseriesMetadata =
              reader.getAllTimeseriesMetadata(false);
          Assert.assertEquals(2, allTimeseriesMetadata.size());
          List<TimeseriesMetadata> timeseriesMetadataList =
              allTimeseriesMetadata.get(new StringArrayDeviceID("table1", "t1_1", "t2"));
          Assert.assertEquals(4, timeseriesMetadataList.size());
          Assert.assertEquals(0, timeseriesMetadataList.get(0).getStatistics().getStartTime());
          Assert.assertEquals(2, timeseriesMetadataList.get(0).getStatistics().getEndTime());
          timeseriesMetadataList =
              allTimeseriesMetadata.get(new StringArrayDeviceID("table1", "t1_2", "t2"));
          Assert.assertEquals(4, timeseriesMetadataList.size());
          Assert.assertEquals(0, timeseriesMetadataList.get(0).getStatistics().getStartTime());
          Assert.assertEquals(2, timeseriesMetadataList.get(0).getStatistics().getEndTime());
        }
      }
    }
  }

  @Test
  public void testAutoGenerateTimeColumnWithoutTag()
      throws IoTDBConnectionException, StatementExecutionException, IOException {
    try (ITableSession session =
        EnvFactory.getEnv().getTableSessionConnectionWithDB(DATABASE_NAME)) {
      SessionDataSet sessionDataSet =
          session.executeQueryStatement(
              "copy (select time as t, tag1 as tag1_field, tag2 as tag2_field, s1, s2 from table1) to '8.tsfile' (memory_threshold 1000000)");
      SessionDataSet.DataIterator iterator = sessionDataSet.iterator();
      while (iterator.next()) {
        targetFilePath = iterator.getString(1);
        long rowCount = iterator.getLong(2);
        long deviceCount = iterator.getLong(3);
        long sizeInBytes = iterator.getLong(4);
        String tableName = iterator.getString(5);
        String timeColumn = iterator.getString(6);
        String tagColumns = iterator.getString(7);

        Assert.assertTrue(new File(targetFilePath).exists());
        Assert.assertEquals(6, rowCount);
        Assert.assertEquals(1, deviceCount);
        Assert.assertTrue(sizeInBytes > 0);
        Assert.assertEquals("table1", tableName);
        Assert.assertEquals("time(auto_gen)", timeColumn);
        Assert.assertEquals("[]", tagColumns);

        try (TsFileSequenceReader reader = new TsFileSequenceReader(targetFilePath)) {
          Map<IDeviceID, List<TimeseriesMetadata>> allTimeseriesMetadata =
              reader.getAllTimeseriesMetadata(false);
          Assert.assertEquals(1, allTimeseriesMetadata.size());
          List<TimeseriesMetadata> timeseriesMetadataList =
              allTimeseriesMetadata.get(new StringArrayDeviceID("table1"));
          Assert.assertEquals(6, timeseriesMetadataList.size());
          Assert.assertEquals(0, timeseriesMetadataList.get(0).getStatistics().getStartTime());
          Assert.assertEquals(5, timeseriesMetadataList.get(0).getStatistics().getEndTime());
        }
      }
    }
  }

  @Test
  public void testMultiTable()
      throws IoTDBConnectionException, StatementExecutionException, IOException {
    try (ITableSession session =
        EnvFactory.getEnv().getTableSessionConnectionWithDB(DATABASE_NAME)) {

      SessionDataSet sessionDataSet =
          session.executeQueryStatement(
              "copy (select table1.time as time1, table1.tag1 as tag1_1, table1.tag2 as tag2_1, table1.s1 as s1_1, table1.s2 as s2_1, table2.s1 as s1_2, table2.s2 as s2_2 from table1 inner join table2 on table1.time = table2.time) to '9.tsfile' (memory_threshold 1000000)");
      SessionDataSet.DataIterator iterator = sessionDataSet.iterator();
      while (iterator.next()) {
        targetFilePath = iterator.getString(1);
        long rowCount = iterator.getLong(2);
        long deviceCount = iterator.getLong(3);
        long sizeInBytes = iterator.getLong(4);
        String tableName = iterator.getString(5);
        String timeColumn = iterator.getString(6);
        String tagColumns = iterator.getString(7);

        Assert.assertTrue(new File(targetFilePath).exists());
        Assert.assertEquals(2, rowCount);
        Assert.assertEquals(1, deviceCount);
        Assert.assertTrue(sizeInBytes > 0);
        Assert.assertEquals("default", tableName);
        Assert.assertEquals("time(auto_gen)", timeColumn);
        Assert.assertEquals("[]", tagColumns);

        try (TsFileSequenceReader reader = new TsFileSequenceReader(targetFilePath)) {
          Map<IDeviceID, List<TimeseriesMetadata>> allTimeseriesMetadata =
              reader.getAllTimeseriesMetadata(false);
          Assert.assertEquals(1, allTimeseriesMetadata.size());
          List<TimeseriesMetadata> timeseriesMetadataList =
              allTimeseriesMetadata.get(new StringArrayDeviceID("default"));
          Assert.assertEquals(8, timeseriesMetadataList.size());
          Assert.assertEquals(0, timeseriesMetadataList.get(0).getStatistics().getStartTime());
          Assert.assertEquals(1, timeseriesMetadataList.get(0).getStatistics().getEndTime());
        }
      }
    }
  }

  @Test
  public void testMultiTable2()
      throws IoTDBConnectionException, StatementExecutionException, IOException {
    try (ITableSession session =
        EnvFactory.getEnv().getTableSessionConnectionWithDB(DATABASE_NAME)) {

      SessionDataSet sessionDataSet =
          session.executeQueryStatement(
              "copy (select table1.time as time1, table1.tag1 as tag1_1, table1.tag2 as tag2_1, table1.s1 as s1_1, table1.s2 as s2_1, table2.s1 as s1_2, table2.s2 as s2_2, table2.time time2 from table1 inner join table2 on table1.time = table2.time) to '10.tsfile' (memory_threshold 1000000)");
      SessionDataSet.DataIterator iterator = sessionDataSet.iterator();
      while (iterator.next()) {
        targetFilePath = iterator.getString(1);
        long rowCount = iterator.getLong(2);
        long deviceCount = iterator.getLong(3);
        long sizeInBytes = iterator.getLong(4);
        String tableName = iterator.getString(5);
        String timeColumn = iterator.getString(6);
        String tagColumns = iterator.getString(7);

        Assert.assertTrue(new File(targetFilePath).exists());
        Assert.assertEquals(2, rowCount);
        Assert.assertEquals(1, deviceCount);
        Assert.assertTrue(sizeInBytes > 0);
        Assert.assertEquals("default", tableName);
        Assert.assertEquals("time(auto_gen)", timeColumn);
        Assert.assertEquals("[]", tagColumns);

        try (TsFileSequenceReader reader = new TsFileSequenceReader(targetFilePath)) {
          Map<IDeviceID, List<TimeseriesMetadata>> allTimeseriesMetadata =
              reader.getAllTimeseriesMetadata(false);
          Assert.assertEquals(1, allTimeseriesMetadata.size());
          List<TimeseriesMetadata> timeseriesMetadataList =
              allTimeseriesMetadata.get(new StringArrayDeviceID("default"));
          Assert.assertEquals(9, timeseriesMetadataList.size());
          Assert.assertEquals(0, timeseriesMetadataList.get(0).getStatistics().getStartTime());
          Assert.assertEquals(1, timeseriesMetadataList.get(0).getStatistics().getEndTime());
        }
      }
    }
  }

  @Test
  public void testUseSameColumn()
      throws IoTDBConnectionException, StatementExecutionException, IOException {
    try (ITableSession session =
        EnvFactory.getEnv().getTableSessionConnectionWithDB(DATABASE_NAME)) {

      SessionDataSet sessionDataSet =
          session.executeQueryStatement(
              "copy (select time, tag2, tag1, s1, s1 as s2 from table1) to '11.tsfile' (tags (tag1,tag2), memory_threshold 1000000)");
      SessionDataSet.DataIterator iterator = sessionDataSet.iterator();
      while (iterator.next()) {
        targetFilePath = iterator.getString(1);
        long rowCount = iterator.getLong(2);
        long deviceCount = iterator.getLong(3);
        long sizeInBytes = iterator.getLong(4);
        String tableName = iterator.getString(5);
        String timeColumn = iterator.getString(6);
        String tagColumns = iterator.getString(7);

        Assert.assertTrue(new File(targetFilePath).exists());
        Assert.assertEquals(6, rowCount);
        Assert.assertEquals(2, deviceCount);
        Assert.assertTrue(sizeInBytes > 0);
        Assert.assertEquals("table1", tableName);
        Assert.assertEquals("time", timeColumn);
        Assert.assertEquals("[tag1, tag2]", tagColumns);

        try (TsFileSequenceReader reader = new TsFileSequenceReader(targetFilePath)) {
          Map<IDeviceID, List<TimeseriesMetadata>> allTimeseriesMetadata =
              reader.getAllTimeseriesMetadata(false);
          Assert.assertEquals(2, allTimeseriesMetadata.size());
          List<TimeseriesMetadata> timeseriesMetadataList =
              allTimeseriesMetadata.get(new StringArrayDeviceID("table1", "t1_1", "t2"));
          Assert.assertEquals(3, timeseriesMetadataList.size());
          Assert.assertEquals(1, timeseriesMetadataList.get(0).getStatistics().getStartTime());
          Assert.assertEquals(3, timeseriesMetadataList.get(0).getStatistics().getEndTime());
          timeseriesMetadataList =
              allTimeseriesMetadata.get(new StringArrayDeviceID("table1", "t1_2", "t2"));
          Assert.assertEquals(3, timeseriesMetadataList.size());
          Assert.assertEquals(1, timeseriesMetadataList.get(0).getStatistics().getStartTime());
          Assert.assertEquals(3, timeseriesMetadataList.get(0).getStatistics().getEndTime());
        }
      }
    }
  }

  @Test
  public void testSpecifiedDatabaseTable()
      throws IoTDBConnectionException, StatementExecutionException, IOException {
    try (ITableSession session =
        EnvFactory.getEnv().getTableSessionConnectionWithDB(DATABASE_NAME)) {

      SessionDataSet sessionDataSet =
          session.executeQueryStatement(
              "copy test_db.table1 to '12.tsfile' (MEMORY_THRESHOLD 1000000)");
      SessionDataSet.DataIterator iterator = sessionDataSet.iterator();
      while (iterator.next()) {
        targetFilePath = iterator.getString(1);
        long rowCount = iterator.getLong(2);
        long deviceCount = iterator.getLong(3);
        long sizeInBytes = iterator.getLong(4);
        String tableName = iterator.getString(5);
        String timeColumn = iterator.getString(6);
        String tagColumns = iterator.getString(7);

        Assert.assertTrue(new File(targetFilePath).exists());
        Assert.assertEquals(6, rowCount);
        Assert.assertEquals(2, deviceCount);
        Assert.assertTrue(sizeInBytes > 0);
        Assert.assertEquals("table1", tableName);
        Assert.assertEquals("time", timeColumn);
        Assert.assertEquals("[tag1, tag2]", tagColumns);

        try (TsFileSequenceReader reader = new TsFileSequenceReader(targetFilePath)) {
          Map<IDeviceID, List<TimeseriesMetadata>> allTimeseriesMetadata =
              reader.getAllTimeseriesMetadata(false);
          Assert.assertEquals(2, allTimeseriesMetadata.size());
          List<TimeseriesMetadata> timeseriesMetadataList =
              allTimeseriesMetadata.get(new StringArrayDeviceID("table1", "t1_1", "t2"));
          Assert.assertEquals(3, timeseriesMetadataList.size());
          Assert.assertEquals(1, timeseriesMetadataList.get(0).getStatistics().getStartTime());
          Assert.assertEquals(3, timeseriesMetadataList.get(0).getStatistics().getEndTime());
          timeseriesMetadataList =
              allTimeseriesMetadata.get(new StringArrayDeviceID("table1", "t1_2", "t2"));
          Assert.assertEquals(3, timeseriesMetadataList.size());
          Assert.assertEquals(1, timeseriesMetadataList.get(0).getStatistics().getStartTime());
          Assert.assertEquals(3, timeseriesMetadataList.get(0).getStatistics().getEndTime());
        }
      }
    }
  }

  @Test
  public void testFindTimeColumn()
      throws IoTDBConnectionException, StatementExecutionException, IOException {
    try (ITableSession session =
        EnvFactory.getEnv().getTableSessionConnectionWithDB(DATABASE_NAME)) {

      SessionDataSet sessionDataSet =
          session.executeQueryStatement(
              "copy (select time, table1.s1 as s1_1, table2.s1 as s1_2 from table1 join table2 using(time) limit 1) to '13.tsfile' (MEMORY_THRESHOLD 1000000)");
      SessionDataSet.DataIterator iterator = sessionDataSet.iterator();
      while (iterator.next()) {
        targetFilePath = iterator.getString(1);
        long rowCount = iterator.getLong(2);
        long deviceCount = iterator.getLong(3);
        long sizeInBytes = iterator.getLong(4);
        String tableName = iterator.getString(5);
        String timeColumn = iterator.getString(6);
        String tagColumns = iterator.getString(7);

        Assert.assertTrue(new File(targetFilePath).exists());
        Assert.assertEquals(1, rowCount);
        Assert.assertEquals(1, deviceCount);
        Assert.assertTrue(sizeInBytes > 0);
        Assert.assertEquals("default", tableName);
        Assert.assertEquals("time", timeColumn);
        Assert.assertEquals("[]", tagColumns);

        try (TsFileSequenceReader reader = new TsFileSequenceReader(targetFilePath)) {
          Map<IDeviceID, List<TimeseriesMetadata>> allTimeseriesMetadata =
              reader.getAllTimeseriesMetadata(false);
          Assert.assertEquals(1, allTimeseriesMetadata.size());
          List<TimeseriesMetadata> timeseriesMetadataList =
              allTimeseriesMetadata.get(new StringArrayDeviceID("default"));
          Assert.assertEquals(3, timeseriesMetadataList.size());
          Assert.assertEquals(1, timeseriesMetadataList.get(0).getStatistics().getStartTime());
          Assert.assertEquals(1, timeseriesMetadataList.get(0).getStatistics().getEndTime());
        }
      }
    }
  }
}
