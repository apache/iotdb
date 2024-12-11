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
package org.apache.iotdb.session.it;

import org.apache.iotdb.isession.ISession;
import org.apache.iotdb.isession.ITableSession;
import org.apache.iotdb.isession.SessionDataSet;
import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.ClusterIT;
import org.apache.iotdb.itbase.category.LocalStandaloneIT;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.RowRecord;
import org.apache.tsfile.write.record.Tablet;
import org.apache.tsfile.write.record.Tablet.ColumnCategory;
import org.apache.tsfile.write.schema.IMeasurementSchema;
import org.apache.tsfile.write.schema.MeasurementSchema;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.fail;

@RunWith(IoTDBTestRunner.class)
public class IoTDBSessionRelationalIT {

  @BeforeClass
  public static void classSetUp() throws Exception {
    EnvFactory.getEnv().initClusterEnvironment();
  }

  @Before
  public void setUp() throws Exception {
    try (ITableSession session = EnvFactory.getEnv().getTableSessionConnection()) {
      session.executeNonQueryStatement("CREATE DATABASE IF NOT EXISTS db1");
      session.executeNonQueryStatement("CREATE DATABASE IF NOT EXISTS db2");
    }
  }

  @After
  public void tearDown() throws Exception {
    try (ITableSession session = EnvFactory.getEnv().getTableSessionConnection()) {
      session.executeNonQueryStatement("DROP DATABASE IF EXISTS db1");
    }
  }

  @AfterClass
  public static void classTearDown() {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  // for manual debugging
  public static void main(String[] args)
      throws IoTDBConnectionException, StatementExecutionException {
    try (ITableSession session = EnvFactory.getEnv().getTableSessionConnection()) {
      session.executeNonQueryStatement("CREATE DATABASE \"db1\"");
      session.executeNonQueryStatement("CREATE DATABASE \"db2\"");
      session.executeNonQueryStatement("USE \"db1\"");
      session.executeNonQueryStatement(
          "CREATE TABLE table10 (id1 string id, attr1 string attribute, "
              + "m1 double "
              + "measurement)");
    }
    // insert without db
    try (ITableSession session = EnvFactory.getEnv().getTableSessionConnection()) {
      long timestamp;

      // no db in session and sql
      assertThrows(
          StatementExecutionException.class,
          () ->
              session.executeNonQueryStatement(
                  String.format(
                      "INSERT INTO table10 (time, id1, attr1, m1) VALUES (%d, '%s', '%s', %f)",
                      0, "id:" + 0, "attr:" + 0, 0 * 1.0)));

      // specify db in sql
      for (long row = 0; row < 15; row++) {
        session.executeNonQueryStatement(
            String.format(
                "INSERT INTO db1.table10 (time, id1, attr1, m1) VALUES (%d, '%s', '%s', %f)",
                row, "id:" + row, "attr:" + row, row * 1.0));
      }

      session.executeNonQueryStatement("FLush");

      for (long row = 15; row < 30; row++) {
        session.executeNonQueryStatement(
            String.format(
                "INSERT INTO db1.table10 (time, id1, attr1, m1) VALUES (%d, '%s', '%s', %f)",
                row, "id:" + row, "attr:" + row, row * 1.0));
      }

      SessionDataSet dataSet =
          session.executeQueryStatement("select * from db1.table10 order by time");
      int cnt = 0;
      while (dataSet.hasNext()) {
        RowRecord rowRecord = dataSet.next();
        timestamp = rowRecord.getFields().get(0).getLongV();
        assertEquals("id:" + timestamp, rowRecord.getFields().get(1).getBinaryV().toString());
        assertEquals("attr:" + timestamp, rowRecord.getFields().get(2).getBinaryV().toString());
        assertEquals(timestamp * 1.0, rowRecord.getFields().get(3).getDoubleV(), 0.0001);
        cnt++;
      }
      assertEquals(30, cnt);
    }
  }

  private static void insertRelationalTabletPerformanceTest()
      throws IoTDBConnectionException, StatementExecutionException {
    try (ITableSession session = EnvFactory.getEnv().getTableSessionConnection()) {
      session.executeNonQueryStatement("USE \"db1\"");
      session.executeNonQueryStatement(
          "CREATE TABLE table1 (id1 string id, attr1 string attribute, "
              + "m1 double "
              + "measurement)");

      List<IMeasurementSchema> schemaList = new ArrayList<>();
      schemaList.add(new MeasurementSchema("id1", TSDataType.STRING));
      schemaList.add(new MeasurementSchema("attr1", TSDataType.STRING));
      schemaList.add(new MeasurementSchema("m1", TSDataType.DOUBLE));
      final List<ColumnCategory> columnTypes =
          Arrays.asList(ColumnCategory.ID, ColumnCategory.ATTRIBUTE, ColumnCategory.MEASUREMENT);

      long timestamp = 0;
      Tablet tablet =
          new Tablet(
              "table1",
              IMeasurementSchema.getMeasurementNameList(schemaList),
              IMeasurementSchema.getDataTypeList(schemaList),
              columnTypes,
              15);

      for (long row = 0; row < 15; row++) {
        int rowIndex = tablet.getRowSize();
        tablet.addTimestamp(rowIndex, timestamp + row);
        tablet.addValue("id1", rowIndex, "id:" + row);
        tablet.addValue("attr1", rowIndex, "attr:" + row);
        tablet.addValue("m1", rowIndex, row * 1.0);
        if (tablet.getRowSize() == tablet.getMaxRowNumber()) {
          session.insert(tablet);
          tablet.reset();
        }
      }

      if (tablet.getRowSize() != 0) {
        session.insert(tablet);
        tablet.reset();
      }

      session.executeNonQueryStatement("FLush");

      for (long row = 15; row < 30; row++) {
        int rowIndex = tablet.getRowSize();
        tablet.addTimestamp(rowIndex, timestamp + row);
        tablet.addValue("id1", rowIndex, "id:" + row);
        tablet.addValue("attr1", rowIndex, "attr:" + row);
        tablet.addValue("m1", rowIndex, row * 1.0);
        if (tablet.getRowSize() == tablet.getMaxRowNumber()) {
          session.insert(tablet);
          tablet.reset();
        }
      }

      if (tablet.getRowSize() != 0) {
        session.insert(tablet);
        tablet.reset();
      }

      SessionDataSet dataSet = session.executeQueryStatement("select * from table1 order by time");
      while (dataSet.hasNext()) {
        RowRecord rowRecord = dataSet.next();
        timestamp = rowRecord.getFields().get(0).getLongV();
        assertEquals("id:" + timestamp, rowRecord.getFields().get(1).getBinaryV().toString());
        assertEquals("attr:" + timestamp, rowRecord.getFields().get(2).getBinaryV().toString());
        assertEquals(timestamp * 1.0, rowRecord.getFields().get(3).getDoubleV(), 0.0001);
      }
    }
  }

  @Test
  @Category({LocalStandaloneIT.class, ClusterIT.class})
  public void insertRelationalSqlTest()
      throws IoTDBConnectionException, StatementExecutionException {
    try (ITableSession session = EnvFactory.getEnv().getTableSessionConnection()) {
      session.executeNonQueryStatement("USE \"db1\"");
      session.executeNonQueryStatement(
          "CREATE TABLE table1 (id1 string id, attr1 string attribute, "
              + "m1 double "
              + "measurement)");

      long timestamp;

      for (long row = 0; row < 15; row++) {
        session.executeNonQueryStatement(
            String.format(
                "INSERT INTO table1 (time, id1, attr1, m1) VALUES (%d, '%s', '%s', %f)",
                row, "id:" + row, "attr:" + row, row * 1.0));
      }

      session.executeNonQueryStatement("FLush");

      for (long row = 15; row < 30; row++) {
        session.executeNonQueryStatement(
            String.format(
                "INSERT INTO table1 (time, id1, attr1, m1) VALUES (%d, '%s', '%s', %f)",
                row, "id:" + row, "attr:" + row, row * 1.0));
      }

      // without specifying column name
      for (long row = 30; row < 40; row++) {
        session.executeNonQueryStatement(
            String.format(
                "INSERT INTO table1 VALUES (%d, '%s', '%s', %f)",
                row, "id:" + row, "attr:" + row, row * 1.0));
      }

      // auto data type conversion
      for (long row = 40; row < 50; row++) {
        session.executeNonQueryStatement(
            String.format(
                "INSERT INTO table1 VALUES (%d, '%s', '%s', %d)",
                row, "id:" + row, "attr:" + row, row));
      }

      SessionDataSet dataSet = session.executeQueryStatement("select * from table1 order by time");
      int cnt = 0;
      while (dataSet.hasNext()) {
        RowRecord rowRecord = dataSet.next();
        timestamp = rowRecord.getFields().get(0).getLongV();
        assertEquals("id:" + timestamp, rowRecord.getFields().get(1).getBinaryV().toString());
        assertEquals("attr:" + timestamp, rowRecord.getFields().get(2).getBinaryV().toString());
        assertEquals(timestamp * 1.0, rowRecord.getFields().get(3).getDoubleV(), 0.0001);
        cnt++;
      }
      assertEquals(50, cnt);

      // sql cannot create column
      assertThrows(
          StatementExecutionException.class,
          () ->
              session.executeNonQueryStatement(
                  String.format(
                      "INSERT INTO table1 (id1, id2, attr1, m1) VALUES ('%s', '%s', '%s', %f)",
                      "id:" + 100, "id:" + 100, "attr:" + 100, 100 * 1.0)));

      // fewer columns than defined
      assertThrows(
          StatementExecutionException.class,
          () ->
              session.executeNonQueryStatement(
                  String.format(
                      "INSERT INTO table1 VALUES ( '%s', %f)", "attr:" + 100, 100 * 1.0)));

      // more columns than defined
      assertThrows(
          StatementExecutionException.class,
          () ->
              session.executeNonQueryStatement(
                  String.format(
                      "INSERT INTO table1 VALUES ('%s', '%s', '%s', '%s', %f)",
                      "id:" + 100, "id:" + 100, "id:" + 100, "attr:" + 100, 100 * 1.0)));

      // invalid conversion - id column
      assertThrows(
          StatementExecutionException.class,
          () ->
              session.executeNonQueryStatement(
                  String.format(
                      "INSERT INTO table1 VALUES ('%d', '%s', '%s', %f)",
                      100, 100, "attr:" + 100, 100 * 1.0)));

      // invalid conversion - attr column
      assertThrows(
          StatementExecutionException.class,
          () ->
              session.executeNonQueryStatement(
                  String.format(
                      "INSERT INTO table1 VALUES ('%d', '%s', '%s', %f)",
                      100, "id:" + 100, 100, 100 * 1.0)));

      // invalid conversion - measurement column
      assertThrows(
          StatementExecutionException.class,
          () ->
              session.executeNonQueryStatement(
                  String.format(
                      "INSERT INTO table1 VALUES ('%d', '%s', '%s', %s)",
                      100, "id:" + 100, "attr:" + 100, "measurement" + (100 * 1.0))));
    }
  }

  @Test
  @Category({LocalStandaloneIT.class, ClusterIT.class})
  public void partialInsertSQLTest() throws IoTDBConnectionException, StatementExecutionException {
    try (ISession session = EnvFactory.getEnv().getSessionConnection()) {
      // disable auto-creation only for this test
      session.executeNonQueryStatement("SET CONFIGURATION \"enable_auto_create_schema\"=\"false\"");
    }
    try (ITableSession session = EnvFactory.getEnv().getTableSessionConnection()) {
      session.executeNonQueryStatement("USE \"db1\"");
      // the table is missing column "m2"
      session.executeNonQueryStatement(
          "CREATE TABLE table2_2 (id1 string id, attr1 string attribute, "
              + "m1 double "
              + "measurement)");
      try {
        session.executeNonQueryStatement(
            "INSERT INTO table2_2 (time, id1, attr1, m1, m2) values (1, '1', '1', 1.0, 2.0)");
        fail("Exception expected");
      } catch (StatementExecutionException e) {
        assertEquals(
            "616: Unknown column category for m2. Cannot auto create column.", e.getMessage());
      }

    } finally {
      try (ISession session = EnvFactory.getEnv().getSessionConnection()) {
        session.executeNonQueryStatement(
            "SET CONFIGURATION \"enable_auto_create_schema\"=\"true\"");
      }
    }
  }

  @Test
  @Category({LocalStandaloneIT.class, ClusterIT.class})
  public void partialInsertRelationalTabletTest()
      throws IoTDBConnectionException, StatementExecutionException {
    try (ISession session = EnvFactory.getEnv().getSessionConnection()) {
      // disable auto-creation only for this test
      session.executeNonQueryStatement("SET CONFIGURATION \"enable_auto_create_schema\"=\"false\"");
    }
    try (ITableSession session = EnvFactory.getEnv().getTableSessionConnection()) {
      session.executeNonQueryStatement("USE \"db1\"");
      // the table is missing column "m2"
      session.executeNonQueryStatement(
          "CREATE TABLE table4 (id1 string id, attr1 string attribute, "
              + "m1 double "
              + "measurement)");

      // the insertion contains "m2"
      List<IMeasurementSchema> schemaList = new ArrayList<>();
      schemaList.add(new MeasurementSchema("id1", TSDataType.STRING));
      schemaList.add(new MeasurementSchema("attr1", TSDataType.STRING));
      schemaList.add(new MeasurementSchema("m1", TSDataType.DOUBLE));
      schemaList.add(new MeasurementSchema("m2", TSDataType.DOUBLE));
      final List<ColumnCategory> columnTypes =
          Arrays.asList(
              ColumnCategory.ID,
              ColumnCategory.ATTRIBUTE,
              ColumnCategory.MEASUREMENT,
              ColumnCategory.MEASUREMENT);

      long timestamp = 0;
      Tablet tablet =
          new Tablet(
              "table4",
              IMeasurementSchema.getMeasurementNameList(schemaList),
              IMeasurementSchema.getDataTypeList(schemaList),
              columnTypes,
              15);

      for (long row = 0; row < 15; row++) {
        int rowIndex = tablet.getRowSize();
        tablet.addTimestamp(rowIndex, timestamp + row);
        tablet.addValue("id1", rowIndex, "id:" + row);
        tablet.addValue("attr1", rowIndex, "attr:" + row);
        tablet.addValue("m1", rowIndex, row * 1.0);
        tablet.addValue("m2", rowIndex, row * 1.0);
        if (tablet.getRowSize() == tablet.getMaxRowNumber()) {
          try {
            session.insert(tablet);
          } catch (StatementExecutionException e) {
            // a partial insertion should be reported
            if (!e.getMessage()
                .equals(
                    "507: Fail to insert measurements [m2] caused by [Column m2 does not exists or fails to be created]")) {
              throw e;
            }
          }
          tablet.reset();
        }
      }

      if (tablet.getRowSize() != 0) {
        try {
          session.insert(tablet);
        } catch (StatementExecutionException e) {
          if (!e.getMessage()
              .equals(
                  "507: Fail to insert measurements [m2] caused by [Column m2 does not exists or fails to be created]")) {
            throw e;
          }
        }
        tablet.reset();
      }

      session.executeNonQueryStatement("FLush");

      for (long row = 15; row < 30; row++) {
        int rowIndex = tablet.getRowSize();
        tablet.addTimestamp(rowIndex, timestamp + row);
        tablet.addValue("id1", rowIndex, "id:" + row);
        tablet.addValue("attr1", rowIndex, "attr:" + row);
        tablet.addValue("m1", rowIndex, row * 1.0);
        tablet.addValue("m2", rowIndex, row * 1.0);
        if (tablet.getRowSize() == tablet.getMaxRowNumber()) {
          try {
            session.insert(tablet);
          } catch (StatementExecutionException e) {
            if (!e.getMessage()
                .equals(
                    "507: Fail to insert measurements [m2] caused by [Column m2 does not exists or fails to be created]")) {
              throw e;
            }
          }
          tablet.reset();
        }
      }

      if (tablet.getRowSize() != 0) {
        try {
          session.insert(tablet);
        } catch (StatementExecutionException e) {
          if (!e.getMessage()
              .equals(
                  "507: Fail to insert measurements [m2] caused by [Column m2 does not exists or fails to be created]")) {
            throw e;
          }
        }
        tablet.reset();
      }

      SessionDataSet dataSet = session.executeQueryStatement("select * from table4 order by time");
      int cnt = 0;
      while (dataSet.hasNext()) {
        RowRecord rowRecord = dataSet.next();
        timestamp = rowRecord.getFields().get(0).getLongV();
        assertEquals("id:" + timestamp, rowRecord.getFields().get(1).getBinaryV().toString());
        assertEquals("attr:" + timestamp, rowRecord.getFields().get(2).getBinaryV().toString());
        assertEquals(timestamp * 1.0, rowRecord.getFields().get(3).getDoubleV(), 0.0001);
        // "m2" should not be present
        assertEquals(4, rowRecord.getFields().size());
        cnt++;
      }
      assertEquals(30, cnt);
    } finally {
      try (ISession session = EnvFactory.getEnv().getSessionConnection()) {
        session.executeNonQueryStatement(
            "SET CONFIGURATION \"enable_auto_create_schema\"=\"true\"");
      }
    }
  }

  @Test
  @Category({LocalStandaloneIT.class, ClusterIT.class})
  public void insertRelationalTabletTest()
      throws IoTDBConnectionException, StatementExecutionException {
    try (ITableSession session = EnvFactory.getEnv().getTableSessionConnection()) {
      session.executeNonQueryStatement("USE \"db1\"");
      session.executeNonQueryStatement(
          "CREATE TABLE table5 (id1 string id, attr1 string attribute, "
              + "m1 double "
              + "measurement)");

      List<IMeasurementSchema> schemaList = new ArrayList<>();
      schemaList.add(new MeasurementSchema("id1", TSDataType.STRING));
      schemaList.add(new MeasurementSchema("attr1", TSDataType.STRING));
      schemaList.add(new MeasurementSchema("m1", TSDataType.DOUBLE));
      final List<ColumnCategory> columnTypes =
          Arrays.asList(ColumnCategory.ID, ColumnCategory.ATTRIBUTE, ColumnCategory.MEASUREMENT);

      long timestamp = 0;
      Tablet tablet =
          new Tablet(
              "table5",
              IMeasurementSchema.getMeasurementNameList(schemaList),
              IMeasurementSchema.getDataTypeList(schemaList),
              columnTypes,
              15);

      for (long row = 0; row < 15; row++) {
        int rowIndex = tablet.getRowSize();
        tablet.addTimestamp(rowIndex, timestamp + row);
        tablet.addValue("id1", rowIndex, "id:" + row);
        tablet.addValue("attr1", rowIndex, "attr:" + row);
        tablet.addValue("m1", rowIndex, row * 1.0);
        if (tablet.getRowSize() == tablet.getMaxRowNumber()) {
          session.insert(tablet);
          tablet.reset();
        }
      }

      if (tablet.getRowSize() != 0) {
        session.insert(tablet);
        tablet.reset();
      }

      session.executeNonQueryStatement("FLush");

      for (long row = 15; row < 30; row++) {
        int rowIndex = tablet.getRowSize();
        tablet.addTimestamp(rowIndex, timestamp + row);
        tablet.addValue("id1", rowIndex, "id:" + row);
        tablet.addValue("attr1", rowIndex, "attr:" + row);
        tablet.addValue("m1", rowIndex, row * 1.0);
        if (tablet.getRowSize() == tablet.getMaxRowNumber()) {
          session.insert(tablet);
          tablet.reset();
        }
      }

      if (tablet.getRowSize() != 0) {
        session.insert(tablet);
        tablet.reset();
      }

      int cnt = 0;
      SessionDataSet dataSet = session.executeQueryStatement("select * from table5 order by time");
      while (dataSet.hasNext()) {
        RowRecord rowRecord = dataSet.next();
        timestamp = rowRecord.getFields().get(0).getLongV();
        assertEquals("id:" + timestamp, rowRecord.getFields().get(1).getBinaryV().toString());
        assertEquals("attr:" + timestamp, rowRecord.getFields().get(2).getBinaryV().toString());
        assertEquals(timestamp * 1.0, rowRecord.getFields().get(3).getDoubleV(), 0.0001);
        cnt++;
      }
      assertEquals(30, cnt);
    }
  }

  @Test
  @Category({LocalStandaloneIT.class, ClusterIT.class})
  public void insertRelationalTabletWithCacheLeaderTest()
      throws IoTDBConnectionException, StatementExecutionException {
    try (ITableSession session = EnvFactory.getEnv().getTableSessionConnection()) {
      session.executeNonQueryStatement("USE \"db1\"");
      session.executeNonQueryStatement(
          "CREATE TABLE table5 (id1 string id, attr1 string attribute, "
              + "m1 double "
              + "measurement)");

      List<IMeasurementSchema> schemaList = new ArrayList<>();
      schemaList.add(new MeasurementSchema("id1", TSDataType.STRING));
      schemaList.add(new MeasurementSchema("attr1", TSDataType.STRING));
      schemaList.add(new MeasurementSchema("m1", TSDataType.DOUBLE));
      final List<ColumnCategory> columnTypes =
          Arrays.asList(ColumnCategory.ID, ColumnCategory.ATTRIBUTE, ColumnCategory.MEASUREMENT);

      long timestamp = 0;
      Tablet tablet =
          new Tablet(
              "table5",
              IMeasurementSchema.getMeasurementNameList(schemaList),
              IMeasurementSchema.getDataTypeList(schemaList),
              columnTypes,
              15);

      for (long row = 0; row < 15; row++) {
        int rowIndex = tablet.getRowSize();
        tablet.addTimestamp(rowIndex, timestamp + row);
        tablet.addValue("id1", rowIndex, "id:" + row);
        tablet.addValue("attr1", rowIndex, "attr:" + row);
        tablet.addValue("m1", rowIndex, row * 1.0);
        if (tablet.getRowSize() == tablet.getMaxRowNumber()) {
          session.insert(tablet);
          tablet.reset();
        }
      }

      if (tablet.getRowSize() != 0) {
        session.insert(tablet);
        tablet.reset();
      }

      session.executeNonQueryStatement("FLush");

      for (long row = 15; row < 30; row++) {
        int rowIndex = tablet.getRowSize();
        tablet.addTimestamp(rowIndex, timestamp + row);
        // cache leader should work for devices that have inserted before
        tablet.addValue("id1", rowIndex, "id:" + (row - 15));
        tablet.addValue("attr1", rowIndex, "attr:" + (row - 15));
        tablet.addValue("m1", rowIndex, row * 1.0);
        if (tablet.getRowSize() == tablet.getMaxRowNumber()) {
          session.insert(tablet);
          tablet.reset();
        }
      }

      if (tablet.getRowSize() != 0) {
        session.insert(tablet);
        tablet.reset();
      }

      int cnt = 0;
      SessionDataSet dataSet = session.executeQueryStatement("select * from table5 order by time");
      while (dataSet.hasNext()) {
        RowRecord rowRecord = dataSet.next();
        timestamp = rowRecord.getFields().get(0).getLongV();
        assertEquals(
            "id:" + (timestamp < 15 ? timestamp : timestamp - 15),
            rowRecord.getFields().get(1).getBinaryV().toString());
        assertEquals(
            "attr:" + (timestamp < 15 ? timestamp : timestamp - 15),
            rowRecord.getFields().get(2).getBinaryV().toString());
        assertEquals(timestamp * 1.0, rowRecord.getFields().get(3).getDoubleV(), 0.0001);
        cnt++;
      }
      assertEquals(30, cnt);
    }
  }

  @Test
  @Category({LocalStandaloneIT.class, ClusterIT.class})
  public void autoCreateNonIdColumnTest()
      throws IoTDBConnectionException, StatementExecutionException {
    try (ITableSession session = EnvFactory.getEnv().getTableSessionConnection()) {
      session.executeNonQueryStatement("USE \"db1\"");
      // only one column in this table, and others should be auto-created
      session.executeNonQueryStatement("CREATE TABLE table7 (id1 string id)");

      List<IMeasurementSchema> schemaList = new ArrayList<>();
      schemaList.add(new MeasurementSchema("id1", TSDataType.STRING));
      schemaList.add(new MeasurementSchema("attr1", TSDataType.STRING));
      schemaList.add(new MeasurementSchema("m1", TSDataType.DOUBLE));
      final List<ColumnCategory> columnTypes =
          Arrays.asList(ColumnCategory.ID, ColumnCategory.ATTRIBUTE, ColumnCategory.MEASUREMENT);

      long timestamp = 0;
      Tablet tablet =
          new Tablet(
              "table7",
              IMeasurementSchema.getMeasurementNameList(schemaList),
              IMeasurementSchema.getDataTypeList(schemaList),
              columnTypes,
              15);

      for (long row = 0; row < 15; row++) {
        int rowIndex = tablet.getRowSize();
        tablet.addTimestamp(rowIndex, timestamp + row);
        tablet.addValue("id1", rowIndex, "id:" + row);
        tablet.addValue("attr1", rowIndex, "attr:" + row);
        tablet.addValue("m1", rowIndex, row * 1.0);
        if (tablet.getRowSize() == tablet.getMaxRowNumber()) {
          session.insert(tablet);
          tablet.reset();
        }
      }

      if (tablet.getRowSize() != 0) {
        session.insert(tablet);
        tablet.reset();
      }

      session.executeNonQueryStatement("FLush");

      for (long row = 15; row < 30; row++) {
        int rowIndex = tablet.getRowSize();
        tablet.addTimestamp(rowIndex, timestamp + row);
        tablet.addValue("id1", rowIndex, "id:" + row);
        tablet.addValue("attr1", rowIndex, "attr:" + row);
        tablet.addValue("m1", rowIndex, row * 1.0);
        if (tablet.getRowSize() == tablet.getMaxRowNumber()) {
          session.insert(tablet);
          tablet.reset();
        }
      }

      if (tablet.getRowSize() != 0) {
        session.insert(tablet);
        tablet.reset();
      }

      SessionDataSet dataSet = session.executeQueryStatement("select * from table7 order by time");
      int cnt = 0;
      while (dataSet.hasNext()) {
        RowRecord rowRecord = dataSet.next();
        timestamp = rowRecord.getFields().get(0).getLongV();
        assertEquals("id:" + timestamp, rowRecord.getFields().get(1).getBinaryV().toString());
        assertEquals("attr:" + timestamp, rowRecord.getFields().get(2).getBinaryV().toString());
        assertEquals(timestamp * 1.0, rowRecord.getFields().get(3).getDoubleV(), 0.0001);
        cnt++;
      }
      assertEquals(30, cnt);
    }
  }

  @Test
  @Category({LocalStandaloneIT.class, ClusterIT.class})
  public void autoCreateTableTest() throws IoTDBConnectionException, StatementExecutionException {
    try (ITableSession session = EnvFactory.getEnv().getTableSessionConnection()) {
      session.executeNonQueryStatement("USE \"db1\"");
      // no table created here

      List<IMeasurementSchema> schemaList = new ArrayList<>();
      schemaList.add(new MeasurementSchema("id1", TSDataType.STRING));
      schemaList.add(new MeasurementSchema("attr1", TSDataType.STRING));
      schemaList.add(new MeasurementSchema("m1", TSDataType.DOUBLE));
      final List<ColumnCategory> columnTypes =
          Arrays.asList(ColumnCategory.ID, ColumnCategory.ATTRIBUTE, ColumnCategory.MEASUREMENT);

      long timestamp = 0;
      Tablet tablet =
          new Tablet(
              "table6",
              IMeasurementSchema.getMeasurementNameList(schemaList),
              IMeasurementSchema.getDataTypeList(schemaList),
              columnTypes,
              15);

      for (int row = 0; row < 15; row++) {
        int rowIndex = tablet.getRowSize();
        tablet.addTimestamp(rowIndex, timestamp + row);
        tablet.addValue("id1", rowIndex, "id:" + row);
        tablet.addValue("attr1", rowIndex, "attr:" + row);
        tablet.addValue("m1", rowIndex, row * 1.0);
      }
      session.insert(tablet);
      tablet.reset();

      session.executeNonQueryStatement("FLush");

      for (int row = 15; row < 30; row++) {
        int rowIndex = tablet.getRowSize();
        tablet.addTimestamp(rowIndex, timestamp + row);
        tablet.addValue("id1", rowIndex, "id:" + row);
        tablet.addValue("attr1", rowIndex, "attr:" + row);
        tablet.addValue("m1", rowIndex, row * 1.0);
      }
      session.insert(tablet);
      tablet.reset();

      int cnt = 0;
      SessionDataSet dataSet = session.executeQueryStatement("select * from table6 order by time");
      while (dataSet.hasNext()) {
        RowRecord rowRecord = dataSet.next();
        timestamp = rowRecord.getFields().get(0).getLongV();
        assertEquals("id:" + timestamp, rowRecord.getFields().get(1).getBinaryV().toString());
        assertEquals("attr:" + timestamp, rowRecord.getFields().get(2).getBinaryV().toString());
        assertEquals(timestamp * 1.0, rowRecord.getFields().get(3).getDoubleV(), 0.0001);
        cnt++;
      }
      assertEquals(30, cnt);
    }
  }

  @Test
  @Category({LocalStandaloneIT.class, ClusterIT.class})
  public void autoCreateIdColumnTest()
      throws IoTDBConnectionException, StatementExecutionException {
    try (ITableSession session = EnvFactory.getEnv().getTableSessionConnection()) {
      session.executeNonQueryStatement("USE \"db1\"");
      // only one column in this table, and others should be auto-created
      session.executeNonQueryStatement("CREATE TABLE table8 (id1 string id)");

      List<IMeasurementSchema> schemaList = new ArrayList<>();
      schemaList.add(new MeasurementSchema("id2", TSDataType.STRING));
      schemaList.add(new MeasurementSchema("attr1", TSDataType.STRING));
      schemaList.add(new MeasurementSchema("m1", TSDataType.DOUBLE));
      final List<ColumnCategory> columnTypes =
          Arrays.asList(ColumnCategory.ID, ColumnCategory.ATTRIBUTE, ColumnCategory.MEASUREMENT);

      long timestamp = 0;
      Tablet tablet =
          new Tablet(
              "table8",
              IMeasurementSchema.getMeasurementNameList(schemaList),
              IMeasurementSchema.getDataTypeList(schemaList),
              columnTypes,
              15);

      for (long row = 0; row < 15; row++) {
        int rowIndex = tablet.getRowSize();
        tablet.addTimestamp(rowIndex, timestamp + row);
        tablet.addValue("id2", rowIndex, "id:" + row);
        tablet.addValue("attr1", rowIndex, "attr:" + row);
        tablet.addValue("m1", rowIndex, row * 1.0);
        if (tablet.getRowSize() == tablet.getMaxRowNumber()) {
          session.insert(tablet);
          tablet.reset();
        }
      }

      if (tablet.getRowSize() != 0) {
        session.insert(tablet);
        tablet.reset();
      }

      session.executeNonQueryStatement("FLush");

      for (long row = 15; row < 30; row++) {
        int rowIndex = tablet.getRowSize();
        tablet.addTimestamp(rowIndex, timestamp + row);
        tablet.addValue("id2", rowIndex, "id:" + row);
        tablet.addValue("attr1", rowIndex, "attr:" + row);
        tablet.addValue("m1", rowIndex, row * 1.0);
        if (tablet.getRowSize() == tablet.getMaxRowNumber()) {
          session.insert(tablet);
          tablet.reset();
        }
      }

      if (tablet.getRowSize() != 0) {
        session.insert(tablet);
        tablet.reset();
      }

      SessionDataSet dataSet = session.executeQueryStatement("select * from table8 order by time");
      int cnt = 0;
      while (dataSet.hasNext()) {
        RowRecord rowRecord = dataSet.next();
        timestamp = rowRecord.getFields().get(0).getLongV();
        // id 1 should be null
        assertNull(rowRecord.getFields().get(1).getDataType());
        assertEquals("id:" + timestamp, rowRecord.getFields().get(2).getBinaryV().toString());
        assertEquals("attr:" + timestamp, rowRecord.getFields().get(3).getBinaryV().toString());
        assertEquals(timestamp * 1.0, rowRecord.getFields().get(4).getDoubleV(), 0.0001);
        cnt++;
      }
      assertEquals(30, cnt);
    }
  }

  @Test
  @Category({LocalStandaloneIT.class, ClusterIT.class})
  public void autoAdjustIdTest() throws IoTDBConnectionException, StatementExecutionException {
    try (ITableSession session = EnvFactory.getEnv().getTableSessionConnection()) {
      session.executeNonQueryStatement("USE \"db1\"");
      // the id order in the table is (id1, id2)
      session.executeNonQueryStatement(
          "CREATE TABLE table9 (id1 string id, id2 string id, attr1 string attribute, "
              + "m1 double "
              + "measurement)");

      // the id order in the row is (id2, id1)
      List<IMeasurementSchema> schemaList = new ArrayList<>();
      schemaList.add(new MeasurementSchema("id2", TSDataType.STRING));
      schemaList.add(new MeasurementSchema("id1", TSDataType.STRING));
      schemaList.add(new MeasurementSchema("attr1", TSDataType.STRING));
      schemaList.add(new MeasurementSchema("m1", TSDataType.DOUBLE));
      final List<ColumnCategory> columnTypes =
          Arrays.asList(
              ColumnCategory.ID,
              ColumnCategory.ID,
              ColumnCategory.ATTRIBUTE,
              ColumnCategory.MEASUREMENT);
      List<String> measurementIds = IMeasurementSchema.getMeasurementNameList(schemaList);
      List<TSDataType> dataTypes = IMeasurementSchema.getDataTypeList(schemaList);

      long timestamp = 0;
      Tablet tablet = new Tablet("table9", measurementIds, dataTypes, columnTypes, 15);

      for (long row = 0; row < 15; row++) {
        int rowIndex = tablet.getRowSize();
        tablet.addTimestamp(rowIndex, timestamp + row);
        tablet.addValue("id2", rowIndex, "id2:" + row);
        tablet.addValue("id1", rowIndex, "id1:" + row);
        tablet.addValue("attr1", rowIndex, "attr1:" + row);
        tablet.addValue("m1", rowIndex, row * 1.0);
      }
      session.insert(tablet);
      tablet.reset();

      session.executeNonQueryStatement("FLush");

      for (long row = 15; row < 30; row++) {
        int rowIndex = tablet.getRowSize();
        tablet.addTimestamp(rowIndex, timestamp + row);
        tablet.addValue("id2", rowIndex, "id2:" + row);
        tablet.addValue("id1", rowIndex, "id1:" + row);
        tablet.addValue("attr1", rowIndex, "attr1:" + row);
        tablet.addValue("m1", rowIndex, row * 1.0);
      }
      session.insert(tablet);
      tablet.reset();

      SessionDataSet dataSet = session.executeQueryStatement("select * from table9 order by time");
      int cnt = 0;
      while (dataSet.hasNext()) {
        RowRecord rowRecord = dataSet.next();
        timestamp = rowRecord.getFields().get(0).getLongV();
        assertEquals("id1:" + timestamp, rowRecord.getFields().get(1).getBinaryV().toString());
        assertEquals("id2:" + timestamp, rowRecord.getFields().get(2).getBinaryV().toString());
        assertEquals("attr1:" + timestamp, rowRecord.getFields().get(3).getBinaryV().toString());
        assertEquals(timestamp * 1.0, rowRecord.getFields().get(4).getDoubleV(), 0.0001);
        cnt++;
      }
      assertEquals(30, cnt);
    }
  }

  @Test
  @Category({LocalStandaloneIT.class, ClusterIT.class})
  public void insertRelationalSqlWithoutDBTest()
      throws IoTDBConnectionException, StatementExecutionException {
    try (ITableSession session = EnvFactory.getEnv().getTableSessionConnection()) {
      session.executeNonQueryStatement("USE \"db1\"");
      session.executeNonQueryStatement(
          "CREATE TABLE table10 (id1 string id, attr1 string attribute, "
              + "m1 double "
              + "measurement)");
    }
    // insert without db
    try (ITableSession session = EnvFactory.getEnv().getTableSessionConnection()) {
      long timestamp;

      // no db in session and sql
      try {
        session.executeNonQueryStatement(
            String.format(
                "INSERT INTO table10 (time, id1, attr1, m1) VALUES (%d, '%s', '%s', %f)",
                0, "id:" + 0, "attr:" + 0, 0 * 1.0));
        fail("Exception expected");
      } catch (StatementExecutionException e) {
        assertEquals("701: database is not specified", e.getMessage());
      }

      // specify db in sql
      for (long row = 0; row < 15; row++) {
        session.executeNonQueryStatement(
            String.format(
                "INSERT INTO db1.table10 (time, id1, attr1, m1) VALUES (%d, '%s', '%s', %f)",
                row, "id:" + row, "attr:" + row, row * 1.0));
      }

      session.executeNonQueryStatement("FLush");

      for (long row = 15; row < 30; row++) {
        session.executeNonQueryStatement(
            String.format(
                "INSERT INTO db1.table10 (time, id1, attr1, m1) VALUES (%d, '%s', '%s', %f)",
                row, "id:" + row, "attr:" + row, row * 1.0));
      }

      SessionDataSet dataSet =
          session.executeQueryStatement("select * from db1.table10 order by time");
      int cnt = 0;
      while (dataSet.hasNext()) {
        RowRecord rowRecord = dataSet.next();
        timestamp = rowRecord.getFields().get(0).getLongV();
        assertEquals("id:" + timestamp, rowRecord.getFields().get(1).getBinaryV().toString());
        assertEquals("attr:" + timestamp, rowRecord.getFields().get(2).getBinaryV().toString());
        assertEquals(timestamp * 1.0, rowRecord.getFields().get(3).getDoubleV(), 0.0001);
        cnt++;
      }
      assertEquals(30, cnt);
    }
  }

  @Test
  @Category({LocalStandaloneIT.class, ClusterIT.class})
  public void insertRelationalSqlAnotherDBTest()
      throws IoTDBConnectionException, StatementExecutionException {
    try (ITableSession session = EnvFactory.getEnv().getTableSessionConnection()) {
      session.executeNonQueryStatement("USE \"db1\"");
      session.executeNonQueryStatement(
          "CREATE TABLE table11 (id1 string id, attr1 string attribute, "
              + "m1 double "
              + "measurement)");
    }
    // use db2 but insert db1
    try (ITableSession session = EnvFactory.getEnv().getTableSessionConnection()) {
      long timestamp;
      session.executeNonQueryStatement("USE \"db2\"");

      // specify db in sql
      for (long row = 0; row < 15; row++) {
        session.executeNonQueryStatement(
            String.format(
                "INSERT INTO db1.table11 (time, id1, attr1, m1) VALUES (%d, '%s', '%s', %f)",
                row, "id:" + row, "attr:" + row, row * 1.0));
      }

      session.executeNonQueryStatement("FLush");

      for (long row = 15; row < 30; row++) {
        // check case sensitivity
        session.executeNonQueryStatement(
            String.format(
                "INSERT INTO DB1.TaBle11 (time, id1, attr1, m1) VALUES (%d, '%s', '%s', %f)",
                row, "id:" + row, "attr:" + row, row * 1.0));
      }

      SessionDataSet dataSet =
          session.executeQueryStatement("select * from db1.table11 order by time");
      int cnt = 0;
      while (dataSet.hasNext()) {
        RowRecord rowRecord = dataSet.next();
        timestamp = rowRecord.getFields().get(0).getLongV();
        assertEquals("id:" + timestamp, rowRecord.getFields().get(1).getBinaryV().toString());
        assertEquals("attr:" + timestamp, rowRecord.getFields().get(2).getBinaryV().toString());
        assertEquals(timestamp * 1.0, rowRecord.getFields().get(3).getDoubleV(), 0.0001);
        cnt++;
      }
      assertEquals(30, cnt);
    }
  }

  @Test
  @Category({LocalStandaloneIT.class, ClusterIT.class})
  public void insertNonExistTableTest()
      throws IoTDBConnectionException, StatementExecutionException {
    try (ITableSession session = EnvFactory.getEnv().getTableSessionConnection()) {
      session.executeNonQueryStatement("USE \"db1\"");

      try {
        session.executeNonQueryStatement(
            String.format(
                "INSERT INTO table13 (time, id1, attr1, m1) VALUES (%d, '%s', '%s', %f)",
                0, "id:" + 0, "attr:" + 0, 0 * 1.0));
        fail("Exception expected");
      } catch (StatementExecutionException e) {
        assertEquals("507: Table table13 does not exist", e.getMessage());
      }

      try {
        session.executeNonQueryStatement(
            String.format(
                "INSERT INTO db2.table13 (time, id1, attr1, m1) VALUES (%d, '%s', '%s', %f)",
                0, "id:" + 0, "attr:" + 0, 0 * 1.0));
        fail("Exception expected");
      } catch (StatementExecutionException e) {
        assertEquals("507: Table table13 does not exist", e.getMessage());
      }
    }
  }

  @Test
  @Category({LocalStandaloneIT.class, ClusterIT.class})
  public void insertNonExistDBTest() throws IoTDBConnectionException, StatementExecutionException {
    try (ITableSession session = EnvFactory.getEnv().getTableSessionConnection()) {
      session.executeNonQueryStatement("USE \"db1\"");

      try {
        session.executeNonQueryStatement(
            String.format(
                "INSERT INTO db3.table13 (time, id1, attr1, m1) VALUES (%d, '%s', '%s', %f)",
                0, "id:" + 0, "attr:" + 0, 0 * 1.0));
        fail("Exception expected");
      } catch (StatementExecutionException e) {
        assertEquals("507: Table table13 does not exist", e.getMessage());
      }
    }
  }

  @Test
  @Category({LocalStandaloneIT.class, ClusterIT.class})
  public void insertWithoutMeasurementTest()
      throws IoTDBConnectionException, StatementExecutionException {
    try (ITableSession session = EnvFactory.getEnv().getTableSessionConnection()) {
      session.executeNonQueryStatement("USE \"db1\"");
      session.executeNonQueryStatement("create table tb (a string id, b string measurement)");
      session.executeNonQueryStatement("insert into tb(a) values ('w')");
      SessionDataSet dataSet = session.executeQueryStatement("select * from tb");
      int cnt = 0;
      while (dataSet.hasNext()) {
        RowRecord rowRecord = dataSet.next();
        assertEquals("w", rowRecord.getFields().get(1).getBinaryV().toString());
        assertNull(rowRecord.getFields().get(2).getDataType());
        cnt++;
      }
      assertEquals(1, cnt);

      session.executeNonQueryStatement("flush");

      dataSet = session.executeQueryStatement("select * from tb");
      cnt = 0;
      while (dataSet.hasNext()) {
        RowRecord rowRecord = dataSet.next();
        assertEquals("w", rowRecord.getFields().get(1).getBinaryV().toString());
        assertNull(rowRecord.getFields().get(2).getDataType());
        cnt++;
      }
      assertEquals(1, cnt);
    }
  }
}
