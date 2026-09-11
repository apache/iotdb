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

package org.apache.iotdb.relational.it.session;

import org.apache.iotdb.isession.ITableSession;
import org.apache.iotdb.isession.NumericRow;
import org.apache.iotdb.isession.SessionDataSet;
import org.apache.iotdb.isession.pool.ITableSessionPool;
import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.TableClusterIT;
import org.apache.iotdb.itbase.category.TableLocalStandaloneIT;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;

import org.apache.tsfile.enums.ColumnCategory;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.RowRecord;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.time.LocalDate;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.apache.iotdb.commons.schema.column.ColumnHeaderConstant.showTablesColumnHeaders;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(IoTDBTestRunner.class)
@Category({TableLocalStandaloneIT.class, TableClusterIT.class})
public class IoTDBTableModelSessionIT {

  @Before
  public void setUp() throws Exception {
    EnvFactory.getEnv().initClusterEnvironment();
  }

  @After
  public void tearDown() throws Exception {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  @Test
  public void testNumericRowInsert() throws Exception {
    // Exercise direct and pooled table sessions, schema reordering and missing STRING columns.
    // The same values must remain readable from MemTable and after flushing to TsFile.
    try (ITableSession session = EnvFactory.getEnv().getTableSessionConnection()) {
      session.executeNonQueryStatement("CREATE DATABASE numeric_rows");
      session.executeNonQueryStatement("USE numeric_rows");
      session.executeNonQueryStatement(
          "CREATE TABLE readings(tag STRING TAG, attr STRING ATTRIBUTE, "
              + "event_time TIMESTAMP FIELD, event_date DATE FIELD, d DOUBLE FIELD, "
              + "f FLOAT FIELD, l INT64 FIELD, i INT32 FIELD, b BOOLEAN FIELD, missing STRING FIELD)");
      List<String> names = Arrays.asList("b", "i", "l", "f", "d", "event_date", "event_time");
      List<TSDataType> types =
          Arrays.asList(
              TSDataType.BOOLEAN,
              TSDataType.INT32,
              TSDataType.INT64,
              TSDataType.FLOAT,
              TSDataType.DOUBLE,
              TSDataType.DATE,
              TSDataType.TIMESTAMP);
      long[] values = {
        -1,
        Integer.MIN_VALUE,
        Long.MIN_VALUE,
        Float.floatToRawIntBits(-1.25f),
        Double.doubleToRawLongBits(2.5),
        20260910,
        1234
      };
      session.insert(
          new NumericRow(
              "readings",
              1,
              names,
              types,
              Collections.nCopies(types.size(), ColumnCategory.FIELD),
              values,
              new String[0]));
      try (ITableSessionPool pool = EnvFactory.getEnv().getTableSessionPool(1, "numeric_rows");
          ITableSession pooledSession = pool.getSession()) {
        pooledSession.insert(
            new NumericRow(
                "readings",
                2,
                names,
                types,
                Collections.nCopies(types.size(), ColumnCategory.FIELD),
                values,
                new String[0]));
      }
      assertNumericRows(session);
      session.executeNonQueryStatement("FLUSH");
      assertNumericRows(session);
    }
  }

  @Test
  public void testNumericRowInsertWithTags() throws Exception {
    // Different TAGs at the same timestamp must stay separate, with identical behavior after flush.
    try (ITableSession session = EnvFactory.getEnv().getTableSessionConnection()) {
      session.executeNonQueryStatement("CREATE DATABASE numeric_tag_rows");
      session.executeNonQueryStatement("USE numeric_tag_rows");
      session.executeNonQueryStatement(
          "CREATE TABLE readings(region STRING TAG, device STRING TAG, "
              + "missing STRING TAG, v INT64 FIELD, enabled BOOLEAN FIELD)");
      List<String> names = Arrays.asList("enabled", "device", "v", "region");
      List<TSDataType> types =
          Arrays.asList(TSDataType.BOOLEAN, TSDataType.STRING, TSDataType.INT64, TSDataType.STRING);
      List<ColumnCategory> categories =
          Arrays.asList(
              ColumnCategory.FIELD, ColumnCategory.TAG, ColumnCategory.FIELD, ColumnCategory.TAG);
      session.insert(
          new NumericRow(
              "readings",
              1,
              names,
              types,
              categories,
              new long[] {1, 10},
              new String[] {"设备甲😀", "华北"}));
      try (ITableSessionPool pool = EnvFactory.getEnv().getTableSessionPool(1, "numeric_tag_rows");
          ITableSession pooled = pool.getSession()) {
        pooled.insert(
            new NumericRow(
                "readings",
                1,
                names,
                types,
                categories,
                new long[] {0, 20},
                new String[] {"设备乙", "华北"}));
      }
      session.insert(
          new NumericRow(
              "readings",
              2,
              names,
              types,
              categories,
              new long[] {1, 30},
              new String[] {"", null}));
      session.insert(
          new NumericRow(
              "readings",
              2,
              names,
              types,
              categories,
              new long[] {0, 40},
              new String[] {null, null}));
      assertNumericTagRows(session);
      session.executeNonQueryStatement("FLUSH");
      assertNumericTagRows(session);
    }
  }

  private void assertNumericTagRows(ITableSession session) throws Exception {
    String[] devices = {"设备甲😀", "设备乙", "", null};
    try (SessionDataSet dataSet =
        session.executeQueryStatement(
            "SELECT region,device,missing,v,enabled FROM readings ORDER BY v")) {
      for (int i = 0; i < devices.length; i++) {
        assertTrue(dataSet.hasNext());
        RowRecord row = dataSet.next();
        if (i < 2) {
          assertEquals("华北", row.getFields().get(0).getStringValue());
        } else {
          assertNull(row.getFields().get(0).getDataType());
        }
        if (devices[i] == null) {
          assertNull(row.getFields().get(1).getDataType());
        } else {
          assertEquals(devices[i], row.getFields().get(1).getStringValue());
        }
        assertNull(row.getFields().get(2).getDataType());
        assertEquals((i + 1) * 10, row.getFields().get(3).getLongV());
        assertEquals(i % 2 == 0, row.getFields().get(4).getBoolV());
      }
      assertFalse(dataSet.hasNext());
    }
  }

  private void assertNumericRows(ITableSession session) throws Exception {
    try (SessionDataSet dataSet =
        session.executeQueryStatement(
            "SELECT time,b,i,l,f,d,event_date,event_time,tag,attr,missing FROM readings ORDER BY time")) {
      for (long time = 1; time <= 2; time++) {
        assertTrue(dataSet.hasNext());
        RowRecord row = dataSet.next();
        assertEquals(time, row.getFields().get(0).getLongV());
        assertTrue(row.getFields().get(1).getBoolV());
        assertEquals(Integer.MIN_VALUE, row.getFields().get(2).getIntV());
        assertEquals(Long.MIN_VALUE, row.getFields().get(3).getLongV());
        assertEquals(-1.25f, row.getFields().get(4).getFloatV(), 0);
        assertEquals(2.5, row.getFields().get(5).getDoubleV(), 0);
        assertEquals(TSDataType.DATE, row.getFields().get(6).getDataType());
        assertEquals(LocalDate.of(2026, 9, 10), row.getFields().get(6).getDateV());
        assertEquals(1234, row.getFields().get(7).getLongV());
        for (int column = 8; column <= 10; column++) {
          assertNull(row.getFields().get(column).getDataType());
        }
      }
      assertFalse(dataSet.hasNext());
    }
  }

  @Test
  public void testUseDatabase() {
    final String[] table1Names = new String[] {"table1"};
    final String[] table1ttls = new String[] {"3600000"};

    final String[] table2Names = new String[] {"table2"};
    final String[] table2ttls = new String[] {"6600000"};

    try (final ITableSession session =
        EnvFactory.getEnv().getTableSessionConnectionWithDB("test2")) {

      session.executeNonQueryStatement("CREATE DATABASE test1");
      session.executeNonQueryStatement("CREATE DATABASE test2");

      // or use full qualified table name
      session.executeNonQueryStatement(
          "create table test1.table1(region_id STRING TAG, plant_id STRING TAG, device_id STRING TAG, model STRING ATTRIBUTE, temperature FLOAT FIELD, humidity DOUBLE FIELD) with (TTL=3600000)");

      session.executeNonQueryStatement(
          "create table table2(region_id STRING TAG, plant_id STRING TAG, color STRING ATTRIBUTE, temperature FLOAT FIELD, speed DOUBLE FIELD) with (TTL=6600000)");

      try (final SessionDataSet dataSet = session.executeQueryStatement("SHOW TABLES")) {
        int cnt = 0;
        assertEquals(showTablesColumnHeaders.size(), dataSet.getColumnNames().size());
        for (int i = 0; i < showTablesColumnHeaders.size(); i++) {
          assertEquals(
              showTablesColumnHeaders.get(i).getColumnName(), dataSet.getColumnNames().get(i));
        }
        while (dataSet.hasNext()) {
          final RowRecord rowRecord = dataSet.next();
          assertEquals(table2Names[cnt], rowRecord.getFields().get(0).getStringValue());
          assertEquals(table2ttls[cnt], rowRecord.getFields().get(1).getStringValue());
          cnt++;
        }
        assertEquals(table2Names.length, cnt);
      }

      session.executeNonQueryStatement("use test1");

      try (final SessionDataSet dataSet = session.executeQueryStatement("SHOW TABLES")) {
        int cnt = 0;
        assertEquals(showTablesColumnHeaders.size(), dataSet.getColumnNames().size());
        for (int i = 0; i < showTablesColumnHeaders.size(); i++) {
          assertEquals(
              showTablesColumnHeaders.get(i).getColumnName(), dataSet.getColumnNames().get(i));
        }
        while (dataSet.hasNext()) {
          final RowRecord rowRecord = dataSet.next();
          assertEquals(table1Names[cnt], rowRecord.getFields().get(0).getStringValue());
          assertEquals(table1ttls[cnt], rowRecord.getFields().get(1).getStringValue());
          cnt++;
        }
        assertEquals(table1Names.length, cnt);
      }

    } catch (final IoTDBConnectionException | StatementExecutionException e) {
      fail(e.getMessage());
    }
  }

  @Test
  public void testCreateSessionWithCapitalDB() {
    final String[] table2Names = new String[] {"table2"};
    final String[] table2ttls = new String[] {"6600000"};

    try (final ITableSession session =
        EnvFactory.getEnv().getTableSessionConnectionWithDB("TEST2")) {

      session.executeNonQueryStatement("CREATE DATABASE test1");
      session.executeNonQueryStatement("CREATE DATABASE test2");

      // or use full qualified table name
      session.executeNonQueryStatement(
          "create table test1.table1(region_id STRING TAG, plant_id STRING TAG, device_id STRING TAG, model STRING ATTRIBUTE, temperature FLOAT FIELD, humidity DOUBLE FIELD) with (TTL=3600000)");

      session.executeNonQueryStatement(
          "create table table2(region_id STRING TAG, plant_id STRING TAG, color STRING ATTRIBUTE, temperature FLOAT FIELD, speed DOUBLE FIELD) with (TTL=6600000)");

      try (final SessionDataSet dataSet = session.executeQueryStatement("SHOW TABLES")) {
        int cnt = 0;
        assertEquals(showTablesColumnHeaders.size(), dataSet.getColumnNames().size());
        for (int i = 0; i < showTablesColumnHeaders.size(); i++) {
          assertEquals(
              showTablesColumnHeaders.get(i).getColumnName(), dataSet.getColumnNames().get(i));
        }
        while (dataSet.hasNext()) {
          final RowRecord rowRecord = dataSet.next();
          assertEquals(table2Names[cnt], rowRecord.getFields().get(0).getStringValue());
          assertEquals(table2ttls[cnt], rowRecord.getFields().get(1).getStringValue());
          cnt++;
        }
        assertEquals(table2Names.length, cnt);
      }

    } catch (final IoTDBConnectionException | StatementExecutionException e) {
      fail(e.getMessage());
    }
  }
}
