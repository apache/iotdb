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

package org.apache.iotdb.relational.it.session.pool;

import org.apache.iotdb.isession.ITableSession;
import org.apache.iotdb.isession.SessionDataSet;
import org.apache.iotdb.isession.pool.ITableSessionPool;
import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.TableClusterIT;
import org.apache.iotdb.itbase.category.TableLocalStandaloneIT;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.RowRecord;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.write.record.Tablet;
import org.apache.tsfile.write.record.Tablet.ColumnCategory;
import org.apache.tsfile.write.schema.IMeasurementSchema;
import org.apache.tsfile.write.schema.MeasurementSchema;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.nio.charset.Charset;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(IoTDBTestRunner.class)
@Category({TableLocalStandaloneIT.class, TableClusterIT.class})
public class IoTDBInsertTableSessionPoolIT {

  @BeforeClass
  public static void setUp() throws Exception {
    EnvFactory.getEnv().initClusterEnvironment();
    ITableSessionPool sessionPool = EnvFactory.getEnv().getTableSessionPool(1);
    try (final ITableSession session = sessionPool.getSession()) {
      session.executeNonQueryStatement("create database if not exists test");
    }
  }

  @AfterClass
  public static void tearDown() throws Exception {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  @Test
  public void testPartialInsertTablet() {
    ITableSessionPool sessionPool = EnvFactory.getEnv().getTableSessionPool(1);
    try (final ITableSession session = sessionPool.getSession()) {
      session.executeNonQueryStatement("use \"test\"");
      session.executeNonQueryStatement("SET CONFIGURATION enable_auto_create_schema='false'");
      session.executeNonQueryStatement(
          "create table sg6 (id1 string id, s1 int64 measurement, s2 int64 measurement)");
      List<IMeasurementSchema> schemaList = new ArrayList<>();
      schemaList.add(new MeasurementSchema("id1", TSDataType.STRING));
      schemaList.add(new MeasurementSchema("s1", TSDataType.INT64));
      schemaList.add(new MeasurementSchema("s2", TSDataType.INT64));
      schemaList.add(new MeasurementSchema("s3", TSDataType.INT64));
      final List<ColumnCategory> columnTypes =
          Arrays.asList(
              ColumnCategory.ID,
              ColumnCategory.MEASUREMENT,
              ColumnCategory.MEASUREMENT,
              ColumnCategory.MEASUREMENT);
      Tablet tablet =
          new Tablet(
              "sg6",
              IMeasurementSchema.getMeasurementNameList(schemaList),
              IMeasurementSchema.getDataTypeList(schemaList),
              columnTypes,
              300);
      long timestamp = 0;
      for (long row = 0; row < 100; row++) {
        int rowIndex = tablet.getRowSize();
        tablet.addTimestamp(rowIndex, timestamp);
        for (int s = 0; s < 4; s++) {
          long value = timestamp;
          if (s == 0) {
            tablet.addValue(schemaList.get(s).getMeasurementName(), rowIndex, "d1");
          } else {
            tablet.addValue(schemaList.get(s).getMeasurementName(), rowIndex, value);
          }
        }
        timestamp++;
      }
      timestamp = System.currentTimeMillis();
      for (long row = 0; row < 100; row++) {
        int rowIndex = tablet.getRowSize();
        tablet.addTimestamp(rowIndex, timestamp);
        for (int s = 0; s < 4; s++) {
          long value = timestamp;
          if (s == 0) {
            tablet.addValue(schemaList.get(s).getMeasurementName(), rowIndex, "d1");
          } else {
            tablet.addValue(schemaList.get(s).getMeasurementName(), rowIndex, value);
          }
        }
        timestamp++;
      }
      try {
        session.insert(tablet);
      } catch (Exception e) {
        if (!e.getMessage().contains("507")) {
          fail(e.getMessage());
        }
      } finally {
        session.executeNonQueryStatement("SET CONFIGURATION enable_auto_create_schema='false'");
      }
      try (SessionDataSet dataSet = session.executeQueryStatement("SELECT * FROM sg6")) {
        assertEquals(4, dataSet.getColumnNames().size());
        assertEquals("time", dataSet.getColumnNames().get(0));
        assertEquals("id1", dataSet.getColumnNames().get(1));
        assertEquals("s1", dataSet.getColumnNames().get(2));
        assertEquals("s2", dataSet.getColumnNames().get(3));
        int cnt = 0;
        while (dataSet.hasNext()) {
          RowRecord rowRecord = dataSet.next();
          long time = rowRecord.getFields().get(0).getLongV();
          assertEquals(time, rowRecord.getFields().get(2).getLongV());
          assertEquals(time, rowRecord.getFields().get(3).getLongV());
          cnt++;
        }
        Assert.assertEquals(200, cnt);
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void testInsertKeyword() throws IoTDBConnectionException, StatementExecutionException {
    ITableSessionPool sessionPool = EnvFactory.getEnv().getTableSessionPool(1);
    try (final ITableSession session = sessionPool.getSession()) {
      session.executeNonQueryStatement("USE \"test\"");
      session.executeNonQueryStatement(
          "create table table20 ("
              + "device_id string id,"
              + "attribute STRING ATTRIBUTE,"
              + "boolean boolean MEASUREMENT,"
              + "int32 int32 MEASUREMENT,"
              + "int64 int64 MEASUREMENT,"
              + "float float MEASUREMENT,"
              + "double double MEASUREMENT,"
              + "text text MEASUREMENT,"
              + "string string MEASUREMENT,"
              + "blob blob MEASUREMENT,"
              + "timestamp01 timestamp MEASUREMENT,"
              + "date date MEASUREMENT)");

      List<IMeasurementSchema> schemas = new ArrayList<>();
      schemas.add(new MeasurementSchema("device_id", TSDataType.STRING));
      schemas.add(new MeasurementSchema("attribute", TSDataType.STRING));
      schemas.add(new MeasurementSchema("boolean", TSDataType.BOOLEAN));
      schemas.add(new MeasurementSchema("int32", TSDataType.INT32));
      schemas.add(new MeasurementSchema("int64", TSDataType.INT64));
      schemas.add(new MeasurementSchema("float", TSDataType.FLOAT));
      schemas.add(new MeasurementSchema("double", TSDataType.DOUBLE));
      schemas.add(new MeasurementSchema("text", TSDataType.TEXT));
      schemas.add(new MeasurementSchema("string", TSDataType.STRING));
      schemas.add(new MeasurementSchema("blob", TSDataType.BLOB));
      schemas.add(new MeasurementSchema("timestamp", TSDataType.TIMESTAMP));
      schemas.add(new MeasurementSchema("date", TSDataType.DATE));
      final List<ColumnCategory> columnTypes =
          Arrays.asList(
              ColumnCategory.ID,
              ColumnCategory.ATTRIBUTE,
              ColumnCategory.MEASUREMENT,
              ColumnCategory.MEASUREMENT,
              ColumnCategory.MEASUREMENT,
              ColumnCategory.MEASUREMENT,
              ColumnCategory.MEASUREMENT,
              ColumnCategory.MEASUREMENT,
              ColumnCategory.MEASUREMENT,
              ColumnCategory.MEASUREMENT,
              ColumnCategory.MEASUREMENT,
              ColumnCategory.MEASUREMENT);

      long timestamp = 0;
      Tablet tablet =
          new Tablet(
              "table20",
              IMeasurementSchema.getMeasurementNameList(schemas),
              schemas.stream().map(IMeasurementSchema::getType).collect(Collectors.toList()),
              columnTypes,
              10);

      for (long row = 0; row < 10; row++) {
        int rowIndex = tablet.getRowSize();
        tablet.addTimestamp(rowIndex, timestamp + row);
        tablet.addValue("device_id", rowIndex, "1");
        tablet.addValue("attribute", rowIndex, "1");
        tablet.addValue("boolean", rowIndex, true);
        tablet.addValue("int32", rowIndex, Integer.valueOf("1"));
        tablet.addValue("int64", rowIndex, Long.valueOf("1"));
        tablet.addValue("float", rowIndex, Float.valueOf("1.0"));
        tablet.addValue("double", rowIndex, Double.valueOf("1.0"));
        tablet.addValue("text", rowIndex, "true");
        tablet.addValue("string", rowIndex, "true");
        tablet.addValue("blob", rowIndex, new Binary("iotdb", Charset.defaultCharset()));
        tablet.addValue("timestamp", rowIndex, 1L);
        tablet.addValue("date", rowIndex, LocalDate.parse("2024-08-15"));
      }
      session.insert(tablet);

      SessionDataSet rs1 =
          session.executeQueryStatement(
              "select time, device_id, attribute, boolean, int32, int64, float, double, text, string, blob, timestamp, date from table20 order by time");
      for (int i = 0; i < 10; i++) {
        RowRecord rec = rs1.next();
        assertEquals(i, rec.getFields().get(0).getLongV());
        assertEquals("1", rec.getFields().get(1).getStringValue());
        assertEquals("1", rec.getFields().get(2).getStringValue());
        assertTrue(rec.getFields().get(3).getBoolV());
        assertEquals(1, rec.getFields().get(4).getIntV());
        assertEquals(1, rec.getFields().get(5).getLongV());
        assertEquals(1.0, rec.getFields().get(6).getFloatV(), 0.001);
        assertEquals(1.0, rec.getFields().get(7).getDoubleV(), 0.001);
        assertEquals("true", rec.getFields().get(8).getStringValue());
        assertEquals("true", rec.getFields().get(9).getStringValue());
        assertEquals("0x696f746462", rec.getFields().get(10).getStringValue());
        assertEquals(1, rec.getFields().get(11).getLongV());
        assertEquals("20240815", rec.getFields().get(12).getStringValue());
      }
      assertFalse(rs1.hasNext());
    }
  }
}
