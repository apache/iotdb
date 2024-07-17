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
import org.apache.iotdb.isession.SessionDataSet;
import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.ClusterIT;
import org.apache.iotdb.itbase.category.LocalStandaloneIT;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.Session;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.RowRecord;
import org.apache.tsfile.write.record.Tablet;
import org.apache.tsfile.write.record.Tablet.ColumnType;
import org.apache.tsfile.write.schema.IMeasurementSchema;
import org.apache.tsfile.write.schema.MeasurementSchema;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.apache.iotdb.itbase.env.BaseEnv.TABLE_SQL_DIALECT;
import static org.junit.Assert.assertEquals;

@RunWith(IoTDBTestRunner.class)
public class IoTDBSessionRelationalIT {

  private static Logger LOGGER = LoggerFactory.getLogger(IoTDBSessionRelationalIT.class);

  @Before
  public void setUp() throws Exception {
    EnvFactory.getEnv().initClusterEnvironment();
    try (ISession session = EnvFactory.getEnv().getSessionConnection(TABLE_SQL_DIALECT)) {
      session.executeNonQueryStatement("CREATE DATABASE db1");
    }
  }

  @After
  public void tearDown() throws Exception {
    try (ISession session = EnvFactory.getEnv().getSessionConnection(TABLE_SQL_DIALECT)) {
      session.executeNonQueryStatement("DROP DATABASE db1");
    }
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  // for manual debugging
  public static void main(String[] args)
      throws IoTDBConnectionException, StatementExecutionException {
    try (ISession session =
        new Session.Builder().host("127.0.0.1").port(6667).sqlDialect(TABLE_SQL_DIALECT).build()) {
      session.open();
      try {
        session.executeNonQueryStatement("DROP DATABASE db1");
      } catch (Exception ignored) {

      }
      session.executeNonQueryStatement("CREATE DATABASE db1");
      session.executeNonQueryStatement("USE \"db1\"");
      session.executeNonQueryStatement(
          "CREATE TABLE table1 (id1 string id, attr1 string attribute, "
              + "m1 double "
              + "measurement)");

      List<IMeasurementSchema> schemaList = new ArrayList<>();
      schemaList.add(new MeasurementSchema("id1", TSDataType.STRING));
      schemaList.add(new MeasurementSchema("attr1", TSDataType.STRING));
      schemaList.add(new MeasurementSchema("m1", TSDataType.DOUBLE));
      final List<ColumnType> columnTypes =
          Arrays.asList(ColumnType.ID, ColumnType.ATTRIBUTE, ColumnType.MEASUREMENT);

      long timestamp = 0;
      Tablet tablet = new Tablet("table1", schemaList, columnTypes, 15);

      for (long row = 0; row < 15; row++) {
        int rowIndex = tablet.rowSize++;
        tablet.addTimestamp(rowIndex, timestamp + row);
        tablet.addValue("id1", rowIndex, "id:" + row);
        tablet.addValue("attr1", rowIndex, "attr:" + row);
        tablet.addValue("m1", rowIndex, row * 1.0);
        if (tablet.rowSize == tablet.getMaxRowNumber()) {
          session.insertRelationalTablet(tablet, true);
          tablet.reset();
        }
      }

      if (tablet.rowSize != 0) {
        session.insertRelationalTablet(tablet);
        tablet.reset();
      }

      session.executeNonQueryStatement("FLush");

      for (long row = 15; row < 30; row++) {
        int rowIndex = tablet.rowSize++;
        tablet.addTimestamp(rowIndex, timestamp + row);
        tablet.addValue("id1", rowIndex, "id:" + row);
        tablet.addValue("attr1", rowIndex, "attr:" + row);
        tablet.addValue("m1", rowIndex, row * 1.0);
        if (tablet.rowSize == tablet.getMaxRowNumber()) {
          session.insertRelationalTablet(tablet, true);
          tablet.reset();
        }
      }

      if (tablet.rowSize != 0) {
        session.insertRelationalTablet(tablet);
        tablet.reset();
      }

      timestamp = 0;
      SessionDataSet dataSet =
          session.executeQueryStatement("select time, id1, attr1, m1 from table1 order by time");
      while (dataSet.hasNext()) {
        RowRecord rowRecord = dataSet.next();
        assertEquals(timestamp, rowRecord.getTimestamp());
        assertEquals("id:" + timestamp, rowRecord.getFields().get(0).getBinaryV().toString());
        assertEquals("attr:" + timestamp, rowRecord.getFields().get(1).getBinaryV().toString());
        assertEquals(timestamp * 1.0, rowRecord.getFields().get(2).getDoubleV(), 0.0001);
        timestamp++;
        //        System.out.println(rowRecord);
      }
    }
  }

  @Test
  @Category({LocalStandaloneIT.class, ClusterIT.class})
  public void insertRelationalTabletTest()
      throws IoTDBConnectionException, StatementExecutionException {
    try (ISession session = EnvFactory.getEnv().getSessionConnection(TABLE_SQL_DIALECT)) {
      try {
        session.executeNonQueryStatement("DROP DATABASE db1");
      } catch (Exception ignored) {

      }
      session.executeNonQueryStatement("CREATE DATABASE db1");
      session.executeNonQueryStatement("USE \"db1\"");
      session.executeNonQueryStatement(
          "CREATE TABLE table1 (id1 string id, attr1 string attribute, "
              + "m1 double "
              + "measurement)");

      List<IMeasurementSchema> schemaList = new ArrayList<>();
      schemaList.add(new MeasurementSchema("id1", TSDataType.STRING));
      schemaList.add(new MeasurementSchema("attr1", TSDataType.STRING));
      schemaList.add(new MeasurementSchema("m1", TSDataType.DOUBLE));
      final List<ColumnType> columnTypes =
          Arrays.asList(ColumnType.ID, ColumnType.ATTRIBUTE, ColumnType.MEASUREMENT);

      long timestamp = 0;
      Tablet tablet = new Tablet("table1", schemaList, columnTypes, 15);

      for (long row = 0; row < 15; row++) {
        int rowIndex = tablet.rowSize++;
        tablet.addTimestamp(rowIndex, timestamp + row);
        tablet.addValue("id1", rowIndex, "id:" + row);
        tablet.addValue("attr1", rowIndex, "attr:" + row);
        tablet.addValue("m1", rowIndex, row * 1.0);
        if (tablet.rowSize == tablet.getMaxRowNumber()) {
          session.insertRelationalTablet(tablet, true);
          tablet.reset();
        }
      }

      if (tablet.rowSize != 0) {
        session.insertRelationalTablet(tablet);
        tablet.reset();
      }

      session.executeNonQueryStatement("FLush");

      for (long row = 15; row < 30; row++) {
        int rowIndex = tablet.rowSize++;
        tablet.addTimestamp(rowIndex, timestamp + row);
        tablet.addValue("id1", rowIndex, "id:" + row);
        tablet.addValue("attr1", rowIndex, "attr:" + row);
        tablet.addValue("m1", rowIndex, row * 1.0);
        if (tablet.rowSize == tablet.getMaxRowNumber()) {
          session.insertRelationalTablet(tablet, true);
          tablet.reset();
        }
      }

      if (tablet.rowSize != 0) {
        session.insertRelationalTablet(tablet);
        tablet.reset();
      }

      timestamp = 0;
      SessionDataSet dataSet = session.executeQueryStatement("select * from table1 order by time");
      while (dataSet.hasNext()) {
        RowRecord rowRecord = dataSet.next();
        assertEquals(timestamp, rowRecord.getTimestamp());
        assertEquals("id:" + timestamp, rowRecord.getFields().get(0).getBinaryV().toString());
        assertEquals("attr:" + timestamp, rowRecord.getFields().get(1).getBinaryV().toString());
        assertEquals(timestamp * 1.0, rowRecord.getFields().get(2).getDoubleV(), 0.0001);
        timestamp++;
        //        System.out.println(rowRecord);
      }
    }
  }

  @Test
  @Category({LocalStandaloneIT.class, ClusterIT.class})
  public void autoCreateColumnTest() throws IoTDBConnectionException, StatementExecutionException {
    try (ISession session = EnvFactory.getEnv().getSessionConnection(TABLE_SQL_DIALECT)) {
      try {
        session.executeNonQueryStatement("DROP DATABASE db1");
      } catch (Exception ignored) {

      }
      session.executeNonQueryStatement("CREATE DATABASE db1");
      session.executeNonQueryStatement("USE \"db1\"");
      session.executeNonQueryStatement("CREATE TABLE table1 (id1 string id)");

      List<IMeasurementSchema> schemaList = new ArrayList<>();
      schemaList.add(new MeasurementSchema("id1", TSDataType.STRING));
      schemaList.add(new MeasurementSchema("attr1", TSDataType.STRING));
      schemaList.add(new MeasurementSchema("m1", TSDataType.DOUBLE));
      final List<ColumnType> columnTypes =
          Arrays.asList(ColumnType.ID, ColumnType.ATTRIBUTE, ColumnType.MEASUREMENT);

      long timestamp = 0;
      Tablet tablet = new Tablet("table1", schemaList, columnTypes, 15);

      for (long row = 0; row < 15; row++) {
        int rowIndex = tablet.rowSize++;
        tablet.addTimestamp(rowIndex, timestamp + row);
        tablet.addValue("id1", rowIndex, "id:" + row);
        tablet.addValue("attr1", rowIndex, "attr:" + row);
        tablet.addValue("m1", rowIndex, row * 1.0);
        if (tablet.rowSize == tablet.getMaxRowNumber()) {
          session.insertRelationalTablet(tablet, true);
          tablet.reset();
        }
      }

      if (tablet.rowSize != 0) {
        session.insertRelationalTablet(tablet);
        tablet.reset();
      }

      session.executeNonQueryStatement("FLush");

      for (long row = 15; row < 30; row++) {
        int rowIndex = tablet.rowSize++;
        tablet.addTimestamp(rowIndex, timestamp + row);
        tablet.addValue("id1", rowIndex, "id:" + row);
        tablet.addValue("attr1", rowIndex, "attr:" + row);
        tablet.addValue("m1", rowIndex, row * 1.0);
        if (tablet.rowSize == tablet.getMaxRowNumber()) {
          session.insertRelationalTablet(tablet, true);
          tablet.reset();
        }
      }

      if (tablet.rowSize != 0) {
        session.insertRelationalTablet(tablet);
        tablet.reset();
      }

      timestamp = 0;
      SessionDataSet dataSet = session.executeQueryStatement("select * from table1 order by time");
      while (dataSet.hasNext()) {
        RowRecord rowRecord = dataSet.next();
        assertEquals(timestamp, rowRecord.getTimestamp());
        assertEquals("id:" + timestamp, rowRecord.getFields().get(0).getBinaryV().toString());
        assertEquals("attr:" + timestamp, rowRecord.getFields().get(1).getBinaryV().toString());
        assertEquals(timestamp * 1.0, rowRecord.getFields().get(2).getDoubleV(), 0.0001);
        timestamp++;
        //        System.out.println(rowRecord);
      }
    }
  }
}
