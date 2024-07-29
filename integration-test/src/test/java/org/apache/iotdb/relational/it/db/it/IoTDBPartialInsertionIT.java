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

import org.apache.iotdb.isession.ISession;
import org.apache.iotdb.isession.SessionDataSet;
import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.ClusterIT;
import org.apache.iotdb.itbase.category.LocalStandaloneIT;
import org.apache.iotdb.itbase.env.BaseEnv;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.RowRecord;
import org.apache.tsfile.write.record.Tablet;
import org.apache.tsfile.write.schema.IMeasurementSchema;
import org.apache.tsfile.write.schema.MeasurementSchema;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(IoTDBTestRunner.class)
@Category({LocalStandaloneIT.class, ClusterIT.class})
public class IoTDBPartialInsertionIT {
  private final Logger logger = LoggerFactory.getLogger(IoTDBPartialInsertionIT.class);

  @Before
  public void setUp() throws Exception {
    EnvFactory.getEnv().getConfig().getCommonConfig().setAutoCreateSchemaEnabled(false);
    EnvFactory.getEnv().initClusterEnvironment();
  }

  @After
  public void tearDown() throws Exception {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  @Test
  public void testPartialInsertionAllFailed() throws SQLException {
    try (Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {

      try {
        statement.execute("CREATE DATABASE test");
        statement.execute("USE \"test\"");
        statement.execute("create table sg1 (id1 string id, s0 int32 measurement)");
        statement.execute("INSERT INTO sg1(id1, timestamp, s0) VALUES ('id', 1, 1)");
        fail();
      } catch (SQLException e) {
        assertTrue(e.getMessage().contains("Unknown column category"));
      }
    }
  }

  @Test
  public void testPartialInsertionRestart() throws SQLException {
    try (Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {

      statement.execute("CREATE DATABASE test");
      statement.execute("USE \"test\"");
      statement.execute(
          "create table sg (id1 string id, s1 text measurement, s2 double measurement)");

      try {
        statement.execute("INSERT INTO sg(id1,time,s1,s2) VALUES('d1', 100,'test','test')");
      } catch (SQLException e) {
        // ignore
      }
    }

    // TODO: replace restartDaemon() with new methods in Env.
    /*
    long time = 0;
    try {
      EnvironmentUtils.restartDaemon();
      StorageEngine.getInstance().recover();
      // wait for recover
      while (!StorageEngine.getInstance().isAllSgReady()) {
        Thread.sleep(500);
        time += 500;
        if (time > 10000) {
          logger.warn("wait too long in restart, wait for: " + time / 1000 + "s");
        }
      }
    } catch (Exception e) {
      fail(e.getMessage());
    }
     */

    try (Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      statement.execute("use \"test\"");

      try (ResultSet resultSet = statement.executeQuery("SELECT s1 FROM sg")) {
        assertNotNull(resultSet);
        int cnt = 0;
        while (resultSet.next()) {
          cnt++;
          assertEquals("test", resultSet.getString("s1"));
        }
        assertEquals(1, cnt);
      }
      try (ResultSet resultSet = statement.executeQuery("SELECT s2 FROM sg")) {
        assertNotNull(resultSet);
        assertFalse(resultSet.next());
      }
    }
  }

  @Test
  public void testPartialInsertTablet() {
    try (ISession session = EnvFactory.getEnv().getSessionConnection(BaseEnv.TABLE_SQL_DIALECT)) {
      session.executeNonQueryStatement("create database test");
      session.executeNonQueryStatement("use \"test\"");
      session.executeNonQueryStatement(
          "create table sg1 (id1 string id, s1 int64 measurement, s2 int64 measurement)");
      List<IMeasurementSchema> schemaList = new ArrayList<>();
      schemaList.add(new MeasurementSchema("id1", TSDataType.STRING));
      schemaList.add(new MeasurementSchema("s1", TSDataType.INT64));
      schemaList.add(new MeasurementSchema("s2", TSDataType.INT64));
      schemaList.add(new MeasurementSchema("s3", TSDataType.INT64));
      final List<Tablet.ColumnType> columnTypes =
          Arrays.asList(
              Tablet.ColumnType.ID,
              Tablet.ColumnType.MEASUREMENT,
              Tablet.ColumnType.MEASUREMENT,
              Tablet.ColumnType.MEASUREMENT);
      Tablet tablet = new Tablet("sg1", schemaList, columnTypes, 300);
      long timestamp = 0;
      for (long row = 0; row < 100; row++) {
        int rowIndex = tablet.rowSize++;
        tablet.addTimestamp(rowIndex, timestamp);
        for (int s = 0; s < 4; s++) {
          long value = timestamp;
          if (s == 0) {
            tablet.addValue(schemaList.get(s).getMeasurementId(), rowIndex, "d1");
          } else {
            tablet.addValue(schemaList.get(s).getMeasurementId(), rowIndex, value);
          }
        }
        timestamp++;
      }
      timestamp = System.currentTimeMillis();
      for (long row = 0; row < 100; row++) {
        int rowIndex = tablet.rowSize++;
        tablet.addTimestamp(rowIndex, timestamp);
        for (int s = 0; s < 4; s++) {
          long value = timestamp;
          if (s == 0) {
            tablet.addValue(schemaList.get(s).getMeasurementId(), rowIndex, "d1");
          } else {
            tablet.addValue(schemaList.get(s).getMeasurementId(), rowIndex, value);
          }
        }
        timestamp++;
      }
      try {
        session.insertRelationalTablet(tablet);
      } catch (Exception e) {
        if (!e.getMessage().contains("507")) {
          fail(e.getMessage());
        }
      }
      try (SessionDataSet dataSet = session.executeQueryStatement("SELECT * FROM sg1")) {
        assertEquals(dataSet.getColumnNames().size(), 4);
        assertEquals(dataSet.getColumnNames().get(0), "time");
        assertEquals(dataSet.getColumnNames().get(1), "id1");
        assertEquals(dataSet.getColumnNames().get(2), "s1");
        assertEquals(dataSet.getColumnNames().get(3), "s2");
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
}
