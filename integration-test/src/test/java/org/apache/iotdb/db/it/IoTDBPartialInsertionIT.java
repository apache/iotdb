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
package org.apache.iotdb.db.it;

import org.apache.iotdb.isession.ISession;
import org.apache.iotdb.isession.SessionDataSet;
import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.ClusterIT;
import org.apache.iotdb.itbase.category.LocalStandaloneIT;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.enums.CompressionType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
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
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {

      statement.execute("CREATE DATABASE root.sg1");

      try {
        statement.execute("INSERT INTO root.sg1(timestamp, s0) VALUES (1, 1)");
        fail();
      } catch (SQLException e) {
        assertTrue(e.getMessage().contains("Path [root.sg1.s0] does not exist"));
      }
    }
  }

  @Test
  public void testPartialInsertionRestart() throws SQLException {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {

      statement.execute("CREATE DATABASE root.sg");
      statement.execute("CREATE TIMESERIES root.sg.d1.s1 datatype=text");
      statement.execute("CREATE TIMESERIES root.sg.d1.s2 datatype=double");

      try {
        statement.execute("INSERT INTO root.sg.d1(time,s1,s2) VALUES(100,'test','test')");
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
      while (!StorageEngine.getInstance().isReadyForReadAndWrite()) {
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

    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {

      try (ResultSet resultSet = statement.executeQuery("SELECT s1 FROM root.sg.d1")) {
        assertNotNull(resultSet);
        int cnt = 0;
        while (resultSet.next()) {
          cnt++;
          assertEquals("test", resultSet.getString("root.sg.d1.s1"));
        }
        assertEquals(1, cnt);
      }
      try (ResultSet resultSet = statement.executeQuery("SELECT s2 FROM root.sg.d1")) {
        assertNotNull(resultSet);
        assertFalse(resultSet.next());
      }
    }
  }

  @Test
  public void testPartialInsertTablet() {
    try (ISession session = EnvFactory.getEnv().getSessionConnection()) {
      session.createTimeseries(
          "root.sg1.d1.s1", TSDataType.INT64, TSEncoding.PLAIN, CompressionType.SNAPPY);
      session.createTimeseries(
          "root.sg1.d1.s2", TSDataType.INT64, TSEncoding.PLAIN, CompressionType.SNAPPY);
      List<IMeasurementSchema> schemaList = new ArrayList<>();
      schemaList.add(new MeasurementSchema("s1", TSDataType.INT64));
      schemaList.add(new MeasurementSchema("s2", TSDataType.INT64));
      schemaList.add(new MeasurementSchema("s3", TSDataType.INT64));
      Tablet tablet = new Tablet("root.sg1.d1", schemaList, 300);
      long timestamp = 0;
      for (long row = 0; row < 100; row++) {
        int rowIndex = tablet.getRowSize();
        tablet.addTimestamp(rowIndex, timestamp);
        for (int s = 0; s < 3; s++) {
          long value = timestamp;
          tablet.addValue(schemaList.get(s).getMeasurementName(), rowIndex, value);
        }
        timestamp++;
      }
      timestamp = System.currentTimeMillis();
      for (long row = 0; row < 100; row++) {
        int rowIndex = tablet.getRowSize();
        tablet.addTimestamp(rowIndex, timestamp);
        for (int s = 0; s < 3; s++) {
          long value = timestamp;
          tablet.addValue(schemaList.get(s).getMeasurementName(), rowIndex, value);
        }
        timestamp++;
      }
      try {
        session.insertTablet(tablet);
      } catch (Exception e) {
        if (!e.getMessage().contains("507")) {
          fail(e.getMessage());
        }
      }
      try (SessionDataSet dataSet = session.executeQueryStatement("SELECT * FROM root.sg1.d1")) {
        assertEquals(dataSet.getColumnNames().size(), 3);
        assertEquals(dataSet.getColumnNames().get(0), "Time");
        assertEquals(dataSet.getColumnNames().get(1), "root.sg1.d1.s1");
        assertEquals(dataSet.getColumnNames().get(2), "root.sg1.d1.s2");
        int cnt = 0;
        while (dataSet.hasNext()) {
          RowRecord rowRecord = dataSet.next();
          long time = rowRecord.getTimestamp();
          assertEquals(time, rowRecord.getFields().get(0).getLongV());
          assertEquals(time, rowRecord.getFields().get(1).getLongV());
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
