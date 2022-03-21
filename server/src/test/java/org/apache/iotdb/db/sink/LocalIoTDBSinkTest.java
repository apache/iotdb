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

package org.apache.iotdb.db.sink;

import org.apache.iotdb.db.engine.trigger.sink.local.LocalIoTDBConfiguration;
import org.apache.iotdb.db.engine.trigger.sink.local.LocalIoTDBEvent;
import org.apache.iotdb.db.engine.trigger.sink.local.LocalIoTDBHandler;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.jdbc.Config;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.utils.Binary;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class LocalIoTDBSinkTest {

  @Before
  public void setUp() throws Exception {
    EnvironmentUtils.envSetUp();
  }

  @After
  public void tearDown() throws Exception {
    EnvironmentUtils.cleanEnv();
  }

  @Test
  public void onEventUsingSingleSensorHandler() throws Exception {
    LocalIoTDBHandler localIoTDBHandler = new LocalIoTDBHandler();
    localIoTDBHandler.open(
        new LocalIoTDBConfiguration(
            "root.sg1.d1", new String[] {"s1"}, new TSDataType[] {TSDataType.INT32}));

    for (int i = 0; i < 10000; ++i) {
      localIoTDBHandler.onEvent(new LocalIoTDBEvent(i, i));
    }

    localIoTDBHandler.close();

    Class.forName(Config.JDBC_DRIVER_NAME);
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      Assert.assertTrue(statement.execute("select * from root.**"));

      try (ResultSet resultSet = statement.getResultSet()) {
        ResultSetMetaData resultSetMetaData = resultSet.getMetaData();

        checkHeader(
            resultSetMetaData,
            "Time,root.sg1.d1.s1,",
            new int[] {
              Types.TIMESTAMP, Types.INTEGER,
            });

        int count = 0;
        while (resultSet.next()) {
          for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
            assertEquals(count, Double.parseDouble(resultSet.getString(i)), 0.0);
          }
          count++;
        }
        Assert.assertEquals(10000, count);
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void onEventUsingMultiSensorsHandler() throws Exception {
    LocalIoTDBHandler localIoTDBHandler = new LocalIoTDBHandler();
    localIoTDBHandler.open(
        new LocalIoTDBConfiguration(
            "root.sg1.d1",
            new String[] {"s1", "s2", "s3", "s4", "s5", "s6"},
            new TSDataType[] {
              TSDataType.INT32,
              TSDataType.INT64,
              TSDataType.FLOAT,
              TSDataType.DOUBLE,
              TSDataType.BOOLEAN,
              TSDataType.TEXT
            }));

    for (int i = 0; i < 10000; ++i) {
      localIoTDBHandler.onEvent(
          new LocalIoTDBEvent(
              i,
              i,
              (long) i,
              (float) i,
              (double) i,
              i % 2 == 0,
              Binary.valueOf(String.valueOf(i))));
    }

    localIoTDBHandler.close();

    Class.forName(Config.JDBC_DRIVER_NAME);
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      Assert.assertTrue(statement.execute("select * from root.**"));

      try (ResultSet resultSet = statement.getResultSet()) {
        ResultSetMetaData resultSetMetaData = resultSet.getMetaData();

        checkHeader(
            resultSetMetaData,
            "Time,root.sg1.d1.s1,root.sg1.d1.s2,root.sg1.d1.s3,"
                + "root.sg1.d1.s4,root.sg1.d1.s5,root.sg1.d1.s6,",
            new int[] {
              Types.TIMESTAMP,
              Types.INTEGER,
              Types.BIGINT,
              Types.FLOAT,
              Types.DOUBLE,
              Types.BOOLEAN,
              Types.VARCHAR,
            });

        int count = 0;
        while (resultSet.next()) {
          for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
            try {
              assertEquals(count, Double.parseDouble(resultSet.getString(i)), 0.0);
            } catch (NumberFormatException e) {
              assertEquals(count % 2 == 0, Boolean.parseBoolean(resultSet.getString(i)));
            }
          }
          count++;
        }
        Assert.assertEquals(10000, count);
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  private void checkHeader(
      ResultSetMetaData resultSetMetaData, String expectedHeaderStrings, int[] expectedTypes)
      throws SQLException {
    String[] expectedHeaders = expectedHeaderStrings.split(",");
    Map<String, Integer> expectedHeaderToTypeIndexMap = new HashMap<>();
    for (int i = 0; i < expectedHeaders.length; ++i) {
      expectedHeaderToTypeIndexMap.put(expectedHeaders[i], i);
    }

    for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
      Integer typeIndex = expectedHeaderToTypeIndexMap.get(resultSetMetaData.getColumnName(i));
      Assert.assertNotNull(typeIndex);
      Assert.assertEquals(expectedTypes[typeIndex], resultSetMetaData.getColumnType(i));
    }
  }

  @Test(expected = QueryProcessException.class)
  public void onEventWithWrongType1() throws Exception {
    LocalIoTDBHandler localIoTDBHandler = new LocalIoTDBHandler();
    localIoTDBHandler.open(
        new LocalIoTDBConfiguration(
            "root.sg1.d1", new String[] {"s1"}, new TSDataType[] {TSDataType.INT32}));

    localIoTDBHandler.onEvent(new LocalIoTDBEvent(0, Binary.valueOf(String.valueOf(0))));

    localIoTDBHandler.close();
  }

  @Test(expected = ClassCastException.class)
  public void onEventWithWrongType2() throws Exception {
    LocalIoTDBHandler localIoTDBHandler = new LocalIoTDBHandler();
    localIoTDBHandler.open(
        new LocalIoTDBConfiguration(
            "root.sg1.d1", new String[] {"s1"}, new TSDataType[] {TSDataType.TEXT}));

    localIoTDBHandler.onEvent(new LocalIoTDBEvent(0, String.valueOf(0)));

    localIoTDBHandler.close();
  }
}
