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

import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.trigger.sink.mqtt.MQTTConfiguration;
import org.apache.iotdb.db.engine.trigger.sink.mqtt.MQTTEvent;
import org.apache.iotdb.db.engine.trigger.sink.mqtt.MQTTHandler;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.jdbc.Config;

import org.fusesource.mqtt.client.QoS;
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

@SuppressWarnings("squid:S2925")
public class MQTTSinkTest {

  @Before
  public void setUp() throws Exception {
    IoTDBDescriptor.getInstance().getConfig().setEnableMQTTService(true);
    EnvironmentUtils.envSetUp();
  }

  @After
  public void tearDown() throws Exception {
    EnvironmentUtils.cleanEnv();
  }

  @Test
  public void onEventUsingSingleSensorHandler() throws Exception {
    MQTTHandler mqttHandler = new MQTTHandler();
    mqttHandler.open(
        new MQTTConfiguration(
            "127.0.0.1",
            1883,
            "root",
            "root",
            new PartialPath("root.sg1.d1"),
            new String[] {"s1"}));

    for (int i = 0; i < 10000; ++i) {
      mqttHandler.onEvent(new MQTTEvent("test", QoS.EXACTLY_ONCE, false, i, i));
    }

    mqttHandler.close();

    Thread.sleep(1000);

    Class.forName(Config.JDBC_DRIVER_NAME);
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      Assert.assertTrue(statement.execute("select * from root"));

      try (ResultSet resultSet = statement.getResultSet()) {
        ResultSetMetaData resultSetMetaData = resultSet.getMetaData();

        checkHeader(
            resultSetMetaData,
            "Time,root.sg1.d1.s1,",
            new int[] {
              Types.TIMESTAMP, Types.FLOAT,
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
    MQTTHandler mqttHandler = new MQTTHandler();
    mqttHandler.open(
        new MQTTConfiguration(
            "127.0.0.1",
            1883,
            "root",
            "root",
            new PartialPath("root.sg1.d1"),
            new String[] {"s1", "s2", "s3", "s4", "s5", "s6"}));

    for (int i = 0; i < 10000; ++i) {
      mqttHandler.onEvent(
          new MQTTEvent(
              "test",
              QoS.EXACTLY_ONCE,
              false,
              i,
              i,
              (long) i,
              (float) i,
              (double) i,
              i % 2 == 0,
              String.valueOf(i)));
    }

    mqttHandler.close();

    Thread.sleep(1000);

    Class.forName(Config.JDBC_DRIVER_NAME);
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      Assert.assertTrue(statement.execute("select * from root"));

      try (ResultSet resultSet = statement.getResultSet()) {
        ResultSetMetaData resultSetMetaData = resultSet.getMetaData();

        checkHeader(
            resultSetMetaData,
            "Time,root.sg1.d1.s1,root.sg1.d1.s2,root.sg1.d1.s3,"
                + "root.sg1.d1.s4,root.sg1.d1.s5,root.sg1.d1.s6,",
            new int[] {
              Types.TIMESTAMP,
              Types.FLOAT,
              Types.FLOAT,
              Types.FLOAT,
              Types.FLOAT,
              Types.BOOLEAN,
              Types.FLOAT,
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
}
